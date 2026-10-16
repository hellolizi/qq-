# -*- coding: utf-8 -*-
"""QQ + DeepSeek 聊天机器人

通过 OneBot v11 正向 WebSocket 接入 QQ(推荐 NapCatQQ 作为 OneBot 实现),
把 QQ 私聊 / 群聊消息转发给 DeepSeek,再把回复发回 QQ。

用法:
    python bot.py            # 启动机器人
    python bot.py --check    # 只测试 DeepSeek API Key 是否可用
"""
from __future__ import annotations

import asyncio
import base64
import ctypes
import hashlib
import hmac
import html
import json
import logging
import mimetypes
import os
import random
import re
import shutil
import sys
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import httpx
import websockets

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"

# 本地表情包库:图片放这里,标签写在 tags.json 里
STICKER_DIR = BASE_DIR / "表情包"
# 记着当前在跑的实例 pid,防止双击两次启动脚本冒出两个机器人
LOCK_FILE = BASE_DIR / "bot.lock"
STICKER_INDEX = STICKER_DIR / "tags.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        # 顺便写一份到文件,断线这类事过去就刷没了,不好查
        logging.FileHandler(BASE_DIR / "bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("qq-ds")

RESET_WORDS = {"重置", "清空", "忘记", "/reset", "重置对话"}

# 聊天模型用它来表示"这句是在要图,该去生图",而不是普通回复
DRAW_MARK = "[DRAW]"

# 生图前先用联网模型把画面描述查准,免得画错角色(比如"奶龙"其实是黄色幼龙)
DRAW_PROMPT_TPL = (
    "你是绘图提示词专家。用户想生成一张图。\n"
    "用户原话:「{req}」\n"
    "助手对此的初步理解(可能不准,涉及具体角色时尤其要以联网核实为准):{hint}\n"
    "如果里面涉及具体的角色 / 人物 / 品牌 / 地标等需要准确外观的东西,"
    "请先联网确认它的真实外观(颜色、体型、标志性细节),不要凭印象编。\n"
    "然后输出两行:\n"
    "第一行:一段适合 AI 绘图的中文描述,写清主体、颜色、造型、场景和画风;\n"
    "第二行:`[REF] 名称` —— 如果画面主角是某个有名字的具体角色 / 人物 / IP,"
    "给出用来搜图的准确名称(例如「奶龙」);如果只是泛指的东西(例如一只猫),写 `[REF] 无`。\n"
    "不要输出其他内容。"
)

# 拿到参考图后,让生图模型照着它画,而不是凭描述硬凑
REF_GUIDE_TPL = (
    "图1是角色「{name}」的官方形象。请以这个角色为主角绘制:{desc}\n"
    "必须严格保持它的配色、头身比例、五官和标志性特征与图1一致。"
)

# 联网问答时顺手让模型给出配图关键词,免得拿问题原句去搜图(那会搜出不相干的东西)
SEARCH_HINT = (
    "\n这次你联网查过资料了。回答的最后请另起一行给出配图指令:\n"
    "适合配图 → `[IMG] 名词性搜索词`(例如「大熊猫」,不要用「大熊猫长什么样」这种问句);\n"
    "不适合配图(观点、是非、计算类问题)→ `[IMG] 无`。"
)

# 这些是给程序看的约定,由代码追加到 system_prompt 后面。
# 单独放在这里,是为了让 config.json 里的人设可以随便改,不会把功能改坏。
ROUTING_HINT = (
    "\n\n【系统约定,务必遵守,可以无视上面人设里与它们冲突的部分】\n"
    "1. 用户要你「画」「生成」「绘制」一张图时(例如「生成一张奶龙」「帮我画只柯基」),"
    "这是要新画,不要拒绝,只回复一行:`[DRAW] 画面描述`,描述要具体。\n"
    "2. 用户要你「发」「找」「来」一张现成的图时(例如「给我发张猫的图」"
    "「找张壁纸」「来点二次元图」),这是要现成的,只回复一行:`[FIND] 搜索词`,"
    "搜索词写成名词,例如「奶龙 壁纸」。\n"
    "3. 分不清是要画还是要找时,一律按第 2 条当「找现成的图」。\n"
    "4. 用户只是问你有没有画图 / 发图能力,或者聊天里随口提到「画」「图」,"
    "都当普通聊天正常回话,不要输出任何标记。\n"
    "5. 其他任何情况都正常聊天。\n"
    "6. 群聊回复必须短:1~2 句、40 字以内。像普通人发微信那样说话——"
    "口语、短句,可以不带标点结尾,不许写列表、标题、Markdown、分点罗列。\n"
    "7. 只有用户明确说「详细讲讲」「解释下原理」时才展开,"
    "展开也只讲结论和要点,不要铺垫,不要总结收尾。\n"
    "8. 别露出 AI 味:不要说「作为AI」「请问有什么可以帮您」「希望以上内容对您有帮助」"
    "这类客服话术;不要解释自己在干什么,不要升华总结,不要每次都热情收尾。\n"
    "9. emoji 和颜文字最多一个,大多数时候一个都别用。\n"
    "10. 群聊里别人说的话前面带发言人昵称(格式「昵称: 内容」),"
    "你自己叫「小鲸鱼」,但你回话的时候不要在自己前面加「小鲸鱼:」,直接说内容就行。"
    "要分清谁是谁,别把张三说的话记到李四头上;"
    "顺着话头接的时候,必要时可以带一下对方名字。\n"
    "11. 记住前面聊过什么。别人说「那个」「刚才说的」「再来一个」这种指代,"
    "要回到上文里找指代对象,不要每次都当成全新话题。"
)


def split_image_marker(text: str) -> tuple[str, str]:
    """拆出模型附在结尾的配图指令 `[IMG] 关键词`,返回(去掉指令的正文, 关键词)。"""
    match = re.search(r"`?\[IMG\][ \t]*`?([^`\n]*)`?", text)
    if not match:
        return text.strip(), ""
    keyword = match.group(1).strip()
    if keyword in {"", "无", "none", "None", "null", "-"}:
        keyword = ""
    body = (text[: match.start()] + text[match.end():]).strip().strip("`").strip()
    return body, keyword


def split_ref_marker(text: str) -> tuple[str, str]:
    """拆出绘图描述里的 `[REF] 角色名` 指令,返回(描述, 角色名)。"""
    match = re.search(r"`?\[REF\][ \t]*`?([^`\n]*)`?", text)
    if not match:
        return text.strip(), ""
    name = match.group(1).strip()
    if name in {"", "无", "none", "None", "null", "-"}:
        name = ""
    body = (text[: match.start()] + text[match.end():]).strip().strip("`").strip()
    return body, name


# 这些只是来源分类,不算情绪
STICKER_CHAR_TAGS = {"奶龙", "二次元", "奶龙动图", "二次元动图"}

# 模型想发表情包时用的标记。清单是运行时按表情包库生成再拼进来的。
# 直接把每张图「什么情绪、什么场合用」摊给它看,让它点名要哪张 ——
# 比只丢一串抽象标签准得多。
STICKER_HINT = (
    "\n\n【能发的表情包】格式:文件名|情绪|什么时候用\n"
    "{items}\n"
    "想顺便甩张表情的时候,在回复最后另起一行写 `[STICKER] 文件名`。\n"
    "文件名必须从上面原样抄一个,不要自己编,也不要写标签。\n"
    "注意:\n"
    "1. 情绪对得上就发,别太保守;但正经回答、算数、查资料、安慰人这类别发。\n"
    "2. 别每次都挑同一张,尤其不要老是挑「得意」那几张。\n"
    "3. 看清方向:被调侃、被说笨、被吐槽的时候,不要挑得意或炫耀的图;"
    "那种时候该挑委屈、奶凶、无语、装傻的。\n"
    "4. 大约每三四条回复才发一次,不要连着发。\n"
    "5. 正文照常写,不要只写标记。"
)

# 群里没点它的那些话,当背景喂给模型,让它知道大家在聊什么
AMBIENT_TPL = (
    "\n\n【群里刚才的聊天(只是让你知道气氛和话头,"
    "别主动去回这些,也别被带跑题)】\n{chat}\n"
)

# 长期记忆捞出来的旧聊天
MEMORY_TPL = (
    "\n\n【你记得以前在这个群里聊过这些(不一定跟现在有关)】\n{items}\n"
    "跟眼前的话题沾边就自然带一句,不沾边就别硬提,别提「根据我的记忆」这种话。\n"
)

# 命中关键词时追加一条反应提示(只给方向,别让它照抄这句话)
REACTION_TPL = (
    "\n\n【这条消息触发了固定反应,照这个意思回,别把这句话原样抄出来】\n{rule}\n"
)

# 模型挂了的时候别装死,随口应一声,免得群里觉得「她不理我」
FAIL_LINES = (
    "啊,我卡了一下,你再说一遍?",
    "网卡了,重发一遍…",
    "刚走神了,你刚才说啥",
)

# DeepSeek 官网的价格(元 / 百万 tokens),查额度时报一句给个参照。官方调价记得回来改。
# 同一个模型的两种叫法都留着:官方文档写 deepseek-v4-flash,但 API 只认 deepseek-flash。
DS_OFFICIAL_PRICE = {
    "deepseek-flash": (1, 2),
    "deepseek-v4-flash": (1, 2),
    "deepseek-v4-pro": (3, 6),
}

# 阿里云的账户余额和优惠券(学生那 300 元就在券里)。这都是账户级的东西,百炼那套接口看不到,
# 得走费用与成本 OpenAPI,而且要一对 AccessKey。签名是老的 RPC 规矩,自己算免得再装 CLI。
BSS_HOST = "https://business.aliyuncs.com/"
BSS_VERSION = "2023-09-30"  # DescribeCoupon 用的版本号
BSS_LEGACY_VERSION = "2017-12-14"  # QueryAccountBalance 还是老版本,别跟着上面那个改


def _bss_sign(params: dict[str, str], secret: str) -> str:
    """阿里云 RPC 签名:参数按 key 排序拼串,再 HMAC-SHA1 + base64。"""
    enc = lambda s: quote(str(s), safe="~")  # noqa: E731
    canon = "&".join(f"{enc(k)}={enc(params[k])}" for k in sorted(params))
    to_sign = f"GET&{enc('/')}&{enc(canon)}"
    digest = hmac.new((secret + "&").encode(), to_sign.encode(), hashlib.sha1).digest()
    return base64.b64encode(digest).decode()


def is_deepseek(model: Any) -> bool:
    """是不是 DeepSeek 家的模型(官网、百炼都叫 deepseek-*)。"""
    return str(model or "").strip().lower().startswith("deepseek")


# 成语接龙判定:让模型看真假和接得上接不上,顺手给出下一句
IDIOM_CHECK_TPL = (
    "成语接龙。上一个成语是「{last}」,玩家接的是「{word}」。\n"
    "判断:1) 「{word}」是不是真实存在的四字成语;2) 它的首字是不是「{last}」的尾字"
    "(同音同字都算);3) 给一个以「{word}」尾字开头、没被用过的常见四字成语当下一句。\n"
    "只回一行 JSON,不要任何解释:\n"
    '{{"ok":true,"next":"下一个成语"}} 或 {{"ok":false,"why":"20字内说明为什么不行"}}'
)


def split_find_marker(text: str) -> tuple[str, str]:
    """拆出「去找张现成的图」的 `[FIND] 搜索词`,返回(去掉标记的正文, 搜索词)。"""
    match = re.search(r"`?\[FIND\][ \t]*`?([^`\n]*)`?", text)
    if not match:
        return text.strip(), ""
    keyword = match.group(1).strip()
    # 模型偶尔会在关键词后面嘟囔一句「不存在的」,别拿它去搜
    for noise in ("不存在的", "不存在", "没有"):
        keyword = keyword.replace(noise, "")
    keyword = keyword.strip(" ,，、")
    if keyword in {"", "无", "none", "None", "null", "-"}:
        keyword = ""
    body = (text[: match.start()] + text[match.end():]).strip().strip("`").strip()
    return body, keyword


def split_sticker_marker(text: str) -> tuple[str, str]:
    """拆出模型附在结尾的 `[STICKER] 标签`,返回(去掉标记的正文, 标签)。"""
    match = re.search(r"`?\[STICKER\][ \t]*`?([^`\n]*)`?", text)
    if not match:
        return text.strip(), ""
    tag = match.group(1).strip()
    if tag in {"", "无", "none", "None", "null", "-"}:
        tag = ""
    body = (text[: match.start()] + text[match.end():]).strip().strip("`").strip()
    return body, tag


# 上下文里别人说话都是「昵称: 内容」,模型有时会照抄这个格式,
# 在自己回复前面顶一个「小鲸鱼:」,或者干脆顶着当前说话人的名字——都要去掉。
SELF_PREFIX_RE = re.compile(r"^\s*(?:小鲸鱼|鲸鲸|鲸鱼娘)\s*[:：]\s*")


def strip_self_prefix(text: str, names: tuple[str, ...] | list[str] = ()) -> str:
    """去掉模型抄格式加在句首的「某某:」:它自己的名字,或者当前说话人的昵称。"""
    out = SELF_PREFIX_RE.sub("", text, count=1).strip()
    for name in names:
        name = (name or "").strip()
        if not name or len(name) > 12:
            continue
        pattern = rf"^{re.escape(name)}\s*[:：]\s*"
        if re.match(pattern, out):
            return re.sub(pattern, "", out, count=1).strip()
    return out


def api_fail_reason(resp: dict[str, Any]) -> str:
    """接口没成功时,把原因说成人话,别老是甩「得让我当群管理」。"""
    msg = str(resp.get("message") or resp.get("wording") or resp.get("msg") or "").strip()
    if msg:
        return msg
    code = resp.get("retcode")
    if code == 1404:
        return "这人不在群里了"
    if code in (1403, 1401):
        return "我权限不够(QQ 那边拒的)"
    return f"QQ 那边拒了(retcode {code})"


def split_draw_marker(text: str) -> tuple[str, bool]:
    """找出 `[DRAW]` 标记,返回(标记之后的画面描述, 是否要生图)。

    模型偶尔会在标记前先说一句废话(「欸?让我康康……」),
    所以这里用搜索而不是看开头,只要出现标记就当要生图。
    """
    match = re.search(r"`?\[DRAW\][ \t]*`?", text)
    if not match:
        return "", False
    return text[match.end():].strip().strip("`").strip(), True


def bytes_to_data_url(raw: bytes) -> str:
    """按文件头猜 mime,拼成 data URL。"""
    if raw[:4] == b"\x89PNG":
        mime = "image/png"
    elif raw[:3] == b"\xff\xd8\xff":
        mime = "image/jpeg"
    elif raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        mime = "image/webp"
    elif raw[:3] == b"GIF":
        mime = "image/gif"
    else:
        mime = "image/jpeg"
    return f"data:{mime};base64," + base64.b64encode(raw).decode()


BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

# ---------- 群功能用到的常量 ----------

DATA_FILE = BASE_DIR / "data.json"  # 积分 / 签到 / 提醒的小存档
MEMORY_FILE = BASE_DIR / "memory.json"  # 长期记忆:聊天存档

# 偷表情包:让识图模型判断这张图是不是表情包
STEAL_PROMPT = (
    "看这张图,判断它是不是聊天用的表情包(而不是照片、截图、商品图、科普图、聊天记录)。\n"
    "只回一行 JSON,不要解释:\n"
    '{"meme":true,"emotion":"核心情绪,4字内","scene":"什么场合会发它,20字内"} '
    '或 {"meme":false}'
)
IMAGE_EXT = {
    "image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png",
    "image/gif": ".gif", "image/webp": ".webp", "image/bmp": ".bmp",
}
CJK_RE = re.compile(r"[\u4e00-\u9fff]")

# 天气(open-meteo,免费不用 key)和热搜(百度官方)
GEO_URL = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
HOT_URL = "https://top.baidu.com/api/board?platform=wise&tab=realtime"

# WMO 天气代码 -> 中文
WMO_CODE = {
    0: "晴", 1: "晴间多云", 2: "多云", 3: "阴",
    45: "雾", 48: "雾凇",
    51: "小毛毛雨", 53: "毛毛雨", 55: "大毛毛雨",
    56: "冻毛毛雨", 57: "强冻毛毛雨",
    61: "小雨", 63: "中雨", 65: "大雨",
    66: "冻雨", 67: "强冻雨",
    71: "小雪", 73: "中雪", 75: "大雪", 77: "雪粒",
    80: "阵雨", 81: "强阵雨", 82: "暴雨",
    85: "阵雪", 86: "强阵雪",
    95: "雷阵雨", 96: "雷阵雨伴冰雹", 99: "强雷暴伴冰雹",
}

DICE_RE = re.compile(r"^(\d{0,3})[dD](\d{1,4})$")
DURATION_RE = re.compile(r"(\d+)\s*(秒|分钟|分|小时|钟头|时|天)")
CLOCK_RE = re.compile(r"(?:今天|明天)?\s*(\d{1,2})\s*[:：点]\s*(\d{1,2})?\s*分?")
DURATION_UNITS = {"秒": 1, "分": 60, "分钟": 60, "时": 3600, "小时": 3600, "钟头": 3600, "天": 86400}

FORTUNE_GOOD = [
    "摸鱼", "白饭加大份", "DeepSleep 补觉", "跟朋友唠嗑", "买杯奶茶",
    "把活往后挪", "听歌", "出门吹风", "换头像", "偷吃老板的 token",
]
FORTUNE_BAD = [
    "加班", "早八", "跟老板对线", "通宵打游戏", "点外卖踩雷",
    "改需求", "跟人争辩", "熬夜刷手机", "冲动购物", "算错数",
]
FORTUNE_QUIP = [
    "别问我准不准,我只负责吃白饭",
    "信不信随你,反正我信了",
    "抽到差的也别哭,鲸鲸今天也一般",
    "算完了,该去吃饭了",
    "转发给三个群能转运(不能)",
]
LUCKY_COLORS = ["深蓝", "奶白", "雾霾蓝", "米黄", "薄荷绿", "藕粉", "银灰", "赤红"]

# 成语接龙的开局成语,后面接不上就让模型续
IDIOM_SEED = [
    "一鸣惊人", "风和日丽", "心花怒放", "万事如意", "马到成功",
    "日新月异", "后来居上", "画蛇添足", "井底之蛙", "守株待兔",
]

HELP_TEXT = (
    "小鲸鱼会这些:\n"
    "/骰 2d6    掷骰子\n"
    "/抽签      抽一支签\n"
    "/运势      今日运势\n"
    "/签到      每天领积分\n"
    "/积分      看自己积分\n"
    "/排行榜    群里积分榜\n"
    "/猜数字    玩猜数字(/猜 50)\n"
    "/成语接龙  玩成语接龙(/结束 退出)\n"
    "/提醒 20分钟后 开会   到点提醒你\n"
    "/提醒列表 /取消提醒 编号\n"
    "/天气 杭州  看天气\n"
    "/热搜      百度实时热搜\n"
    "/统计      今天这个群聊了多少\n"
    "/余额      看鲸鲸还剩多少额度\n"
    "/表情 [标签]  来张表情包\n"
    "/画 描述   让鲸鲸画一张\n"
    "/搜 问题   联网查\n"
    "/重置      清空对话记忆\n"
    "群里 @ 我 或直接聊天也行"
)

COMMANDS = {
    "帮助", "help", "菜单", "骰", "骰子", "抽签", "运势", "签到", "打卡", "积分",
    "排行榜", "榜", "猜数字", "猜", "成语接龙", "接龙", "结束", "提醒", "提醒列表",
    "取消提醒", "天气", "热搜", "统计", "余额", "额度", "禁言", "解除禁言", "全体禁言",
    "踢", "踢人",
}
# 这些不用 @ 机器人也能直接触发(画图/搜索/重置沿用原来的处理)
BYPASS_COMMANDS = COMMANDS | {"画", "搜", "表情", "重置"}

# 不带 / 也能用的命令(比如直接说"签到")
KEYWORDS = {
    "签到": "/签到", "打卡": "/签到", "运势": "/运势", "抽签": "/抽签",
    "热搜": "/热搜", "帮助": "/帮助", "菜单": "/帮助", "余额": "/余额", "额度": "/余额",
}

# 没人会老老实实打 /余额,「查一下余额」「还剩多少额度」也得认。
# 只认短句,而且要排除「余额宝」「支付宝」这种正常聊天。
NATURAL_CMDS = (("余额", "余额"), ("额度", "余额"))
NATURAL_STOP = ("余额宝", "支付宝", "微信", "银行卡")


def natural_command(text: str) -> str:
    """把口语说法认成命令,认不出来就返回空。"""
    body = re.sub(r"[\s,，。!！?？~、]+", "", text.strip().lstrip("/"))
    if not body or len(body) > 20:
        return ""
    if any(w in body for w in NATURAL_STOP):
        return ""
    for kw, cmd in NATURAL_CMDS:
        if kw in body:
            return cmd
    return ""


def find_bl() -> str | None:
    """找阿里云百炼的命令行工具 bl。PATH 里没有就去默认安装位置翻。"""
    found = shutil.which("bl")
    if found:
        return found
    guess = Path(os.environ.get("LOCALAPPDATA") or "") / "bailian-cli" / "bin" / "bl.exe"
    return str(guess) if guess.exists() else None


def strip_when(text: str) -> str:
    """干掉时间后面剩下的「后 / 之后 / 以内」这类尾巴。"""
    return re.sub(r"^[后之以内]{1,2}\s*", "", text.strip(" ,,、")).strip(" ,,、")


def parse_duration(text: str) -> tuple[int, str, str]:
    """解析「20分钟后 开会」这类提醒。返回(秒数, 提醒内容, 出错说明)。"""
    now = datetime.now()
    m = DURATION_RE.search(text)
    if m:
        seconds = int(m.group(1)) * DURATION_UNITS.get(m.group(2), 60)
        if seconds <= 0:
            return 0, "", "时间得是正数"
        if seconds > 30 * 86400:
            return 0, "", "最多提醒 30 天内的事"
        return seconds, strip_when(text[: m.start()] + text[m.end():]), ""
    m = CLOCK_RE.search(text)
    if m:
        hour, minute = int(m.group(1)), int(m.group(2) or 0)
        if hour > 23 or minute > 59:
            return 0, "", "这个时间点看不懂"
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if "明天" in m.group(0) or target <= now:
            target += timedelta(days=1)
        return (
            int((target - now).total_seconds()),
            strip_when(text[: m.start()] + text[m.end():]),
            "",
        )
    return 0, "", "要写成「20分钟后 要干嘛」或者「9:30 要干嘛」"


def roll_dice(spec: str) -> str:
    """掷骰子,支持 2d6 / d20 / 3d100。"""
    m = DICE_RE.match(spec.strip() or "1d6")
    if not m:
        return "格式是 /骰 2d6(个数d面数),比如 /骰 2d6、/骰 d20"
    count = int(m.group(1) or 1)
    faces = int(m.group(2))
    if not 1 <= count <= 10:
        return "一次最多 10 个骰子"
    if not 2 <= faces <= 10000:
        return "骰子面数得在 2 到 10000 之间"
    rolls = [random.randint(1, faces) for _ in range(count)]
    if count == 1:
        return f"🎲 掷出 {rolls[0]}(1-{faces})"
    return f"🎲 {count}d{faces}:{'+'.join(map(str, rolls))} = {sum(rolls)}"


# 模型爱吐 markdown、bullet、「好的，我帮你看看」这类客服腔。QQ 里 markdown 根本不渲染,
# 照原样发进群里一眼就露馅,所以出门之前先洗一遍。
MD_FENCE_RE = re.compile(r"```[a-zA-Z0-9_+-]*\n?")
MD_BOLD_RE = re.compile(r"\*\*(.+?)\*\*", re.S)
# 单星号包起来的多半是 *叹气* 这种动作;别动 3*4*5 这种正常写法,所以只在动作词上生效
MD_STAR_ACTION_RE = re.compile(r"(?<!\*)\*([^*\n]{1,12})\*(?!\*)")
MD_CODE_RE = re.compile(r"`([^`\n]+)`")
MD_HEAD_RE = re.compile(r"^\s{0,3}#{1,6}\s*", re.M)
MD_QUOTE_RE = re.compile(r"^\s{0,3}>\s?", re.M)
MD_BULLET_RE = re.compile(r"^\s{0,3}(?:[-*+•]|\d{1,2}[.、)])\s+", re.M)
# 「（挠头）」「(笑)」这种动作描写,真人打字不会这么干
ACTION_RE = re.compile(r"[（(]\s*\*?([^（()）]{0,10})\*?\s*[）)]")
ACTION_WORDS = (
    "挠头", "歪头", "叹气", "拍桌", "摊手", "捂脸", "沉思", "眨眼", "叉腰", "鼓脸",
    "点头", "摇头", "小声", "伸懒腰", "打哈欠", "皱眉", "脸红", "捂嘴", "比心",
    "吐舌", "撇嘴", "眼神", "假装", "角落", "画圈", "doge", "低头", "抬头",
)
# 客服味的开场白和收尾话术,最该砍的就是这两头
BOT_OPENERS = (
    "好的，", "好的,", "当然，", "当然,", "明白了，", "明白了,", "没问题，",
    "以下是我", "作为AI", "作为一个AI", "作为一个人工智能", "我是AI",
)
BOT_TAIL_RE = re.compile(
    r"[，,。！!]?\s*(?:希望(?:以上|这些)?(?:回答|内容|信息)?(?:能)?(?:对你|对您)?"
    r"(?:有|能)?(?:所)?(?:帮助|帮到你)|还有什么(?:可以|需要)?(?:帮|问)[^。！？]*|"
    r"有其他问题[^。！？]*|如果你(?:还)?有其他[^。！？]*)[。！？]?"
)
PUNCT_RUN_RE = re.compile(r"([，,。！!？?])\1+")


def polish_reply(text: str, max_chars: int = 0) -> str:
    """把模型的回复洗成能直接发进 QQ 群的样子。"""
    body = (text or "").strip()
    if not body:
        return ""
    # 有代码块就手下留情:长度上限不砍它,缩进也别动
    has_code = "```" in body or bool(re.search(r"\n\s{4,}\S", body))
    body = MD_FENCE_RE.sub("", body)
    body = MD_BOLD_RE.sub(r"\1", body)
    body = MD_CODE_RE.sub(r"\1", body)
    body = MD_HEAD_RE.sub("", body)
    body = MD_QUOTE_RE.sub("", body)
    body = MD_BULLET_RE.sub("", body)

    def _drop_action(m: re.Match[str]) -> str:
        inner = (m.group(1) or "").strip().lower()
        # 只有确实是动作词才删,(如列表) 这种正经括号得留着
        return "" if inner and any(w in inner for w in ACTION_WORDS) else m.group(0)

    body = ACTION_RE.sub(_drop_action, body)
    body = MD_STAR_ACTION_RE.sub(_drop_action, body)
    for opener in BOT_OPENERS:
        if body.startswith(opener) and len(body) > len(opener) + 1:
            body = body[len(opener):].lstrip("，,、 ").strip()
    body = BOT_TAIL_RE.sub("", body)
    body = PUNCT_RUN_RE.sub(r"\1", body)
    body = re.sub(r"\n{3,}", "\n\n", body).strip()
    if body.endswith("!"):
        body = body[:-1] + "！"
    elif body.endswith("?"):
        body = body[:-1] + "？"
    if max_chars and len(body) > max_chars and not has_code:
        cut = body[:max_chars]
        mark = max((cut.rfind(c) for c in "。！？…；"), default=-1)
        body = cut[: mark + 1] if mark >= max_chars // 2 else cut.rstrip("，,、 ") + "…"
    return body.strip()


class Store:
    """很小的本地存档:积分 / 签到 / 提醒。坏了就当空的,不能让机器人起不来。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        try:
            self.data = json.loads(path.read_text("utf-8"))
            if not isinstance(self.data, dict):
                raise ValueError
        except Exception:  # noqa: BLE001
            self.data = {}
        self.data.setdefault("points", {})
        self.data.setdefault("reminders", [])

    def save(self) -> None:
        try:
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            tmp.replace(self.path)
        except Exception as exc:  # noqa: BLE001
            log.warning("存档写不进去:%s", exc)

    # --- 积分 ---
    def points(self, user_id: str) -> dict[str, Any]:
        rec = self.data["points"].setdefault(
            user_id, {"score": 0, "total": 0, "days": 0, "last": "", "name": ""}
        )
        return rec

    def add_points(self, user_id: str, amount: int, name: str = "") -> dict[str, Any]:
        rec = self.points(user_id)
        rec["score"] = max(0, int(rec.get("score", 0)) + amount)
        if amount > 0:
            rec["total"] = int(rec.get("total", 0)) + amount
        if name:
            rec["name"] = name
        return rec

    def ranking(self, top: int = 10) -> list[tuple[str, dict[str, Any]]]:
        items = sorted(
            self.data["points"].items(),
            key=lambda kv: int(kv[1].get("total", 0)),
            reverse=True,
        )
        return items[:top]


class Memory:
    """轻量长期记忆:按会话把聊天存档,再用关键词捞回来。

    没上向量库(RAG)——那个得额外跑数据库。这里就是存文本 + 两个字两个字地比对,
    应付「上次说的那个东西」这种指代够用了。
    """

    def __init__(self, path: Path, keep_days: int = 30) -> None:
        self.path = path
        self.keep_days = keep_days
        self.data: dict[str, list[list[Any]]] = {}
        self.dirty = False
        self.last_save = 0.0
        try:
            raw = json.loads(path.read_text("utf-8"))
            if isinstance(raw, dict):
                self.data = raw
        except Exception:  # noqa: BLE001
            self.data = {}

    def add(self, session_key: str, who: str, text: str) -> None:
        text = text.strip()
        if not text or len(text) > 200:
            return
        rows = self.data.setdefault(session_key, [])
        rows.append([time.time(), who, text])
        cutoff = time.time() - self.keep_days * 86400
        if len(rows) > 2000 or rows[0][0] < cutoff:
            self.data[session_key] = [r for r in rows if r[0] >= cutoff][-2000:]
        self.dirty = True

    def search(
        self, session_key: str, query: str, limit: int = 4, skip_recent: int = 600
    ) -> list[str]:
        """找跟这句话有关的旧聊天,返回「谁: 说了啥」。"""
        rows = self.data.get(session_key) or []
        grams = {
            query[i : i + 2]
            for i in range(len(query) - 1)
            if CJK_RE.match(query[i]) and CJK_RE.match(query[i + 1])
        }
        if not grams:
            return []
        # 刚聊完的还在上下文里,不用重复喂
        cutoff = time.time() - skip_recent
        scored: list[tuple[int, str]] = []
        for ts, who, text in rows[-1000:]:
            if ts > cutoff:
                continue
            score = sum(text.count(g) for g in grams)
            if score:
                scored.append((score, f"{who}: {text}"))
        if not scored:
            return []
        scored.sort(key=lambda item: -item[0])
        out: list[str] = []
        for _, line in scored:
            if line not in out:
                out.append(line)
            if len(out) >= limit:
                break
        return out

    def save(self, force: bool = False) -> None:
        """攒一会儿再写盘,别每条消息都写一次。"""
        if not self.dirty:
            return
        now = time.time()
        if not force and now - self.last_save < 30:
            return
        self.last_save = now
        self.dirty = False
        try:
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.data, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self.path)
        except Exception as exc:  # noqa: BLE001
            log.warning("记忆写不进去:%s", exc)


def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise SystemExit(
            f"未找到配置文件 {CONFIG_PATH}\n"
            "请把 config.example.json 复制一份改名为 config.json,并填好里面的内容。"
        )
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


class Bot:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.ws: Any = None
        self.self_id = str(cfg.get("self_id") or "")
        self.history: dict[str, list[dict[str, str]]] = defaultdict(list)
        self.locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.http = httpx.AsyncClient(timeout=float(cfg.get("reply_timeout", 60)))
        self.safe_domains = [
            str(d).lower().lstrip(".") for d in (cfg.get("safe_domains") or [])
        ]
        # 每个会话最近发过的图,供紧接着的追问使用
        self.recent_images: dict[str, tuple[float, list[str]]] = {}
        # 每个会话最近发过的表情包,免得老是甩同一张
        self.recent_stickers: dict[str, list[str]] = defaultdict(list)
        # 本地表情包库(读不到就是空的,功能自动隐身)
        self.stickers, self.sticker_tags = self._load_stickers()
        # 最近一次收到任何消息的时间,NapCat 每 30 秒会推心跳,拿它判断连接死没死
        self.last_msg_at = time.time()
        # 群里没点它的话,也顺手记下来当背景,不然它不知道群里在聊什么
        self.ambient: dict[str, list[str]] = defaultdict(list)
        # 群功能:积分和提醒存本地,游戏状态和防撤回缓存只在内存
        self.store = Store(DATA_FILE)
        self.games: dict[str, dict[str, Any]] = {}
        self.recall_cache: deque[tuple[str, str, str, str]] = deque(
            maxlen=int(cfg.get("recall_cache", 300))
        )
        # 复读接梗:记每个会话最后一句和它连着出现了几次
        self.said: dict[str, tuple[str, int]] = {}
        self.repeated: set[str] = set()
        # call_api_wait 等回包用
        self.pending: dict[str, asyncio.Future] = {}
        # 长期记忆:把聊天存档,以后能捞回来
        self.memory = Memory(MEMORY_FILE, int(cfg.get("memory_days", 30)))
        # 群统计 / 日报 / 主动消息
        self.stats: dict[str, dict[str, Any]] = {}
        self.day_log: dict[str, list[str]] = defaultdict(list)
        self.last_active: dict[str, float] = {}
        self.reported_day = ""
        self.steal_count: dict[str, Any] = {"day": "", "n": 0}
        self.known_hashes: set[str] = self._hash_stickers()
        # 试收过的图指纹 -> 见过几次。见得够多才认为是真表情包,别把截图也收了
        self.seen_counts: dict[str, int] = {}
        # 她说过话之后,群里连着多少条没人理她(数够了就先闭嘴)
        self.silent_streak: dict[str, int] = {}
        self.said_at: dict[str, float] = {}
        # 本条消息命中的关键词反应,用完就丢
        self.reaction: dict[str, str] = {}

    def _hash_stickers(self) -> set[str]:
        """给库里已有的表情包算指纹,免得偷回来一堆重复的。"""
        out = set()
        for item in self.stickers:
            path = STICKER_DIR / item["file"]
            try:
                out.add(hashlib.md5(path.read_bytes()).hexdigest())
            except Exception:  # noqa: BLE001
                continue
        return out

    # ---------- 表情包 ----------

    def _load_stickers(self) -> tuple[list[dict[str, Any]], list[str]]:
        """读 表情包/tags.json,返回(可用图片列表, 去重后的标签列表)。"""
        if not STICKER_INDEX.exists():
            return [], []
        try:
            data = json.loads(STICKER_INDEX.read_text("utf-8"))
        except Exception as exc:  # noqa: BLE001
            log.warning("表情包索引读不了(%s): %s", STICKER_INDEX.name, exc)
            return [], []
        images = [
            item
            for item in (data.get("images") or [])
            if item.get("file") and (STICKER_DIR / item["file"]).exists()
        ]
        tags: list[str] = []
        for item in images:
            for tag in item.get("tags") or []:
                if tag not in tags:
                    tags.append(tag)
        if images:
            log.info("表情包库:%d 张 / %d 个标签", len(images), len(tags))
        return images, tags

    def sticker_list_text(self) -> str:
        """把表情包摊成一行一张的清单,喂给模型让它点名。"""
        lines = []
        for item in self.stickers:
            emotion = item.get("emotion") or next(
                (t for t in item.get("tags") or [] if t not in STICKER_CHAR_TAGS), ""
            )
            scene = item.get("scene") or item.get("usage") or ""
            lines.append(f"{item['file']}｜{emotion}｜{scene}")
        return "\n".join(lines)

    def pick_sticker(self, tag: str = "", session_key: str = "") -> Path | None:
        """挑一张表情包:优先按文件名认(模型直接点名),再按情绪/关键词/场景找,都没有就随机。

        模型有很强的默认偏好(会反复点同一张),所以同一个会话最近发过的先避开。
        """
        if not self.stickers:
            return None
        sent = self.recent_stickers.get(session_key or "", [])
        fresh = [i for i in self.stickers if i["file"] not in sent] or self.stickers

        key = tag.strip().strip("`。.,、 ").split()[0] if tag.strip() else ""
        if key:
            for item in self.stickers:
                name = item["file"]
                if key == name or key == Path(name).stem or key in name:
                    if item["file"] not in sent:
                        return STICKER_DIR / name
                    break  # 刚发过,换个同情绪的
            # 标签对不上时再拿关键词和「这图是干嘛用的」那句话兜一下
            hit = [
                item
                for item in fresh
                if any(key in t or t in key for t in item.get("tags") or [])
                or key in (item.get("scene") or "")
            ]
            if hit:
                return STICKER_DIR / random.choice(hit)["file"]
        return STICKER_DIR / random.choice(fresh)["file"]

    def remember_sticker(self, session_key: str, name: str, keep: int = 6) -> None:
        """记住这个会话刚发过哪几张,免得连着刷同一张。"""
        sent = self.recent_stickers[session_key]
        sent.append(name)
        del sent[:-keep]

    def remember_ambient(
        self, session_key: str, speaker: str, text: str, has_image: bool
    ) -> None:
        """记下没点它的那句群消息,当上下文背景用。"""
        keep = int(self.cfg.get("ambient_lines", 30))
        if keep <= 0:
            return
        line = (f"{speaker}: {text}" if speaker else text).strip()
        if has_image:
            line = f"{line} [图片]".strip()
        if not line:
            return
        buf = self.ambient[session_key]
        buf.append(line[:100])
        del buf[:-keep]

    # ---------- 域名白名单 ----------

    def is_safe_url(self, url: str) -> bool:
        """只有白名单里的域名允许访问;白名单为空表示不限制。"""
        if not self.safe_domains:
            return True
        try:
            host = (urlsplit(url).hostname or "").lower()
        except ValueError:
            return False
        return bool(host) and any(
            host == d or host.endswith("." + d) for d in self.safe_domains
        )

    # ---------- OneBot 通信 ----------

    async def call_api(self, action: str, params: dict[str, Any]) -> None:
        """调用 OneBot 接口(不等待响应,失败只记日志)。"""
        if self.ws is None:
            return
        payload = {"action": action, "params": params, "echo": action}
        try:
            await self.ws.send(json.dumps(payload, ensure_ascii=False))
        except Exception as exc:  # noqa: BLE001
            log.warning("调用 %s 失败: %s", action, exc)

    async def call_api_wait(
        self, action: str, params: dict[str, Any], timeout: float = 8
    ) -> dict[str, Any]:
        """调用接口并等回包(查群成员身份、发消息拿 message_id 时要用)。"""
        if self.ws is None:
            return {}
        echo = f"{action}-{random.randint(10**6, 10**7)}"
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self.pending[echo] = future
        try:
            await self.ws.send(
                json.dumps({"action": action, "params": params, "echo": echo}, ensure_ascii=False)
            )
            return await asyncio.wait_for(future, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            log.warning("调用 %s 没等到回包:%s", action, exc)
            return {}
        finally:
            self.pending.pop(echo, None)

    async def member_role(self, group_id: Any, user_id: Any) -> str:
        """查这个人在群里的身份:owner / admin / member;查不到返回空串。"""
        resp = await self.call_api_wait(
            "get_group_member_info",
            {"group_id": group_id, "user_id": user_id, "no_cache": False},
        )
        data = resp.get("data") if isinstance(resp, dict) else None
        return str((data or {}).get("role") or "")

    async def send(self, event: dict[str, Any], message: Any) -> None:
        """发送消息。message 可以是纯文本,也可以是 OneBot 消息段列表。"""
        if event.get("message_type") == "group":
            await self.call_api(
                "send_group_msg",
                {"group_id": event["group_id"], "message": message},
            )
        else:
            await self.call_api(
                "send_private_msg",
                {"user_id": event["user_id"], "message": message},
            )

    async def reply(self, event: dict[str, Any], text: str) -> None:
        await self.send(event, text)

    async def send_sticker(self, event: dict[str, Any], path: Path) -> None:
        """把本地表情包当图片发出去。"""
        b64 = base64.b64encode(path.read_bytes()).decode()
        await self.send(event, [{"type": "image", "data": {"file": f"base64://{b64}"}}])

    # ---------- 消息解析 ----------

    def parse_message(self, event: dict[str, Any]) -> tuple[str, bool, list[str]]:
        """把 OneBot 消息段解析成 (纯文本, 是否@了机器人, 图片引用列表)。"""
        segments = event.get("message")
        if not isinstance(segments, list):  # 字符串形式的兜底
            return str(event.get("raw_message", "")).strip(), False, []

        parts: list[str] = []
        images: list[str] = []
        at_bot = False
        for seg in segments:
            if not isinstance(seg, dict):
                continue
            seg_type = seg.get("type")
            data = seg.get("data") or {}
            if seg_type == "text":
                parts.append(str(data.get("text", "")))
            elif seg_type == "at":
                if str(data.get("qq", "")) == self.self_id:
                    at_bot = True
            elif seg_type == "image":
                ref = str(data.get("url") or data.get("file") or "").strip()
                if ref:
                    images.append(ref)
        return "".join(parts).strip(), at_bot, images[:4]

    def should_reply(
        self, event: dict[str, Any], text: str, at_bot: bool, has_images: bool = False
    ) -> str | None:
        """判断是否该回复。返回待处理的内容(允许为空串,表示只发了图片),不回复返回 None。"""
        has_content = bool(text) or has_images
        if event.get("message_type") == "private":
            return text if has_content else None

        # 群聊
        mode = str(self.cfg.get("group_trigger", "at")).lower()
        if mode == "all":
            return text if has_content else None
        if mode == "prefix":
            prefix = str(self.cfg.get("group_prefix", "/ds"))
            if not text.startswith(prefix):
                return None
            stripped = text[len(prefix):].strip()
            return stripped if (stripped or has_images) else None
        # 默认: 需要 @机器人
        return text if (at_bot and has_content) else None

    # ---------- DeepSeek ----------

    async def _image_to_data_url(self, ref: str) -> str | None:
        """把图片引用(URL / file:// / 本地路径)转成 data URL,失败返回 None。"""
        is_http = ref.startswith(("http://", "https://"))
        if is_http and not self.is_safe_url(ref):
            log.warning("拒绝访问非白名单域名:%s", urlsplit(ref).hostname)
            return None
        try:
            if is_http:
                resp = await self.http.get(
                    ref,
                    headers={"User-Agent": BROWSER_UA},
                    follow_redirects=True,
                    timeout=30,
                )
                resp.raise_for_status()
                raw = resp.content
                mime = resp.headers.get("content-type", "image/jpeg").split(";")[0]
            else:
                path = ref[7:] if ref.startswith("file://") else ref
                raw = Path(path).read_bytes()
                mime = mimetypes.guess_type(path)[0] or "image/jpeg"
        except Exception as exc:  # noqa: BLE001
            # 下不动就直接放弃这张图。别把 http 链接原样丢给模型服务端:
            # 它那边同样抓不到,会返回 400 把整条回复一起吞掉。
            log.warning("图片获取失败(%s): %s", ref[:60], exc)
            return None

        limit = int(self.cfg.get("max_image_mb", 8)) * 1024 * 1024
        if len(raw) > limit:
            log.warning("图片超过 %dMB,已忽略", limit // 1024 // 1024)
            return None
        return f"data:{mime};base64," + base64.b64encode(raw).decode()

    def chat_endpoint(self) -> tuple[str, str, bool]:
        """文字聊天走哪个接口:配了 chat_api_key 就单独接一家(比如 DeepSeek 官网),
        没配就跟识图/搜索/生图共用同一个端点。"""
        base = str(self.cfg.get("chat_base_url") or "").strip()
        key = str(self.cfg.get("chat_api_key") or "").strip()
        if base and key:
            return base.rstrip("/"), key, True
        return "", "", False

    async def ask_deepseek(
        self,
        session_key: str,
        user_text: str,
        images: list[str] | None = None,
        use_search: bool = False,
        speaker: str = "",
    ) -> str:
        history = self.history[session_key]
        images = images or []
        # 群里好几个人说话,不标名字模型分不清谁说的,上下文就废了一半
        text = f"{speaker}: {user_text}".strip() if speaker else user_text

        # 本轮消息:带图就拼成多模态内容
        current: Any = text
        if images:
            parts: list[dict[str, Any]] = []
            lost = 0
            for ref in images:
                data_url = await self._image_to_data_url(ref)
                if data_url:
                    parts.append({"type": "image_url", "image_url": {"url": data_url}})
                else:
                    lost += 1
            if parts:
                parts.append({"type": "text", "text": text or "看看这张图"})
                current = parts
            elif lost:
                # 一张都没取到就直说,别让模型对着空图瞎猜
                note = "(图我这边没取到,你重新发一次,或者直接描述一下图上是什么也行)"
                current = f"{text} {note}".strip()

        max_history = int(self.cfg.get("max_history", 20))
        messages: list[dict[str, Any]] = []
        # 人设归 config,画图 / 搜索这些技术约定由代码追加,改人设不会把功能改坏
        system_prompt = (self.cfg.get("system_prompt") or "") + ROUTING_HINT
        if use_search and not images:
            system_prompt += SEARCH_HINT
        if self.stickers and not images:
            system_prompt += STICKER_HINT.format(items=self.sticker_list_text())
        if self.ambient.get(session_key):
            system_prompt += AMBIENT_TPL.format(chat="\n".join(self.ambient[session_key]))
        # 以前的聊天里有没有相关的,捞出来提一句(轻量长期记忆)
        recalled = self.memory.search(
            session_key, user_text, int(self.cfg.get("memory_recall", 4))
        )
        if recalled:
            system_prompt += MEMORY_TPL.format(items="\n".join(recalled))
        # 早上好 / 晚安 / 生日快乐 这类,给一句方向就行,话由她自己说
        rule = self.reaction.pop(session_key, "")
        if rule:
            system_prompt += REACTION_TPL.format(rule=rule)
        messages.append({"role": "system", "content": system_prompt})
        messages.extend(history[-max_history:])
        messages.append({"role": "user", "content": current})

        # 文字聊天可以单独接到别家(比如 DeepSeek 官网)。
        # 识图和联网搜索是百炼独有的能力(enable_search),所以它们留在原来的端点。
        chat_base, chat_key, on_official = self.chat_endpoint()
        on_official = on_official and not images and not use_search
        if on_official:
            url = chat_base + "/chat/completions"
            api_key = chat_key
            model = str(self.cfg.get("chat_model") or self.cfg.get("model") or "")
        else:
            url = self.cfg.get("deepseek_base_url", "https://api.deepseek.com").rstrip("/")
            url += "/chat/completions"
            api_key = self.cfg["deepseek_api_key"]
            # 带图走视觉模型,联网走搜索模型,其余用默认的快模型
            if images and self.cfg.get("vision_model"):
                model = self.cfg["vision_model"]
            elif use_search:
                model = self.cfg.get("search_model") or self.cfg.get("model", "")
            else:
                model = self.cfg.get("model", "")

        payload: dict[str, Any] = {"model": model, "messages": messages, "stream": False}
        # 聊天回复单独限长:max_tokens 还要留给生图提示词用,别一起调小。
        # 搜索的答案是把资料揉进去讲,比闲聊长,还按 120 会拦腰截断。
        limit = self.cfg.get("reply_max_tokens") or self.cfg.get("max_tokens", 400)
        if use_search and not images:
            limit = self.cfg.get("max_tokens", 400)
        payload["max_tokens"] = int(limit)
        # 把 config 里额外的 OpenAI 兼容参数原样透传
        for key in ("temperature", "top_p"):
            if key in self.cfg:
                payload[key] = self.cfg[key]
        if on_official:
            # 官网的思考模式默认就是开的,群里聊天要快,明确关掉;
            # 它也不认百炼那套 enable_thinking / enable_search,所以只发它认的参数
            payload["thinking"] = {
                "type": "enabled" if self.cfg.get("chat_thinking") else "disabled"
            }
        else:
            if use_search and not images:
                payload["enable_search"] = True
                # 百炼的 DeepSeek 默认开着思考,正文会被思考烧成一串空字符,
                # 所以搜索这条路上也得显式关掉。通义本来不用,但传了也无害。
                if is_deepseek(model):
                    payload["enable_thinking"] = False
            for key in ("enable_thinking", "thinking"):
                if key in self.cfg and "enable_search" not in payload:
                    payload[key] = self.cfg[key]

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        resp = await self.http.post(url, json=payload, headers=headers)
        if resp.status_code != 200:
            log.error("模型返回 %s: %s", resp.status_code, resp.text[:300])
            raise RuntimeError(f"模型接口错误 {resp.status_code}")

        content = strip_self_prefix(
            (resp.json()["choices"][0]["message"].get("content") or "").strip(), (speaker,)
        )

        # 上下文里只留纯文字,避免每轮重传整张图
        history.append(
            {"role": "user", "content": f"{text} [图片]".strip() if images else text}
        )
        # [DRAW] / [FIND] / [IMG] / [STICKER] 只是给程序看的标记,别原样留在上下文里
        _, want_draw = split_draw_marker(content)
        if want_draw:
            stored = "[已生成图片]"
        else:
            stored, find_kw = split_find_marker(content)
            if find_kw:
                stored = "[已发图]"
            stored, _ = split_image_marker(stored)
            stored, _ = split_sticker_marker(stored)
        history.append({"role": "assistant", "content": stored})
        # 只保留最近 max_history 条,防止上下文无限增长
        if len(history) > max_history:
            del history[:-max_history]
        return content

    async def ask_plain(self, prompt: str, max_tokens: int = 80) -> str:
        """问模型一句话,不进对话历史(小游戏判定这种用完就丢的场合用)。"""
        chat_base, chat_key, on_official = self.chat_endpoint()
        if on_official:
            # 跟聊天走同一家,免得日报/冒泡用的是另一个模型,口吻不统一
            url = chat_base
            api_key = chat_key
            payload: dict[str, Any] = {
                "model": str(self.cfg.get("chat_model") or self.cfg.get("model") or ""),
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "thinking": {"type": "disabled"},
            }
        else:
            url = self.cfg.get("deepseek_base_url", "https://api.deepseek.com").rstrip("/")
            api_key = self.cfg["deepseek_api_key"]
            payload = {
                "model": self.cfg.get("model", "qwen-flash"),
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "enable_thinking": False,
            }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        try:
            resp = await self.http.post(url + "/chat/completions", json=payload, headers=headers)
            if resp.status_code != 200:
                log.warning("小模型调用返回 %s", resp.status_code)
                return ""
            return strip_self_prefix(
                (resp.json()["choices"][0]["message"].get("content") or "").strip()
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("小模型调用失败:%s", exc)
            return ""

    # ---------- 生图 ----------

    async def build_image_prompt(self, request: str, hint: str = "") -> tuple[str, str]:
        """把用户的话扩写成绘图描述。返回(描述, 要搜参考图的角色名)。

        配置里 draw_prompt_model 为空或调用失败时,退回原始描述、不搜参考图。
        """
        model = str(self.cfg.get("draw_prompt_model", "") or "").strip()
        if not model:
            return hint or request, ""
        url = self.cfg.get("deepseek_base_url", "").rstrip("/") + "/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.cfg['deepseek_api_key']}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": DRAW_PROMPT_TPL.format(req=request, hint=hint or "(无)"),
                }
            ],
            "stream": False,
            "max_tokens": self.cfg.get("max_tokens", 400),
            "enable_search": True,
        }
        # 同上:DeepSeek 不关思考的话扩写结果会变成空串,白退回原始描述
        if is_deepseek(model):
            payload["enable_thinking"] = False
        try:
            resp = await self.http.post(url, json=payload, headers=headers, timeout=180)
            resp.raise_for_status()
            text = (resp.json()["choices"][0]["message"].get("content") or "").strip()
        except Exception as exc:  # noqa: BLE001
            log.warning("扩写绘图描述失败,退回原始描述: %s", exc)
            return hint or request, ""
        if not text:
            return hint or request, ""
        desc, ref_name = split_ref_marker(text)
        log.info("绘图描述(%s): %s", model, desc[:70])
        log.info("需搜参考图: %s", ref_name or "(不需要)")
        return desc or request, ref_name

    async def gen_image(self, prompt: str, ref_raw: bytes | None = None) -> str:
        """调生图接口,返回图片的 base64(不含前缀),失败抛异常。

        传了 ref_raw 就把参考图一起送给模型,让它照着画,角色才像。
        """
        url = self.cfg.get("deepseek_base_url", "").rstrip("/") + "/images/generations"
        payload = {
            "model": self.cfg.get("image_model", "qwen-image-2.1-pro"),
            "prompt": prompt,
            "n": 1,
            "size": self.cfg.get("image_size", "1024x1024"),
        }
        if ref_raw:
            payload["image"] = bytes_to_data_url(ref_raw)
        headers = {
            "Authorization": f"Bearer {self.cfg['deepseek_api_key']}",
            "Content-Type": "application/json",
        }
        resp = await self.http.post(url, json=payload, headers=headers, timeout=240)
        if resp.status_code != 200:
            log.error("生图返回 %s: %s", resp.status_code, resp.text[:300])
            raise RuntimeError(f"生图接口错误 {resp.status_code}")

        item = (resp.json().get("data") or [{}])[0]
        if item.get("b64_json"):  # 有的模型直接回 base64
            return item["b64_json"]

        img_url = item.get("url")
        if not img_url:
            raise RuntimeError("生图接口没有返回图片")
        # 出图的 URL 是临时的,先下回来再发,免得过期
        return base64.b64encode(await self._download_image(img_url)).decode()

    async def _download_image(self, url: str) -> bytes:
        """下载图片。阿里云 OSS 加速域名偶尔解析不出来,所以重试并换区域节点兜底。"""
        if not self.is_safe_url(url):
            raise RuntimeError(f"图片地址不在白名单内:{urlsplit(url).hostname}")
        sp = urlsplit(url)
        targets = [url]
        if "oss-accelerate" in sp.netloc:
            bucket = sp.netloc.split(".")[0]
            targets.append(
                urlunsplit(
                    (sp.scheme, f"{bucket}.oss-cn-beijing.aliyuncs.com", sp.path, sp.query, "")
                )
            )

        last_exc: Exception | None = None
        for target in targets:
            for attempt in range(3):
                try:
                    resp = await self.http.get(target, follow_redirects=True, timeout=60)
                    resp.raise_for_status()
                    return resp.content
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    await asyncio.sleep(1 + attempt)
        raise RuntimeError(f"图片下载失败: {last_exc}")

    async def send_generated_image(
        self, event: dict[str, Any], request: str, hint: str = ""
    ) -> None:
        """提示 → 把描述查准 → 找到角色参考图 → 照参考图生图 → 发图;失败就回一句原因。"""
        await self.reply(
            event, self.cfg.get("image_hint", "正在作画(1080p),大概要 1 分钟…")
        )
        started = time.perf_counter()
        prompt, ref_name = await self.build_image_prompt(request, hint)
        ref_raw = await self.find_reference_image(ref_name)
        if ref_raw:
            prompt = REF_GUIDE_TPL.format(name=ref_name, desc=prompt)
        try:
            b64 = await self.gen_image(prompt, ref_raw)
        except Exception as exc:  # noqa: BLE001
            log.exception("生图出错")
            await self.reply(event, f"(生图失败了:{exc})")
            return
        log.info(
            "生图耗时 %.1fs(参考图:%s)", time.perf_counter() - started, "有" if ref_raw else "无"
        )
        await self.send(event, [{"type": "image", "data": {"file": f"base64://{b64}"}}])

    # ---------- 图片搜索 ----------

    async def _fetch_image_bytes(self, url: str) -> bytes | None:
        """下载一张图,失败 / 不是图片 / 太大都返回 None。"""
        if not self.is_safe_url(url):
            log.warning("拒绝访问非白名单域名:%s", urlsplit(url).hostname)
            return None
        try:
            resp = await self.http.get(
                url, headers={"User-Agent": BROWSER_UA}, follow_redirects=True, timeout=20
            )
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            log.warning("配图下载失败(%s): %s", url[:60], exc)
            return None
        if not resp.headers.get("content-type", "").startswith("image/"):
            return None
        limit = int(self.cfg.get("max_image_mb", 8)) * 1024 * 1024
        if len(resp.content) > limit:
            return None
        return resp.content

    async def bing_image_urls(self, query: str, want: int) -> list[str]:
        """Bing 图片搜索,返回前 want 个通过白名单的图片地址。"""
        search_url = "https://cn.bing.com/images/search"
        if not self.is_safe_url(search_url):
            log.warning("图片搜索地址不在白名单内,已跳过")
            return []
        try:
            resp = await self.http.get(
                search_url,
                params={"q": query, "form": "HDRSC2"},
                headers={"User-Agent": BROWSER_UA, "Accept-Language": "zh-CN,zh;q=0.9"},
                timeout=25,
            )
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            log.warning("图片搜索失败: %s", exc)
            return []

        found = re.findall(r'"murl":"(.*?)"', html.unescape(resp.text))
        # 先按白名单筛掉不安全站点,再去重
        candidates: list[str] = []
        for url in found:
            if url in candidates or not self.is_safe_url(url):
                continue
            candidates.append(url)
            if len(candidates) >= want:
                break
        log.info("图片搜索「%s」候选 %d 个(已过滤非白名单)", query[:30], len(candidates))
        return candidates

    async def baidu_image_urls(self, query: str, want: int) -> list[str]:
        """百度图片搜索(JSON 接口),返回前 want 个通过白名单的图片地址。

        找中文角色时比 Bing 稳:Bing 会间歇性返回一堆不相干的风景照。
        """
        api = "https://image.baidu.com/search/acjson"
        if not self.is_safe_url(api):
            return []
        try:
            resp = await self.http.get(
                api,
                params={
                    "tn": "resultjson_com",
                    "word": query,
                    "pn": 0,
                    "rn": 30,
                    "ie": "utf-8",
                    "fp": "result",
                    "queryWord": query,
                },
                headers={
                    "User-Agent": BROWSER_UA,
                    "Accept-Language": "zh-CN,zh;q=0.9",
                    "Referer": "https://image.baidu.com/",
                },
                timeout=25,
            )
            resp.raise_for_status()
            items = resp.json().get("data") or []
        except Exception as exc:  # noqa: BLE001
            log.warning("百度图片搜索失败: %s", exc)
            return []
        urls: list[str] = []
        for item in items:
            url = item.get("middleURL") or item.get("thumbURL") or ""
            if url and url not in urls and self.is_safe_url(url):
                urls.append(url)
            if len(urls) >= want:
                break
        log.info("百度图片搜索「%s」候选 %d 个", query[:30], len(urls))
        return urls

    async def search_images(self, query: str, count: int) -> list[str]:
        """按关键词找现成的图,返回可发送的 base64 列表。

        百度当主源(Bing 找中文的东西会间歇性跑偏),Bing 兜底。
        """
        candidates = await self.baidu_image_urls(query, count * 4)
        for url in await self.bing_image_urls(query, count * 3):
            if url not in candidates:
                candidates.append(url)
        images: list[str] = []
        for url in candidates:
            if len(images) >= count:
                break
            raw = await self._fetch_image_bytes(url)
            if raw and len(raw) >= 5_000:  # 太小的多半是图标 / 缩略图
                images.append(base64.b64encode(raw).decode())
        log.info("图片搜索「%s」命中 %d 张", query[:30], len(images))
        return images

    async def ask_vision(self, text: str, data_url: str) -> str:
        """拿一张图问识图模型一句话。"""
        c = self.cfg
        resp = await self.http.post(
            c.get("deepseek_base_url", "").rstrip("/") + "/chat/completions",
            json={
                "model": c.get("vision_model"),
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": text},
                            {"type": "image_url", "image_url": {"url": data_url}},
                        ],
                    }
                ],
                "max_tokens": 200,
                "enable_thinking": False,
            },
            headers={
                "Authorization": f"Bearer {c['deepseek_api_key']}",
                "Content-Type": "application/json",
            },
            timeout=120,
        )
        resp.raise_for_status()
        return strip_self_prefix(
            (resp.json()["choices"][0]["message"].get("content") or "").strip()
        )

    async def find_reference_image(self, name: str) -> bytes | None:
        """搜一张该角色的真实图,并用识图模型确认图里确实是它 —— 确认不了就返回 None。

        这一步是"画得像"的关键:纯文字描述只能画出神似的东西,把官方图喂给
        生图模型才能保住配色和造型。
        """
        if not name or not self.cfg.get("ref_image_enabled", True):
            return None
        if not self.cfg.get("vision_model"):
            return None
        # 百度当主源(中文角色稳),Bing 兜底;最多验证 12 张,免得一直等
        candidates = await self.baidu_image_urls(name, 12)
        for url in await self.bing_image_urls(name, 12):
            if url not in candidates:
                candidates.append(url)
        for url in candidates[:12]:
            raw = await self._fetch_image_bytes(url)
            if not raw or len(raw) < 20_000:  # 太小的多半是缩略图 / 图标
                continue
            try:
                desc = await self.ask_vision(
                    "只回答一句话:图里的卡通角色或人物叫什么名字?它是什么颜色、什么样?"
                    "如果不是角色,说明这是什么。",
                    bytes_to_data_url(raw),
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("参考图识别失败: %s", exc)
                continue
            if name in desc or name.lower() in desc.lower():
                log.info("参考图已确认「%s」:%s", name, desc[:60])
                return raw
            log.info("跳过参考图候选 %s:%s", urlsplit(url).hostname, desc[:50])
        log.info("没找到「%s」的可信参考图,改用纯文字生图", name)
        return None

    async def send_search_images(self, event: dict[str, Any], query: str) -> None:
        """联网搜索后,顺带把相关图片发给对方。"""
        count = int(self.cfg.get("search_image_count", 3))
        if count <= 0:
            return
        images = await self.search_images(query, count)
        if not images:
            return
        await self.send(
            event,
            [{"type": "image", "data": {"file": f"base64://{b64}"}} for b64 in images],
        )

    # ---------- 事件处理 ----------

    async def on_event(self, event: dict[str, Any]) -> None:
        if event.get("post_type") != "message":
            return
        if event.get("self_id"):
            self.self_id = str(event["self_id"])

        text, at_bot, images = self.parse_message(event)
        is_group = event.get("message_type") == "group"
        session_key = (
            f"group_{event['group_id']}" if is_group else f"user_{event['user_id']}"
        )
        sender = event.get("sender") or {}
        # 群里用群名片,没有就用昵称;上下文里带着它,模型才分得清谁在说话
        speaker = str(sender.get("card") or sender.get("nickname") or "").strip()

        # 群里常见操作:先单独发张图,再 @ 机器人问「这是什么」。
        # 所以图片消息就算没点它,也先记下来,供后面几条消息用。
        if images:
            self.recent_images[session_key] = (time.time(), images)
            # 群里别人发的表情包,顺手收了(白名单外的图会被拒)
            if is_group:
                asyncio.create_task(self.steal_stickers(images, event))

        # 冷落计数:没点她、而她又刚说过话,就算一条「没人接」;点了她就归零
        if is_group:
            if at_bot:
                self.silent_streak[session_key] = 0
            elif text and self.said_at.get(session_key) and (
                time.time() - self.said_at[session_key] < 1800
            ):
                self.silent_streak[session_key] = self.silent_streak.get(session_key, 0) + 1

        # 防撤回:先把这条原文记下来,因为撤回事件里只有一个 message_id
        if is_group and text:
            self.recall_cache.append(
                (
                    str(event.get("group_id")),
                    str(event.get("message_id") or ""),
                    speaker or "某人",
                    text[:200],
                )
            )
            # 统计、日报素材、冷场计时、长期记忆都记在这一笔里
            self.track_group(session_key, speaker, text)

        # 命令不用 @ 机器人也能用;成语接龙进行中,直接发四个字也算接龙
        playing_idiom = self.games.get(session_key, {}).get("kind") == "idiom"
        looks_idiom = bool(re.fullmatch(r"[\u4e00-\u9fff]{4}", text.strip()))
        if (self.cfg.get("commands_anyone", True) and self.is_command(text)) or (
            playing_idiom and looks_idiom
        ):
            ask_text = text.strip()
        else:
            ask_text = self.should_reply(event, text, at_bot, bool(images))

        if ask_text is None:
            # 只 @ 了一下、什么都没说:别装没看见,随口应一声
            if is_group and at_bot and not text.strip() and not images:
                lines = [
                    str(x)
                    for x in (self.cfg.get("empty_at_replies") or ["怎么了？"])
                    if str(x).strip()
                ]
                await self.reply(event, random.choice(lines) if lines else "怎么了？")
                return
            # 没点它的话也听着,顺带该复读就复读
            self.remember_ambient(session_key, speaker, text, bool(images))
            await self.maybe_repeat(event, session_key, text, is_group)
            return

        # 早上好 / 晚安 / 生日快乐 这类词,给这轮回复带个方向
        rule = self.keyword_reaction(ask_text or text)
        if rule:
            self.reaction[session_key] = rule

        # 成语接龙:接的这一句不走聊天,直接判定
        if playing_idiom and re.fullmatch(r"[\u4e00-\u9fff]{4}", ask_text):
            await self.reply(
                event,
                await self.answer_idiom(
                    session_key, str(event.get("user_id") or ""), speaker, ask_text
                ),
            )
            return

        # 斜杠命令 / 关键词命令
        if await self.handle_command(event, ask_text, session_key, is_group, speaker):
            return

        # 这条没带图,但刚发过图,就把它接上。
        # 用完就丢:不然接下来几分钟里每条消息都拖着这张图,
        # 模型会一直对着同一张图回答,看起来就像在复读。
        if not images:
            cached = self.recent_images.pop(session_key, None)
            if cached and time.time() - cached[0] < float(
                self.cfg.get("recent_image_seconds", 180)
            ):
                images = cached[1]
                log.info("沿用 %d 秒前发的图(%d 张)", int(time.time() - cached[0]), len(images))

        if ask_text in RESET_WORDS:
            self.history[session_key].clear()
            await self.reply(event, "清了,重新来过")
            return

        # /表情 [标签]:手动要一张表情包
        if ask_text.startswith("/表情"):
            tag = ask_text[len("/表情"):].strip()
            sticker = self.pick_sticker(tag, session_key)
            if sticker:
                log.info("手动发表情包:%s(%s)", sticker.name, tag or "随机")
                await self.send_sticker(event, sticker)
                self.remember_sticker(session_key, sticker.name)
            else:
                await self.reply(event, "表情包库是空的,往 表情包/ 里丢几张图就有了")
            return

        # 联网搜索:消息以 search_prefix 开头时走支持搜索的模型
        search_prefix = str(self.cfg.get("search_prefix", "/搜"))
        use_search = ask_text.startswith(search_prefix)
        if use_search:
            ask_text = ask_text[len(search_prefix):].strip()
            if not ask_text:
                await self.reply(event, f"用法:{search_prefix} 你要查的内容")
                return

        # 生图:消息以 image_prefix 开头时走生图接口
        image_prefix = str(self.cfg.get("image_prefix", "/画"))
        use_image = ask_text.startswith(image_prefix)
        if use_image:
            ask_text = ask_text[len(image_prefix):].strip()
            if not ask_text:
                await self.reply(event, f"用法:{image_prefix} 你想要的画面")
                return

        log.info(
            "[%s] %s%s: %s%s",
            session_key,
            "[生图] " if use_image else ("[联网] " if use_search else ""),
            speaker or "?",
            ask_text[:60],
            f" (+{len(images)}图)" if images else "",
        )

        async with self.locks[session_key]:
            if use_image:  # 显式打了 /画
                await self.send_generated_image(event, ask_text)
                return

            if use_search:
                await self.reply(
                    event, self.cfg.get("search_hint", "正在联网搜索,请稍等…")
                )
            started = time.perf_counter()
            failed = False
            try:
                answer = await self.ask_deepseek(
                    session_key,
                    ask_text,
                    images=images,
                    use_search=use_search,
                    speaker=speaker,
                )
            except Exception:  # noqa: BLE001
                log.exception("生成回复出错")
                failed = True
                answer = ""
            log.info("生成耗时 %.1fs", time.perf_counter() - started)

            if failed:
                # 别装死:随口应一声再甩张表情包,免得群里以为「她不理我」
                await self.reply(event, random.choice(FAIL_LINES))
                sticker = self.pick_sticker("", session_key)
                if sticker:
                    await self.send_sticker(event, sticker)
                    self.remember_sticker(session_key, sticker.name)
                return

            # 没打 /画,但模型自己判断出用户是在要图(自然语言)
            guess, want_draw = split_draw_marker(answer)
            if want_draw:
                log.info("模型判定要画图:%s", guess[:60])
                # 拿用户原话去联网查准,模型的理解只当参考(它可能压根不认识那角色)
                await self.send_generated_image(event, ask_text, guess)
                return

            # 用户要的是现成的图,那就直接上网找,别费劲去画
            remark, find_kw = split_find_marker(answer)
            if find_kw:
                log.info("模型判定要找图:%s", find_kw[:60])
                if remark:
                    await self.reply(event, remark)
                await self.send_search_images(event, find_kw)
                return

            # 联网问答:模型在末尾附了配图关键词,拆出来再发
            img_keyword = ""
            if use_search:
                answer, img_keyword = split_image_marker(answer)
                log.info("配图关键词:%s", img_keyword or "(不配图)")

            # 情绪到了,模型会在末尾附 [STICKER] 标签,拆出来顺手甩张表情包。
            # 再抽一次签:模型基本每句都想发,真每条都发就成刷屏了。
            answer, sticker_tag = split_sticker_marker(answer)
            sticker = None
            if sticker_tag and random.random() < float(
                self.cfg.get("sticker_chance", 0.4)
            ):
                sticker = self.pick_sticker(sticker_tag, session_key)
            log.info("表情包:%s(%s)", sticker.name if sticker else "不发", sticker_tag or "-")

            if answer:
                # 出门前洗一遍:拆 markdown、去客服腔、太长就截。
                # 万一洗空了(整条都是 markdown),那就原样发,别让她真的不说话
                answer = polish_reply(
                    answer, int(self.cfg.get("reply_max_chars", 300))
                ) or answer.strip()
            if answer:
                # 长回复拆成几条发,第一条引用对方,看着更像真人
                await self.send_answer(event, answer)
            if sticker:
                await self.send_sticker(event, sticker)
                self.remember_sticker(session_key, sticker.name)

            # 联网搜索顺手把相关图片也发过去
            if img_keyword and self.cfg.get("search_images", True):
                await self.send_search_images(event, img_keyword)

    # ---------- 群里的功能命令 ----------

    def keyword_reaction(self, text: str) -> str:
        """消息里带了「早上好」「晚安」这类词,就给一句反应方向。"""
        rules = self.cfg.get("keyword_reactions") or {}
        if not isinstance(rules, dict) or not text:
            return ""
        for word, rule in rules.items():
            if str(word) and str(word) in text:
                return str(rule or "").strip()
        return ""

    def is_command(self, text: str) -> bool:
        """这句要不要当命令处理(命令不用 @ 机器人)。"""
        t = text.strip()
        if not t:
            return False
        head = t.split(maxsplit=1)[0]
        if (head[1:] if head.startswith("/") else t) in BYPASS_COMMANDS:
            return True
        return bool(natural_command(t))

    def parse_at_targets(self, event: dict[str, Any]) -> list[str]:
        """这条消息 @ 了谁(不含机器人自己)。"""
        out = []
        for seg in event.get("message") or []:
            if isinstance(seg, dict) and seg.get("type") == "at":
                qq = str((seg.get("data") or {}).get("qq") or "")
                if qq and qq != self.self_id:
                    out.append(qq)
        return out

    def target_event(self, event: dict[str, Any], user_id: Any = None) -> dict[str, Any]:
        """通知类事件没有 message_type,转成能用来发消息的事件。"""
        if event.get("group_id"):
            return {"message_type": "group", "group_id": event["group_id"]}
        return {"message_type": "private", "user_id": user_id or event.get("user_id")}

    # --- 小玩意们 ---

    def make_fortune(self, kind: str) -> str:
        """运势 / 抽签,本地随机,不花 token。"""
        good = random.sample(FORTUNE_GOOD, 2)
        bad = random.sample(FORTUNE_BAD, 2)
        quip = random.choice(FORTUNE_QUIP)
        if kind == "抽签":
            level = random.choices(["上上签", "上签", "中签", "下签"], [1, 3, 4, 2])[0]
            return f"🎋 {level}\n宜:{good[0]}、{good[1]}\n忌:{bad[0]}、{bad[1]}\n{quip}"
        stars = random.randint(1, 5)
        return (
            f"今日运势 {'★' * stars}{'☆' * (5 - stars)}\n"
            f"宜:{good[0]}、{good[1]}\n忌:{bad[0]}、{bad[1]}\n"
            f"幸运数字:{random.randint(0, 99)}  幸运色:{random.choice(LUCKY_COLORS)}\n{quip}"
        )

    def do_checkin(self, user_id: str, nick: str) -> str:
        today = datetime.now().strftime("%Y-%m-%d")
        rec = self.store.points(user_id)
        if rec.get("last") == today:
            return f"{nick} 今天已经签过了,明天再来(现在 {rec['score']} 分)"
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        rec["days"] = int(rec.get("days", 0)) + 1 if rec.get("last") == yesterday else 1
        gain = 10 + min(int(rec["days"]), 7)
        self.store.add_points(user_id, gain, nick)
        rec["last"] = today
        self.store.save()
        return f"签到成功 +{gain} 分(连续 {rec['days']} 天),现在 {rec['score']} 分"

    def rank_of(self, user_id: str) -> int:
        for i, (uid, _) in enumerate(self.store.ranking(999), 1):
            if uid == user_id:
                return i
        return 0

    def ranking_text(self) -> str:
        rows = self.store.ranking(int(self.cfg.get("ranking_size", 10)))
        if not rows:
            return "还没人有积分,先 /签到 吧"
        lines = [
            f"{i}. {rec.get('name') or uid} {rec.get('total', 0)} 分"
            for i, (uid, rec) in enumerate(rows, 1)
        ]
        return "积分榜:\n" + "\n".join(lines)

    def do_guess(self, session_key: str, user_id: str, nick: str, arg: str) -> str:
        game = self.games.get(session_key)
        if not game or game.get("kind") != "number":
            return "先 /猜数字 开一局吧"
        if not arg.strip().isdigit():
            return "猜一个数字,比如 /猜 50"
        guess = int(arg.strip())
        game["tries"] = int(game.get("tries", 0)) + 1
        target = int(game["target"])
        if guess == target:
            self.games.pop(session_key, None)
            self.store.add_points(user_id, 5, nick)
            self.store.save()
            return f"中了!就是 {target},{game['tries']} 次猜出来,+5 分"
        hint = "大了" if guess > target else "小了"
        return f"{guess} {hint},再猜(第 {game['tries']} 次)"

    def start_idiom(self, session_key: str) -> str:
        word = random.choice(IDIOM_SEED)
        self.games[session_key] = {"kind": "idiom", "last": word, "count": 0}
        return f"成语接龙,我先来:{word}\n接的成语要用「{word[-1]}」开头,直接发四个字就行(/结束 退出)"

    async def answer_idiom(
        self, session_key: str, user_id: str, nick: str, word: str
    ) -> str:
        game = self.games.get(session_key)
        if not game or game.get("kind") != "idiom":
            return f"{word} 是成语,不过现在没在玩接龙,想玩就 /成语接龙"
        last = str(game.get("last") or "")
        verdict = await self.ask_plain(IDIOM_CHECK_TPL.format(last=last, word=word))
        data = {}
        try:
            start, end = verdict.find("{"), verdict.rfind("}")
            data = json.loads(verdict[start : end + 1])
        except Exception:  # noqa: BLE001
            data = {}
        if not data.get("ok"):
            why = str(data.get("why") or "不太对").strip()
            # 有人乱发四个字也能被当成回答,连错三次就散场,别一直应声
            game["miss"] = int(game.get("miss", 0)) + 1
            if game["miss"] >= 3:
                self.games.pop(session_key, None)
                return f"{word} 不行:{why}\n连错三次,散了散了,想玩再 /成语接龙"
            return f"{word} 不行:{why}"
        game["miss"] = 0
        nxt = str(data.get("next") or "").strip()
        game["last"] = nxt or word
        game["count"] = int(game.get("count", 0)) + 1
        self.store.add_points(user_id, 2, nick)
        self.store.save()
        return f"{nxt or word}(接上第 {game['count']} 个,+2 分)"

    # --- 提醒 ---

    def do_remind(
        self, event: dict[str, Any], user_id: str, nick: str, rest: str
    ) -> str:
        if not rest:
            return "写法:/提醒 20分钟后 开会"
        seconds, what, err = parse_duration(rest)
        if err:
            return err
        reminders = self.store.data["reminders"]
        if len(reminders) >= int(self.cfg.get("reminder_max", 50)):
            return "提醒太多了,先取消几个"
        if not what:
            what = "你让我提醒的事"
        rid = max((int(r.get("id", 0)) for r in reminders), default=0) + 1
        reminders.append(
            {
                "id": rid,
                "ts": time.time() + seconds,
                "group": event.get("group_id"),
                "user": user_id,
                "nick": nick,
                "text": what[:100],
            }
        )
        self.store.save()
        when = datetime.now() + timedelta(seconds=seconds)
        return f"好,{when.strftime('%H:%M')} 提醒你「{what}」(编号 {rid})"

    def list_reminders(self, event: dict[str, Any]) -> str:
        gid = event.get("group_id")
        mine = [
            r
            for r in self.store.data["reminders"]
            if (gid is None or r.get("group") == gid)
        ]
        if not mine:
            return "这边没有待提醒的事"
        lines = [
            f"{r['id']}. {datetime.fromtimestamp(r['ts']).strftime('%m-%d %H:%M')} {r['text']}"
            for r in mine
        ]
        return "待提醒:\n" + "\n".join(lines)

    def cancel_reminder(self, event: dict[str, Any], arg: str) -> str:
        arg = arg.strip()
        gid = event.get("group_id")
        if not arg.isdigit():
            return "要取消哪个?看 /提醒列表 的编号,比如 /取消提醒 3"
        rid = int(arg)
        before = len(self.store.data["reminders"])
        self.store.data["reminders"] = [
            r
            for r in self.store.data["reminders"]
            if not (
                int(r.get("id", 0)) == rid
                and (gid is None or r.get("group") == gid)
            )
        ]
        if len(self.store.data["reminders"]) == before:
            return f"没找到编号 {rid} 的提醒"
        self.store.save()
        return f"取消了 {rid}"

    async def reminder_loop(self) -> None:
        """到点了就 @ 人提醒。"""
        while True:
            await asyncio.sleep(10)
            now = time.time()
            due = [r for r in self.store.data["reminders"] if float(r.get("ts", 0)) <= now]
            if not due:
                continue
            self.store.data["reminders"] = [
                r for r in self.store.data["reminders"] if float(r.get("ts", 0)) > now
            ]
            self.store.save()
            for r in due:
                target = {"message_type": "group", "group_id": r["group"]} if r.get("group") else {"message_type": "private", "user_id": r["user"]}
                msg: list[dict[str, Any]] = [
                    {"type": "at", "data": {"qq": r["user"]}},
                    {"type": "text", "data": {"text": f" 到点了:{r['text']}"}},
                ]
                log.info("提醒 %s:%s", r.get("nick") or r["user"], r["text"])
                await self.send(target, msg)

    # --- 天气 / 热搜 ---

    async def fetch_weather(self, city: str) -> str:
        city = (city or str(self.cfg.get("default_city", "北京"))).strip()
        if not (self.is_safe_url(GEO_URL) and self.is_safe_url(WEATHER_URL)):
            return "天气接口的域名不在白名单里,先往 config 的 safe_domains 加 open-meteo.com"
        try:
            geo = (
                await self.http.get(
                    GEO_URL,
                    params={"name": city, "count": 1, "language": "zh"},
                    headers={"User-Agent": BROWSER_UA},
                    timeout=12,
                )
            ).json()
            hit = (geo.get("results") or [None])[0]
            if not hit:
                return f"没找到「{city}」这个地方,换个写法试试"
            w = (
                await self.http.get(
                    WEATHER_URL,
                    params={
                        "latitude": hit["latitude"],
                        "longitude": hit["longitude"],
                        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,wind_speed_10m",
                        "daily": "temperature_2m_max,temperature_2m_min,precipitation_probability_max",
                        "timezone": "auto",
                        "forecast_days": 1,
                    },
                    headers={"User-Agent": BROWSER_UA},
                    timeout=12,
                )
            ).json()
        except Exception as exc:  # noqa: BLE001
            return f"天气没查到({exc})"
        cur = w.get("current") or {}
        daily = w.get("daily") or {}
        where = hit.get("name") or city
        if hit.get("admin1") and hit["admin1"] != where:
            where = f"{where}({hit['admin1']})"
        code = cur.get("weather_code")
        head = (
            f"{where}现在 {cur.get('temperature_2m')}℃,{WMO_CODE.get(code, '未知')},"
            f"体感 {cur.get('apparent_temperature')}℃,"
            f"湿度 {cur.get('relative_humidity_2m')}%,风 {cur.get('wind_speed_10m')}km/h"
        )
        low = (daily.get("temperature_2m_min") or ["?"])[0]
        high = (daily.get("temperature_2m_max") or ["?"])[0]
        rain = (daily.get("precipitation_probability_max") or ["?"])[0]
        return f"{head}\n今天 {low}~{high}℃,降水概率 {rain}%"

    async def fetch_hot(self) -> str:
        if not self.is_safe_url(HOT_URL):
            return "热搜的域名不在白名单里,先往 config 的 safe_domains 加 top.baidu.com"
        try:
            data = (
                await self.http.get(
                    HOT_URL, headers={"User-Agent": BROWSER_UA}, timeout=12
                )
            ).json()
        except Exception as exc:  # noqa: BLE001
            return f"热搜没拉到({exc})"
        items: list[dict[str, Any]] = []
        for card in (data.get("data") or {}).get("cards") or []:
            node = card.get("content")
            while isinstance(node, list) and node and isinstance(node[0], dict) and "content" in node[0]:
                node = node[0]["content"]
            if isinstance(node, list) and node and "word" in node[0]:
                items = node
                break
        if not items:
            return "热搜今天有点安静,没拉到"
        count = int(self.cfg.get("hot_count", 10))
        lines = [
            f"{i}. {it['word']}"
            for i, it in enumerate(items[:count], 1)
            if it.get("word")
        ]
        return "百度实时热搜:\n" + "\n".join(lines)

    # --- 额度:查阿里云百炼的免费额度 ---

    def balance_models(self) -> list[str]:
        """报额度时报哪几个模型,就是机器人自己在用的那几个。"""
        _, _, on_official = self.chat_endpoint()
        keys = ["vision_model", "search_model", "image_model", "draw_prompt_model"]
        if not on_official:  # 文字聊天已经接去别家了,就别在阿里云这边找它
            keys.insert(0, "model")
        out: list[str] = []
        for key in keys:
            name = str(self.cfg.get(key) or "").strip()
            if name and name not in out:
                out.append(name)
        return out

    @staticmethod
    def short_num(value: Any) -> str:
        """610488 写成 61.0万,省得刷屏。"""
        try:
            num = float(value)
        except (TypeError, ValueError):
            return "?"
        return f"{num / 10000:.1f}万" if num >= 10000 else f"{num:.0f}"

    def balance_text(self, rows: list[dict[str, Any]]) -> str:
        """把 bl usage free 的结果写成一小段人话。"""
        by_model = {str(r.get("model")): r for r in rows if isinstance(r, dict)}
        lines, used = [], []
        for name in self.balance_models():
            row = by_model.get(name)
            if not row:
                lines.append(f"· {name}: 没查到(这个模型不送免费额度,或者已经用完)")
                continue
            used.append(row)
            pct = row.get("remainingPercent")
            pct = f"{float(pct):.0f}%" if pct is not None else "?"
            lines.append(
                f"· {name}: 剩 {self.short_num(row.get('remaining'))}"
                f" / {self.short_num(row.get('total'))}({pct})"
            )
        expires = [str(r.get("expires")) for r in used if r.get("expires")]
        tail = f"\n最早到期:{min(expires)}" if expires else ""
        _, _, on_official = self.chat_endpoint()
        title = (
            "【阿里云百炼 · 识图 / 搜索 / 生图】"
            if on_official
            else "鲸鲸的白饭还剩这些(阿里云免费额度):"
        )
        return title + "\n" + "\n".join(lines) + tail

    async def fetch_ds_balance(self) -> str:
        """问 DeepSeek 官网还剩多少钱。"""
        base, key, on_official = self.chat_endpoint()
        if not (base and key):
            return ""
        name = str(self.cfg.get("chat_model") or "")
        price = DS_OFFICIAL_PRICE.get(name)
        money = (
            f"输入 {price[0]} 元 / 输出 {price[1]} 元(每百万 tokens)"
            if price
            else "按量扣费"
        )
        head = f"【DeepSeek 官网 · 文字聊天】\n· 模型:{name}, {money}"
        try:
            resp = await self.http.get(
                base + "/user/balance",
                headers={"Authorization": f"Bearer {key}"},
            )
            if resp.status_code != 200:
                return head + f"\n· 余额没查到(HTTP {resp.status_code})"
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            log.warning("查 DeepSeek 余额失败: %s", exc)
            return head + "\n· 余额没查到(连不上)"
        infos = data.get("balance_infos") or []
        if not infos:
            return head + "\n· 余额没查到(接口没给数据)"
        segs = []
        for info in infos:
            cur = str(info.get("currency") or "").upper()
            sym = {"CNY": "¥", "USD": "$"}.get(cur, cur + " ")
            total = str(info.get("total_balance") or "0")
            top = str(info.get("topped_up_balance") or "0")
            gift = str(info.get("granted_balance") or "0")
            segs.append(f"{sym}{total}(充值 {top} / 赠送 {gift})")
        if data.get("is_available") is False:
            segs.append("余额见底了,可能已经扣不动")
        return head + "\n· 余额 " + " ".join(segs)

    async def fetch_aliyun_balance(self) -> str:
        """跑一次 bl usage free,问阿里云还剩多少额度。"""
        exe = find_bl()
        if not exe:
            return "【阿里云百炼】\n没找到命令行工具(bl),装上才能查额度"
        try:
            proc = await asyncio.create_subprocess_exec(
                exe,
                "usage",
                "free",
                "--output",
                "json",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=25)
        except asyncio.TimeoutError:
            return "【阿里云百炼】\n查额度超时了,过会儿再喊我"
        except Exception as exc:  # noqa: BLE001
            return f"【阿里云百炼】\n查额度失败了({exc})"
        text = out.decode("utf-8-sig", "replace").strip()
        try:
            rows = json.loads(text)
        except ValueError:
            return (
                "【阿里云百炼】\n查额度没成功,可能要重新登录一次"
                f"(bl auth login --console)\n{text[:80]}"
            )
        if isinstance(rows, dict):
            msg = str((rows.get("error") or {}).get("message") or text[:80])
            return f"【阿里云百炼】\n查额度没成功:{msg}\n重新登录一次就好了"
        if not isinstance(rows, list) or not rows:
            return "【阿里云百炼】\n没查到免费额度"
        return self.balance_text(rows)

    async def _bss_call(
        self, action: str, version: str, extra: dict[str, str] | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        """调一次阿里云费用中心的 RPC 接口,回 (数据, 错误说明)。签名自己算。"""
        ak = str(self.cfg.get("aliyun_access_key_id") or "").strip()
        sk = str(self.cfg.get("aliyun_access_key_secret") or "").strip()
        params: dict[str, str] = {
            "Action": action,
            "Version": version,
            "Format": "JSON",
            "AccessKeyId": ak,
            "SignatureMethod": "HMAC-SHA1",
            "SignatureVersion": "1.0",
            "Timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "SignatureNonce": str(uuid.uuid4()),
        }
        params.update(extra or {})
        params["Signature"] = _bss_sign(params, sk)
        try:
            resp = await self.http.get(BSS_HOST, params=params)
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            log.warning("调阿里云 %s 失败: %s", action, exc)
            return None, "连不上"
        if resp.status_code != 200 or data.get("Code") not in (None, "200", "Success"):
            return None, str(data.get("Message") or f"HTTP {resp.status_code}")
        return data, ""

    @staticmethod
    def _num(v: Any) -> float:
        try:
            return float(v or 0)
        except (TypeError, ValueError):
            return 0.0

    async def fetch_aliyun_money(self) -> str:
        """问阿里云账户还剩多少钱:现金余额 + 优惠券(学生那 300 元就在券里)。

        这两个是分开的接口——现金走 QueryAccountBalance(老版本号),
        券走 DescribeCoupon。账户里现金常常是零,能花的钱其实都在券里,所以两样都报。
        """
        ak = str(self.cfg.get("aliyun_access_key_id") or "").strip()
        sk = str(self.cfg.get("aliyun_access_key_secret") or "").strip()
        if not (ak and sk):
            return ""
        cash_body, cash_err = await self._bss_call(
            "QueryAccountBalance", BSS_LEGACY_VERSION
        )
        coupon_body, coupon_err = await self._bss_call(
            "DescribeCoupon", BSS_VERSION, {"CurrentPage": "1", "PageSize": "50"}
        )
        lines: list[str] = []
        num = self._num
        if cash_body is None:
            lines.append(f"· 现金余额:没查到({cash_err})")
        else:
            cash = cash_body.get("Data") or {}
            cur = str(cash.get("Currency") or "CNY").upper()
            sym = {"CNY": "¥", "USD": "$"}.get(cur, cur + " ")
            lines.append(f"· 现金余额:{sym}{num(cash.get('AvailableCashAmount')):.2f}")
        if coupon_body is None:
            lines.append(f"· 抵用券:没查到({coupon_err})")
        else:
            live = [
                c
                for c in (coupon_body.get("Data") or [])
                if str(c.get("Status") or "").lower().startswith("available")
            ]
            if not live:
                lines.append("· 抵用券:没有可用的了(要么用完了,要么过期了)")
            else:
                total = 0.0
                for c in live:
                    rem = num(c.get("RemainAmount"))
                    full = num(c.get("Amount"))
                    total += rem
                    name = str(c.get("CouponTypeName") or c.get("CouponType") or "优惠券")
                    cur = str(c.get("Currency") or "CNY").upper()
                    sym = {"CNY": "¥", "USD": "$"}.get(cur, cur + " ")
                    line = f"· {name}: 剩 {sym}{rem:.2f}"
                    if full:
                        line += f" / 共 {sym}{full:.2f}"
                    remark = str(c.get("Remark") or "").strip()
                    if remark:
                        line += f"({remark})"
                    end = str(c.get("EndTime") or "")[:10]
                    if end:
                        line += f",{end} 到期"
                    lines.append(line)
                lines.append(f"(券合计还剩 {sym}{total:.2f})")
        return "【阿里云账户 · 学生权益】\n" + "\n".join(lines)

    async def fetch_balance(self) -> str:
        """三处都问一遍:DeepSeek 官网余额 + 阿里云账户(现金+券) + 百炼免费额度。"""
        ds, coupon, ali = await asyncio.gather(
            self.fetch_ds_balance(),
            self.fetch_aliyun_money(),
            self.fetch_aliyun_balance(),
        )
        parts = [p for p in (ds, coupon, ali) if p]
        if not parts:
            return "没查到余额,几个 key 可能都没配上"
        return "鲸鲸的白饭账本:\n\n" + "\n\n".join(parts)

    # --- 群管:禁言 / 踢人 ---

    async def do_admin(
        self, event: dict[str, Any], head: str, rest: str, is_group: bool
    ) -> str:
        if not is_group:
            return "这个只能在群里用"
        if not self.cfg.get("admin_cmds", True):
            return "群管命令没开(config 里 admin_cmds)"
        group_id = event.get("group_id")
        user_id = str(event.get("user_id") or "")
        allowed = user_id in [str(u) for u in (self.cfg.get("admin_users") or [])]
        if not allowed:
            allowed = await self.member_role(group_id, user_id) in ("owner", "admin")
        if not allowed:
            return "这个只有群管理能用哦"
        targets = self.parse_at_targets(event)

        if head == "全体禁言":
            resp = await self.call_api_wait(
                "set_group_whole_ban", {"group_id": group_id, "enable": True}
            )
            return "全员先闭嘴" if resp.get("status") == "ok" else "没弄成,得让我当群管理"
        if head == "解除禁言":
            if targets:
                for t in targets:
                    await self.call_api_wait(
                        "set_group_ban", {"group_id": group_id, "user_id": t, "duration": 0}
                    )
                return "放出来了"
            resp = await self.call_api_wait(
                "set_group_whole_ban", {"group_id": group_id, "enable": False}
            )
            return "解除全员禁言" if resp.get("status") == "ok" else "没弄成,得让我当群管理"
        if not targets:
            return "要 @ 一个人,比如 /禁言 @某人 10分钟"
        # 机器人自己顶多是个管理员:管理员之间、管理员对群主,QQ 都不让动,先拦下来
        self_role = await self.member_role(group_id, self.self_id)
        for t in targets:
            role = await self.member_role(group_id, t)
            if role == "owner":
                return "那是群主,我没资格动"
            if role == "admin" and self_role != "owner":
                return "他也是管理员,跟我平级 —— 管理员之间踢不动也禁不了,只有群主能"
        if head in ("踢", "踢人"):
            for t in targets:
                resp = await self.call_api_wait(
                    "set_group_kick",
                    {"group_id": group_id, "user_id": t, "reject_add_request": False},
                )
                if resp.get("status") != "ok":
                    return "没踢动:" + api_fail_reason(resp)
            return "请出去了"
        seconds, _, _ = parse_duration(rest)
        seconds = seconds or int(self.cfg.get("ban_seconds", 600))
        seconds = min(seconds, 30 * 86400)
        for t in targets:
            resp = await self.call_api_wait(
                "set_group_ban",
                {"group_id": group_id, "user_id": t, "duration": seconds},
            )
            if resp.get("status") != "ok":
                return "没禁上:" + api_fail_reason(resp)
        return f"禁言 {seconds // 60} 分钟"

    async def handle_command(
        self, event: dict[str, Any], text: str, session_key: str, is_group: bool, speaker: str
    ) -> bool:
        """处理命令。返回 True 表示这条已经处理掉了,不用再走聊天。"""
        raw = text.strip()
        name = KEYWORDS.get(raw, raw)
        if name.startswith("/"):
            name = name[1:]
        head, _, rest = name.partition(" ")
        head, rest = head.strip(), rest.strip()
        if head not in COMMANDS:
            # 「查一下余额」这种口语说法,也当命令走
            head, rest = natural_command(raw) or head, ""
        if head not in COMMANDS:
            return False
        user_id = str(event.get("user_id") or "")
        nick = speaker or user_id

        if head in ("帮助", "help", "菜单"):
            extra = (
                "\n【群管理(得让我当群管理)】\n"
                "/禁言 @某人 10分钟\n/全体禁言  /解除禁言\n/踢 @某人"
                if self.cfg.get("admin_cmds", True)
                else ""
            )
            await self.reply(event, HELP_TEXT + extra)
        elif head in ("骰", "骰子"):
            await self.reply(event, roll_dice(rest))
        elif head in ("抽签", "运势"):
            await self.reply(event, self.make_fortune(head))
        elif head == "签到":
            await self.reply(event, self.do_checkin(user_id, nick))
        elif head == "积分":
            rec = self.store.points(user_id)
            await self.reply(
                event, f"{nick} 有 {rec['score']} 分(总榜第 {self.rank_of(user_id)} 名)"
            )
        elif head in ("排行榜", "榜"):
            await self.reply(event, self.ranking_text())
        elif head == "猜数字":
            self.games[session_key] = {
                "kind": "number",
                "target": random.randint(1, 100),
                "tries": 0,
            }
            await self.reply(event, "我想好了一个 1-100 的数。/猜 50 这样猜,猜中 +5 分")
        elif head == "猜":
            await self.reply(event, self.do_guess(session_key, user_id, nick, rest))
        elif head == "成语接龙":
            await self.reply(event, self.start_idiom(session_key))
        elif head == "接龙":
            await self.reply(
                event,
                await self.answer_idiom(session_key, user_id, nick, rest)
                if rest
                else self.start_idiom(session_key),
            )
        elif head == "结束":
            if self.games.pop(session_key, None):
                await self.reply(event, "不玩了,收工")
            else:
                await self.reply(event, "现在没在玩游戏")
        elif head == "提醒":
            await self.reply(event, self.do_remind(event, user_id, nick, rest))
        elif head == "提醒列表":
            await self.reply(event, self.list_reminders(event))
        elif head == "取消提醒":
            await self.reply(event, self.cancel_reminder(event, rest))
        elif head == "天气":
            await self.reply(event, await self.fetch_weather(rest))
        elif head == "热搜":
            await self.reply(event, await self.fetch_hot())
        elif head == "统计":
            await self.reply(event, self.stats_text(session_key))
        elif head == "余额":
            await self.reply(event, await self.fetch_balance())
        elif head in ("禁言", "解除禁言", "全体禁言", "踢", "踢人"):
            await self.reply(event, await self.do_admin(event, head, rest, is_group))
        return True

    async def maybe_repeat(
        self, event: dict[str, Any], session_key: str, text: str, is_group: bool
    ) -> None:
        """群里连着刷同一句话时跟一句。同一句只接一次,不刷屏。"""
        if not is_group or not self.cfg.get("repeat_enabled", True):
            return
        body = text.strip()
        if not 2 <= len(body) <= 40 or body.startswith("/"):
            return
        last, times = self.said.get(session_key, ("", 0))
        times = times + 1 if body == last else 1
        self.said[session_key] = (body, times)
        need = int(self.cfg.get("repeat_min", 3))
        if times < need or body in self.repeated:
            return
        self.repeated.add(body)
        if len(self.repeated) > 300:
            self.repeated.clear()
        if random.random() > float(self.cfg.get("repeat_chance", 0.7)):
            return
        log.info("跟着复读一句:%s", body)
        await self.reply(event, body)

    # ---------- 说话方式:分段、引用 ----------

    def split_reply(self, text: str) -> list[str]:
        """把长回复拆成几条短的,像真人那样一条条发,而不是糊一大块。"""
        body = text.strip()
        if not self.cfg.get("split_reply", True) or len(body) < 24:
            return [text]
        limit = max(1, int(self.cfg.get("split_max", 3)))
        if limit == 1:
            return [text]
        parts = [p.strip() for p in re.split(r"\n+", body) if p.strip()]
        if len(parts) < 2:
            parts = [p.strip() for p in re.split(r"(?<=[。！？!?~…])\s*", body) if p.strip()]
        if len(parts) < 2:
            return [text]
        # 一句只有两三个字的,跟前面那条并起来,不然像挤牙膏
        merged: list[str] = []
        for part in parts:
            if merged and len(part) <= 4:
                merged[-1] += part
            else:
                merged.append(part)
        while len(merged) > limit:
            merged[-2] += merged[-1]
            merged.pop()
        return [p for p in merged if p.strip()]

    def as_segments(self, event: dict[str, Any], text: str, quote: bool = False) -> Any:
        """要引用原消息就拼成消息段,不然直接发纯文本。"""
        if not (quote and self.cfg.get("quote_reply", True) and event.get("message_id")):
            return text
        return [
            {"type": "reply", "data": {"id": str(event["message_id"])}},
            {"type": "text", "data": {"text": text}},
        ]

    async def send_answer(self, event: dict[str, Any], text: str) -> None:
        """聊天回复:分段发,第一条引用对方。"""
        # 「她刚说过话」和「冷落计数清零」都记在这里,冒泡那条线要靠它
        if event.get("message_type") == "group" and event.get("group_id") is not None:
            key = f"group_{event['group_id']}"
        else:
            key = f"user_{event.get('user_id')}"
        self.said_at[key] = time.time()
        self.silent_streak[key] = 0
        chunks = self.split_reply(text)
        for i, chunk in enumerate(chunks):
            await self.send(event, self.as_segments(event, chunk, quote=(i == 0)))
            if i < len(chunks) - 1:
                await asyncio.sleep(float(self.cfg.get("split_delay", 0.8)))

    # ---------- 群统计 / 日报 / 主动消息 ----------

    def track_group(self, session_key: str, speaker: str, text: str) -> None:
        """记一笔群消息:统计、日报素材、冷场计时、长期记忆都靠它。"""
        today = datetime.now().strftime("%Y-%m-%d")
        stat = self.stats.get(session_key)
        if not stat or stat.get("day") != today:
            stat = {"day": today, "count": 0, "users": {}, "hours": {}}
            self.stats[session_key] = stat
            self.day_log[session_key] = []
        stat["count"] += 1
        who = speaker or "某人"
        stat["users"][who] = stat["users"].get(who, 0) + 1
        hour = str(datetime.now().hour)
        stat["hours"][hour] = stat["hours"].get(hour, 0) + 1
        if text and len(self.day_log[session_key]) < 400:
            self.day_log[session_key].append(f"{who}: {text[:80]}")
        self.last_active[session_key] = time.time()
        self.memory.add(session_key, who, text)

    def stats_text(self, session_key: str) -> str:
        stat = self.stats.get(session_key) or {}
        if not stat.get("count"):
            return "今天这个群还没聊几句,没统计到啥"
        users = sorted(stat.get("users", {}).items(), key=lambda kv: -kv[1])[:3]
        hours = stat.get("hours", {})
        busiest = max(hours.items(), key=lambda kv: kv[1])[0] if hours else "?"
        top = "、".join(f"{name}({n})" for name, n in users)
        return (
            f"今天这个群聊了 {stat['count']} 条\n"
            f"最活跃:{top}\n"
            f"最热闹的时段:{busiest} 点\n"
            f"今天偷到 {self.steal_count.get('n', 0)} 张表情包"
        )

    async def summarize_day(self, session_key: str) -> str:
        lines = self.day_log.get(session_key) or []
        prompt = (
            "下面是今天群里的聊天记录,格式是「昵称: 内容」。写一份群聊日报:\n"
            "1. 第一行用一句话说今天群里主要在聊什么;\n"
            "2. 再列 2-4 条今天的话题或名场面,每条一行、一句话;\n"
            "3. 最后点一下今天最活跃的人。\n"
            "中文,总共 150 字以内,口语,不要 Markdown 标题和表格,不要客套开场。\n\n"
            + "\n".join(lines[-200:])
        )
        return await self.ask_plain(prompt, max_tokens=400)

    async def maybe_daily_report(self) -> None:
        """到点了给每个有聊天的群发一份日报。"""
        if not self.cfg.get("daily_report", True):
            return
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        if now.strftime("%H:%M") < str(self.cfg.get("report_time", "21:00")):
            return
        if self.reported_day == today:
            return
        self.reported_day = today
        need = int(self.cfg.get("report_min_messages", 30))
        for key in list(self.day_log):
            if not key.startswith("group_"):
                continue
            if len(self.day_log[key]) < need:
                continue
            text = await self.summarize_day(key)
            if not text:
                continue
            log.info("发群聊日报:%s", key)
            await self.send(
                {"message_type": "group", "group_id": key.split("_", 1)[1]},
                f"今天的群聊日报:\n{text}",
            )

    async def proactive_line(self, session_key: str) -> str:
        chat = "\n".join(self.ambient.get(session_key, [])[-8:])
        prompt = (
            f"{self.cfg.get('system_prompt') or ''}\n\n"
            f"群里刚才聊的是:\n{chat}\n\n"
            "现在群里安静了一阵子,你想主动冒个泡。要求:一句中文,20 字以内,"
            "口语自然,可以接刚才的话头、也可以随口抛个问题,不 @ 任何人,"
            "不客套、不说自己是 AI。只输出这一句话。"
        )
        return (await self.ask_plain(prompt, max_tokens=60)).strip()

    async def maybe_speak(self) -> None:
        """冷场时主动说一句。"""
        if not self.cfg.get("proactive", True):
            return
        hour = datetime.now().hour
        quiet = self.cfg.get("quiet_hours") or [23, 8]
        start, end = int(quiet[0]), int(quiet[1])
        if (start > end and (hour >= start or hour < end)) or (
            start <= end and start <= hour < end
        ):
            return
        idle = float(self.cfg.get("proactive_idle_minutes", 45)) * 60
        need = int(self.cfg.get("proactive_min_messages", 6))
        sulk = int(self.cfg.get("sulk_after", 5))
        now = time.time()
        for key, last in list(self.last_active.items()):
            if not key.startswith("group_") or now - last < idle:
                continue
            if (self.stats.get(key) or {}).get("count", 0) < need:
                continue
            # 上次说完话之后一直没人接,那就别再往上凑了
            if sulk and self.silent_streak.get(key, 0) >= sulk:
                log.info("%s 被冷落 %d 条了,先不冒泡", key, self.silent_streak[key])
                continue
            if random.random() > float(self.cfg.get("proactive_chance", 0.35)):
                continue
            self.last_active[key] = now  # 冒过泡就重新计时,别连着蹦出来
            line = await self.proactive_line(key)
            if not line:
                continue
            log.info("主动冒个泡:%s", line)
            await self.send(
                {"message_type": "group", "group_id": key.split("_", 1)[1]}, line
            )
            return

    async def proactive_loop(self) -> None:
        """每 40 秒看一眼:该发日报了没、该冒泡了没。"""
        while True:
            await asyncio.sleep(40)
            try:
                await self.maybe_daily_report()
                await self.maybe_speak()
                self.memory.save()
            except Exception:  # noqa: BLE001
                log.exception("主动消息/日报出错")

    # ---------- 偷表情包 ----------

    async def steal_stickers(self, images: list[str], event: dict[str, Any]) -> None:
        """群里别人发的表情包,顺手收进自己库里。"""
        if not self.cfg.get("steal_stickers", True):
            return
        today = datetime.now().strftime("%Y-%m-%d")
        if self.steal_count.get("day") != today:
            self.steal_count = {"day": today, "n": 0}
        limit = int(self.cfg.get("steal_max_per_day", 8))
        for ref in images[:2]:
            if self.steal_count["n"] >= limit:
                return
            raw = await self._fetch_image_bytes(ref)
            if not raw or len(raw) < 5000:  # 太小的多半是缩略图
                continue
            digest = hashlib.md5(raw).hexdigest()
            if digest in self.known_hashes:
                continue
            # 见过一次不算数:截图/照片/商品图基本只出现一次,
            # 真表情包会被不同的人反复发,攒够次数再收,顺便省一次识图钱
            seen = self.seen_counts.get(digest, 0) + 1
            self.seen_counts[digest] = seen
            if seen < int(self.cfg.get("steal_min_seen", 2)):
                log.info("这张图第 %d 次见,还不够格,先不收", seen)
                continue
            kind = str(
                mimetypes.guess_type(ref.split("?")[0])[0] or "image/jpeg"
            ).lower()
            ext = IMAGE_EXT.get(kind, ".jpg")
            data_url = f"data:{kind};base64,{base64.b64encode(raw).decode()}"
            verdict = await self.ask_vision(STEAL_PROMPT, data_url)
            start, end = verdict.find("{"), verdict.rfind("}")
            try:
                info = json.loads(verdict[start : end + 1])
            except Exception:  # noqa: BLE001
                continue
            if not info.get("meme"):
                continue
            name = f"偷来的_{digest[:6]}{ext}"
            target = STICKER_DIR / name
            try:
                STICKER_DIR.mkdir(exist_ok=True)
                target.write_bytes(raw)
                item = {
                    "file": name,
                    "emotion": str(info.get("emotion") or "无语")[:8],
                    "scene": str(info.get("scene") or "")[:40],
                    "tags": [str(t)[:8] for t in (info.get("tags") or [])][:5],
                }
                self._append_sticker_index(item)
            except Exception as exc:  # noqa: BLE001
                log.warning("偷表情包没存住:%s", exc)
                continue
            self.stickers.append(item)
            self.known_hashes.add(digest)
            self.steal_count["n"] += 1
            log.info("偷到一张表情包:%s(%s)", name, item["emotion"])

    def _append_sticker_index(self, item: dict[str, Any]) -> None:
        """把新表情包写进 tags.json(读不出来的话就丢一张,别把库弄坏)。"""
        try:
            data = json.loads(STICKER_INDEX.read_text("utf-8"))
        except Exception:  # noqa: BLE001
            return
        data.setdefault("images", []).append(item)
        STICKER_INDEX.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # ---------- 通知事件 ----------

    async def on_notice(self, event: dict[str, Any]) -> None:
        """入群欢迎、防撤回。"""
        ntype = str(event.get("notice_type") or "")
        if ntype == "group_increase":
            text = str(self.cfg.get("welcome_text") or "").strip()
            if not text:
                return
            who = str(event.get("user_id") or "")
            log.info("新人进群:%s", who)
            await self.send(
                self.target_event(event),
                [
                    {"type": "at", "data": {"qq": who}},
                    {"type": "text", "data": {"text": f" {text}"}},
                ],
            )
            return
        if ntype == "group_recall" and self.cfg.get("recall_notice", True):
            mid = str(event.get("message_id") or "")
            if not mid:
                return
            for _, m_id, who, body in reversed(self.recall_cache):
                if m_id == mid:
                    log.info("有人撤回:%s", body[:30])
                    await self.send(
                        self.target_event(event), f"「{who}」撤回了一条消息:{body}"
                    )
                    return

    # ---------- 主循环 ----------

    async def watch_stale(self) -> None:
        """盯着心跳。NapCat 每 30 秒推一次,90 秒一点动静都没有就当中断了。"""
        while True:
            await asyncio.sleep(15)
            if self.ws is not None and time.time() - self.last_msg_at > 90:
                log.warning("90 秒没收到任何心跳,连接像死了,主动重连")
                self.last_msg_at = time.time()
                try:
                    await self.ws.close()
                except Exception:  # noqa: BLE001
                    pass

    async def run(self) -> None:
        url = self.cfg["onebot_ws_url"]
        headers = {}
        token = self.cfg.get("onebot_access_token")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        headers["User-Agent"] = "qq-deepseek-bot/1.0"

        asyncio.create_task(self.watch_stale())
        asyncio.create_task(self.reminder_loop())
        asyncio.create_task(self.proactive_loop())
        tries = 0
        while True:
            try:
                async with websockets.connect(
                    url,
                    additional_headers=headers,
                    max_size=None,
                    # 关键:NapCat 不回 pong,库自带的心跳会把好端端的连接掐掉,
                    # 表现就是每 40 秒断一次。关掉它,改用上面的 watch_stale
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=5,
                ) as ws:
                    self.ws = ws
                    self.last_msg_at = time.time()
                    tries = 0
                    log.info("已连接 OneBot:%s", url)
                    async for raw in ws:
                        self.last_msg_at = time.time()
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(data, dict):
                            continue
                        # 接口回包(call_api_wait 在等),别当事件处理
                        echo = data.get("echo")
                        if echo and "post_type" not in data:
                            future = self.pending.get(echo)
                            if future and not future.done():
                                future.set_result(data)
                            continue
                        if data.get("post_type") == "message":
                            asyncio.create_task(self.on_event(data))
                        elif data.get("post_type") == "notice":
                            asyncio.create_task(self.on_notice(data))
            except Exception as exc:  # noqa: BLE001
                self.ws = None
                self.last_msg_at = time.time()
                tries += 1
                log.warning("与 OneBot 的连接断开(%s),5 秒后重连(第 %d 次)", exc, tries)
                await asyncio.sleep(5)


async def check_api(cfg: dict[str, Any]) -> None:
    """只验证 DeepSeek Key 是否可用。"""
    url = cfg.get("deepseek_base_url", "https://api.deepseek.com").rstrip("/")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            url + "/chat/completions",
            headers={
                "Authorization": f"Bearer {cfg['deepseek_api_key']}",
                "Content-Type": "application/json",
            },
            json={
                "model": cfg.get("model", "deepseek-chat"),
                "messages": [{"role": "user", "content": "只回复两个字:正常"}],
                "stream": False,
            },
        )
    if resp.status_code == 200:
        print("API 正常,回复:", resp.json()["choices"][0]["message"]["content"])
    else:
        print(f"API 异常 {resp.status_code}:", resp.text[:300])


def pid_alive(pid: int) -> bool:
    """这个 pid 还活着吗(只管有没有,不碰它)。"""
    try:
        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    except Exception:
        return False


def other_instance() -> int | None:
    """已经有另一个小鲸鱼在跑就返回它的 pid。

    不然两个实例同时接 OneBot,群里一句话会被回两遍。
    """
    try:
        pid = int(LOCK_FILE.read_text("utf-8").strip())
    except Exception:
        return None
    return pid if pid != os.getpid() and pid_alive(pid) else None


def main() -> None:
    cfg = load_config()
    if "--check" in sys.argv:
        asyncio.run(check_api(cfg))
        return
    running = other_instance()
    if running is not None:
        log.error("已经有一个小鲸鱼在跑了(PID %d),这次就不启动了,免得群里一句话回两遍", running)
        return
    LOCK_FILE.write_text(str(os.getpid()), "utf-8")
    try:
        asyncio.run(Bot(cfg).run())
    except KeyboardInterrupt:
        log.info("已退出")
    finally:
        LOCK_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
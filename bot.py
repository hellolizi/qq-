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
import math
import mimetypes
import os
import random
import re
import sys
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta
from io import BytesIO
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
    "不适合配图(观点、是非、计算类问题)→ `[IMG] 无`。\n"
    "【答案要拿搜到的东西核对过再说】只讲搜到的内容,别拿记忆里的旧信息补;"
    "搜到的东西互相打架、或者只搜到旧消息,就如实说一声(「网上说法不一致」"
    "「只搜到旧的」),别硬凑一个结论;真没搜到就直说没查到。"
    "【先看发布时间,只拿最新的说事】资料里带日期的挑最近那条当现状,"
    "明显的旧日期(去年、上个月、往届赛事)别当现在讲——那件事可能早结束了"
    "(世界杯早落幕了就别还讲谁踢谁);只有旧消息就说「搜到的都是旧消息,可能过时了」。"
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
    "9. 说话必须带口癖:每句话句尾都挂一个「喵」,每个断句后面再挂一个颜文字"
    "(像 (≧▽≦) (´・ω・`) 这种)。闲聊、正经回答、报错、日报一律照挂,不用省;"
    "emoji 反而要克制。\n"
    "10. 群聊里别人说的话前面带发言人昵称(格式「昵称: 内容」),"
    "你自己叫「小鲸鱼」,但你回话的时候不要在自己前面加「小鲸鱼:」,直接说内容就行。"
    "要分清谁是谁,别把张三说的话记到李四头上;"
    "顺着话头接的时候,必要时可以带一下对方名字。\n"
    "11. 记住前面聊过什么。别人说「那个」「刚才说的」「再来一个」这种指代,"
    "要回到上文里找指代对象,不要每次都当成全新话题。\n"
    "12. 群里之前聊的只是背景。别人现在问什么就答什么,"
    "别把上一轮的旧话题(比如刚聊过的踢人/管理员)硬接过来;"
    "别人只是叫你一声、没说别的事,就简单应一句,别自己扯开。\n"
    "13. 认账。别人说「你刚才不是说……吗」「你自己说的」时,先看两条:"
    "上下文里你自己的发言,以及【你自己最近说过的话】那一块(那都是你说出口的);"
    "有就承认(「欸对 我说过」「是我说的」),不许反过来说「我没说过」;"
    "都没有、你也确实想不起来,就含糊点(「我记不太清了」),别一口咬定自己没说过。\n"
    "14. 别人消息里带「(引用 某某: …)」的,那是他引用的旧消息,"
    "不是他现在说的话;他现在想说的只是引用后面的那几句。\n"
    "15. 同一个人的游戏 ID、真名、昵称、外号都很常见,别因为叫法不一样就说"
    "「这是两个人」(比如 ZMJJKK 和康康就是同一个人)。拿不准谁是谁、或者"
    "谁跟谁是不是一个人,先联网查一句再答,别硬猜;查不到就老实说不知道。\n"
    "16. 别人问「他现在还……吗」「他今年多大了」这类**关于某个人近况**的问题时,"
    "别拿脑子里的旧印象答 —— 那几个数字(年龄、成绩、还在不在队里)每年都在变。"
    "拿不准就先搜,搜不到就说「我不确定啊,得查一下」。\n"
    "17. 别老拿「？」「怎么了？」「嗯？」这种没内容的话糊弄人:对方重复发同一件事、"
    "又把同一句/同一张图发一遍、或者又戳你一下,意思多半跟刚才一样 —— 就按同一件事回,"
    "或者干脆不吭声,别一遍遍反问。只有他发的东西你是真看不懂(一串标点、一串乱码、"
    "没头没尾半句话),才回一句「？」或者「怎么了」。\n"
    "18. 用户要「表情包」「表情」「梗图」(例如「发张表情包」「来张委屈的表情包」)时,"
    "是让你**从【能发的表情包】清单里挑一张现成的发**,不是让你画一张:"
    "照清单的规矩在回复最后另起一行写 `[STICKER] 文件名`,不要在正文里输出 `[DRAW]` "
    "或 `[FIND]`,也别上网找、更别生图。他点名要哪个角色/哪种情绪的表情包,"
    "先在清单里找对得上;清单里实在没有那样儿的,才如实说库里没有这种。"
)


# 文字聊天给模型挂的工具。她不用谁打斜杠命令 —— 想查什么自己决定要不要调。
# 只挂在这条主聊天上;日报、冒泡、偷表情包那些内部小调用不带,省 token。
# 注意:踢人 / 禁言这种拉不回来的动作永远不在这里,那条只走硬规则。
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "check_balance",
            "description": "查这个号还剩多少钱(官网余额 + 阿里云现金/券)",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "查某地现在的天气。不给城市就用默认城市",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": {"type": "string", "description": "城市名,比如 杭州"}
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_hot",
            "description": "看百度实时热搜榜(全网热点,不是本群在聊的东西)",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "group_topic",
            "description": "看本群最近反复在聊什么话题(群里自己的常聊话题)。"
            "问「群里有什么热点话题 / 最近在聊啥」就用这个,不是百度全网热搜",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "group_stats",
            "description": "看本群今天聊了多少条、谁最活跃",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "联网查最新的、或者你不知道的事",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string", "description": "要查的问题"}},
                "required": ["query"],
            },
        },
    },
]

# 代码替她先搜过的那些,就别再挂 web_search 工具了:
# 快模型不一定理「别再搜一遍」那句提示,自己又搜一次就是白等十几秒。
TOOLS_NO_SEARCH = [t for t in TOOLS if t["function"]["name"] != "web_search"]

# 挂了工具时补一句,让她知道这些能自己办,别嘴上说「我查不了」
NATURAL_HINT = (
    "\n\n【你自己的本事(需要时直接调工具拿结果,别嘴上说「我查不了」)】\n"
    "查钱/还剩多少→check_balance;查天气→get_weather;看百度全网热搜→get_hot;"
    "看本群今天活跃度→group_stats;看本群最近爱聊什么话题→group_topic(群里的热点,不是全网);"
    "要最新或你自己拿不准的事→web_search。\n"
    "【哪些事必须联网查,不许凭记忆答】\n"
    "你的知识有截止时间,凡是带「现在 / 最新 / 最近 / 出了没 / 什么时候出 / 什么版本」的事实问题,"
    "一律先 web_search 再开口,比如:\n"
    "游戏 / 动漫 / 影视 / 软件 / 模型 出没出、什么版本、最新改动、角色强度;"
    "比赛结果(谁赢、谁能赢、谁夺冠、比分、几比几、今天打没打、几点开打)、谁转会、股价、票价;"
    "新品发布、政策、价格变动这类新闻。\n"
    "拿不准就去搜;搜不到就说「我搜了下没找到准的」或者「这我不确定」,"
    "**绝对不许凭记忆断言「还没出」「不存在」「没这回事」**——"
    "你不知道的东西,很可能是你训练数据之后才出的。\n"
    "普通闲聊、常识、写代码这些不用搜,别什么都去查一遍。\n"
    "拿到结果以后用你自己的口气说一两句,别把原始结果原样复述一遍。\n"
)

# 模型的知识有截止时间。把「今天」告诉它,它才分得清哪些事超出记忆、必须联网
TODAY_TPL = (
    "\n\n【今天是 {date}】你的训练数据比这天早,"
    "所以「最近 / 最新 / 出了没」这类问题,先搜再答,别拿记忆当事实。\n"
)

# 光靠模型自己想起来「该联网」不保险(快模型经常不开工具就凭记忆答),
# 所以代码替它认一遍:这句是不是时效性问题。认出来就先搜,把资料塞进上下文。
# 赛事名要单独列:「世界杯」三个字里没有「比赛 / 谁赢」这些词,以前压根不认,
# 结果有人问世界杯它就拿旧闻答,赛事早就结束了还在讲。
FRESH_RE = re.compile(
    r"(比赛|赛事|比分|几比几|谁赢|谁能赢|谁夺冠|夺冠|半决赛|决赛|"
    r"最新|出了没|出没出|还没出|发售|上映|转会|股价|票价|"
    r"几点开|几点开始|什么时候出|什么时候开|版本更新|"
    r"世界杯|欧洲杯|亚洲杯|美洲杯|奥运会|奥运|冬奥|亚运会|世预赛|"
    r"NBA|欧冠|欧联|中超|亚冠|英超|西甲|德甲|意甲|法甲|季后赛|总决赛|常规赛|"
    r"大选|选举|公投|"
    # 「谁强 / 谁厉害」这种比高下的问题,答案每年都变,别凭印象答
    # (踩过:有人问「zmjjkk 和 kangkang 谁强」,她拿旧印象当成两个人分别点评)
    r"谁强|谁更强|谁厉害|谁更厉害|谁牛|谁更牛|"
    # 「现在…厉害吗 / 今年多大」这类问某个人近况的,年龄和状态一直在变
    # (踩过:老詹都 40 了,她还说 39)
    r"退役|复出|转会|"
    r"现在.{0,8}(厉害|强不强|怎么样|咋样|怎样|状态|退役|复出|多大|几岁|还在|在不在)|"
    r"今年.{0,8}(多大|几岁|退役|复出|还在|怎么样|咋样|厉害))"
)

# 时效问题还得是「在问事」的,才算数。上面那一堆词只是「可能跟时间有关」,
# 光命中就把整句话搜一遍的话,群里随口一句「我进决赛了」「票价好贵」也会被搜,
# 又慢又费钱。这里再认一遍:带问号,或者带「谁/哪/几/多少/什么时候/吗/呢」这种问句腔。
ASK_RE = re.compile(
    r"[?？]|谁|哪|几|多少|多大|几岁|什么时候|几点|是不是|有没有|咋|怎么|为什么|为啥|"
    r"吗|呢|最新|出了没|出没出|还没出"
)

# 发给搜索模型的要求。以前只甩一句用户原话,搜索模型不知道「现在」是几号,
# 逮着几年前那届世界杯的旧网页就当有效结果返回。
# 把「今天几号」和「只看最近发布的、过期的排掉」写进 system,让它在服务端就按时间筛。
SEARCH_ASK_SYS = (
    "你是一个联网搜索助手,今天是 {date}。\n"
    "只采用最近发布的内容,优先最新的来源;往年旧闻、已经结束或者停更的事,"
    "一律当过期信息排掉,别拿它描述现状。\n"
    "【写短点,别啰嗦】先一句话给结论(60 字以内,能一行说完最好),"
    "再列 2 条来源,每条都标上发布时间(比如「2026年7月20日」)。\n"
    "【最后另起一行,只写一个词】\n"
    "两条来源说法一样 → 写「一致」;\n"
    "说法不一样 → 写「不一致」,并点一句分歧在哪;\n"
    "实在只搜到一条来源 → 写「单源」;\n"
    "搜到的全是旧消息、没有更新的 → 写「只搜到旧消息」。\n"
    "另外:要是搜索结果本身是个梗 / 流行语(比如群里有人提了你不认识的词,"
    "一搜发现是个梗),就顺带说一句它出自哪儿、平时怎么用。\n"
)

# 来源打架时不重问一遍同样的问题,换一批来源找;要求也换成「专门核对分歧」。
SEARCH_ASK_SYS_B = (
    "你是一个联网搜索助手,今天是 {date}。\n"
    "上一个搜索给出的两条来源说法不一致,请换一批来源(换个说法查、找官方或更权威的)"
    "重新核对,重点把分歧点说清楚。\n"
    "只采用最近发布的内容;写短点:一句话结论(60 字以内)+ 2 条来源,每条标发布时间。"
)

# 搜索模型自己标的「不一致 / 单源 / 只搜到旧消息」。命中就说明没搜到能互相印证的结论,
# 这才值得再补搜一趟 —— 一致的话一次就收工,别白等第二次。
CONFLICT_RE = re.compile(r"不一致|单源|只搜到旧消息")

# 群里最近在聊什么,拿它给搜索定个语境。没有语境的话,问题里的名字/缩写会被按字面
# 当成别的领域的人 —— 踩过:群里一直在聊无畏契约,问「ZMJJKK 和 LUKXO 谁厉害」,
# 搜索模型却匹配到足球、网球运动员头上。这里要求它先按群里的话题去搜,
# 这条线索搜不到再放宽重搜,但绝不能换成同名的其他人来充数。
SEARCH_TOPIC_TPL = (
    "\n【语境】群里最近在聊:{topic}\n"
    "问题里出现的名字、缩写,很可能就是这段语境下的人或角色,"
    "请优先按这个话题去搜(比如把话题关键词一起带上),"
    "不要按字面去匹配同名的运动员、明星等其他领域的人。\n"
    "先用话题相关的关键词搜;这条线索确实搜不到,再换一个更宽的说法重搜一遍;"
    "还是搜不到就如实说「没搜到」,不许拿别的领域同名的人顶上。\n"
)

# ---- 常聊话题预热 ----
# 群里反复出现的词,就是这个群「爱聊什么」。攒够了就提前替他们搜好存着,
# 下次聊到这个话题直接拿现成的资料答 —— 不用当场联网干等十几秒(这就是「回答很慢」的根源)。
TOPIC_CACHE_FILE = BASE_DIR / "topic_cache.json"
TOPIC_HOT_MIN = 3  # 至少几条消息里都出现,才算常聊话题(免得把随口一句当成话题)
TOPIC_MAX = 3  # 每个会话最多跟踪几个话题
# 话题越老搜得越稀的衰减指数:hours = 最新频率 × (活了几天 + 1) ^ 0.43
# 0.43 是配着来的 —— 刚出来(0 天)正好一天一次,活到 90 天(约三个月)正好顶到一周一次
TOPIC_AGE_POW = 0.43
TOPIC_TERM_RE = re.compile(r"[\u4e00-\u9fff]+|[A-Za-z0-9_]{3,}")

# 「群里最近都在聊什么 / 有什么热点话题」这类问题,代码直接拿常聊话题答,
# 不指望快模型自己想起来调工具(它经常想不起来)。只认明显在问「群里的」话题,
# 不带「群」的「热点」不算 —— 那是全网热搜,走 get_hot,别混一起。
TOPIC_Q_RE = re.compile(
    r"(热点话题|热门话题|话题榜|常聊|群里.*话题|都在聊什么|都在聊啥|聊什么话题|什么话题)"
)
# 这些词天天见、遍地都是,不算话题,免得拿去搜白花钱
TOPIC_STOPWORDS = {
    "你们", "我们", "他们", "什么", "这个", "那个", "就是", "可以", "不是", "没有",
    "这么", "那么", "怎么", "因为", "所以", "现在", "已经", "一个", "一下", "真的",
    "哈哈", "笑死", "自己", "知道", "觉得", "应该", "还是", "只是", "但是", "而且",
    "时候", "东西", "事情", "问题", "为啥", "为什么", "有没有", "怎么样", "什么样",
    # 纯口头语 / 时间词:天天出现却不是话题 —— 混进「常聊话题」既难看又白费一次搜索
    "好的", "嗯嗯", "哦哦", "行吧", "没事", "确实", "有点", "好像", "感觉", "大家",
    "今天", "明天", "昨天", "早上", "晚上",
}
# 提前搜好的话题资料,当作背景摆给她 —— 让她知道群里在聊什么,聊到时不必再搜
TOPIC_BRIEF_TPL = (
    "\n【你之前替群里查过的资料】群里最近常聊这些话题,你已经提前搜过、资料在这儿:\n"
    "{items}\n"
    "(聊到这些话题就直接用上面的,别再说「我去搜一下」;上面没有的就直说没查到,别硬编。)\n"
)

# ---- 网上的梗 ----
# 群里说什么梗、网上最近流行什么,她未必认识。这里每天上网扒一批最近流行的梗存着,
# 说话时她自己判断能不能自然用上 —— 能就顺口来一句,对不上就绝不硬塞。
# 梗只从网上来,不从群聊里抠(群聊里抠出来的多是错的、或者过两天就没人懂了)。
MEME_CACHE_FILE = BASE_DIR / "meme_cache.json"
MEME_MAX = 8  # 每次注入几条给聊天模型
MEME_FORGET_DAYS = 30  # 梗也有保质期:没人提超过这么多天就删
MEME_FEEDBACK_MSGS = 3  # 她用了梗之后,收到几条群消息就分析一下大家感不感冒

# 让它上网查梗时用的要求:只回 JSON 数组,好解析;只要短句,别要长篇段子。
MEME_ASK_SYS = (
    "你是一个网络流行语助手,今天是 {date}。\n"
    "请联网查一下最近(近一两个月)网上流行的梗、热梗、流行语,"
    "以及常见的「评论区句式」(别人爱在评论里跟着刷的那种短句)。\n"
    "只挑日常聊天里真能拿来用的短句,一句话那种;"
    "长篇段子、过时好几年的老梗、需要长篇背景才懂的,都不要。\n"
    "【只输出一个 JSON 数组,不要任何别的字】每个元素形如:\n"
    '{"phrase":"梗的短句原话","origin":"出处(哪来的、什么事件/剧集/主播/热点)","usage":"什么场合能拿来用、怎么用"}\n'
    "最多 10 条;实在没查到就输出 []。"
)

# 把攒下的梗当背景摆给她 —— 关键是「能自然结合才用、不能就别用」,
# 免得她把梗硬往不相干的话上贴,又尬又出戏。也别说破「我在玩梗」。
MEME_TPL = (
    "\n【最近网上的梗(只是备用)】这些是最近流行的梗 / 流行语 / 评论区句式,记在心里:\n"
    "{items}\n"
    "用法:只有当话题、气氛正好对得上,才把它顺口揉进你要说的话里,像平时聊天一样自然;"
    "对不上就别提,宁可用自己的话说 —— 硬凑的梗又尬又出戏,不如不用。"
    "也别特意解释「我在玩梗」,更别照搬整句,用自己的口气带出来就行。\n"
)

# 她用了梗之后,读群里接下来这几句反应,判断大家对这梗感不感冒。
# 判断要保守:「没人接话」不算不感冒,只有明确看出无感 / 尴尬 / 吐槽 / 岔开才算 ——
# 宁可留着一个她以为大家爱听的梗,也别因为一次冷场就误杀。
MEME_FEEDBACK_ASK = (
    "群里一个机器人刚主动用了个梗:「{phrase}」。群里接下来的反应是:\n{chat}\n"
    "请判断大家对这梗的态度。注意:只是没人接话、或者接着聊别的事去了,那只是没反应,不算不喜欢;"
    "只有从字里行间能看出「没意思 / 尴尬 / 过时 / 别说了」这类明确的无感或反感,才算不喜欢。\n"
    "只回答一个词:感冒 或 不感冒。"
)

# 代码先搜好、直接喂给聊天模型的资料。让它据此回答,免得快模型凭记忆下结论。
WEB_NOTE_TPL = (
    "\n\n【代码刚替你联网查过了,这是查到的资料】\n{note}\n"
    "回答以这些资料为准,别拿记忆里的旧信息下结论;"
    "资料里没有、或者写着没搜到,就直说没查到,别硬编一个结论出来。"
    "【先看发布时间,只拿最新的说事】资料里带日期的,挑最近的那条当现状;"
    "明显是旧日期的(去年、上个月、往届赛事)就别当现在讲 —— "
    "那件事可能早就结束了(比如世界杯早落幕了,别还在讲谁踢谁);"
    "要是只有旧消息,就如实说一句「搜到的都是旧消息,可能已经过时了」。"
    "这份资料就是刚查的,别再多此一举自己再搜一遍(那是白等十几秒)。\n"
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

# 你自己最近开口说的话。单拎出来是因为别人追问「你刚不是说 XX 吗」时,
# 光靠短期上下文不稳:聊几句就被刷掉了,重启一次更是全没了。
SAID_TPL = (
    "\n\n【你自己最近说过的话(这些就是你说出口的,别否认)】\n{items}\n"
    "别人追问「你刚才不是说……」时以这里为准:确实说过就认账,"
    "这儿没有、你也想不起来,才说记不太清,别一口咬定自己没说过。\n"
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


# 「发张表情包」这种要求,本意是「从本地库里挑一张现成的」,而不是「现画一张」。
# 但快模型经常理解成「给我做一张表情包」,于是吐 [DRAW] 去生图 —— 又慢又费钱
# (实测 37~53 秒一次,还按张计费),群里等半天还以为是卡了。
# 提示词已经在系统约定第 18 条说清了,这里再兜一道:命中「表情包/表情/梗图」
# 且没同时说要「画/做/合成」的,就归到本地库这条路上。
STICKER_ASK_RE = re.compile(r"表情包|表情|梗图")
DRAW_WORD_RE = re.compile(r"画|绘制|生成|做|合成|拼|p图|P图|PS")


def wants_local_sticker(text: str) -> bool:
    """这句是在要「发一张表情包」吗(而且要的不是「画/做一张表情包」)。"""
    t = str(text or "").strip()
    if not t or not STICKER_ASK_RE.search(t):
        return False
    return not DRAW_WORD_RE.search(t)


def split_draw_marker(text: str) -> tuple[str, bool]:
    """找出 `[DRAW]` 标记,返回(标记之后的画面描述, 是否要生图)。

    模型偶尔会在标记前先说一句废话(「欸?让我康康……」),
    所以这里用搜索而不是看开头,只要出现标记就当要生图。
    """
    match = re.search(r"`?\[DRAW\][ \t]*`?", text)
    if not match:
        return "", False
    return text[match.end():].strip().strip("`").strip(), True


def guess_image_mime(raw: bytes) -> str:
    """按文件头猜 mime。"""
    if raw[:4] == b"\x89PNG":
        return "image/png"
    if raw[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        return "image/webp"
    if raw[:3] == b"GIF":
        return "image/gif"
    return "image/jpeg"


def bytes_to_data_url(raw: bytes) -> str:
    """按文件头猜 mime,拼成 data URL。"""
    return f"data:{guess_image_mime(raw)};base64," + base64.b64encode(raw).decode()


# 识图前先把图处理一下:长边超过这个像素就缩,再统一转成 JPEG。
# 识图按像素多少算钱、也按上传体积等时间 —— GIF 动图压完只剩零头(只留第一帧),
# 长边 1280 也是个平衡:截图里的报错文字照样看得清,又比原图少一大截像素。
# 嫌贵就调小(比如 896),代价是截图里的小字可能糊。
VISION_MAX_EDGE = 1280
VISION_JPEG_QUALITY = 85
VISION_CACHE_HOURS = 168  # 同一张图的识图结论留 7 天,期间再发不用重新看
VISION_CACHE_MAX = 500  # 最多留多少个指纹,超了扔最旧的


def shrink_image_for_vision(
    raw: bytes, max_edge: int = VISION_MAX_EDGE, quality: int = VISION_JPEG_QUALITY
) -> tuple[bytes, str]:
    """把要送去识图的图压小,返回(字节, mime)。

    GIF 动图只取第一帧(识图模型本来也只能看一帧),带透明通道的先垫白底(JPEG 没 alpha);
    长边超 max_edge 再用 LANCZOS 缩下来。压完反而更大(小图)就原样留着,别白折腾。
    没装 Pillow 或者格式太怪,一律原样返回 —— 压缩这条路坏了也不能把识图带崩。
    """
    try:
        from PIL import Image  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        return raw, guess_image_mime(raw)
    try:
        im = Image.open(BytesIO(raw))
        if getattr(im, "is_animated", False):
            im.seek(0)
        if "A" in im.getbands():
            im = im.convert("RGBA")
            bg = Image.new("RGB", im.size, (255, 255, 255))
            bg.paste(im, mask=im.split()[-1])
            im = bg
        elif im.mode != "RGB":
            im = im.convert("RGB")
        w, h = im.size
        edge = max(w, h)
        if edge > max_edge:
            scale = max_edge / float(edge)
            im = im.resize(
                (max(1, round(w * scale)), max(1, round(h * scale))), Image.LANCZOS
            )
        out = BytesIO()
        im.save(out, "JPEG", quality=quality, optimize=True)
        small = out.getvalue()
        if not small or len(small) >= len(raw):
            return raw, guess_image_mime(raw)
        return small, "image/jpeg"
    except Exception as exc:  # noqa: BLE001
        log.debug("识图前压缩没成功,用原图:%s", exc)
        return raw, guess_image_mime(raw)


BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

# ---------- 群功能用到的常量 ----------

DATA_FILE = BASE_DIR / "data.json"  # 积分 / 签到 / 提醒的小存档
MEMORY_FILE = BASE_DIR / "memory.json"  # 长期记忆:聊天存档
WEB_CACHE_FILE = BASE_DIR / "search_cache.json"  # 搜过的东西攒着,同一个问题不用反复联网
VISION_CACHE_FILE = BASE_DIR / "vision_cache.json"  # 识过的图攒着,同一张图不用反复看
# 她自己生图产物的指纹:这些图绝不能再被当成「表情包」收进库(见 steal_stickers)
GEN_HASH_FILE = BASE_DIR / "gen_hashes.json"

# 偷表情包:让识图模型仔细判断这张图是不是「聊天用的表情包」,并说清什么时候用。
# 判断标准写细一点:宁可漏收,也别把照片/截图/商品图收进库里。
STEAL_PROMPT = (
    "仔细看这张图,判断它是不是聊天里拿来发的那种「表情包 / 梗图」。\n"
    "算:卡通形象、夸张表情、配文字的搞笑图,用来表达情绪、玩梗、怼人的。\n"
    "不算:真人照片、风景、纯截图(聊天记录、网页、APP 界面、带聊天气泡或相框的)、"
    "商品图、科普图、二维码、logo。\n"
    "拿不准就当不是,别硬认。\n"
    "想清楚再回,只回一行 JSON,不要任何解释:\n"
    '{"meme":true,"confidence":0.0,"emotion":"核心情绪,4字内",'
    '"scene":"什么情况/什么时候会发它,20字内","tags":["只写你百分百确定的标签"]}\n'
    "tags 只写你**百分百确定**、图上真能看出来、而且确实能当检索词用的。"
    "只要有一点拿不准,那个标签就别写 —— 宁可少,写错才要命:"
    "这些标签是用来判断「库里有没有同款」的,错标签会让库里塞满重复的图,也挑不准图。\n"
    "别凑数(没有就不写、一个也行)、别写同义词堆量、别猜、"
    "别把「搞笑」「表情包」「图片」这种谁都对得上的废话写进去。\n"
    "确定不是表情包就回:{\"meme\":false}\n"
    "confidence 是 0.0~1.0,你有多确信它是表情包,不确定就往低了打。"
)
IMAGE_EXT = {
    "image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png",
    "image/gif": ".gif", "image/webp": ".webp", "image/bmp": ".bmp",
}
CJK_RE = re.compile(r"[\u4e00-\u9fff]")

# 只 @ 她一下 / 叫一声、或者只甩一串「？？？」这种没实内容的戳,她随口应一句就完。
# 但同一个人短时间里反复戳,那是在逗她 / 习惯性提醒,不是每次都真想问「怎么了」——
# 第一次应一声,再来就先不吭声,不然她就成了复读「怎么了？」的机器。
EMPTY_POKE_REPEAT_SECONDS = 300

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

VERSION = "2.4.0"  # 发布版本号,改了记得打个 tag(仓库根目录 VERSION 文件也要同步)

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
    "/余额      看还剩多少钱\n"
    "/撤回      撤自己刚发的一条(引用某条再说「撤回」就撤那条)\n"
    "/表情 [标签]  来张表情包\n"
    "/画 描述   让鲸鲸画一张\n"
    "/搜 问题   联网查\n"
    "/重置      清空对话记忆\n"
    "群里 @ 我 或直接聊天也行\n"
    f"小鲸鱼 v{VERSION}"
)

COMMANDS = {
    "帮助", "help", "菜单", "骰", "骰子", "抽签", "运势", "签到", "打卡", "积分",
    "排行榜", "榜", "猜数字", "猜", "成语接龙", "接龙", "结束", "提醒", "提醒列表",
    "取消提醒", "天气", "热搜", "统计", "余额", "额度", "禁言", "解除禁言", "全体禁言",
    "踢", "踢人", "版本", "撤回", "撤销",
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
# 「把这个人踢了」「踢了他」这种口语也得认,不然没人老实打 /踢 @某人
# 只认汉字「踢」:之前把英文 T/t 也算进去,结果「你不用 at 了」里的 t 被当成了踢
KICK_RE = re.compile(r"(?:把|将|给)[^，。!？?\s]{0,12}?踢|踢(?:出去|掉|走|了)")
# 但在聊踢球的群里,「把球踢飞了」也是这个句式,别误认
KICK_STOP = ("踢球", "足球", "踢毽", "踢腿", "踢踏", "踢飞", "球踢")
# 问「你会啥」的时候得答,不能只认「帮助」「菜单」那两个字
HELP_WORDS = (
    "命令", "会啥", "会什么", "会做什么", "能干啥", "能干嘛", "能做什么",
    "什么功能", "有啥功能", "哪些功能", "帮助", "菜单",
)
# 问它「你什么版本」「小鲸鱼几版了」得能报出来。
# 只认明确的问法,免得「这个版本不行」这种闲聊被当成命令。
VERSION_WORDS = (
    "什么版本", "什么版", "几版本", "几版", "哪个版本", "哪版", "哪一版",
    "啥版本", "啥版", "多少版本", "版本多少", "版本是啥", "版本号多少",
    "版本是多少", "报版本",
)


def strip_at_head(text: str) -> str:
    """去掉开头连着写的 @某某(有的客户端把 @ 并进文字里),
    这样「@小鲸鱼 /踢 @某人」也能认出那个 /踢。"""
    return re.sub(r"^(?:@\S+\s*|\[CQ:at,[^\]]*\]\s*)+", "", text.strip()).strip()


# 两张表情包的标签撞到这个比例以上,就算「同一个使用场合」,只留一张(省地方)
STICKER_TAG_SIMILAR = 0.6


def tag_key(text: str) -> str:
    """把情绪/标签归一化,好比对两张图是不是「同一种情况」。"""
    return re.sub(r"[\s,，。.!！?？~、·\-_/\\()（）]+", "", str(text or "")).lower()


def natural_command(text: str) -> str:
    """把口语说法认成命令,认不出来就返回空。"""
    body = re.sub(r"[\s,，。!！?？~、]+", "", text.strip().lstrip("/"))
    if not body or len(body) > 20:
        return ""
    if any(w in body for w in NATURAL_STOP):
        return ""
    if KICK_RE.search(body) and not any(w in body for w in KICK_STOP):
        return "踢"
    if any(w in body for w in VERSION_WORDS):
        return "版本"
    if any(w in body for w in HELP_WORDS):
        return "帮助"
    for kw, cmd in NATURAL_CMDS:
        if kw in body:
            return cmd
    return ""


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


# 她说话的两条死规矩:每句话句尾挂「喵」,每个断句再挂一个颜文字。
# 人设提示词里已经这么要求了,这里再兜一道 —— 快模型偶尔会忘,而这是她要的效果。
NYAN = "喵"
KAOMOJI_POOL = (
    "(≧▽≦)", "(´・ω・`)", "(๑•̀ㅂ•́)و✧", "(￣▽￣)", "(๑•́ ₃ •̀๑)",
    "(｡･ω･｡)", "(＾▽＾)", "(=^･ω･^=)", "٩(ˊᗜˋ)و", "(；´Д｀)",
    "(*・ω・)", "(´∀｀)", "(・ω・)", "(๑>؂<๑)",
)
# 认「这句里已经有颜文字了」用这些字符,不用真去解析括号
KAOMOJI_CHARS = set("´`ˋˊω・∀≧≦▽△￣＾∇゚ﾟ°·｡✧୧ᗜᐛᴗヮᵕ๑و٩εσヘノつづღ♡")
# 句尾已经挂着的颜文字单独摘出来,好把「喵」插在它前面
TAIL_KAO_RE = re.compile(r"[（(【〔][^)）】〕\n]{0,14}[)）】〕]\s*$")
# 断句:句末标点和换行都算
SENT_END_RE = re.compile(r"[。！？!?…～~]+")
_LAST_KAOMOJI = [""]


def pick_kaomoji() -> str:
    """挑一个颜文字。同一个不连着用两次 —— 连着两句挂得一模一样就露馅了。"""
    pool = [k for k in KAOMOJI_POOL if k != _LAST_KAOMOJI[0]] or list(KAOMOJI_POOL)
    _LAST_KAOMOJI[0] = random.choice(pool)
    return _LAST_KAOMOJI[0]


def _decorate_clause(chunk: str) -> str:
    """给一个断句挂上「喵」和颜文字。已经有喵、已经有颜文字的都不重复挂。"""
    body = chunk.rstrip()
    if not body:
        return chunk
    kao = ""
    m = TAIL_KAO_RE.search(body)
    if m:
        kao = m.group(0).strip()
        body = body[: m.start()].rstrip()
        if not body:  # 整句就是个颜文字,那就原样发
            return chunk
    # 句末标点先摘掉(「……」也一并摘),问号叹号留着挂到喵后面
    m = SENT_END_RE.search(body)
    if m and m.end() == len(body):
        tail = m.group(0)
        keep = "?" if ("?" in tail or "？" in tail) else ("!" if ("!" in tail or "！" in tail) else "")
        body = body[: m.start()].rstrip()
    else:
        keep = ""
    if not body:
        return chunk
    if not body.endswith(NYAN):
        body += NYAN
    out = body + keep
    if not kao and not any(c in KAOMOJI_CHARS for c in out):
        kao = pick_kaomoji()
    return out + (" " + kao if kao else "")


def decorate_speech(text: str) -> str:
    """把要说的话按断句挂上「喵」+ 颜文字。一行算一句,行内再按句末标点断。"""
    lines: list[str] = []
    for line in str(text).split("\n"):
        if not line.strip():
            lines.append(line)
            continue
        pieces: list[str] = []
        start = 0
        for m in SENT_END_RE.finditer(line):
            # 标点后面还有字才算一个断句,行尾那个标点留在本句里
            if line[m.end():].strip():
                pieces.append(line[start: m.end()])
                start = m.end()
        pieces.append(line[start:])
        lines.append("".join(_decorate_clause(p) if p.strip() else p for p in pieces))
    return "\n".join(lines)


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


# 长期记忆每条存多长。默认跟 reply_max_chars(300)对齐,再多就截断。
MEMORY_MAX_CHARS = 300
# 同一件事最多算「被提过几次」。每多提一次记性翻倍,到顶就不再加。
MEMORY_HITS_CAP = 6
# 自己说过的话记性更好(人对自己做过的事记得更牢),衰减稳定性翻倍。
SELF_SAID_BOOST = 2.0


class Memory:
    """轻量长期记忆:按会话把聊天存档,再用关键词捞回来。

    没上向量库(RAG)——那个得额外跑数据库。这里就是存文本 + 两个字两个字地比对,
    应付「上次说的那个东西」这种指代够用了。

    遗忘照着人的样子来:
      · 三天之内(full_days)说过的话一句都不许忘 —— 这段时间是完整记录;
      · 三天之前的按艾宾浩斯曲线往下掉,还活着的概率 = exp(-(过了几天-3) / 稳定性);
      · 同一件事被反复提起,稳定性翻倍 —— 这就是「多次提及的东西记得住」;
      · 自己说出口的话记性更好(人对自己做过的事记得更牢),稳定性也翻倍。
    每条存进来的时候掷一次骰子(luck),掷完就定死。所以一件旧事要么记着、要么忘了,
    不会一会儿想起来一会儿想不起来 —— 那不像人,像 bug。

    忘了的条目会真的从 memory.json 里划掉(见 prune)。以前是「读的时候过滤掉」,
    条目本身一直躺在硬盘上,文件只涨不缩 —— 那不算忘,那叫藏起来。
    """

    def __init__(
        self,
        path: Path,
        keep_days: int = 30,
        full_days: float = 3,
        decay_days: float = 7,
        self_names: set[str] | None = None,
    ) -> None:
        self.path = path
        self.keep_days = keep_days
        self.full_days = max(0.0, float(full_days))
        self.decay_days = max(1.0, float(decay_days))
        # 哪些名字算「她自己」——她说的话衰减得慢,删的时候也得按这个标准来
        self.self_names = set(self_names or set())
        self.data: dict[str, list[list[Any]]] = {}
        self.dirty = False
        self.last_save = 0.0
        try:
            raw = json.loads(path.read_text("utf-8"))
            if isinstance(raw, dict):
                self.data = raw
        except Exception:  # noqa: BLE001
            self.data = {}
        # 老存档每条只有 [时间, 谁, 说了啥],把后两格补上(骰子 + 被提过几次)
        patched = False
        for rows in self.data.values():
            for row in rows:
                if isinstance(row, list) and len(row) == 3:
                    row.extend([random.random(), 1])
                    patched = True
        if patched:
            self.dirty = True

    @staticmethod
    def _norm(text: str) -> str:
        """比「是不是同一件事」时先去掉空白,别让空格和换行搅和。"""
        return re.sub(r"\s+", "", str(text))

    def _alive(self, row: list[Any], now: float, self_boost: float = 1.0) -> bool:
        """这条还记得住吗。三天之内必在;再往前就按艾宾浩斯曲线往下掉。"""
        age = (now - float(row[0])) / 86400.0
        if age <= self.full_days:
            return True
        hits = min(max(int(row[4]), 1), MEMORY_HITS_CAP)
        stability = self.decay_days * self_boost * (2 ** (hits - 1))
        return float(row[3]) <= math.exp(-(age - self.full_days) / stability)

    def prune(self, now: float | None = None) -> int:
        """把已经想不起来的旧账真的从存档里删掉,返回删了几条。

        遗忘得发生在硬盘上 —— 只把条目留着一辈子,读的时候假装看不见,那文件只会越堆越大。
        判断标准跟捞记忆时完全一样,而且挑最宽松的那一档(自己说的话稳定性翻倍):
        一条记录只有在「所有能把它捞出来的路子」都已经捞不着了,才算真忘,才肯删。
        三天以内的记录一律不碰,所以刚聊完的东西不会被误伤。
        """
        now = time.time() if now is None else now
        gone = 0
        for key in list(self.data.keys()):
            rows = self.data[key]
            kept: list[list[Any]] = []
            for row in rows:
                # 格子不全的老数据判不了,先留着,别乱删
                if len(row) < 5:
                    kept.append(row)
                    continue
                boost = SELF_SAID_BOOST if str(row[1]) in self.self_names else 1.0
                if self._alive(row, now, boost):
                    kept.append(row)
                else:
                    gone += 1
            if len(kept) != len(rows):
                self.data[key] = kept
            if not kept:
                del self.data[key]
        if gone:
            self.dirty = True
            log.info("忘掉 %d 条已经想不起来的旧账(从 memory.json 里删掉了)", gone)
        return gone

    def add(self, session_key: str, who: str, text: str) -> None:
        text = text.strip()
        if not text:
            return
        # 太长就截一段存,不能整条扔。以前超过 200 字直接丢弃,可她自己回的话
        # 最长能到 reply_max_chars(默认 300)—— 恰恰是信息量大的长回答存不进去,
        # 回头别人问「你刚不是说…」她就真没得捞,只能死不认账。
        if len(text) > MEMORY_MAX_CHARS:
            text = text[:MEMORY_MAX_CHARS] + "…"
        rows = self.data.setdefault(session_key, [])
        # 同一件事又提了一遍 → 之前那条也涨牢度,这才是人记东西的样子
        body = self._norm(text)
        repeats = 0
        for row in rows[-1000:]:
            if len(row) < 5:
                continue
            other = self._norm(row[2])
            if other == body:
                hit = True
            elif len(other) >= 4 and len(body) >= 4:
                hit = body in other or other in body
            else:
                hit = False
            if hit:
                row[4] = min(int(row[4]) + 1, MEMORY_HITS_CAP)
                repeats += 1
        rows.append(
            [time.time(), who, text, random.random(), min(1 + repeats, MEMORY_HITS_CAP)]
        )
        cutoff = time.time() - self.keep_days * 86400
        if len(rows) > 2000 or rows[0][0] < cutoff:
            self.data[session_key] = [r for r in rows if r[0] >= cutoff][-2000:]
        self.dirty = True

    def recent_said(
        self, session_key: str, who: str, limit: int = 5, self_boost: float = SELF_SAID_BOOST
    ) -> list[str]:
        """倒着捞她自己最近说过的话。

        不受时间窗限制 —— 刚说完的恰恰最要紧,而这会儿她的话很可能已经被
        短期历史刷掉了(或者重启过一次,历史整个没了)。
        但也不是铁打的:三天之前照样会淡,只是淡得比别人说的慢(自己说的话记得更牢)。
        """
        rows = self.data.get(session_key) or []
        now = time.time()
        out: list[str] = []
        for row in reversed(rows):
            if len(row) < 5 or str(row[1]) != who:
                continue
            if not self._alive(row, now, self_boost):
                continue
            out.append(str(row[2]))
            if len(out) >= limit:
                break
        return list(reversed(out))

    def search(
        self, session_key: str, query: str, limit: int = 4, skip_recent: int = 600
    ) -> list[str]:
        """找跟这句话有关的旧聊天,返回「谁: 说了啥」。

        三天内的记录一定捞得到;再往前的那些,掷骰子时没活下来的就当想不起来了,
        压根不往外捞 —— 省得她一本正经地讲出早该忘掉的事。
        """
        rows = self.data.get(session_key) or []
        grams = {
            query[i : i + 2]
            for i in range(len(query) - 1)
            if CJK_RE.match(query[i]) and CJK_RE.match(query[i + 1])
        }
        if not grams:
            return []
        now = time.time()
        # 刚聊完的还在上下文里,不用重复喂
        cutoff = now - skip_recent
        scored: list[tuple[float, str]] = []
        for row in rows[-1000:]:
            if len(row) < 5:
                continue
            ts, who, text = float(row[0]), row[1], row[2]
            if ts > cutoff or not self._alive(row, now):
                continue
            score = sum(text.count(g) for g in grams)
            if not score:
                continue
            if now - ts <= self.full_days * 86400:
                score += 3  # 三天内的往上顶:这段时间的事不该想不起来
            score += max(min(int(row[4]), MEMORY_HITS_CAP) - 1, 0)  # 反复提过的更打眼
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
        """攒一会儿再写盘,别每条消息都写一次。写之前先把真忘掉的条目划掉。"""
        # 先清理再落盘:遗忘了的东西必须真的从硬盘上消失,不能只是读的时候假装看不见。
        self.prune()
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


# 搜索缓存的 TTL:强时效性(比分、谁赢了、最新版本)过几个小时就得重搜,
# 免得拿昨天的比分答今天的比赛;普通资料可以多存一天。
WEB_FRESH_HOURS = 6
WEB_PLAIN_HOURS = 24
WEB_CACHE_MAX = 300  # 最多留多少条,超了扔最旧的


class WebCache:
    """把搜到过的答案攒下来,同一个问题不用每次重新联网。

    每次问「VIT 和 100T 谁赢了」都要联网搜一遍,又慢又费钱 —— 而且同一个问题
    群里可能一会儿问三遍。这里按归一化后的问题存一份结果,命中就直接拿出来用。
    时效性的(比分/最新)只保留几小时,过点自动当没有、重新搜,不会拿旧答案硬答。
    """

    def __init__(
        self,
        path: Path,
        fresh_hours: float = WEB_FRESH_HOURS,
        plain_hours: float = WEB_PLAIN_HOURS,
    ) -> None:
        self.path = path
        self.fresh_ttl = max(60.0, float(fresh_hours) * 3600)
        self.plain_ttl = max(60.0, float(plain_hours) * 3600)
        self.data: dict[str, list[Any]] = {}
        self.dirty = False
        self.last_save = 0.0
        try:
            raw = json.loads(path.read_text("utf-8"))
            if isinstance(raw, dict):
                self.data = raw
        except Exception:  # noqa: BLE001
            self.data = {}

    @staticmethod
    def _key(query: str) -> str:
        """同一个问题换个问法(空格、大小写、标点)也该命中,先归一化再当 key。"""
        return re.sub(r"[\s,，。.?!？！~`'\"“”‘’、]+", "", str(query)).lower()

    def get(self, query: str) -> str:
        """命中了就返回上次搜到的原文,没有(或过期)就返回空串。"""
        row = self.data.get(self._key(query))
        if not isinstance(row, list) or len(row) < 3:
            return ""
        ts, text = float(row[0]), str(row[1])
        ttl = self.fresh_ttl if row[2] else self.plain_ttl
        if time.time() - ts > ttl:
            return ""
        return text

    def put(self, query: str, text: str, fresh: bool) -> None:
        text = str(text).strip()
        if not text:
            return  # 没搜到就别存,不然以后一直拿「没搜到」当答案
        self.data[self._key(query)] = [time.time(), text, bool(fresh)]
        if len(self.data) > WEB_CACHE_MAX:
            keep = sorted(self.data.items(), key=lambda kv: -float(kv[1][0]))[:WEB_CACHE_MAX]
            self.data = dict(keep)
        self.dirty = True

    def save(self, force: bool = False) -> None:
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
            log.warning("搜索缓存写不进去:%s", exc)


class VisionCache:
    """同一张图识过了就把结论记下来,再发一遍不用重新花钱看。

    最烧钱的是那些「不是表情包」的图(照片、截图、商品图):它们收不进库,
    所以永远进不了 known_hashes,群里每被发一次就重新识一遍 —— 又慢又费。
    这里按图片指纹存一份识图结论,命中就直接拿上次的,省一趟接口往返。
    """

    def __init__(self, path: Path, hours: float = VISION_CACHE_HOURS) -> None:
        self.path = path
        self.ttl = max(60.0, float(hours) * 3600)
        self.data: dict[str, list[Any]] = {}
        self.dirty = False
        self.last_save = 0.0
        try:
            raw = json.loads(path.read_text("utf-8"))
            if isinstance(raw, dict):
                self.data = raw
        except Exception:  # noqa: BLE001
            self.data = {}

    def get(self, digest: str) -> str:
        """命中了就返回上次识图的原文,没有(或过期)就返回空串。"""
        row = self.data.get(digest)
        if not isinstance(row, list) or len(row) < 2:
            return ""
        ts, text = float(row[0]), str(row[1])
        if time.time() - ts > self.ttl:
            del self.data[digest]
            self.dirty = True
            return ""
        return text

    def put(self, digest: str, text: str) -> None:
        text = str(text).strip()
        if not text:
            return  # 没识出东西(接口抽风/返回空)就别存,免得以后一直拿它当结论
        self.data[digest] = [time.time(), text]
        if len(self.data) > VISION_CACHE_MAX:
            keep = sorted(self.data.items(), key=lambda kv: -float(kv[1][0]))[:VISION_CACHE_MAX]
            self.data = dict(keep)
        self.dirty = True

    def save(self, force: bool = False) -> None:
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
            log.warning("识图缓存写不进去:%s", exc)


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
        self.stickers, _ = self._load_stickers()
        # 最近一次收到任何消息的时间,NapCat 每 30 秒会推心跳,拿它判断连接死没死
        self.last_msg_at = time.time()
        # 群里没点它的话,也顺手记下来当背景,不然它不知道群里在聊什么
        self.ambient: dict[str, list[str]] = defaultdict(list)
        # 「用户爱聊什么」:每个会话里反复出现的词 -> [出现过几条消息, 最后出现时间, 说过的人]
        self.topic_hits: dict[str, dict[str, list[Any]]] = defaultdict(dict)
        # 提前替群里搜好的话题资料:会话 -> 话题 -> [搜的时间, 资料原文]
        self.topic_briefs: dict[str, dict[str, list[Any]]] = defaultdict(dict)
        # 每天最多提前搜几次(烧额度有天花板)
        self.topic_prefetch: dict[str, Any] = {"day": "", "n": 0}
        self._load_topics()
        # 网上攒下的梗:每项 {"phrase","origin","usage","first_seen","last_seen"};说的时候她自己挑着用
        self.memes: list[dict[str, Any]] = []
        self.meme_last = 0.0  # 上次上网搜梗的时间(隔 meme_search_hours 才再搜)
        # 被群友「不太感冒」否掉的梗:phrase -> 拉黑时间。以后不再注入、搜到也过滤掉
        self.meme_rejected: dict[str, float] = {}
        # 她刚用了某个梗、正等群里反应:会话 -> {"phrase","t","collected"}
        self.meme_watch: dict[str, dict[str, Any]] = {}
        self._load_memes()
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
        # 长期记忆:把聊天存档,以后能捞回来。三天内一句不忘,再往前按人的样子慢慢淡,
        # 淡没了的会真从 memory.json 里删掉(不是留着只是不显示)
        self.memory = Memory(
            MEMORY_FILE,
            int(cfg.get("memory_days", 30)),
            cfg.get("memory_full_days", 3),
            cfg.get("memory_decay_days", 7),
            self_names=set(self.bot_names()),
        )
        # 搜过的东西记一份:同一个问题再来就直接用,不用重新联网(快很多)
        self.web_cache = WebCache(
            WEB_CACHE_FILE,
            cfg.get("search_cache_fresh_hours", WEB_FRESH_HOURS),
            cfg.get("search_cache_hours", WEB_PLAIN_HOURS),
        )
        # 识过的图记一份:同一张再被发进群,直接拿上次的结论,不用重新花钱看一遍
        self.vision_cache = VisionCache(
            VISION_CACHE_FILE, cfg.get("vision_cache_hours", VISION_CACHE_HOURS)
        )
        # 群统计 / 日报 / 主动消息
        self.stats: dict[str, dict[str, Any]] = {}
        self.day_log: dict[str, list[str]] = defaultdict(list)
        self.last_active: dict[str, float] = {}
        # 冷场主动冒泡的退避倍数:一直没人说话就 1→2→4…(proactive_backoff_max 封顶),有人说话归 1
        self.proactive_backoff: dict[str, int] = {}
        # 接话窗里只认「刚才跟她对话的那个人」:别人插话不算冲她说的
        self.followup_partner: dict[str, str] = {}
        # 已经靠接话窗连着回了这个人几句。到顶就停,不然她一开口就变成「群里每一条都回」
        self.followup_streak: dict[str, int] = {}
        # 没人叫它、它自己插话的时间点,免得连着搭话刷屏
        self.last_chime: dict[str, float] = {}
        # 她最近一次在群里说话的时刻,用来开「接话窗」:刚聊完那一小会儿,别人接着说的短句也理
        self.last_bot_speak: dict[str, float] = {}
        # 群成员名单的缓存(群号 -> (取的时刻, 名单)),点名踢人时要用
        self.member_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        # 刷屏检测:群号 -> 人 -> [(时刻, 说了啥)],只看最近一小会儿的
        self.spam_track: dict[str, dict[str, list[tuple[float, str]]]] = {}
        self.mute_count: dict[str, Any] = {"day": "", "n": 0}
        self.reported_day = ""
        self.steal_count: dict[str, Any] = {"day": "", "n": 0}
        self.known_hashes: set[str] = self._hash_stickers()
        # 她自己生图产出的图,指纹存这儿。这些是「画」出来的,不是表情包,
        # 绝不能被 steal_stickers 当成聊天表情收进库(见那条硬规则)
        self.gen_hashes: set[str] = self._load_gen_hashes()
        # 她自己发出去的每条消息的 message_id(按会话/群记最近若干条):要能撤回自己刚说的话。
        # 只留最近 20 条,内存不会只涨不缩
        self.bot_msgs: dict[str, deque[tuple[float, str]]] = defaultdict(
            lambda: deque(maxlen=20)
        )
        # 试收过的图指纹 -> 见过几次(steal_min_seen 想调回「见过几次才收」时用得上)
        self.seen_counts: dict[str, int] = {}
        # 今天为了偷图花了多少次识图调用。识图要钱,给个天花板兜底
        self.steal_vision: dict[str, Any] = {"day": "", "n": 0}
        # 她说过话之后,群里连着多少条没人理她(数够了就先闭嘴)
        self.silent_streak: dict[str, int] = {}
        # 谁刚戳过她(会话+人 -> 时刻)。同一个人反复空 @ 就不每次都回「怎么了」了
        self.poked_at: dict[str, float] = {}
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

    def _load_gen_hashes(self) -> set[str]:
        """读她自己生图产物的指纹存档(读不到就是空的)。"""
        try:
            raw = json.loads(GEN_HASH_FILE.read_text("utf-8"))
            if isinstance(raw, list):
                return {str(x) for x in raw}
        except Exception:  # noqa: BLE001
            pass
        return set()

    def _remember_gen_image(self, raw: bytes) -> None:
        """记下她刚生出来的这张图的指纹 —— 以后谁把它转发进群,也不会被当成表情包收走。"""
        if not raw:
            return
        digest = hashlib.md5(raw).hexdigest()
        if digest in self.gen_hashes:
            return
        self.gen_hashes.add(digest)
        try:
            tmp = GEN_HASH_FILE.with_suffix(".tmp")
            # 只留最近 500 条,文件别只涨不缩
            items = list(self.gen_hashes)[-500:]
            tmp.write_text(json.dumps(items, ensure_ascii=False), encoding="utf-8")
            tmp.replace(GEN_HASH_FILE)
        except Exception as exc:  # noqa: BLE001
            log.warning("生图指纹写不进去:%s", exc)

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

    def recent_topic(self, session_key: str, limit: int = 4) -> str:
        """群里最近在聊什么,给搜索定个语境。

        取最近几条背景消息(没点她的那些),把发言人昵称去掉、拼成一句短语。
        搜索时带上它,「ZMJJKK 和 LUKXO」就不会被按字面当成两个足球/网球运动员。
        """
        lines = self.ambient.get(session_key) or []
        if not lines:
            return ""
        tail: list[str] = []
        for line in lines[-limit:]:
            body = line.split(":", 1)[-1] if ":" in line else line
            body = body.replace("[图片]", "").strip()
            if body:
                tail.append(body)
        return " / ".join(tail)[-100:]

    # ---------- 用户爱聊什么(常聊话题) ----------

    def _load_topics(self) -> None:
        """读上次攒下的常聊话题和提前搜好的资料(读不到就从头攒)。"""
        try:
            raw = json.loads(TOPIC_CACHE_FILE.read_text("utf-8"))
        except Exception:  # noqa: BLE001
            return
        if not isinstance(raw, dict):
            return
        hits = raw.get("hits")
        if isinstance(hits, dict):
            for session_key, terms in hits.items():
                if isinstance(terms, dict):
                    rows: dict[str, list[Any]] = {}
                    for t, v in terms.items():
                        if isinstance(v, list) and len(v) >= 2:
                            first = float(v[3]) if len(v) >= 4 else float(v[1])
                            users = list(v[2]) if len(v) >= 3 else []
                            rows[str(t)] = [int(v[0]), float(v[1]), users, first]
                    self.topic_hits[session_key] = rows
        briefs = raw.get("briefs")
        if isinstance(briefs, dict):
            for session_key, terms in briefs.items():
                if isinstance(terms, dict):
                    self.topic_briefs[session_key] = {
                        str(t): [float(v[0]), str(v[1])]
                        for t, v in terms.items()
                        if isinstance(v, list) and len(v) >= 2
                    }

    def _save_topics(self) -> None:
        try:
            tmp = TOPIC_CACHE_FILE.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(
                    {"hits": self.topic_hits, "briefs": self.topic_briefs},
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            tmp.replace(TOPIC_CACHE_FILE)
        except Exception as exc:  # noqa: BLE001
            log.warning("话题缓存写不进去:%s", exc)

    @staticmethod
    def _topic_terms(text: str) -> list[str]:
        """从一句话里切出可能的话题词:中文按 2~4 字滑动切,英文/数字整段留。

        切得糙没关系 —— 反复出现的自然会被顶上来,出现一次的没人搭理。
        """
        clean = re.sub(r"https?://\S+", " ", str(text)).replace("[图片]", " ")
        out: list[str] = []
        for run in TOPIC_TERM_RE.findall(clean):
            if run[0].isascii():
                out.append(run.lower())
                continue
            for size in (2, 3, 4):
                if size > len(run):
                    break
                for i in range(0, len(run) - size + 1):
                    term = run[i : i + size]
                    if term not in TOPIC_STOPWORDS:
                        out.append(term)
        return out

    def observe_topic(self, session_key: str, user_id: str, text: str) -> None:
        """记一笔群里在聊什么。反反复复出现的词,就是「用户爱聊什么」。"""
        hits = self.topic_hits[session_key]
        now = time.time()
        uid = str(user_id or "")
        for term in set(self._topic_terms(text)):
            row = hits.get(term)
            if row is None:
                # [出现过几条消息, 最后出现时间, 说过的人, 第一次出现时间]
                hits[term] = [1, now, [uid], now]
            else:
                row[0] = int(row[0]) + 1
                row[1] = now
                if len(row) < 4:  # 老存档没有「第一次出现时间」,补上
                    row.append(now)
                if uid and uid not in row[2]:
                    row[2].append(uid)
        # 很久没人提的词直接扔,别越攒越多(话题的热度能撑好些天,默认留 15 天)
        forget = float(self.cfg.get("topic_forget_hours", 360)) * 3600
        for term in [t for t, r in hits.items() if now - float(r[1]) > forget]:
            del hits[term]
        if len(hits) > 400:
            for term, _ in sorted(hits.items(), key=lambda kv: int(kv[1][0]))[: len(hits) - 400]:
                del hits[term]

    def hot_topics(self, session_key: str, limit: int | None = None) -> list[str]:
        """这个会话里反复出现、值得提前去搜的词。

        按「出现次数」排,次数一样就挑更长的那个 —— 因为「无畏契约」被切出来的
        「无畏」「契约」跟它的次数一模一样,不挑长的永远轮不到它。
        同一个话题的碎片(长的包含短的)只留一个,免得为一个话题搜三遍。
        """
        hits = self.topic_hits.get(session_key) or {}
        min_hits = int(self.cfg.get("topic_hot_min", TOPIC_HOT_MIN))
        # 调用方没点名要几个,就按配置来(topic_max);点名了(比如「报一下群里的热点话题」
        # 想要前 5 个)就以调用方为准
        if limit is None:
            limit = int(self.cfg.get("topic_max", TOPIC_MAX))
        ranked = sorted(
            hits.items(),
            key=lambda kv: (-int(kv[1][0]), -len(kv[0]), -len(kv[1][2]), kv[0]),
        )
        picked: list[str] = []
        for term, row in ranked:
            if int(row[0]) < min_hits:
                break
            # 停用词在切词时已经挡过一道;这里再挡一道,免得改之前攒下的旧垃圾词
            # (口头语、纯时间词)还留在存档里,被报成「常聊话题」或者拿去搜
            if term in TOPIC_STOPWORDS:
                continue
            if any(term in p or p in term for p in picked):
                continue
            picked.append(term)
            if len(picked) >= limit:
                break
        return picked

    def topic_report(self, session_key: str) -> str:
        """本群最近反复在聊什么 —— 给「群里有什么热点话题 / 最近在聊啥」这类问题用。

        话题还没攒够(凑不满 topic_hot_min)就返回空串,让调用方自己决定怎么答,别硬编。
        """
        topics = self.hot_topics(session_key, limit=5)
        if not topics:
            return ""
        inner = "、".join(f"「{t}」" for t in topics)
        return f"最近群里聊得最多的是 {inner}"

    def topic_interval(self, session_key: str, topic: str) -> float:
        """这个话题该隔多久搜一次(顺便也是资料能放多久)。

        话题活得越久,搜得越稀 —— 刚冒出来的最勤一天一次,活了好几个月的一周一次就够了。
        道理很直白:一个话题在群里活了大半年,该知道的群里早知道了,
        再搜也搜不出什么新鲜的,白花钱。

        hours = `topic_fresh_hours` × (活了几天 + 1) ^ 0.43,夹在 [刷新频率, 周频率] 之间。
        指数 0.43 是配着来的:刚出来(0 天)正好一天一次,活到 90 天(约三个月)正好顶到一周一次。
        """
        fresh = float(self.cfg.get("topic_fresh_hours", 24))
        old = float(self.cfg.get("topic_old_hours", 168))
        row = (self.topic_hits.get(session_key) or {}).get(topic)
        if not row:
            return fresh * 3600
        first = float(row[3]) if len(row) > 3 else float(row[1])
        age_days = max(0.0, (time.time() - first) / 86400.0)
        hours = fresh * (age_days + 1) ** TOPIC_AGE_POW
        return min(max(hours, fresh), old) * 3600

    def prefetched_note(self, session_key: str, text: str) -> tuple[str, str]:
        """问的东西正好落在某个常聊话题上,有没有提前搜好的资料。

        有就返回 (话题, 资料原文),没有就返回空 —— 空的话照旧当场联网搜。
        问题里没直接点名话题、但用了英文名/缩写(比如 ZMJJKK),那就看群里最近在聊什么,
        对得上也算命中。别的不相关的问题(比如问天气)不会被话题带跑。
        """
        briefs = self.topic_briefs.get(session_key) or {}
        if not briefs:
            return "", ""
        low = str(text).lower()
        # 问题里带了英文名/缩写这种「需要靠上下文才能认出来」的东西
        loose = bool(re.search(r"[A-Za-z0-9_]{3,}", str(text)))
        ambient = self.recent_topic(session_key) if loose else ""
        now = time.time()
        for topic in sorted(briefs, key=lambda t: -float(briefs[t][0])):
            if now - float(briefs[topic][0]) > self.topic_interval(session_key, topic):
                continue
            if topic and topic.lower() in low:
                return topic, str(briefs[topic][1])
            if ambient and topic and topic in ambient:
                return topic, str(briefs[topic][1])
        return "", ""

    def topic_background(self, session_key: str, limit: int = 1) -> str:
        """把提前搜好的话题资料摆成一小段背景(没搜过就空着)。"""
        briefs = self.topic_briefs.get(session_key) or {}
        now = time.time()
        items: list[str] = []
        for topic in sorted(briefs, key=lambda t: -float(briefs[t][0])):
            if len(items) >= limit:
                break
            if now - float(briefs[topic][0]) > self.topic_interval(session_key, topic):
                continue
            items.append(f"关于「{topic}」:{str(briefs[topic][1])[:300]}")
        return "\n".join(items)

    async def prefetch_topics(self) -> None:
        """替群里把常聊话题提前搜好,聊到时直接拿现成的,不用当场等十几秒。"""
        if not self.cfg.get("topic_prefetch", True):
            return
        today = datetime.now().strftime("%Y-%m-%d")
        if self.topic_prefetch.get("day") != today:
            self.topic_prefetch = {"day": today, "n": 0}
        budget = int(self.cfg.get("topic_prefetch_per_day", 12))
        now = time.time()
        for session_key in list(self.topic_hits.keys()):
            for topic in self.hot_topics(session_key):
                if int(self.topic_prefetch["n"]) >= budget:
                    self._prune_topic_briefs()
                    self._save_topics()
                    return
                brief = (self.topic_briefs.get(session_key) or {}).get(topic)
                # 还没到这个话题该搜的间隔就跳过(话题越老,间隔越长)
                if brief and now - float(brief[0]) < self.topic_interval(session_key, topic):
                    continue
                log.info("提前替群里搜一下常聊话题:%s", topic)
                try:
                    # 话题预热是后台活儿,走便宜档就行,别拿主力搜索模型(要钱)去磨
                    note = await self.tool_web_search(
                        f"{topic} 最新消息",
                        session_key,
                        str(self.cfg.get("topic_search_model") or ""),
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning("预热话题失败(%s):%s", topic, exc)
                    continue
                if note and not note.startswith(("没搜到", "搜索没连上")):
                    self.topic_briefs[session_key][topic] = [time.time(), note]
                    self.topic_prefetch["n"] = int(self.topic_prefetch["n"]) + 1
        self._prune_topic_briefs()
        self._save_topics()

    def _prune_topic_briefs(self) -> None:
        """话题早没人提了,提前搜的资料也跟着删掉 —— 这就叫「很久没提就删了」。"""
        for session_key in list(self.topic_briefs.keys()):
            live = set(self.topic_hits.get(session_key) or {})
            briefs = self.topic_briefs[session_key]
            for topic in [t for t in briefs if t not in live]:
                del briefs[topic]
            if not briefs:
                del self.topic_briefs[session_key]

    async def topic_loop(self) -> None:
        """每隔一阵子,替群里把常聊话题提前搜好,省得聊到时当场等。"""
        while True:
            await asyncio.sleep(float(self.cfg.get("topic_prefetch_minutes", 20)) * 60)
            try:
                await self.prefetch_topics()
            except Exception:  # noqa: BLE001
                log.exception("话题预热出错")
            try:
                await self._prefetch_memes()
            except Exception:  # noqa: BLE001
                log.exception("预热梗出错")

    # ---------- 网上的梗 ----------

    def _load_memes(self) -> None:
        """读上次攒下的梗(读不到就从头攒)。"""
        try:
            raw = json.loads(MEME_CACHE_FILE.read_text("utf-8"))
        except Exception:  # noqa: BLE001
            return
        if not isinstance(raw, dict):
            return
        items = raw.get("memes")
        if isinstance(items, list):
            for m in items:
                if not isinstance(m, dict):
                    continue
                phrase = str(m.get("phrase") or "").strip()
                if not phrase:
                    continue
                self.memes.append(
                    {
                        "phrase": phrase[:40],
                        "origin": str(m.get("origin") or "").strip()[:80],
                        "usage": str(m.get("usage") or "").strip()[:80],
                        "first_seen": float(m.get("first_seen") or 0),
                        # 老存档没有 last_seen:拿 first_seen 顶上,免得一升级全被判过期
                        "last_seen": float(
                            m.get("last_seen") or m.get("first_seen") or 0
                        ),
                    }
                )
        rejected = raw.get("rejected")
        if isinstance(rejected, dict):
            for phrase, ts in rejected.items():
                self.meme_rejected[str(phrase)[:40]] = float(ts or 0)
        elif isinstance(rejected, list):  # 兼容存成列表的老格式
            now = time.time()
            for phrase in rejected:
                self.meme_rejected[str(phrase)[:40]] = now
        self.meme_last = float(raw.get("last") or 0)

    def _save_memes(self) -> None:
        try:
            tmp = MEME_CACHE_FILE.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(
                    {
                        "memes": self.memes,
                        "rejected": self.meme_rejected,
                        "last": self.meme_last,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            tmp.replace(MEME_CACHE_FILE)
        except Exception as exc:  # noqa: BLE001
            log.warning("梗缓存写不进去:%s", exc)

    @staticmethod
    def _parse_memes(text: str) -> list[dict[str, Any]]:
        """从搜索模型回的话里抠出那个 JSON 数组,容错点:带了废话、夹着别的方括号也能捞出来。"""
        if not text:
            return []
        # 候选:先试「最大的一段」(从第一个 [ 到最后一个 ]);再试每一对方括号。
        # 模型偶尔会在数组前面写「来源[1]」这种,只认第一个 [ 会连带把正文也塞进来解析失败。
        spans = re.findall(r"\[.*\]", text, re.S) + re.findall(r"\[[^\[\]]*\]", text, re.S)
        for span in spans:
            try:
                arr = json.loads(span)
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(arr, list):
                continue
            out: list[dict[str, Any]] = []
            for m in arr:
                if not isinstance(m, dict):
                    continue
                phrase = str(m.get("phrase") or "").strip()
                if phrase:
                    out.append(
                        {
                            "phrase": phrase[:40],
                            "origin": str(m.get("origin") or "").strip()[:80],
                            "usage": str(m.get("usage") or "").strip()[:80],
                        }
                    )
            if out:
                return out
        return []

    def meme_note(self, limit: int | None = None) -> str:
        """把攒下的梗摆成几行给聊天模型 —— 够不够用、用不用得上,让她自己判断。"""
        if not self.memes:
            return ""
        if limit is None:
            limit = int(self.cfg.get("meme_max", MEME_MAX))
        lines: list[str] = []
        for m in self.memes[-limit:]:
            line = f"- {m['phrase']}"
            if m.get("usage"):
                line += f"(用法:{m['usage']})"
            lines.append(line)
        return "\n".join(lines)

    async def _prefetch_memes(self) -> None:
        """每隔一阵子上网搜一批最近流行的梗存着,说话时她自己挑着用。"""
        if not self.cfg.get("meme_enabled", True):
            return
        now = time.time()
        gap = float(self.cfg.get("meme_search_hours", 24)) * 3600
        if now - self.meme_last < gap:
            return
        log.info("上网搜一搜最近流行的梗")
        try:
            raw = await self._search_once(
                "最近网络热梗 流行语 评论区句式 近一个月",
                MEME_ASK_SYS.format(date=datetime.now().strftime("%Y年%m月%d日")),
                str(self.cfg.get("meme_search_model") or ""),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("搜梗失败:%s", exc)
            return
        # 不管这次搜到没搜到,都把时间戳往前推 —— 免得搜不到就一直重试烧钱
        self.meme_last = now
        got = self._parse_memes(raw)
        if got:
            seen = {m["phrase"] for m in self.memes}
            for m in got:
                # 被群友否过的梗就别再收了 —— 搜出来也不用
                if m["phrase"] in seen or m["phrase"] in self.meme_rejected:
                    continue
                seen.add(m["phrase"])
                m["first_seen"] = now
                m["last_seen"] = now
                self.memes.append(m)
        self._prune_memes()
        self._save_memes()

    def _prune_memes(self) -> None:
        """过期的梗删掉:跟「常聊话题」一个路子 —— 没人提就留不住。

        判删看 `last_seen`(最近一次在群里出现的时间):超过 meme_forget_days 没人提就删,
        新加的梗 last_seen = 加入时间,所以不会刚进来就被清掉。
        拉黑名单也跟着过期:缓几个月那个梗又流行了,给它一次机会。
        顺带压个总数上限,免得攒成一本字典。
        """
        cutoff = time.time() - float(self.cfg.get("meme_forget_days", MEME_FORGET_DAYS)) * 86400
        self.memes = [
            m
            for m in self.memes
            if float(m.get("last_seen") or m.get("first_seen") or 0) >= cutoff
        ]
        self.meme_rejected = {
            p: float(ts) for p, ts in self.meme_rejected.items() if float(ts or 0) >= cutoff
        }
        cap = max(int(self.cfg.get("meme_max", MEME_MAX)) * 4, 32)
        if len(self.memes) > cap:
            self.memes = self.memes[-cap:]

    def observe_memes(self, text: str) -> None:
        """群消息里提到了某个攒着的梗,就给它续一笔命 —— 没人提的梗会过期被删。

        跟「常聊话题」的衰减同一个路子:梗靠群里还在用它才活着。
        """
        if not self.memes or not text:
            return
        now = time.time()
        hit = False
        for m in self.memes:
            phrase = m["phrase"]
            if len(phrase) >= 2 and phrase in text:
                m["last_seen"] = now
                hit = True
        if hit:
            self._save_memes()

    def note_meme_used(self, session_key: str, answer: str) -> None:
        """她刚回的话里带没带攒着的梗?带了就盯着群里接下来几句的反应。

        宁可漏判也别误判(误判要白花一次分析、还可能误杀好梗):先用整句 phrase 匹配,
        再退化到取前 4 个字 —— 她被要求「用自己的话带出来」,整句未必原样出现。
        """
        if not self.cfg.get("meme_feedback_enabled", True) or not self.memes or not answer:
            return
        for m in self.memes:
            phrase = m["phrase"]
            if phrase in answer or (len(phrase) >= 5 and phrase[:4] in answer):
                self.meme_watch[session_key] = {
                    "phrase": phrase,
                    "t": time.time(),
                    "collected": [],
                }
                log.info("她用了梗「%s」,盯着群里反应", phrase)
                return

    def collect_meme_feedback(self, session_key: str, text: str) -> None:
        """收群里接下来的反应;收够 meme_feedback_msgs 条就交给模型判断感不感冒。"""
        w = self.meme_watch.get(session_key)
        if not w or not text:
            return
        window = float(self.cfg.get("meme_feedback_minutes", 5)) * 60
        if time.time() - float(w["t"]) > window:
            self.meme_watch.pop(session_key, None)
            return
        w["collected"].append(text[:80])
        need = int(self.cfg.get("meme_feedback_msgs", MEME_FEEDBACK_MSGS))
        if len(w["collected"]) >= need:
            self.meme_watch.pop(session_key, None)
            # 别卡住群消息处理:分析扔进后台跑,fire-and-forget
            asyncio.create_task(
                self._judge_meme_reaction(w["phrase"], list(w["collected"]))
            )

    async def _judge_meme_reaction(self, phrase: str, lines: list[str]) -> None:
        """让便宜模型读群里的反应:明显不感冒就把这梗拉黑,以后不再用。"""
        chat = "\n".join(f"- {ln}" for ln in lines if ln.strip())
        if not chat:
            return
        try:
            verdict = await self.ask_plain(
                MEME_FEEDBACK_ASK.format(phrase=phrase, chat=chat), max_tokens=8
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("判断梗反应失败:%s", exc)
            return
        if "不感冒" in verdict:
            self._reject_meme(phrase)
            log.info("群里对梗「%s」不感冒,拉黑,以后不用了", phrase)

    def _reject_meme(self, phrase: str) -> None:
        """把一个梗拉黑:从库里删掉、记进名单,搜到了也过滤,以后不再提。"""
        self.meme_rejected[phrase] = time.time()
        self.memes = [m for m in self.memes if m["phrase"] != phrase]
        self._save_memes()

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
        # 出嘴边统一过一道「喵 + 颜文字」:所有回话都走这个口子,漏不掉
        if isinstance(message, str):
            message = decorate_speech(message)
        elif isinstance(message, list):
            message = [
                (
                    {**seg, "data": {**seg["data"], "text": decorate_speech(seg["data"]["text"])}}
                    if seg.get("type") == "text"
                    and isinstance(seg.get("data"), dict)
                    and isinstance(seg["data"].get("text"), str)
                    else seg
                )
                for seg in message
            ]
        if event.get("message_type") == "group":
            resp = await self.call_api_wait(
                "send_group_msg",
                {"group_id": event["group_id"], "message": message},
                timeout=6,
            )
            target = str(event.get("group_id") or "")
        else:
            resp = await self.call_api_wait(
                "send_private_msg",
                {"user_id": event["user_id"], "message": message},
                timeout=6,
            )
            target = str(event.get("user_id") or "")
        # 记下这条的 message_id:待会儿有人喊「撤回」时,才知道撤哪一条
        data = resp.get("data") if isinstance(resp, dict) else None
        mid = str((data or {}).get("message_id") or "") if isinstance(data, dict) else ""
        if mid and target:
            self.bot_msgs[target].append((time.time(), mid))

    async def reply(self, event: dict[str, Any], text: str) -> None:
        await self.send(event, text)

    async def open_chat_window(self, event: dict[str, Any], session_key: str) -> None:
        """她刚跟人聊完,给这一小会儿开个「接话窗」,别人接着说的短句没 @ 也理。
        只有主聊天/应答才开;日报、主动冒泡、偷表情包这种自己冒出来的不算。"""
        if event.get("message_type") == "group":
            self.last_bot_speak[session_key] = time.time()
            # 记下刚才是跟谁在聊:接话窗只接这个人的下文
            self.followup_partner[session_key] = str(event.get("user_id") or "")

    async def empty_poke_reply(
        self, event: dict[str, Any], session_key: str, speaker: str
    ) -> None:
        """被戳了一下(只 @ 了它 / 只叫了声名字 / 只甩了串标点)时的应声。

        群里有人习惯连着戳两下叫你,每戳一次都回「怎么了？」「？」就成了复读机,
        又吵又没用 —— 同一个人重复干同一件事,意思基本就是刚才那个,不必每次反问。
        所以第一次应一句,同一个人 EMPTY_POKE_REPEAT_SECONDS 之内再来就先不吭声;
        接话窗照样开着,他真要接着说事,该回还是回(那时走的是正常聊天那一路)。
        """
        who = str(event.get("user_id") or speaker or "")
        key = f"{session_key}|{who}"
        now = time.time()
        last = self.poked_at.get(key, 0.0)
        self.poked_at[key] = now
        if last and now - last < EMPTY_POKE_REPEAT_SECONDS:
            log.info("同一个人又戳了一下(%s),不跟着回「怎么了」了", speaker or who)
            await self.open_chat_window(event, session_key)
            return
        lines = [
            str(x)
            for x in (self.cfg.get("empty_at_replies") or ["怎么了？"])
            if str(x).strip()
        ]
        await self.reply(event, random.choice(lines) if lines else "怎么了？")
        await self.open_chat_window(event, session_key)

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

    def bot_names(self) -> list[str]:
        """别人平时怎么叫它。叫到名字它就得答应。"""
        names = self.cfg.get("bot_names") or ["小鲸鱼", "鲸鱼", "小鲸", "鲸鲸"]
        return [str(n).strip() for n in names if str(n).strip()]

    def mentions_me(self, text: str) -> bool:
        """这句里点名它了没(没 @ 也算)。"""
        return any(n in text for n in self.bot_names())

    def _strip_call(self, text: str) -> str:
        """把这句里叫它的部分(@它 + 名字)抠掉,看剩下多少实内容。"""
        t = strip_at_head(text)
        for n in self.bot_names():
            t = t.replace(n, "")
        return re.sub(r"[\s,，。!！?？~、]+", "", t)

    def in_quiet_hours(self) -> bool:
        """现在是不是免打扰时段(默认 23:00~10:00)。只挡主动说话,被 @ / 点名 / 私聊照样回。"""
        hour = datetime.now().hour
        quiet = self.cfg.get("quiet_hours") or [23, 10]
        start, end = int(quiet[0]), int(quiet[1])
        if start > end:
            return hour >= start or hour < end
        return start <= hour < end

    def should_reply(
        self, event: dict[str, Any], text: str, at_bot: bool, has_images: bool = False
    ) -> str | None:
        """判断是否该回复。返回待处理的内容(允许为空串,表示只发了图片),不回复返回 None。"""
        has_content = bool(text) or has_images
        if event.get("message_type") == "private":
            return text if has_content else None

        # 群聊:@ 了它、或者叫了它的名字,这两种必须回,跟什么触发模式无关
        if at_bot or self.mentions_me(text):
            # 被点名说明是正经找她,接话窗从头开
            self.followup_streak[f"group_{event.get('group_id')}"] = 0
            return text if has_content else None

        mode = str(self.cfg.get("group_trigger", "smart")).lower()
        if mode == "all":
            return text if has_content else None
        if mode == "prefix":
            prefix = str(self.cfg.get("group_prefix", "/ds"))
            if not text.startswith(prefix):
                return None
            stripped = text[len(prefix):].strip()
            return stripped if (stripped or has_images) else None
        if mode == "at":
            return None
        # smart:没人叫它也能搭一句,但先得弄清楚「这句是不是冲它说的」
        if not has_content:
            return None
        key = f"group_{event.get('group_id')}"
        now = time.time()
        # 这句 @ 了别人(又不是在叫它)——那是在跟别人说话,一句都别插
        if self.parse_at_targets(event):
            return None
        # 她刚跟某人说完话的一小会儿:那个人自己接着说、而且明显还在跟她聊,才算接话。
        # 只认问句,或者话里带着「你 / 咋 / 怎么」这种指着她的词 —— 不然群里随口一句
        # 短话(「没事」「完了」)都会被当成在跟她说话,就成了「群里每一条都回」。
        # 再加一道闸:同一个会话连着接话最多 followup_max 次,接满了就闭嘴等下一次点名。
        follow = float(self.cfg.get("followup_seconds", 60))
        if follow > 0 and now - self.last_bot_speak.get(key, 0) <= follow:
            talking_to = self.followup_partner.get(key, "")
            if (
                talking_to
                and str(event.get("user_id") or "") == talking_to
                and self.followup_streak.get(key, 0) < int(self.cfg.get("followup_max", 2))
                and self._looks_at_me(self._strip_call(text))
                and not self.in_quiet_hours()
            ):
                self.followup_streak[key] = self.followup_streak.get(key, 0) + 1
                return text
        if now - self.last_chime.get(key, 0) < float(self.cfg.get("chime_cooldown", 300)):
            return None
        # 上次说完话一直没人接,就别再往上凑了
        sulk = int(self.cfg.get("sulk_after", 5))
        if sulk and self.silent_streak.get(key, 0) >= sulk:
            return None
        if self.in_quiet_hours():
            return None
        if random.random() < float(self.cfg.get("chime_chance", 0.03)):
            self.last_chime[key] = now
            self.followup_streak[key] = 0  # 自己插话是新的一轮,接话次数从头算
            return text
        return None

    def _looks_at_me(self, tail: str) -> bool:
        """这句像是接着跟她说的吗:问句,或者话里明显指着她(你 / 咋 / 怎么 / 帮 …)。
        只按长度猜太松了,群里随便一句短话都会被她接走。"""
        if not tail:
            return True  # 只 @ 了一声、没说事,后面单独处理
        if tail.endswith(("?", "？")):
            return True
        return bool(re.search(r"(你|您|咋|怎么|为啥|为什么|帮|看看|觉得|呢|吗)", tail))

    def needs_fresh(self, text: str) -> bool:
        """这句是不是时效性问题(比赛谁赢 / 最新版本 / 出了没这类)。

        光在提示词里喊「时效性问题要先联网」不够 —— 快模型经常不开工具就凭记忆答,
        所以代码替它认一遍:认出来就先替它搜,搜到的资料直接塞进上下文。

        两道闸门,免得「每句话都去搜」:
          ① 得命中时效关键词(FRESH_RE);
          ② 还得是**在问事**的(问号 / 谁哪几多少 / 什么时候 / 吗呢…)。
        不是问句的别搜 —— 「我进决赛了」「演唱会票价好贵」这种只是随口一说,搜了也没用,
        白等十几秒还费钱。
        """
        t = self._strip_call(text)
        return (
            bool(t)
            and len(t) <= 40
            and bool(FRESH_RE.search(t))
            and bool(ASK_RE.search(t))
        )

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
            else:
                path = ref[7:] if ref.startswith("file://") else ref
                raw = Path(path).read_bytes()
        except Exception as exc:  # noqa: BLE001
            # 下不动就直接放弃这张图。别把 http 链接原样丢给模型服务端:
            # 它那边同样抓不到,会返回 400 把整条回复一起吞掉。
            log.warning("图片获取失败(%s): %s", ref[:60], exc)
            return None

        limit = int(self.cfg.get("max_image_mb", 8)) * 1024 * 1024
        if len(raw) > limit:
            log.warning("图片超过 %dMB,已忽略", limit // 1024 // 1024)
            return None
        return self.vision_data_url(raw)

    def vision_data_url(self, raw: bytes) -> str:
        """送去识图的 data URL:先压一道再拼。

        原图动辄几 MB,直接 base64 传上去又慢又费 token;压完通常只剩几十 KB,
        认出来的东西没差别。
        """
        small, mime = shrink_image_for_vision(
            raw,
            int(self.cfg.get("vision_max_edge", VISION_MAX_EDGE)),
            int(self.cfg.get("vision_jpeg_quality", VISION_JPEG_QUALITY)),
        )
        return f"data:{mime};base64," + base64.b64encode(small).decode()

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
        tools: bool = False,
        speaker: str = "",
        quote: str = "",
        web_note: str = "",
        topic_note: str = "",
    ) -> str:
        history = self.history[session_key]
        images = images or []
        # 群里好几个人说话,不标名字模型分不清谁说的,上下文就废了一半
        text = f"{speaker}: {user_text}".strip() if speaker else user_text
        # 他引用了哪条,就把那条一起摆出来,不然「你怎么这么说」她根本不知道指的是什么
        if quote:
            text = f"(引用 {quote}) {text}"

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
        # 挂工具时补一句「你会什么」;带图 / 联网那条路不挂工具,就别说
        if tools and not images and not use_search:
            system_prompt += NATURAL_HINT
        # 「今天是几号」这条哪条路都得给:模型不知道现在是什么时候,
        # 就会把过期的事当现状答(世界杯早结束了还在讲谁踢谁)。
        if not images:
            system_prompt += TODAY_TPL.format(
                date=datetime.now().strftime("%Y年%m月%d日")
            )
        if use_search and not images:
            system_prompt += SEARCH_HINT
        # 时效性问题由代码先搜过一遍,资料直接摆在这儿,别让她凭记忆答
        if web_note and not images:
            system_prompt += WEB_NOTE_TPL.format(note=web_note)
        # 提前替群里搜好的常聊话题资料:这一轮没走当场搜,就把话题背景带上,
        # 让她知道群里爱聊什么、也已经有过现成资料(聊到时不必再搜)
        if topic_note and not images and not web_note:
            system_prompt += TOPIC_BRIEF_TPL.format(items=topic_note)
        # 最近网上的梗:随身带着,聊到对得上的话头她就能顺口来一句。
        # 正在给正经结论(web_note)时不掺,免得把梗硬塞进答案里。
        meme_note = self.meme_note()
        if meme_note and not web_note:
            system_prompt += MEME_TPL.format(items=meme_note)
        if self.stickers and not images:
            system_prompt += STICKER_HINT.format(items=self.sticker_list_text())
        if self.ambient.get(session_key):
            system_prompt += AMBIENT_TPL.format(chat="\n".join(self.ambient[session_key]))
        # 以前的聊天里有没有相关的,捞出来提一句(轻量长期记忆)。
        # 短期历史还在时,近 10 分钟的话本来就在上下文里,不必再捞一遍占位置;
        # 但历史是空的(刚重启 / 刚 /重置),说明真的断片了,这时候连最新的也得捞。
        recalled = self.memory.search(
            session_key,
            user_text,
            int(self.cfg.get("memory_recall", 4)),
            skip_recent=600 if history else 0,
        )
        if recalled:
            system_prompt += MEMORY_TPL.format(items="\n".join(recalled))
        # 她自己最近说过的话单独摆一份:光靠短期上下文不稳,聊几句就被刷掉、
        # 重启一次全没,别人追问「你刚不是说…」她翻不到就只能死不认账。
        said = self.memory.recent_said(
            session_key, self.bot_names()[0], int(self.cfg.get("self_said_recall", 5))
        )
        if said:
            system_prompt += SAID_TPL.format(items="\n".join(f"- {s}" for s in said))
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
        # 带图、联网这两条路不挂工具:一条走视觉模型、一条本来就在搜
        attach_tools = tools and not images and not use_search
        if attach_tools:
            payload["tools"] = TOOLS_NO_SEARCH if web_note else TOOLS
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

        msg = resp.json()["choices"][0]["message"]
        # 模型要调工具:本地把工具跑了,把结果塞回去让它接着说话。
        # 最多两跳,防它一直调工具转圈。
        hops = 0
        tool_notes: list[str] = []
        while msg.get("tool_calls") and hops < 2:
            hops += 1
            messages.append(
                {
                    "role": "assistant",
                    "content": msg.get("content") or "",
                    "tool_calls": msg["tool_calls"],
                }
            )
            for call in msg["tool_calls"]:
                fn = call.get("function") or {}
                try:
                    targs = json.loads(fn.get("arguments") or "{}")
                except (ValueError, TypeError):
                    targs = {}
                if not isinstance(targs, dict):
                    targs = {}
                name = str(fn.get("name") or "")
                result = await self.run_tool(name, targs, session_key)
                log.info("模型调工具 %s → %s", name, result[:60].replace("\n", " "))
                tool_notes.append(f"{name} 得到: {result.strip()[:120]}")
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.get("id") or "",
                        "content": result,
                    }
                )
            resp = await self.http.post(url, json=payload, headers=headers)
            if resp.status_code != 200:
                log.error("工具回环后返回 %s: %s", resp.status_code, resp.text[:300])
                raise RuntimeError(f"模型接口错误 {resp.status_code}")
            msg = resp.json()["choices"][0]["message"]

        content = strip_self_prefix((msg.get("content") or "").strip(), (speaker,))

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
        if tool_notes:
            # 刚查到的结果也记进上下文,不然下一轮追问「那刚才那个数呢」它会断片
            history.append(
                {"role": "assistant", "content": "(我查了 " + "; ".join(tool_notes) + ")"}
            )
        history.append({"role": "assistant", "content": stored})
        # 只保留最近 max_history 条,防止上下文无限增长
        if len(history) > max_history:
            del history[:-max_history]
        return content

    async def run_tool(self, name: str, args: dict[str, Any], session_key: str) -> str:
        """模型点名要调的工具,就在本地把它跑了,结果原样还回去让它接着说话。

        这里只放「查一下」这种良性操作。踢人 / 禁言不在这,那走硬规则。
        """
        try:
            if name == "check_balance":
                return await self.fetch_balance()
            if name == "get_weather":
                city = str((args or {}).get("city") or "").strip()
                return await self.fetch_weather(city)
            if name == "get_hot":
                return await self.fetch_hot()
            if name == "group_stats":
                return self.stats_text(session_key)
            if name == "group_topic":
                return self.topic_report(session_key) or "群里最近还没聊出什么固定话题"
            if name == "web_search":
                query = str((args or {}).get("query") or "").strip()
                if not query:
                    return "没给要查的内容"
                return await self.tool_web_search(query, session_key)
        except Exception as exc:  # noqa: BLE001
            log.warning("工具 %s 执行失败:%s", name, exc)
            return f"这个我这边没跑成({name})"
        log.warning("模型要调不存在的工具:%s", name)
        return f"没有 {name} 这么个东西"

    async def _search_once(self, query: str, sys_prompt: str, model: str = "") -> str:
        """跑一趟联网搜索,返回搜索模型给的原文。出错就返回「没搜到…」那句话。

        model 留空就用配置里的主力搜索模型(要准);传了就用传进来的 ——
        话题预热这种后台活儿用便宜档就行,别拿 Max 去搜。
        """
        url = self.cfg.get("deepseek_base_url", "").rstrip("/") + "/chat/completions"
        model = model or str(self.cfg.get("search_model") or self.cfg.get("model", ""))
        payload: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": query},
            ],
            "stream": False,
            # 搜索答案不用写长:输出越长等得越久。给短一点,一句话结论 + 两条来源就够。
            "max_tokens": int(
                self.cfg.get("search_max_tokens") or self.cfg.get("max_tokens", 400)
            ),
            "enable_search": True,
        }
        # 百炼的 DeepSeek 不关思考会烧成空白正文,老坑。
        # qwen3 系列的搜索模型同理:搜索答案要短,思考开着容易把 max_tokens 烧光。
        if is_deepseek(model) or str(model).strip().lower().startswith("qwen3"):
            payload["enable_thinking"] = False
        headers = {
            "Authorization": f"Bearer {self.cfg['deepseek_api_key']}",
            "Content-Type": "application/json",
        }
        try:
            resp = await self.http.post(url, json=payload, headers=headers, timeout=30)
            if resp.status_code != 200:
                return f"没搜到(接口 {resp.status_code})"
            text = (resp.json()["choices"][0]["message"].get("content") or "").strip()
            return text or "没搜到相关的东西"
        except Exception as exc:  # noqa: BLE001
            log.warning("工具联网搜索失败:%s", exc)
            return f"搜索没连上({exc})"

    async def tool_web_search(
        self, query: str, session_key: str = "", model: str = ""
    ) -> str:
        """给模型当工具用的联网搜索。

        model 留空走主力搜索模型;话题预热那类后台活儿传个便宜档进来。

        四件事:
          ① 同一个问题问第二遍直接拿缓存,不再联网;
          ② 把群里最近聊的话题塞进搜索要求,免得问题里的名字被按字面理解成别的领域的人;
          ③ 搜到的答案短(搜索答案越长等得越久),只要一句话结论 + 两条带日期的来源;
          ④ 那两条来源要是**说法一致**就够了,一次收工;不一致(或者只搜到一条)
             再补搜一遍 —— 绝大多数情况都停在第一趟,不会白等第二次。
        """
        fresh = bool(FRESH_RE.search(query))
        cached = self.web_cache.get(query)
        if cached:
            log.info("这个问题搜过,直接用缓存:%s", query[:30])
            return cached
        ask = SEARCH_ASK_SYS.format(date=datetime.now().strftime("%Y年%m月%d日"))
        # 群里刚在聊什么,给搜索定个语境。没有这条,「ZMJJKK 和 LUKXO」会被当成两个运动员
        topic = self.recent_topic(session_key)
        if topic:
            ask += SEARCH_TOPIC_TPL.format(topic=topic)
            log.info("带上群里的话题去搜:%s | %s", topic[:40], query[:30])
        started = time.perf_counter()
        text = await self._search_once(query, ask, model)
        log.info("搜索耗时 %.1fs:%s", time.perf_counter() - started, query[:30])
        if not text or text.startswith(("没搜到", "搜索没连上")):
            return text or "没搜到相关的东西"
        # 来源打架、或者只有一个来源,才再搜一趟(最多补一次,别转圈)
        if CONFLICT_RE.search(text):
            log.info("来源说法不一致,再补搜一趟核对:%s", query[:30])
            again = await self._search_once(query, SEARCH_ASK_SYS_B, model)
            if again and not again.startswith(("没搜到", "搜索没连上")):
                text = f"{text}\n---\n另找的一批资料:\n{again}"
        self.web_cache.put(query, text, fresh)
        return text

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
        # 记下这张图的指纹:她生出来的是「画」,不是表情包,别让偷图逻辑收进库
        try:
            self._remember_gen_image(base64.b64decode(b64))
        except Exception:  # noqa: BLE001
            pass
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

    async def ask_vision(
        self, text: str, raw: bytes, max_tokens: int = 200, model: str = ""
    ) -> str:
        """拿一张图问识图模型一句话。max_tokens 大一点能让它看得更仔细。

        传进来的是原图字节,这里先压小再发 —— 上传体积小一截,等待和花费都跟着降。
        model 给了就用它,不然用 vision_model:偷表情包单独配了更强的识图模型,只走这一条路。
        """
        c = self.cfg
        resp = await self.http.post(
            c.get("deepseek_base_url", "").rstrip("/") + "/chat/completions",
            json={
                "model": model or c.get("vision_model"),
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": text},
                            {"type": "image_url", "image_url": {"url": self.vision_data_url(raw)}},
                        ],
                    }
                ],
                "max_tokens": max_tokens,
                "enable_thinking": False,
            },
            headers={
                "Authorization": f"Bearer {c['deepseek_api_key']}",
                "Content-Type": "application/json",
            },
            timeout=60,
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
                    raw,
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
        # 别理自己发的消息:有的 NapCat 会把她自己发的话也推回来,
        # 不挡住她会自问自答(尤其刚加的接话窗,会自己接自己的话)
        if self.self_id and str(event.get("user_id") or "") == self.self_id:
            return

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

        # 刷屏就先禁言。这条是硬规则、不问模型 —— 误踢人拉不回来,禁言到点自己会解
        if is_group and text:
            await self.check_spam(event, text)

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
            # 「用户爱聊什么」:反复出现的词攒起来,回头替他们提前搜好
            self.observe_topic(session_key, str(event.get("user_id") or ""), text)
            # 群里在说到某个攒着的梗就给它续命(没人提的梗会过期被删),
            # 顺便收一下她对上次那个梗的反应
            self.observe_memes(text)
            self.collect_meme_feedback(session_key, text)

        # 命令不用 @ 机器人也能用;成语接龙进行中,直接发四个字也算接龙
        playing_idiom = self.games.get(session_key, {}).get("kind") == "idiom"
        looks_idiom = bool(re.fullmatch(r"[\u4e00-\u9fff]{4}", text.strip()))
        if (self.cfg.get("commands_anyone", True) and self.is_command(text)) or (
            playing_idiom and looks_idiom
        ):
            ask_text = text.strip()
        else:
            ask_text = self.should_reply(event, text, at_bot, bool(images))

        # 群里只是叫一声「小鲸鱼」、没说事:直接应一声,别让模型去接群里之前的话题。
        # 同一个人反复叫 / 反复甩「？？？」就不跟着复读了(见 empty_poke_reply)
        if is_group and ask_text is not None and not self._strip_call(ask_text):
            await self.empty_poke_reply(event, session_key, speaker)
            return

        if ask_text is None:
            # 只 @ 了一下、什么都没说:别装没看见,随口应一声(反复戳就不应了)
            if is_group and at_bot and not text.strip() and not images:
                await self.empty_poke_reply(event, session_key, speaker)
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

            # 「群里最近在聊什么 / 有什么热点话题」——代码直接报常聊话题,
            # 不等模型自己想起来调 group_topic(快模型经常想不起来,又要多等一轮)
            if not use_search and TOPIC_Q_RE.search(ask_text):
                topic_ans = self.topic_report(session_key)
                if topic_ans:
                    log.info("问群里的热点话题,直接报常聊话题")
                    await self.reply(event, topic_ans)
                    return

            if use_search:
                await self.reply(
                    event, self.cfg.get("search_hint", "正在联网搜索,请稍等…")
                )
            # 时效性问题(谁赢了 / 最新 / 出了没)不等模型自己想起来联网,这里先搜一遍。
            # 搜到的原文当资料塞进上下文,再由她用自己口气讲,免得拿旧记忆下结论。
            # 这一趟搜索和「取引用那条消息」互不依赖,并着跑,少等一次往返。
            # 而且同一个问题搜过就直接用缓存,压根不再联网。
            # 提前替群里搜好的常聊话题资料,当背景带着(有就带,没有就算了)
            topic_note = self.topic_background(session_key)
            web_note = ""
            if not use_search and self.needs_fresh(ask_text):
                # 正好问到常聊话题、又有提前搜好的资料 → 直接用,不再当场联网干等
                hot_topic, brief = self.prefetched_note(session_key, ask_text)
                if brief:
                    log.info("聊到常聊话题「%s」,用提前搜好的资料,免搜", hot_topic)
                    web_note, quote = brief, await self.quoted_snippet(event)
                else:
                    log.info("时效性问题,先替她搜一遍:%s", ask_text[:40])
                    web_note, quote = await asyncio.gather(
                        self.tool_web_search(ask_text, session_key),
                        self.quoted_snippet(event),
                    )
            else:
                # 他引用了哪条消息,就把那条也取出来一起看
                quote = await self.quoted_snippet(event)
            if quote:
                log.info("引用:%s", quote[:60])
            started = time.perf_counter()
            failed = False
            try:
                answer = await self.ask_deepseek(
                    session_key,
                    ask_text,
                    images=images,
                    use_search=use_search,
                    tools=not use_search,
                    speaker=speaker,
                    quote=quote,
                    web_note=web_note,
                    topic_note=topic_note,
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

            guess, want_draw = split_draw_marker(answer)
            remark, find_kw = split_find_marker(answer)
            # 用户嘴上说的是「发张表情包」,模型却吐了 [DRAW]/[FIND]:
            # 他要的是本地库里现成的一张,不是现画、也不是上网找。改走库,别去生图。
            if (want_draw or find_kw) and wants_local_sticker(ask_text) and self.stickers:
                sticker = self.pick_sticker(self._strip_call(ask_text) or ask_text, session_key)
                if sticker:
                    log.info("要表情包,从库里发:%s(模型本来给了要图的标记)", sticker.name)
                    await self.send_sticker(event, sticker)
                    self.remember_sticker(session_key, sticker.name)
                    await self.open_chat_window(event, session_key)
                    return

            # 没打 /画,但模型自己判断出用户是在要图(自然语言)
            if want_draw:
                log.info("模型判定要画图:%s", guess[:60])
                # 拿用户原话去联网查准,模型的理解只当参考(它可能压根不认识那角色)
                await self.send_generated_image(event, ask_text, guess)
                return

            # 用户要的是现成的图,那就直接上网找,别费劲去画
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
                await self.open_chat_window(event, session_key)
                # 自己说过的话也留一份。以前只存别人说的,她转头就不认账:
                # 「你刚不是说 XX 吗」→「我没说过」。存下来她才能捞回自己的原话。
                self.memory.add(session_key, self.bot_names()[0], answer)
                # 这句里要是用了攒着的梗,就盯着群里接下来的反应(不感冒就拉黑)
                self.note_meme_used(session_key, answer)
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
        t = strip_at_head(text)
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

    async def group_members(self, group_id: Any) -> list[dict[str, Any]]:
        """群成员名单。缓存 5 分钟,免得点一次人就去拉一遍。"""
        key = str(group_id)
        now = time.time()
        hit = self.member_cache.get(key)
        if hit and now - hit[0] < 300:
            return hit[1]
        resp = await self.call_api_wait("get_group_member_list", {"group_id": group_id})
        raw = resp.get("data") if isinstance(resp, dict) else None
        members = [m for m in (raw or []) if isinstance(m, dict)]
        if members:
            self.member_cache[key] = (now, members)
        return members

    async def member_name(self, group_id: Any, user_id: Any) -> str:
        """这人在群里叫什么(优先群名片,其次昵称)。"""
        for m in await self.group_members(group_id):
            if str(m.get("user_id") or "") == str(user_id):
                return str(m.get("card") or m.get("nickname") or "")
        return ""

    async def quoted_user(self, event: dict[str, Any]) -> str:
        """「引用」的那条消息是谁发的。得回问一次 NapCat 才知道。"""
        for seg in event.get("message") or []:
            if not isinstance(seg, dict) or seg.get("type") != "reply":
                continue
            mid = str((seg.get("data") or {}).get("id") or "")
            if not mid:
                continue
            resp = await self.call_api_wait("get_msg", {"message_id": mid})
            data = resp.get("data") if isinstance(resp, dict) else None
            qq = str(((data or {}).get("sender") or {}).get("user_id") or "")
            if qq and qq != self.self_id:
                return qq
        return ""

    async def quoted_snippet(self, event: dict[str, Any]) -> str:
        """这条消息引用了哪条?把被引用那条的「谁: 说了啥」取回来。

        以前只有踢人时才会去翻被引用的消息,聊天时引用内容一律看不见 ——
        别人「引用她某句话再怼一句」,她看不到那句话,自然答得驴唇不对马嘴。
        """
        for seg in event.get("message") or []:
            if not isinstance(seg, dict) or seg.get("type") != "reply":
                continue
            mid = str((seg.get("data") or {}).get("id") or "")
            if not mid:
                continue
            resp = await self.call_api_wait("get_msg", {"message_id": mid})
            data = resp.get("data") if isinstance(resp, dict) else None
            if not isinstance(data, dict):
                continue
            sender = data.get("sender") or {}
            who = str(sender.get("card") or sender.get("nickname") or "").strip()
            if who == "" or str(sender.get("user_id") or "") == self.self_id:
                who = self.bot_names()[0]  # 引用的是她自己说的话
            raw = data.get("message")
            if isinstance(raw, list):
                texts = [
                    str((s.get("data") or {}).get("text") or "")
                    for s in raw
                    if isinstance(s, dict) and s.get("type") == "text"
                ]
                body = "".join(texts).strip()
                if not body and any(
                    isinstance(s, dict) and s.get("type") == "image" for s in raw
                ):
                    body = "[图片]"
            else:
                body = str(data.get("raw_message") or "").strip()
            if not body:
                body = "[非文字消息]"
            return f"{who}: {body[:200]}"
        return ""

    async def do_recall(self, event: dict[str, Any]) -> str:
        """撤回。群里谁都能喊一声,但她真正撤得掉的只有自己发出去的东西。

        ① 他引用了某条消息再喊「撤回」→ 撤被引用的那条(这就是「撤回他引用的那条」);
        ② 没引用 → 撤她在这个会话里最近发的那一条。
        撤不掉(不是她发的、她不是群管理、或者过了能撤的时间)就如实说,别假装撤成功了。
        """
        for seg in event.get("message") or []:
            if isinstance(seg, dict) and seg.get("type") == "reply":
                mid = str((seg.get("data") or {}).get("id") or "")
                if mid:
                    return await self._delete_msg(mid, "他引用的那条")
        target = str(event.get("group_id") or event.get("user_id") or "")
        buf = self.bot_msgs.get(target)
        while buf:
            _, mid = buf.pop()
            if mid:
                return await self._delete_msg(mid, "我刚说的")
        return "最近没找到能撤的喵"

    async def _delete_msg(self, message_id: str, what: str) -> str:
        """撤一条消息,成功失败都如实反馈。"""
        resp = await self.call_api_wait("delete_msg", {"message_id": message_id})
        ok = isinstance(resp, dict) and (
            str(resp.get("status") or "").lower() == "ok" or resp.get("retcode") == 0
        )
        if ok:
            log.info("撤回%s:%s", what, message_id)
            return f"撤了{what}喵"
        log.info("没撤掉%s(%s):%s", what, message_id, resp)
        return "这条我撤不了喵(不是我发的,或者已经过了能撤的时候)"

    async def guess_kick_target(self, event: dict[str, Any], text: str) -> tuple[str, str]:
        """口语里要踢谁:① @ 的 ② 引用的是谁 ③ 说「我」就是他自己 ④ 名字对得上。
        返回 (QQ, 名字),猜不出来给两个空串,让调用方去反问。
        """
        group_id = event.get("group_id")
        target = (self.parse_at_targets(event) or [""])[0]
        if not target:
            target = await self.quoted_user(event)
        if not target and re.search(r"我|自己", text):
            target = str(event.get("user_id") or "")
        if target:
            return target, await self.member_name(group_id, target)
        # 直接把名字打出来了:挑名字最长的那条,一样长(分不清)就不动手
        hit: list[tuple[int, str, str]] = []
        for m in await self.group_members(group_id):
            name = str(m.get("card") or m.get("nickname") or "").strip()
            if name and name in text:
                hit.append((len(name), name, str(m.get("user_id") or "")))
        if not hit:
            return "", ""
        hit.sort(reverse=True)
        if len(hit) > 1 and hit[0][0] == hit[1][0]:
            return "", ""
        return hit[0][2], hit[0][1]

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
        """问两处:DeepSeek 官网余额 + 阿里云账户(现金+券)。"""
        ds, coupon = await asyncio.gather(
            self.fetch_ds_balance(),
            self.fetch_aliyun_money(),
        )
        parts = [p for p in (ds, coupon) if p]
        if not parts:
            return "没查到余额,几个 key 可能都没配上"
        return "鲸鲸的白饭账本:\n\n" + "\n\n".join(parts)

    # --- 群管:禁言 / 踢人 ---

    async def check_spam(self, event: dict[str, Any], text: str) -> None:
        """同一个人短时间里反复刷同一句话,就先禁言。
        故意做成硬规则、不交给模型裁决 —— 踢人拉不回来,禁言到点自己会解。
        """
        if not self.cfg.get("auto_mute_spam", True) or not text:
            return
        if len(text) < int(self.cfg.get("spam_min_chars", 4)):
            return  # 「6」「哈哈」这种跟着刷的,不算
        group_id = event.get("group_id")
        user_id = str(event.get("user_id") or "")
        if not (group_id and user_id):
            return
        window = float(self.cfg.get("spam_window", 15))
        need = int(self.cfg.get("spam_count", 6))
        now = time.time()
        per_group = self.spam_track.setdefault(str(group_id), {})
        hist = [x for x in per_group.get(user_id, []) if now - x[0] <= window]
        hist.append((now, text))
        per_group[user_id] = hist
        if len([x for x in hist if x[1] == text]) < need:
            return
        # 还有别人也在刷同一句 —— 那是群里在接梗复读,不是他一个人刷,别动
        for other, h in per_group.items():
            if other == user_id:
                continue
            if len([x for x in h if x[1] == text and now - x[0] <= window]) >= need // 2:
                return
        per_group[user_id] = []  # 认定了就清掉,别重复禁
        if user_id in [str(u) for u in (self.cfg.get("admin_users") or [])]:
            return
        if await self.member_role(group_id, user_id) in ("owner", "admin"):
            return
        if await self.member_role(group_id, self.self_id) not in ("owner", "admin"):
            log.warning("有人刷屏,可我不是群管理,禁不了")
            return
        day = datetime.now().strftime("%Y-%m-%d")
        if self.mute_count.get("day") != day:
            self.mute_count = {"day": day, "n": 0}
        if self.mute_count["n"] >= int(self.cfg.get("spam_mute_max_per_day", 10)):
            return
        seconds = int(self.cfg.get("spam_mute_seconds", 600))
        resp = await self.call_api_wait(
            "set_group_ban",
            {"group_id": group_id, "user_id": user_id, "duration": seconds},
        )
        if resp.get("status") != "ok":
            log.warning("刷屏禁言没成功: %s", api_fail_reason(resp))
            return
        self.mute_count["n"] += 1
        who = await self.member_name(group_id, user_id) or user_id
        log.info("刷屏禁言: %s %s 秒", who, seconds)
        await self.reply(event, f"{who} 别刷了 先歇 {seconds // 60} 分钟")

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
        if not targets and head in ("踢", "踢人"):
            # 没人 @、也没引用,那大概是「把 XX 踢了」这种说法,自己认一下
            one, _name = await self.guess_kick_target(event, self.parse_message(event)[0])
            if one:
                targets = [one]
            else:
                return "踢谁?@ 一下、引用他那条消息,或者直接把名字打出来都行"
        if not targets:
            return "要 @ 一个人,比如 /禁言 @某人 10分钟"
        # 踢人这动作是「我这个号」发出去的,QQ 只认我自己的身份,不认指使的人
        self_role = await self.member_role(group_id, self.self_id)
        for t in targets:
            role = await self.member_role(group_id, t)
            if role == "owner":
                return "那是群主。QQ 不许踢群主 —— 只能他自己退群,或者先把群主转给别人"
            if role == "admin" and self_role != "owner":
                return "他也是管理员,我这号也是管理员就压不住他 —— 把我这号设成群主就能踢了,或者你自己在群里点一下"
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
        raw = strip_at_head(text)
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
        elif head == "版本":
            await self.reply(event, f"小鲸鱼 v{VERSION}")
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
        elif head in ("撤回", "撤销"):
            await self.reply(event, await self.do_recall(event))
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
        self.proactive_backoff[session_key] = 1  # 有人说话了,冷场冒泡的退避重置
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
        """冷场时主动说一句。

        一直没人说话就一次比一次晚:每冒完一次泡还是没人接,下次的等待时间翻倍
        (45→90→180 分钟…,到 `proactive_backoff_max` 倍封顶);群里一有人说话就归位。
        """
        if not self.cfg.get("proactive", True):
            return
        if self.in_quiet_hours():
            return
        idle = float(self.cfg.get("proactive_idle_minutes", 45)) * 60
        need = int(self.cfg.get("proactive_min_messages", 6))
        sulk = int(self.cfg.get("sulk_after", 5))
        cap = max(1, int(self.cfg.get("proactive_backoff_max", 6)))
        now = time.time()
        for key, last in list(self.last_active.items()):
            if not key.startswith("group_"):
                continue
            # 退避倍数越大,越要等更久才冒泡
            backoff = max(1, int(self.proactive_backoff.get(key, 1)))
            if now - last < idle * backoff:
                continue
            if (self.stats.get(key) or {}).get("count", 0) < need:
                continue
            # 上次说完话之后一直没人接,那就别再往上凑了
            if sulk and self.silent_streak.get(key, 0) >= sulk:
                log.info("%s 被冷落 %d 条了,先不冒泡", key, self.silent_streak[key])
                continue
            if random.random() > float(self.cfg.get("proactive_chance", 0.35)):
                continue
            # 到这一步说明又是靠冷场触发的,多半还是没人理:下次隔更久(翻倍,封顶)
            self.proactive_backoff[key] = min(backoff * 2, cap)
            self.last_active[key] = now  # 冒过泡就重新计时,别连着蹦出来
            line = await self.proactive_line(key)
            if not line:
                continue
            log.info(
                "主动冒个泡:%s(下次至少隔 %d 分钟)",
                line,
                int(idle * self.proactive_backoff[key] // 60),
            )
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
                self.web_cache.save()
                self.vision_cache.save()
            except Exception:  # noqa: BLE001
                log.exception("主动消息/日报出错")

    # ---------- 偷表情包 ----------

    async def steal_stickers(self, images: list[str], event: dict[str, Any]) -> None:
        """群里别人发的表情包,顺手收进自己库里。

        识图这一步按「准确性优先」:判断标准写严、token 给足,
        不是表情包一律不收;同一种情况(情绪/场景)的只留一张。
        """
        if not self.cfg.get("steal_stickers", True):
            return
        today = datetime.now().strftime("%Y-%m-%d")
        if self.steal_count.get("day") != today:
            self.steal_count = {"day": today, "n": 0}
        if self.steal_vision.get("day") != today:
            self.steal_vision = {"day": today, "n": 0}
        limit = int(self.cfg.get("steal_max_per_day", 8))
        vision_limit = int(self.cfg.get("steal_vision_per_day", 50))
        min_conf = float(self.cfg.get("steal_min_confidence", 0.6))
        for ref in images[:2]:
            if self.steal_count["n"] >= limit:
                return
            raw = await self._fetch_image_bytes(ref)
            if not raw or len(raw) < 5000:  # 太小的多半是缩略图
                continue
            digest = hashlib.md5(raw).hexdigest()
            # 库里已有的、以及她自己生图画出来的,都不收 —— 表情包只能是从群里别人那儿来的
            if digest in self.known_hashes or digest in self.gen_hashes:
                continue
            # 见过一次就算数(steal_min_seen 想调回「见过几次才收」就把它调大)。
            # 以前默认 2,结果群里一口气发一堆新图时每张都只出现一次,一张都收不到
            seen = self.seen_counts.get(digest, 0) + 1
            self.seen_counts[digest] = seen
            if seen < int(self.cfg.get("steal_min_seen", 1)):
                log.info("这张图第 %d 次见,还不够格,先不收", seen)
                continue
            kind = str(
                mimetypes.guess_type(ref.split("?")[0])[0] or "image/jpeg"
            ).lower()
            ext = IMAGE_EXT.get(kind, ".jpg")
            # 这张图识过就不用再看一遍:照片/截图这类永远收不进库,以前每被发一次都要重新识,
            # 又慢又费;命中缓存直接拿上次的结论(原图照样原样存库,压的只是送去识图那份)
            verdict = self.vision_cache.get(digest)
            if verdict:
                log.info("这张图识过了,直接用上次的结论,不再重复识图")
            else:
                # 识图要花钱,每天给个天花板:到点了就先不收新图,别把一年的额度烧光。
                # 缓存命中的不算,所以反复发的图基本不占额度
                if self.steal_vision["n"] >= vision_limit:
                    log.info("今天偷图的识图次数已经用满了(%d 次),新的图先不收", vision_limit)
                    return
                # token 给足,让它把「是不是表情包、什么时候用」看仔细(准确性优先)。
                # 偷表情包可以单独配一个更强的识图模型(steal_vision_model),和普通看图分开
                self.steal_vision["n"] += 1
                verdict = await self.ask_vision(
                    STEAL_PROMPT, raw, max_tokens=320,
                    model=str(self.cfg.get("steal_vision_model") or ""),
                )
                self.vision_cache.put(digest, verdict)
            start, end = verdict.find("{"), verdict.rfind("}")
            try:
                info = json.loads(verdict[start : end + 1])
            except Exception:  # noqa: BLE001
                continue
            if not info.get("meme"):
                continue
            conf = info.get("confidence", 1.0)
            try:
                conf = float(conf)
            except (TypeError, ValueError):
                conf = 1.0
            if conf < min_conf:  # 拿不准的宁可不要,别把非表情包收进来
                log.info("识图只有 %.2f 确信是表情包,不到 %.2f,不收", conf, min_conf)
                continue
            emotion = str(info.get("emotion") or "无语")[:8]
            scene = str(info.get("scene") or "")[:40]
            tags = [str(t)[:8] for t in (info.get("tags") or [])][:5]
            taken = self._scene_taken(emotion, scene, tags)
            if taken:  # 同一种情况已经有一张了,留旧的就行
                log.info("「%s」这种表情包库里有了(%s),这张就不留了", emotion, taken)
                continue
            name = f"偷来的_{digest[:6]}{ext}"
            target = STICKER_DIR / name
            try:
                STICKER_DIR.mkdir(exist_ok=True)
                target.write_bytes(raw)
                item = {
                    "file": name,
                    "emotion": emotion,
                    "scene": scene,
                    "tags": tags,
                }
                self._append_sticker_index(item)
            except Exception as exc:  # noqa: BLE001
                log.warning("偷表情包没存住:%s", exc)
                continue
            self.stickers.append(item)
            self.known_hashes.add(digest)
            self.steal_count["n"] += 1
            log.info("偷到一张表情包:%s(%s)", name, emotion)

    def _scene_taken(self, emotion: str, scene: str, tags: list[str] | None = None) -> str:
        """库里有没有「同一种情况」的表情包了?有就返回那张的文件名,同种情况只留一张。

        三样一起比,撞上一样就算重合:
        - 情绪:一样、或者互相包含(「得意」和「得意忘形」算同一类);
        - 场景:互相包含;
        - 标签:按重合比例算 —— 新图和旧图谁的标签少,就以谁为分母,
          重合到这个比例(≥ `sticker_tag_similar`,默认 0.6)就算重合。
          不用交并比:两张图标签数常常不一样(识图模型每张给 3~5 个),
          交并比会被标签多的那张把分母撑大 —— 新图 4 个标签里撞了 3 个,交并比才 0.5,
          明明「撞了一大半」却判成不重合,该省的没省下来。
          表情包本来就带好几个标签,两张图标签撞到这个程度,基本就是同一个使用场合,
          收进来只是白占地方(用户要的就是「省空间,但每个情况都有一张能用的」)。
        阈值调高就不容易判重、库里留得多;调低就删得狠、更省地方。
        """
        e, s = tag_key(emotion), tag_key(scene)
        # 角色名(奶龙/二次元)是固定值,不是「使用场合」,剔掉再比 ——
        # 不剔的话库里清一色同一只角色的图,谁跟谁都像同款,会误判成重合。
        new_tags = {
            tag_key(t)
            for t in (tags or [])
            if tag_key(t) and t not in STICKER_CHAR_TAGS
        }
        need = float(self.cfg.get("sticker_tag_similar", STICKER_TAG_SIMILAR))
        for item in self.stickers:
            old_e = tag_key(item.get("emotion") or "")
            old_s = tag_key(item.get("scene") or "")
            if e and old_e and (e == old_e or e in old_e or old_e in e):
                return str(item.get("file") or "")
            if s and old_s and (s == old_s or s in old_s or old_s in s):
                return str(item.get("file") or "")
            if len(new_tags) >= 2:
                old_tags = {
                    tag_key(t)
                    for t in (item.get("tags") or [])
                    if tag_key(t) and t not in STICKER_CHAR_TAGS
                }
                if len(old_tags) >= 2:
                    shared = len(new_tags & old_tags)
                    if shared / min(len(new_tags), len(old_tags)) >= need:
                        return str(item.get("file") or "")
        return ""

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
        asyncio.create_task(self.topic_loop())
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
    log.info("小鲸鱼 v%s 启动中…", VERSION)
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
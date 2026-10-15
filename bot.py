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
import html
import json
import logging
import mimetypes
import random
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
import websockets

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"

# 本地表情包库:图片放这里,标签写在 tags.json 里
STICKER_DIR = BASE_DIR / "表情包"
STICKER_INDEX = STICKER_DIR / "tags.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
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
    "1. 只有用户明确要你产出图片时(例如「生成一张奶龙」「帮我画只柯基」「来张 XX 的图」),"
    "才不要拒绝、也不要说自己做不到,只回复一行:`[DRAW] 画面描述`"
    "(除了这一行不要有任何别的字),描述要具体。\n"
    "2. 如果用户只是在问你有没有画图能力,或者只是聊天里提到「画」字,"
    "都当普通聊天正常回话,不要输出 [DRAW]。\n"
    "3. 其他任何情况都正常聊天。\n"
    "4. 群聊回复必须短:1~2 句、40 字以内。像普通人发微信那样说话——"
    "口语、短句,可以不带标点结尾,不许写列表、标题、Markdown、分点罗列。\n"
    "5. 只有用户明确说「详细讲讲」「解释下原理」时才展开,"
    "展开也只讲结论和要点,不要铺垫,不要总结收尾。\n"
    "6. 别露出 AI 味:不要说「作为AI」「请问有什么可以帮您」「希望以上内容对您有帮助」"
    "这类客服话术;不要解释自己在干什么,不要升华总结,不要每次都热情收尾。\n"
    "7. emoji 和颜文字最多一个,大多数时候一个都别用。"
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


# 模型想发表情包时用的标记。标签清单是运行时按表情包库生成再拼进来的。
STICKER_HINT = (
    "\n\n【表情包】\n"
    "情绪到了的时候,可以在回复最后另起一行写:`[STICKER] 标签`,\n"
    "表示「顺便甩张表情包」。标签只能从这份清单里挑一个:{tags}\n"
    "大约每三四条回复才发一次,不要连着发,更不要每条都发;\n"
    "正经回答、算数、查资料这类回复一律不发。觉得不合适就干脆不写。"
)


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
        # 本地表情包库(读不到就是空的,功能自动隐身)
        self.stickers, self.sticker_tags = self._load_stickers()

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

    def pick_sticker(self, tag: str = "") -> Path | None:
        """按标签挑一张表情包,没标签或没匹配上就随机来一张。"""
        if not self.stickers:
            return None
        pool = self.stickers
        if tag:
            hit = [
                item
                for item in self.stickers
                if any(tag in t or t in tag for t in item.get("tags") or [])
            ]
            if hit:
                pool = hit
        return STICKER_DIR / random.choice(pool)["file"]

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

    async def ask_deepseek(
        self,
        session_key: str,
        user_text: str,
        images: list[str] | None = None,
        use_search: bool = False,
    ) -> str:
        history = self.history[session_key]
        images = images or []

        # 本轮消息:带图就拼成多模态内容
        current: Any = user_text
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
                parts.append({"type": "text", "text": user_text or "看看这张图"})
                current = parts
            elif lost:
                # 一张都没取到就直说,别让模型对着空图瞎猜
                note = "(图我这边没取到,你重新发一次,或者直接描述一下图上是什么也行)"
                current = f"{user_text} {note}".strip()

        max_history = int(self.cfg.get("max_history", 20))
        messages: list[dict[str, Any]] = []
        # 人设归 config,画图 / 搜索这些技术约定由代码追加,改人设不会把功能改坏
        system_prompt = (self.cfg.get("system_prompt") or "") + ROUTING_HINT
        if use_search and not images:
            system_prompt += SEARCH_HINT
        if self.sticker_tags and not images:
            system_prompt += STICKER_HINT.format(tags="、".join(self.sticker_tags[:30]))
        messages.append({"role": "system", "content": system_prompt})
        messages.extend(history[-max_history:])
        messages.append({"role": "user", "content": current})

        url = self.cfg.get("deepseek_base_url", "https://api.deepseek.com").rstrip("/")
        url += "/chat/completions"
        # 带图走视觉模型,联网走搜索模型,其余用默认的快模型
        if images and self.cfg.get("vision_model"):
            model = self.cfg["vision_model"]
        elif use_search:
            model = self.cfg.get("search_model") or self.cfg.get("model", "")
        else:
            model = self.cfg.get("model", "")

        payload: dict[str, Any] = {"model": model, "messages": messages, "stream": False}
        # 聊天回复单独限长:max_tokens 还要留给生图提示词用,别一起调小
        payload["max_tokens"] = int(
            self.cfg.get("reply_max_tokens") or self.cfg.get("max_tokens", 400)
        )
        # 把 config 里额外的 OpenAI 兼容参数原样透传
        for key in ("temperature", "top_p"):
            if key in self.cfg:
                payload[key] = self.cfg[key]
        if use_search and not images:
            payload["enable_search"] = True  # 搜索模型不支持关思考,别一起传
        for key in ("enable_thinking", "thinking"):
            if key in self.cfg and "enable_search" not in payload:
                payload[key] = self.cfg[key]

        headers = {
            "Authorization": f"Bearer {self.cfg['deepseek_api_key']}",
            "Content-Type": "application/json",
        }

        resp = await self.http.post(url, json=payload, headers=headers)
        if resp.status_code != 200:
            log.error("模型返回 %s: %s", resp.status_code, resp.text[:300])
            raise RuntimeError(f"模型接口错误 {resp.status_code}")

        content = (resp.json()["choices"][0]["message"].get("content") or "").strip()

        # 上下文里只留纯文字,避免每轮重传整张图
        history.append(
            {"role": "user", "content": f"{user_text} [图片]".strip() if images else user_text}
        )
        # [DRAW] / [IMG] / [STICKER] 只是给程序看的标记,别原样留在上下文里
        _, want_draw = split_draw_marker(content)
        if want_draw:
            stored = "[已生成图片]"
        else:
            stored, _ = split_image_marker(content)
            stored, _ = split_sticker_marker(stored)
        history.append({"role": "assistant", "content": stored})
        # 只保留最近 max_history 条,防止上下文无限增长
        if len(history) > max_history:
            del history[:-max_history]
        return content

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
        """用 Bing 图片搜索找相关图,返回可发送的 base64 列表。"""
        images: list[str] = []
        for url in await self.bing_image_urls(query, count * 3):  # 多挑几个当备胎
            if len(images) >= count:
                break
            raw = await self._fetch_image_bytes(url)
            if raw:
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
        return (resp.json()["choices"][0]["message"].get("content") or "").strip()

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

        # 群里常见操作:先单独发张图,再 @ 机器人问「这是什么」。
        # 所以图片消息就算没点它,也先记下来,供后面几条消息用。
        if images:
            self.recent_images[session_key] = (time.time(), images)

        ask_text = self.should_reply(event, text, at_bot, bool(images))
        if ask_text is None:
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
            sticker = self.pick_sticker(tag)
            if sticker:
                log.info("手动发表情包:%s(%s)", sticker.name, tag or "随机")
                await self.send_sticker(event, sticker)
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
            event.get("sender", {}).get("nickname", "?"),
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
            try:
                answer = await self.ask_deepseek(
                    session_key, ask_text, images=images, use_search=use_search
                )
            except Exception as exc:  # noqa: BLE001
                log.exception("生成回复出错")
                answer = f"(出错了:{exc})"
            log.info("生成耗时 %.1fs", time.perf_counter() - started)

            # 没打 /画,但模型自己判断出用户是在要图(自然语言)
            guess, want_draw = split_draw_marker(answer)
            if want_draw:
                log.info("模型判定要画图:%s", guess[:60])
                # 拿用户原话去联网查准,模型的理解只当参考(它可能压根不认识那角色)
                await self.send_generated_image(event, ask_text, guess)
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
                sticker = self.pick_sticker(sticker_tag)
            log.info("表情包:%s(%s)", sticker.name if sticker else "不发", sticker_tag or "-")

            if answer:
                await self.reply(event, answer)
            if sticker:
                await self.send_sticker(event, sticker)

            # 联网搜索顺手把相关图片也发过去
            if img_keyword and self.cfg.get("search_images", True):
                await self.send_search_images(event, img_keyword)

    # ---------- 主循环 ----------

    async def run(self) -> None:
        url = self.cfg["onebot_ws_url"]
        headers = {}
        token = self.cfg.get("onebot_access_token")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        headers["User-Agent"] = "qq-deepseek-bot/1.0"

        while True:
            try:
                async with websockets.connect(
                    url, additional_headers=headers, max_size=None
                ) as ws:
                    self.ws = ws
                    log.info("已连接 OneBot:%s", url)
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(data, dict) and "post_type" in data:
                            asyncio.create_task(self.on_event(data))
            except Exception as exc:  # noqa: BLE001
                self.ws = None
                log.warning("与 OneBot 的连接断开(%s),5 秒后重连……", exc)
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


def main() -> None:
    cfg = load_config()
    if "--check" in sys.argv:
        asyncio.run(check_api(cfg))
        return
    try:
        asyncio.run(Bot(cfg).run())
    except KeyboardInterrupt:
        log.info("已退出")


if __name__ == "__main__":
    main()
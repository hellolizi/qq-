"""把 NapCat 的登录二维码直接画在终端里,手机对着屏幕扫就行,不用去开图片文件。

窗口放不下的时候会自动弹一个图片窗口兜底,反正不用你手动去翻文件夹。

用法:
    python 二维码.py              立刻画当前存在的码,画完等你按回车
    python 二维码.py --wait 120   最多等 120 秒等新码出现,出现就画
    python 二维码.py --watch      一直挂着,每出新码就重画(手动登小号时用)
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from PIL import Image, ImageOps

ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "NapCat" / "cache"
DST = ROOT / "二维码.png"

FRESH_SECONDS = 600   # 超过这个岁数的码算过期,别画出来让人扫
SAME_BATCH = 120      # 同一批文件里按这个时间窗挑最大的(最清晰)
MIN_PIXELS = 100      # 宽度小于这个的,不是正经二维码
DARK = 128
QUIET = 3             # 左右各留几个字符的静默区,没有它扫不出来


def candidates() -> list[Path]:
    """库里所有可能是二维码的图:NapCat 缓存里的 + 我们自己存的。"""
    files = list(CACHE.glob("qrcode*.png"))
    if DST.exists():
        files.append(DST)
    return [p for p in files if time.time() - p.stat().st_mtime < FRESH_SECONDS]


def pick(files: list[Path]) -> Image.Image | None:
    """挑最新那批里最大的一张,并确认它是张像样的二维码。"""
    if not files:
        return None
    newest = max(files, key=lambda p: p.stat().st_mtime)
    batch = [
        p for p in files if newest.stat().st_mtime - p.stat().st_mtime < SAME_BATCH
    ]
    for path in sorted(batch, key=lambda p: p.stat().st_size, reverse=True):
        try:
            img = Image.open(path)
            img.load()
        except Exception:
            continue
        if img.width >= MIN_PIXELS:
            if path != DST:
                img.save(DST)  # 顺手覆盖存一份,方便手动扫
            return img.convert("L")
    return None


def term_size() -> tuple[int, int]:
    try:
        size = os.get_terminal_size()
        return size.columns, size.lines
    except OSError:
        return 80, 25


def fit_console(cols: int, lines: int) -> None:
    """尽量把窗口撑大,好把二维码画进来。撑不大就算了,后面会退回弹图片。"""
    if term_size()[1] >= lines:
        return
    try:
        subprocess.run(
            ["mode", "con", f"cols={cols}", f"lines={lines}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
            shell=True,
        )
    except Exception:
        pass
    if term_size()[1] >= lines:
        return
    # mode 命令在 Windows Terminal 里不管用,直接调 API 改
    try:
        import ctypes
        from ctypes import wintypes

        k = ctypes.windll.kernel32
        handle = k.GetStdHandle(-11)
        if not handle or handle == -1:
            return
        small = wintypes.SMALL_RECT(0, 0, 39, 9)  # 先把视口缩到最小,免得挡住缓冲区调整
        k.SetConsoleWindowInfo(handle, True, ctypes.byref(small))
        if not k.SetConsoleScreenBufferSize(handle, (cols & 0xFFFF) | (lines << 16)):
            return
        view = wintypes.SMALL_RECT(0, 0, cols - 1, min(lines, 50) - 1)
        k.SetConsoleWindowInfo(handle, True, ctypes.byref(view))
    except Exception:
        pass


def crop_blank(img: Image.Image) -> Image.Image:
    """裁掉二维码自带的空白边:那圈白边占 8 个模块,小窗口就靠省下它塞进去。"""
    mask = ImageOps.invert(img.point(lambda v: 0 if v < DARK else 255))
    box = mask.getbbox()
    return img.crop(box) if box else img


def module_count(img: Image.Image) -> int:
    """数这张二维码有多少个模块(含自带静默区)。

    取若干行的连续同色段,抖掉抗锯齿产生的碎片,取出现次数最多的那个长度当一个模块。
    """
    width, height = img.size
    px = img.load()
    counts: dict[int, int] = {}
    for i in range(1, 16):
        row = min(height - 1, height * i // 16)
        prev, cur = px[0, row] < DARK, 1
        for x in range(1, width):
            now = px[x, row] < DARK
            if now == prev:
                cur += 1
            else:
                if width * 0.005 < cur <= width / 8:  # 太短是锯齿,太长是白边
                    counts[cur] = counts.get(cur, 0) + 1
                cur, prev = 1, now
    if not counts:
        return 21
    unit = max(counts.items(), key=lambda kv: (kv[1], -kv[0]))[0]
    return max(21, round(width / unit))


def render(img: Image.Image, width: int, top_blank: bool = True) -> str:
    """把二维码画成黑白方块:一格 = 一个模块,上下两行用半块拼成方形。"""
    height = round(img.height * width / img.width)
    height += height % 2
    px = img.resize((width, height), Image.NEAREST).load()

    def dark(x: int, y: int) -> bool:
        return px[x, y] < DARK

    margin = " " * QUIET  # 左右白边留着,扫码靠它
    lines = [margin * 2] if top_blank else []
    for y in range(0, height, 2):
        cells = []
        for x in range(width):
            top, bottom = dark(x, y), dark(x, y + 1)
            cells.append("█" if top and bottom else "▀" if top else "▄" if bottom else " ")
        lines.append(margin + "".join(cells) + margin)
    return "\n".join(lines)


def fallback(msg: str) -> str:
    print(msg)
    print("  已经用图片窗口打开了,扫完关掉它就行。")
    try:
        os.startfile(DST)  # noqa: S606
    except Exception as exc:  # noqa: BLE001
        print(f"  图片也没打开({exc}),手动开一下 {DST.name} 吧")
    return "window"


def paint(text: str, newline: bool = True) -> None:
    """黑字白底刷出来,整块白底连静默区一起。"""
    sys.stdout.write("\x1b[30;107m" + text + "\x1b[0m" + ("\n" if newline else ""))
    sys.stdout.flush()


def draw(img: Image.Image, header: str = "") -> str:
    """把码画出来,返回画法:term(宽裕,能放大) / compact(塞满窗口) / window(放不下弹图片)。"""
    fit_console(120, 70)
    cols, lines = term_size()
    img = crop_blank(img)
    modules = module_count(img)
    qr_lines = modules // 2 + modules % 2   # 码本身占几行
    need_cols = modules + QUIET * 2

    if cols < need_cols or lines < qr_lines:
        return fallback(
            f"  终端窗口放不下这张码(要 {need_cols} 列 x {qr_lines} 行,现在 {cols}x{lines}),"
        )

    if lines >= qr_lines + 4:
        # 宽裕:每个模块整数倍放大,越大越好扫
        scale = max(1, min((cols - QUIET * 2) // modules, (lines - 4) * 2 // modules, 3))
        if header:
            print(header)
        paint(render(img, modules * scale))
        return "term"

    if lines >= qr_lines + 1:
        paint(render(img, modules))
        return "compact"

    # 一行都不多占,把整张码铺满窗口
    paint(render(img, modules, top_blank=False), newline=False)
    return "compact"


def pause(prompt: str) -> None:
    try:
        input(prompt)
    except (EOFError, KeyboardInterrupt):
        print()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wait", type=int, default=0, help="最多等多少秒等到二维码")
    ap.add_argument("--watch", action="store_true", help="一直监听,出新码就重画")
    ap.add_argument("--file", help="直接画指定文件(调试用)")
    args = ap.parse_args()

    if args.file:
        img = Image.open(args.file).convert("L")
        print("\n调试:直接画指定文件")
        draw(img, "扫码:")
        return 0

    deadline = time.time() + args.wait
    last, shown = "", False
    while True:
        files = candidates()
        newest = max(files, key=lambda p: p.stat().st_mtime, default=None)
        stamp = f"{newest}:{newest.stat().st_mtime}" if newest else ""

        if newest and stamp != last:
            img = pick(files)
            if img:
                last, shown = stamp, True
                mode = draw(img, "\n请用手机 QQ 扫下面的码登录:")
                if not args.watch:
                    # 紧凑模式下窗口被塞满了,多印一个字就会把码顶掉,所以什么都不印
                    pause("" if mode == "compact" else "\n扫码完成后按回车继续…")
                    return 0
                deadline = time.time() + args.wait

        if not args.watch and time.time() >= deadline:
            break
        time.sleep(2)

    if not shown:
        print("\n没发现新的登录二维码(可能已经登录过了),直接继续。")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n已停止。")
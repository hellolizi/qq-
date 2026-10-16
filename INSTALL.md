# 安装指南（Windows）

小鲸鱼其实就三样东西拼起来：**QQ + NapCatQQ + 一个 Python 文件**。
跟着下面走，一般十几分钟能在自己电脑上把它跑起来。

> 它是跑在**你自己电脑上**的，所以电脑得开着、QQ 得挂着。想 7×24 就丢到家里那台常开的机器上。

---

## 0. 先备齐

- 一台 Windows 电脑
- 一个 QQ 号（建议用小号，因为要长期挂在电脑上登录）
- 一个好用的大模型 Key（见第 4 步，OpenAI 兼容接口的都行）
- 不用会编程，会「双击 .bat」「复制粘贴」就够了

---

## 1. 装 Python

1. 去 <https://www.python.org/downloads/> 下 **3.10 或更高**
2. 安装时**一定要勾上 `Add Python to PATH`**（忘了勾会导致后面的命令全都找不到 python）
3. 装完开一个命令行，敲 `python --version`，能打印版本号就成

---

## 2. 把这份代码弄下来

```powershell
git clone https://github.com/hellolizi/qq-.git
cd qq-
pip install -r requirements.txt
```

没有 git 的话，在仓库页面点 **Code → Download ZIP**，解压后进目录再执行 `pip install -r requirements.txt`。

依赖就三个：`websockets`（连 NapCat）、`httpx`（调模型/搜索）、`Pillow`（表情包处理）。

---

## 3. 装 NapCatQQ 并登录

NapCat 是给 QQ 加钩子的程序，真正负责收发消息。**它不属于这个仓库，得你自己下。**

1. 从 NapCatQQ 的官方仓库下载，解压到本目录下的 `NapCat/`
2. 把 `loadNapCat.js` 放到 **QQ 主程序所在的同一个目录**
3. **用管理员权限**跑一次 NapCat —— 普通权限挂不上钩子，QQ 会「一闪就没了」
4. 扫码登录你的 QQ。仓库里的 `启动.bat` 会拉起 NapCat，并把登录二维码画在终端窗口里，手机对着屏幕扫就行

---

## 4. 配置 `config.json`

```powershell
copy config.example.json config.json
```

然后用记事本打开 `config.json` 填这几项：

**必需**

| 项 | 说明 |
| --- | --- |
| `onebot_ws_url` | NapCat 的地址，默认 `ws://127.0.0.1:3001` |
| `onebot_access_token` | 如果你在 NapCat 里设了 token 就填，没设留空 |
| `deepseek_base_url` / `deepseek_api_key` | 主模型服务，OpenAI 兼容的都能用 |
| `model` | 兜底聊天模型（没单独配 `chat_api_key` 时用它） |

**可选**

| 项 | 说明 |
| --- | --- |
| `chat_base_url` / `chat_api_key` / `chat_model` | 想让**文字聊天**单独走另一家（比如 DeepSeek 官网）再填，不填则一切照旧 |
| `aliyun_access_key_id` / `aliyun_access_key_secret` | 想查阿里云账户余额（现金 + 优惠券）才需要，见第 7 步 |
| `bot_names` | 别人怎么叫它算「点名」 |
| `admin_users` | 写死几个 QQ 号，让他们能用群管命令 |

> `config.json` 里放着你的 Key，**它已经在 `.gitignore` 里**，别修改后手滑传上去。

---

## 5. 在 NapCat 里开一个 WebSocket

在 NapCat 的 WebUI → **网络配置** → 新建一个 **正向 WebSocket**：

- 端口填 `3001`（要和 `onebot_ws_url` 对得上）
- 如果设了 token，把它同步填回 `config.json` 的 `onebot_access_token`

---

## 6. 启动

双击 **`启动.bat`**，它会：拉起 NapCat → 画二维码 → 启动 `bot.py`。

看到日志里出现 `已连接 OneBot` 就成功了。然后：

- 群里 **@ 它**，或者**直接叫它的名字**（`bot_names` 里那几个）
- 私聊直接说话也行

---

## 7. 验证一下

在群里发 **`/帮助`**，或者直接问 **「你会啥」**，它会把自己的本事回给你。

想确认余额那段能查：

1. 去阿里云控制台建一个 **RAM 子账号**
2. 只给它两条只读权限：`bss:QueryAccountBalance` 和 `bss:DescribeCoupons`
3. 把它的 AccessKey 填进 `aliyun_access_key_id` / `aliyun_access_key_secret`
4. 重启，发 `/余额`，就能看到官网余额 + 阿里云账户余额两段

不填也能用，只是 `/余额` 不显示阿里云那一段，不会报错。

---

## 8. 表情包（可选）

往 `表情包/` 目录里丢图就行，标签写在同目录的 `tags.json`。
群里别人发的表情包，它也会自己看一眼、够格就收进来，下次聊天能用。

---

## 常见问题

**QQ 一闪就没了 / NapCat 注入不上**
`loadNapCat.js` 得和 QQ 主程序放同一个目录，而且启动 NapCat 要给管理员权限。

**它突然变傻 / 不理人**
先看命令行窗口有没有报 **400**。多半是 `chat_model` 填了个 API 不认的名字 —— 必须是官方认的
（比如 DeepSeek 官网是 `deepseek-flash` / `deepseek-v4-pro`，写别的会被拒）。另外**思考模式默认是开的**，
`chat_thinking` 记得设成 `false`，不然又慢又贵。

**几十秒断一次线**
已经关掉了客户端心跳，改成看 NapCat 自己的心跳（90 秒没动静才重连）。出问题先看 `bot.log`。

**发图 @ 它没反应**
图和 @ 可以分两条发，`recent_image_seconds`（默认 180 秒）内追问都还算数。

**`/余额` 查不到阿里云那段**
没填 `aliyun_access_key_id/secret` 时，这一段就是不显示的，正常现象。

---

跑起来了？回 [README](README.md) 看看它到底都会些什么，或者直接去
[Issues](https://github.com/hellolizi/qq-/issues) 找我提问题 / 报 bug。

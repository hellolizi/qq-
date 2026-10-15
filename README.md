# 小鲸鱼 · QQ 群机器人

把大模型接进 QQ 群，让她像群友一样聊天，认真问题时又很靠谱。

聊天、识图、联网搜索、AI 生图四件事各用各的模型，谁强用谁。

---

## 能干什么

| 能力 | 怎么触发 | 说明 |
| --- | --- | --- |
| 聊天 | 群里 @ 她，或私聊 | 短句口语，看不出来是机器人 |
| 识图 | 先发图，再 @ 她问 | 图和 @ 是两条消息也能接上（默认 180 秒内） |
| 联网搜索 | `/搜 问题` | 查完自动判断该不该配图，能配就顺手发图 |
| AI 生图 | `/画 描述`，或直接说「生成一张 XX 的图」 | 1080p，画角色前先联网核对长相，再用参考图引导 |
| 重置上下文 | 发 `重置` / `清空` | 清掉这个会话的聊天记录 |

群聊默认需要 @ 才回复（`group_trigger` 可改）。

## 原理

```
QQ 客户端
   │
NapCatQQ（注入 QQ，实现 OneBot v11 协议）
   │  正向 WebSocket  ws://127.0.0.1:3001
   ▼
bot.py（本仓库）
   │  OpenAI 兼容接口
   ▼
大模型服务（qianwen / DeepSeek 等）
```

`bot.py` 只做三件事：收 OneBot 事件 → 按任务挑模型问一句 → 把结果发回群里。

人设写在 `config.json` 里，而「什么时候该生图 / 该配图」这类技术约定由代码追加，
所以你可以随便改人设，不会把功能改坏。

## 目录

```
bot.py                机器人主程序
config.example.json   配置模板（复制成 config.json 再改）
requirements.txt      Python 依赖
启动.bat             一键启动（NapCat + 二维码监听 + 机器人）
二维码.bat           单独启动二维码监听
watch_qr.ps1         监听 NapCat 登录二维码，有新码就覆盖存成 二维码.png
NapCat/              第三方程序本体，需自行下载，未包含在本仓库
```

## 跑起来

1. **装依赖**

   ```
   pip install -r requirements.txt
   ```

2. **装 NapCatQQ**，注入 QQ 并扫码登录
   下载后解压到本目录的 `NapCat/`，首次要用管理员权限运行一次。

3. **配 OneBot 正向 WebSocket**
   在 NapCat 的 WebUI 里开一个正向 WS 服务，端口和 `onebot_ws_url` 一致（默认 3001）。

4. **写配置**

   ```
   copy config.example.json config.json
   ```

   然后把 `deepseek_api_key` 换成你自己的 Key。
   `config.json` 已在 `.gitignore` 里，不会被提交。

5. **启动**

   双击 `启动.bat`。会依次拉起 NapCat、二维码监听和机器人。
   登录二维码会存成 `二维码.png`，扫一下就行。

## 主要配置项

| 字段 | 作用 |
| --- | --- |
| `onebot_ws_url` | NapCat 的正向 WS 地址 |
| `deepseek_base_url` / `deepseek_api_key` | 模型服务地址和 Key（OpenAI 兼容） |
| `model` | 日常聊天用的快模型 |
| `vision_model` | 识图用的多模态模型 |
| `search_model` | 联网搜索用的模型 |
| `image_model` | 生图模型 |
| `draw_prompt_model` | 生图前联网核对角色的模型 |
| `reply_max_tokens` | 聊天回复长度上限（生图提示词不受影响） |
| `max_history` | 每个会话保留多少轮上下文 |
| `recent_image_seconds` | 发图后多少秒内 @ 她还能认出这张图 |
| `group_trigger` | 群聊触发方式：`at` / `prefix` / `all` |
| `system_prompt` | 人设，随便改 |
| `safe_domains` | 域名白名单，不在名单里的图片一律拒绝下载 |
| `ref_image_enabled` | 画角色时是否先用参考图引导，像不像就靠它 |

不同能力对应不同模型，是为了在速度、画质、搜索准确率之间各取所需，
不是所有活都丢给同一个模型。

## 安全

- **域名白名单**：只允许访问 `safe_domains` 里的站点，其余直接拒绝，不会把请求发出去。
- **Key 不外传**：`config.json` 被 git 忽略，仓库里只有占位符版本的 `config.example.json`。
- **上下文只留文字**：图片不会每轮重传，聊天记录也不会无限增长。

## 常见问题

**QQ 一闪就没了 / NapCat 注入失败**
`loadNapCat.js` 需要和 QQ 主程序在同一目录，且启动 NapCat 要给管理员权限。

**发图 @ 她没反应**
确认是在 `recent_image_seconds` 时间内 @ 的；再不行看日志里有没有「图片获取失败」。

**连不上 github**
本机 443 不通的话，走代理：`git -c http.proxy=http://127.0.0.1:7890 push`
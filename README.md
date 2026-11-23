# 字幕生成 API

一个基于 FastAPI 的异步字幕生成服务。客户端上传音频后即可取得任务 ID，并通过状态接口查看排队和识别进度；任务结果可下载为 LRC、SRT、WebVTT 或 JSON。服务将任务与结果保存在 SQLite 中，进程重启后会恢复未完成的任务。

## 功能

- 接受常见音频格式并转换为 16 kHz 单声道 WAV 后识别。
- 使用 `whisper-server` 提供语音识别；服务在有任务时按需启动它。
- 单 worker 顺序处理任务，SQLite 队列持久化，启动时恢复中断任务。
- 上传时计算 SHA-256；若内容、语言和识别配置与保留期内已成功的任务一致，则新建一条直接完成的任务记录并复用字幕结果。相同文件若仍在排队或处理中，不会合并。
- 提供任务创建、状态查询、结果下载和删除 API，以及本机管理页面。
- 可配置 API Key、上传大小、队列长度、失败重试次数和结果保留时间。

## 环境要求

- Python 3.10 或更高版本。
- 可执行的 `whisper-server`（whisper.cpp），且命令可从 `PATH` 找到。
- Whisper 主模型和 VAD 模型文件。默认路径为 `/Users/lyaotian/my/models/ggml-large-v3-q5_0.bin` 和 `/Users/lyaotian/my/models/ggml-silero-v5.1.2.bin`，应按部署环境设置路径。
- `ffmpeg`（推荐，需包含 `ffprobe` 以探测音频时长）。macOS 可使用 `afconvert` 作为转换后备、`afinfo` 作为时长探测后备。

安装 Python 依赖：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows 当前不可直接运行：程序使用 `fcntl.flock` 实现数据目录单实例锁。

## 配置

所有配置通过环境变量设置；未设置时使用下表默认值。

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WHISPER_MODEL` | `/Users/lyaotian/my/models/ggml-large-v3-q5_0.bin` | Whisper 主模型路径 |
| `WHISPER_VAD_MODEL` | `/Users/lyaotian/my/models/ggml-silero-v5.1.2.bin` | VAD 模型路径 |
| `WHISPER_HOST` | `127.0.0.1` | whisper-server 监听地址，同时作为 API 调用地址 |
| `WHISPER_PORT` | `8080` | whisper-server 端口 |
| `DEFAULT_LANGUAGE` | `ja` | 默认识别语言；上传任务可单独指定 |
| `DATA_DIR` | `./data` | SQLite 数据库、任务文件和 whisper-server 日志目录 |
| `MAX_UPLOAD_MB` | `500` | 单个上传文件大小上限（MB） |
| `MAX_QUEUE` | `50` | 等待处理的任务数量上限，不含正在运行的任务 |
| `MAX_ATTEMPTS` | `3` | 任务最多执行次数；服务重启打断也会计入 |
| `RESULT_TTL_SECONDS` | `86400` | 任务结束后保留时长（秒），默认 24 小时 |
| `SERVER_WAIT_SECONDS` | `180` | 启动 whisper-server 的等待秒数 |
| `API_KEY` | 空 | 非空时，API 请求需在 `X-API-Key` 请求头提供相同密钥 |
| `ADMIN_ALLOW_REMOTE` | 空 | 设置为 `1` 时允许远程访问管理页；默认仅本机可访问 |

例如：

```bash
export WHISPER_MODEL=/models/ggml-large-v3-q5_0.bin
export WHISPER_VAD_MODEL=/models/ggml-silero-v5.1.2.bin
export DATA_DIR=./data
export API_KEY='replace-with-a-long-random-secret'
```

## 启动

在项目目录执行：

```bash
uvicorn app:app --host 127.0.0.1 --port 5001
```

也可以单次设置密钥启动：

```bash
API_KEY='replace-with-a-long-random-secret' uvicorn app:app --host 127.0.0.1 --port 5001
```

服务必须以单个 Uvicorn worker 运行（不要使用 `--workers` 或多进程模式）。同一个 `DATA_DIR` 也只能由一个服务实例使用。默认监听本机；需要让其他机器调用时，可将 API 绑定到适当的网络地址，并通过防火墙或反向代理保护服务。`/admin` 页面没有登录认证，远程访问需显式设置 `ADMIN_ALLOW_REMOTE=1`，应只在受控网络和受保护的代理后开启。

启动后可打开：

- API 文档：`http://127.0.0.1:5001/docs`
- 管理页面：`http://127.0.0.1:5001/admin`

任务数据库位于 `DATA_DIR/jobs.db`；任务输入文件和 whisper-server 日志分别保存在 `DATA_DIR/jobs/` 和 `DATA_DIR/whisper-server.log`。

## API

如果设置了 `API_KEY`，以下所有 `/v1` 请求都需带认证头：

```bash
curl -H 'X-API-Key: replace-with-a-long-random-secret' http://127.0.0.1:5001/v1/jobs
```

### 创建任务

`POST /v1/jobs` 使用 `multipart/form-data`，字段 `file` 必填，`language` 可选（默认 `DEFAULT_LANGUAGE`）。支持扩展名：`.wav`、`.mp3`、`.m4a`、`.flac`、`.ogg`、`.opus`、`.aac`、`.wma`、`.webm`。

```bash
curl -X POST http://127.0.0.1:5001/v1/jobs \
  -H 'X-API-Key: replace-with-a-long-random-secret' \
  -F 'file=@episode.mp3' \
  -F 'language=ja'
```

创建请求返回 HTTP `202 Accepted` 和任务 ID。新任务通常为 `queued`；命中结果复用时，响应会直接显示 `succeeded`：

```json
{
  "id": "任务 ID",
  "filename": "episode.mp3",
  "language": "ja",
  "status": "queued",
  "stage": "queued",
  "progress": 0,
  "queue_position": 1,
  "cached_from": null,
  "file_size": 123456,
  "duration": 60.0
}
```

复用规则如下：

- 文件必须字节完全一致（SHA-256 相同），语言也必须相同。
- 识别配置指纹包含主模型和 VAD 模型的路径、文件大小及修改时间，`whisper-server` 可执行文件信息、推理参数、音频转换参数和内部流程版本。
- 只查找结果保留期内已成功且有字幕结果的任务。失败任务不会被复用。
- 命中时会创建新的任务 ID，状态为 `succeeded`，并在 `cached_from` 中返回来源任务 ID；不占用排队名额，即使队列已满也可复用。
- 排队中或正在运行的重复文件不会合并，会各自排队。
- 旧数据库中的历史任务没有文件摘要，不能作为复用来源；升级后新完成的任务会记录摘要。

若修改了识别或结果处理逻辑，应递增代码中的 `RECOGNITION_PIPELINE_VERSION`，让新任务不再复用旧流程产生的结果。

### 查询任务

`GET /v1/jobs/{job_id}` 返回任务状态。`status` 可能是 `queued`、`running`、`succeeded` 或 `failed`；运行阶段 `stage` 可能是 `queued`、`preparing`、`loading_model`、`transcribing` 或 `done`。转录时 `progress` 为识别百分比；如果无法从 whisper-server 日志读取进度，则为 `null`。复用结果的任务会在 `cached_from` 中给出来源任务 ID，普通任务该字段为 `null`。成功时响应还会提供四种格式的 `result_urls`。

```bash
curl http://127.0.0.1:5001/v1/jobs/JOB_ID \
  -H 'X-API-Key: replace-with-a-long-random-secret'
```

### 下载结果

`GET /v1/jobs/{job_id}/result?format=lrc` 下载字幕。`format` 可选值为 `lrc`、`srt`、`vtt`、`json`，默认 `lrc`。JSON 响应结构为 `{"segments":[{"start":0.0,"end":1.2,"text":"..."}]}`。

```bash
curl 'http://127.0.0.1:5001/v1/jobs/JOB_ID/result?format=srt' \
  -H 'X-API-Key: replace-with-a-long-random-secret' \
  -o episode.srt
```

### 删除任务

`DELETE /v1/jobs/{job_id}` 删除任务及其文件，成功返回 HTTP `204`。运行中的任务不能删除（HTTP `409`）。

### 常见错误

| HTTP 状态码 | 含义 |
| --- | --- |
| `401` | API Key 缺失或不正确（仅启用 API Key 时） |
| `403` | 非本机请求访问管理页面 |
| `404` | 任务不存在 |
| `409` | 任务尚未完成，或正在运行而无法删除 |
| `413` | 上传文件超过 `MAX_UPLOAD_MB` |
| `415` | 文件扩展名不受支持 |
| `422` | 任务识别失败 |
| `429` | 等待队列已满 |

管理页 `/admin` 展示服务、whisper-server 和任务概况；管理接口位于 `/admin/api/`，与页面一样默认只允许本机访问。

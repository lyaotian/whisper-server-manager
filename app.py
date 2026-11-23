"""字幕生成 HTTP API：包装 whisper-server，任务保存在 SQLite，重启后自动续跑。

启动（必须单进程）：
    uvicorn app:app --host 127.0.0.1 --port 8000

接口：
    POST   /v1/jobs                    上传音频，创建任务（202）
    GET    /v1/jobs/{id}               查询状态与进度
    GET    /v1/jobs/{id}/result        下载字幕 ?format=lrc|srt|vtt|json
    DELETE /v1/jobs/{id}               删除任务（运行中的不可删除）
    GET    /admin                      后台管理页面（默认仅允许本机访问，无需登录）

并发与持久化设计：
    * 整个程序只持有 1 个 SQLite 连接，所有读写都经过同一把锁（Store.lock），
      写操作使用 BEGIN IMMEDIATE 事务；同时开启 WAL + busy_timeout。
    * 通过 DATA_DIR/app.lock 文件锁保证同一数据目录只有一个实例在运行。
    * 队列就是数据库里 status='queued' 的行，worker 用事务"取出并标记 running"，
      因此不会重复领取，也不会与 DELETE 冲突。
    * 启动时把遗留的 running 任务放回队列（超过 MAX_ATTEMPTS 次仍被打断的标记失败）。
"""
from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import uuid
import wave
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import (Depends, FastAPI, File, Form, Header, HTTPException,
                     Query, Request, Response, UploadFile)
from fastapi.responses import HTMLResponse

# ---------------- 配置（可用环境变量覆盖） ----------------
MODEL = os.getenv("WHISPER_MODEL", "/Users/lyaotian/my/models/ggml-large-v3-q5_0.bin")
VAD_MODEL = os.getenv("WHISPER_VAD_MODEL", "/Users/lyaotian/my/models/ggml-silero-v5.1.2.bin")
WHISPER_HOST = os.getenv("WHISPER_HOST", "127.0.0.1")
WHISPER_PORT = int(os.getenv("WHISPER_PORT", "8080"))
DEFAULT_LANGUAGE = os.getenv("DEFAULT_LANGUAGE", "ja")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "500"))
MAX_QUEUE = int(os.getenv("MAX_QUEUE", "50"))                 # 排队上限
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))            # 任务最多被执行（含被打断）的次数
RESULT_TTL_SECONDS = int(os.getenv("RESULT_TTL_SECONDS", str(24 * 3600)))
SERVER_WAIT_SECONDS = int(os.getenv("SERVER_WAIT_SECONDS", "180"))
API_KEY = os.getenv("API_KEY", "")                            # 为空则不鉴权
RECOGNITION_PIPELINE_VERSION = "1"  # 修改识别参数或结果处理逻辑时递增，避免复用旧结果

# /admin 不做登录，默认只允许本机访问；确需远程访问（如放在内网反向代理后）时设为 1
ADMIN_ALLOW_REMOTE = os.getenv("ADMIN_ALLOW_REMOTE", "") == "1"

BASE_URL = f"http://{WHISPER_HOST}:{WHISPER_PORT}"
ALLOWED_EXT = {".wav", ".mp3", ".m4a", ".flac", ".ogg", ".opus", ".aac", ".wma", ".webm"}
FORMATS = ("lrc", "srt", "vtt", "json")

JOBS_DIR = DATA_DIR / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------- SQLite 存储（所有访问串行化） ----------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,   -- 入队顺序
    id          TEXT NOT NULL UNIQUE,
    filename    TEXT NOT NULL,
    language    TEXT NOT NULL,
    status      TEXT NOT NULL,       -- queued | running | succeeded | failed
    stage       TEXT NOT NULL,       -- queued | preparing | loading_model | transcribing | done
    error       TEXT,
    attempts    INTEGER NOT NULL DEFAULT 0,
    log_offset  INTEGER NOT NULL DEFAULT 0,
    segments    TEXT,                -- JSON
    file_size   INTEGER,             -- 上传文件大小（字节）
    duration    REAL,                -- 音频时长（秒）
    content_sha256 TEXT,             -- 上传文件内容摘要
    recognition_key TEXT,            -- 模型与识别配置摘要
    cached_from TEXT,                -- 复用结果的来源任务 ID
    created_at  REAL NOT NULL,
    started_at  REAL,
    finished_at REAL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status_seq ON jobs(status, seq);
"""
LIGHT_COLS = ("seq,id,filename,language,status,stage,error,attempts,log_offset,"
              "file_size,duration,content_sha256,recognition_key,cached_from,"
              "created_at,started_at,finished_at")


class Store:
    def __init__(self, path: Path) -> None:
        self.lock = threading.RLock()
        # isolation_level=None：自动提交模式，事务由 tx() 显式控制
        self.conn = sqlite3.connect(path, check_same_thread=False,
                                    isolation_level=None, timeout=30)
        self.conn.row_factory = sqlite3.Row
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=30000")
            self.conn.executescript(SCHEMA)
            # 旧版本数据库升级：补充后加的列
            have = {r["name"] for r in self.conn.execute("PRAGMA table_info(jobs)")}
            for col, ddl in (("file_size", "INTEGER"), ("duration", "REAL"),
                             ("content_sha256", "TEXT"), ("recognition_key", "TEXT"),
                             ("cached_from", "TEXT")):
                if col not in have:
                    self.conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {ddl}")
            # 在旧表完成列迁移后创建索引，避免升级时引用尚不存在的列。
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_cache "
                "ON jobs(status, content_sha256, language, recognition_key, finished_at)")

    @contextmanager
    def tx(self):
        """写事务：持锁 + BEGIN IMMEDIATE，异常时回滚。"""
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
            else:
                self.conn.execute("COMMIT")

    def _one(self, sql: str, args: tuple = ()) -> dict | None:
        with self.lock:
            row = self.conn.execute(sql, args).fetchone()
        return dict(row) if row else None

    def _col(self, sql: str, args: tuple = ()) -> list:
        with self.lock:
            return [r[0] for r in self.conn.execute(sql, args).fetchall()]

    # ---- 读 ----
    def get(self, job_id: str, with_segments: bool = False) -> dict | None:
        cols = LIGHT_COLS + (",segments" if with_segments else "")
        return self._one(f"SELECT {cols} FROM jobs WHERE id=?", (job_id,))

    def queued_count(self) -> int:
        return self._one("SELECT COUNT(*) AS n FROM jobs WHERE status='queued'")["n"]

    def queue_position(self, seq: int) -> int:
        return self._one("SELECT COUNT(*) AS n FROM jobs WHERE status='queued' AND seq<=?",
                         (seq,))["n"]

    def queued_ids(self) -> list[str]:
        return self._col("SELECT id FROM jobs WHERE status='queued' ORDER BY seq")

    def all_ids(self) -> set[str]:
        return set(self._col("SELECT id FROM jobs"))

    def expired_ids(self, cutoff: float) -> list[str]:
        return self._col("SELECT id FROM jobs WHERE finished_at IS NOT NULL AND finished_at<?",
                         (cutoff,))

    def counts(self) -> dict:
        with self.lock:
            rows = self.conn.execute(
                "SELECT status, COUNT(*) AS n FROM jobs GROUP BY status").fetchall()
        return {r["status"]: r["n"] for r in rows}

    def avg_duration(self) -> float | None:
        row = self._one("SELECT AVG(finished_at - started_at) AS d FROM jobs "
                        "WHERE status='succeeded' AND started_at IS NOT NULL")
        return row["d"] if row else None

    def queued_audio_seconds(self) -> float:
        row = self._one("SELECT COALESCE(SUM(duration), 0) AS s FROM jobs WHERE status='queued'")
        return row["s"]

    def list_jobs(self, status: str | None, limit: int) -> list[dict]:
        """运行中 > 排队（先入队的在前）> 已结束（新的在前）。"""
        where, args = ("WHERE status=?", (status,)) if status else ("", ())
        sql = (f"SELECT {LIGHT_COLS} FROM jobs {where} ORDER BY "
               "CASE status WHEN 'running' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END, "
               "CASE WHEN status='queued' THEN seq END ASC, seq DESC LIMIT ?")
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, args + (limit,)).fetchall()]

    # ---- 写 ----
    def add(self, job_id: str, filename: str, language: str,
            file_size: int | None, duration: float | None, content_sha256: str,
            recognition_key: str, max_queue: int) -> str:
        """原子复用成功结果或检查容量后入队；返回 queued | reused | full。"""
        with self.tx() as c:
            now = time.time()
            cached = c.execute(
                "SELECT id,segments FROM jobs WHERE status='succeeded' AND segments IS NOT NULL "
                "AND content_sha256=? AND language=? AND recognition_key=? AND finished_at>=? "
                "ORDER BY finished_at DESC LIMIT 1",
                (content_sha256, language, recognition_key,
                 now - RESULT_TTL_SECONDS)).fetchone()
            if cached:
                c.execute(
                    "INSERT INTO jobs(id,filename,language,status,stage,segments,file_size,duration,"
                    "content_sha256,recognition_key,cached_from,created_at,finished_at) "
                    "VALUES (?,?,?,'succeeded','done',?,?,?,?,?,?,?,?)",
                    (job_id, filename, language, cached["segments"], file_size, duration,
                     content_sha256, recognition_key, cached["id"], now, now))
                return "reused"

            queued = c.execute(
                "SELECT COUNT(*) FROM jobs WHERE status='queued'").fetchone()[0]
            if queued >= max_queue:
                return "full"
            c.execute(
                "INSERT INTO jobs(id,filename,language,status,stage,file_size,duration,"
                "content_sha256,recognition_key,created_at) "
                "VALUES (?,?,?,'queued','queued',?,?,?,?,?)",
                (job_id, filename, language, file_size, duration,
                 content_sha256, recognition_key, now))
            return "queued"

    def set_duration(self, job_id: str, duration: float) -> None:
        with self.tx() as c:
            c.execute("UPDATE jobs SET duration=? WHERE id=?", (duration, job_id))

    def claim_next(self) -> dict | None:
        """取出最早的排队任务并标记为 running（同一事务内完成，不会被重复领取）。"""
        with self.tx() as c:
            row = c.execute("SELECT id FROM jobs WHERE status='queued' "
                            "ORDER BY seq LIMIT 1").fetchone()
            if row is None:
                return None
            c.execute("UPDATE jobs SET status='running', stage='preparing', "
                      "attempts=attempts+1, started_at=?, finished_at=NULL, error=NULL, "
                      "log_offset=0 WHERE id=?", (time.time(), row["id"]))
            job_id = row["id"]
        return self.get(job_id)

    def set_stage(self, job_id: str, stage: str, log_offset: int | None = None) -> None:
        with self.tx() as c:
            c.execute("UPDATE jobs SET stage=?, log_offset=COALESCE(?, log_offset) WHERE id=?",
                      (stage, log_offset, job_id))

    def finish_success(self, job_id: str, segments: list) -> None:
        with self.tx() as c:
            c.execute("UPDATE jobs SET status='succeeded', stage='done', error=NULL, "
                      "segments=?, finished_at=? WHERE id=?",
                      (json.dumps(segments, ensure_ascii=False), time.time(), job_id))

    def finish_failed(self, job_id: str, error: str) -> None:
        with self.tx() as c:
            c.execute("UPDATE jobs SET status='failed', error=?, finished_at=? WHERE id=?",
                      (error, time.time(), job_id))

    def delete_unless_running(self, job_id: str) -> str:
        """返回 'deleted' | 'running' | 'missing'；检查与删除在同一事务里，避免与 worker 竞争。"""
        with self.tx() as c:
            row = c.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return "missing"
            if row["status"] == "running":
                return "running"
            c.execute("DELETE FROM jobs WHERE id=?", (job_id,))
            return "deleted"

    def delete_many(self, ids: list[str]) -> None:
        with self.tx() as c:
            c.executemany("DELETE FROM jobs WHERE id=?", [(i,) for i in ids])

    def recover(self) -> int:
        """启动时调用：把上次遗留的 running 任务放回队列，反复被打断的标记失败。"""
        now = time.time()
        with self.tx() as c:
            c.execute("UPDATE jobs SET status='failed', finished_at=?, error=? "
                      "WHERE status='running' AND attempts>=?",
                      (now, f"interrupted {MAX_ATTEMPTS} times; giving up", MAX_ATTEMPTS))
            cur = c.execute("UPDATE jobs SET status='queued', stage='queued', "
                            "started_at=NULL, log_offset=0 WHERE status='running'")
            return cur.rowcount


store = Store(DATA_DIR / "jobs.db")
wake = threading.Event()              # 有新任务时唤醒 worker
shutting_down = threading.Event()
START_TIME = time.time()
worker_thread: threading.Thread | None = None


# ---------------- 单实例文件锁 ----------------
_lock_fh = None


def acquire_instance_lock() -> None:
    global _lock_fh
    fh = open(DATA_DIR / "app.lock", "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        raise RuntimeError(f"另一个实例正在使用 {DATA_DIR}，请勿同时运行多个实例") from None
    _lock_fh = fh


def release_instance_lock() -> None:
    global _lock_fh
    if _lock_fh:
        _lock_fh.close()          # 关闭文件即释放 flock
        _lock_fh = None


# ---------------- whisper-server 管理 ----------------
class WhisperServer:
    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None   # 仅当由本程序启动时才有值
        self.lock = threading.Lock()
        self.log_path = DATA_DIR / "whisper-server.log"

    @staticmethod
    def alive() -> bool:
        try:
            httpx.get(BASE_URL + "/", timeout=2)
            return True
        except httpx.HTTPError:
            return False

    def ensure(self) -> None:
        with self.lock:
            if self.alive():
                return
            if self.proc is None or self.proc.poll() is not None:
                self._spawn()
            deadline = time.time() + SERVER_WAIT_SECONDS
            while time.time() < deadline:
                if self.proc.poll() is not None:
                    raise RuntimeError(f"whisper-server 异常退出，详见 {self.log_path}")
                if self.alive():
                    return
                time.sleep(1)
            raise RuntimeError(f"whisper-server 在 {SERVER_WAIT_SECONDS}s 内未就绪")

    def _spawn(self) -> None:
        for f in (MODEL, VAD_MODEL):
            if not os.access(f, os.R_OK):
                raise RuntimeError(f"模型文件不可读: {f}")
        args = [
            "whisper-server", "-m", MODEL, "--vad", "-vm", VAD_MODEL,
            "-l", DEFAULT_LANGUAGE, "-sns", "-mc", "0", "-bs", "5", "-bo", "5",
            "-nlp", "-pp", "--host", WHISPER_HOST, "--port", str(WHISPER_PORT),
        ]
        log = open(self.log_path, "wb")
        self.proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)

    def log_size(self) -> int:
        try:
            return self.log_path.stat().st_size
        except OSError:
            return 0

    def progress_since(self, offset: int) -> int | None:
        """读取自 offset 以来日志里最新的 'progress = N%'。复用外部 server 时返回 None。"""
        if self.proc is None:
            return None
        try:
            with open(self.log_path, "rb") as f:
                f.seek(offset)
                text = f.read().decode("utf-8", "ignore")
        except OSError:
            return None
        found = re.findall(r"progress\s*=\s*(\d+)%", text)
        return int(found[-1]) if found else 0

    def log_tail(self, lines: int = 30) -> str:
        """日志末尾若干行（过滤掉刷屏的 progress 行）。"""
        try:
            with open(self.log_path, "rb") as f:
                f.seek(0, os.SEEK_END)
                f.seek(max(0, f.tell() - 16384))
                text = f.read().decode("utf-8", "ignore")
        except OSError:
            return ""
        kept = [ln for ln in text.splitlines() if "progress =" not in ln]
        return "\n".join(kept[-lines:])

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


server = WhisperServer()


# ---------------- 音频转换 & 字幕格式 ----------------
def convert_to_wav(src: Path, dst: Path) -> None:
    if shutil.which("ffmpeg"):
        cmd = ["ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-i", str(src),
               "-ar", "16000", "-ac", "1", "-c:a", "pcm_s16le", str(dst)]
    elif shutil.which("afconvert"):
        cmd = ["afconvert", "-f", "WAVE", "-d", "LEI16@16000", "-c", "1", str(src), str(dst)]
    else:
        raise RuntimeError("找不到 ffmpeg 或 afconvert")
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"音频转换失败: {r.stderr.strip()[:300]}")


def probe_duration(path: Path) -> float | None:
    """读取音频时长（秒）。优先 ffprobe，其次 macOS 的 afinfo；都不可用或失败时返回 None。"""
    try:
        if shutil.which("ffprobe"):
            r = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
                capture_output=True, text=True, timeout=30)
            value = float(r.stdout.strip())
            return value if value > 0 else None
        if shutil.which("afinfo"):
            r = subprocess.run(["afinfo", str(path)], capture_output=True, text=True, timeout=30)
            m = re.search(r"estimated duration:\s*([\d.]+)", r.stdout)
            return float(m.group(1)) if m else None
    except (subprocess.TimeoutExpired, ValueError, OSError):
        pass
    return None


def wav_duration(path: Path) -> float | None:
    try:
        with wave.open(str(path), "rb") as w:
            return w.getnframes() / w.getframerate()
    except (wave.Error, EOFError, OSError, ZeroDivisionError):
        return None


def _clean(text: str) -> str:
    return re.sub(r"\s*\n\s*", " ", text).strip()


def _ts(sec: float, sep: str) -> str:
    ms = round(sec * 1000)
    h, ms = divmod(ms, 3_600_000)
    m, ms = divmod(ms, 60_000)
    s, ms = divmod(ms, 1000)
    return f"{h:02}:{m:02}:{s:02}{sep}{ms:03}"


def to_lrc(segs: list) -> str:
    out = []
    for s in segs:
        cs = round(s["start"] * 100)
        out.append(f"[{cs // 6000:02}:{cs // 100 % 60:02}.{cs % 100:02}] {s['text']}")
    return "\n".join(out) + "\n"


def to_srt(segs: list) -> str:
    return "".join(
        f"{i}\n{_ts(s['start'], ',')} --> {_ts(s['end'], ',')}\n{s['text']}\n\n"
        for i, s in enumerate(segs, 1))


def to_vtt(segs: list) -> str:
    body = "".join(
        f"{_ts(s['start'], '.')} --> {_ts(s['end'], '.')}\n{s['text']}\n\n" for s in segs)
    return "WEBVTT\n\n" + body


def recognition_config_key() -> str:
    """返回识别配置指纹；改动推理参数时也需递增 pipeline version。"""
    def file_identity(raw_path: str) -> dict:
        path = Path(raw_path).expanduser()
        try:
            stat = path.stat()
            return {"path": str(path.resolve()), "size": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns}
        except OSError:
            return {"path": str(path.resolve()), "size": None, "mtime_ns": None}

    models = []
    for model_path in (MODEL, VAD_MODEL):
        models.append(file_identity(model_path))
    whisper_executable = shutil.which("whisper-server")
    config = {
        "pipeline_version": RECOGNITION_PIPELINE_VERSION,
        "models": models,
        "whisper_server": file_identity(whisper_executable) if whisper_executable else None,
        "default_language": DEFAULT_LANGUAGE,
        "whisper_args": ["--vad", "-sns", "-mc", "0", "-bs", "5", "-bo", "5",
                         "-nlp", "-pp"],
        "audio_conversion": {"sample_rate": 16000, "channels": 1, "encoding": "pcm_s16le"},
        "response_format": "verbose_json",
    }
    encoded = json.dumps(config, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


# ---------------- 后台 worker ----------------
def process(job: dict) -> None:
    job_id = job["id"]
    job_dir = JOBS_DIR / job_id
    wav = job_dir / "audio.wav"
    try:
        src = next(job_dir.glob("input.*"), None)
        if src is None:
            raise RuntimeError("输入文件已丢失")
        convert_to_wav(src, wav)
        if job["duration"] is None:        # 上传时没能探测到时长，用转换后的 wav 补上
            d = wav_duration(wav)
            if d:
                store.set_duration(job_id, d)

        store.set_stage(job_id, "loading_model")
        server.ensure()

        store.set_stage(job_id, "transcribing", log_offset=server.log_size())
        with open(wav, "rb") as f:
            resp = httpx.post(
                BASE_URL + "/inference",
                files={"file": ("audio.wav", f, "audio/wav")},
                data={"response_format": "verbose_json", "language": job["language"]},
                timeout=httpx.Timeout(None, connect=10),
            )
        resp.raise_for_status()
        raw = resp.json().get("segments")
        if raw is None:
            raise RuntimeError(f"server 返回了意料之外的内容: {resp.text[:200]}")
        segments = [
            {"start": float(s["start"]), "end": float(s["end"]), "text": _clean(s["text"])}
            for s in raw if _clean(s.get("text", ""))
        ]
        store.finish_success(job_id, segments)
    except Exception as e:  # noqa: BLE001
        if shutting_down.is_set():
            return          # 因关机被打断：保持 running 状态和输入文件，下次启动自动续跑
        store.finish_failed(job_id, str(e))
    for p in list(job_dir.glob("input.*")) + [wav]:
        p.unlink(missing_ok=True)


def worker_loop() -> None:
    while not shutting_down.is_set():
        try:
            job = store.claim_next()
        except Exception:  # noqa: BLE001  数据库异常时稍后重试，避免线程退出
            time.sleep(2)
            continue
        if job is None:
            wake.wait(timeout=5)
            wake.clear()
            continue
        try:
            process(job)
        except Exception as e:  # noqa: BLE001  单个任务的意外错误不能杀死 worker
            job_id = job["id"]
            job_dir = JOBS_DIR / job_id
            print(f"[worker] job {job_id} failed unexpectedly: {e!r}")
            try:
                current = store.get(job_id)
            except Exception as state_error:  # noqa: BLE001
                current = {"status": "running"}
                print(f"[worker] cannot read status for {job_id}: {state_error!r}")
            if current is None or current["status"] != "running":
                continue
            # 处理错误的错误通常来自短暂的数据库故障。持续重试写入终态，
            # 避免任务永远停在 running；关机时保留 running 供下次启动恢复。
            while not shutting_down.is_set():
                try:
                    store.finish_failed(job_id, f"worker error: {e}")
                    for p in job_dir.glob("input.*"):
                        p.unlink(missing_ok=True)
                    (job_dir / "audio.wav").unlink(missing_ok=True)
                    break
                except Exception as persist_error:  # noqa: BLE001
                    print(f"[worker] cannot save failure for {job_id}: {persist_error!r}")
                    shutting_down.wait(2)


def cleanup_loop() -> None:
    while not shutting_down.wait(600):
        ids = store.expired_ids(time.time() - RESULT_TTL_SECONDS)
        if ids:
            store.delete_many(ids)
            for i in ids:
                shutil.rmtree(JOBS_DIR / i, ignore_errors=True)


def startup_recovery() -> None:
    resumed = store.recover()
    # 排队任务的输入文件若已丢失则直接标记失败
    for job_id in store.queued_ids():
        if next((JOBS_DIR / job_id).glob("input.*"), None) is None:
            store.finish_failed(job_id, "输入文件已丢失")
    # 清理没有数据库记录的孤儿目录（如上传中途崩溃遗留的）
    known = store.all_ids()
    for d in JOBS_DIR.iterdir():
        if d.is_dir() and d.name not in known:
            shutil.rmtree(d, ignore_errors=True)
    if resumed:
        print(f"[recovery] 已恢复 {resumed} 个未完成的任务")


# ---------------- FastAPI ----------------
@asynccontextmanager
async def lifespan(_: FastAPI):
    global worker_thread, START_TIME
    acquire_instance_lock()
    shutting_down.clear()
    START_TIME = time.time()
    startup_recovery()
    worker = worker_thread = threading.Thread(target=worker_loop, daemon=True)
    worker.start()
    threading.Thread(target=cleanup_loop, daemon=True).start()
    wake.set()
    yield
    shutting_down.set()           # 先置位，再停 server，这样被打断的任务不会被标成失败
    wake.set()
    server.stop()
    worker.join(timeout=15)
    if worker.is_alive():
        # 线程仍可能在处理任务或访问 SQLite。保留 flock，直到进程退出，
        # 防止另一个实例提前接管同一数据目录。
        print("[shutdown] worker did not stop; retaining the data directory lock")
    else:
        release_instance_lock()


app = FastAPI(title="Subtitle API", lifespan=lifespan)


def check_key(x_api_key: str = Header(default="")) -> None:
    if API_KEY and not hmac.compare_digest(x_api_key, API_KEY):
        raise HTTPException(401, "invalid API key")


def describe(row: dict) -> dict:
    status = row["status"]
    if status == "succeeded":
        progress = 100
    elif status == "running" and row["stage"] == "transcribing":
        progress = server.progress_since(row["log_offset"])   # 拿不到时为 None
    elif status in ("queued", "running"):
        progress = 0
    else:
        progress = None
    d = {
        "id": row["id"], "filename": row["filename"], "language": row["language"],
        "status": status, "stage": row["stage"], "progress": progress,
        "queue_position": store.queue_position(row["seq"]) if status == "queued" else None,
        "file_size": row["file_size"], "duration": row["duration"],
        "attempts": row["attempts"], "error": row["error"],
        "cached_from": row["cached_from"],
        "created_at": row["created_at"], "started_at": row["started_at"],
        "finished_at": row["finished_at"],
    }
    if status == "succeeded":
        d["result_urls"] = {f: f"/v1/jobs/{row['id']}/result?format={f}" for f in FORMATS}
    return d


@app.post("/v1/jobs", status_code=202, dependencies=[Depends(check_key)])
def create_job(response: Response,
               file: UploadFile = File(...),
               language: str = Form(DEFAULT_LANGUAGE),
               original_filename: str = Form("")):
    # The multipart file-part filename may be corrupted by Android's native
    # encoder. Prefer the UTF-8 text field, while retaining compatibility with
    # clients that only send the file and language fields.
    submitted_name = original_filename.strip() or file.filename or "input.mp3"
    submitted_name = submitted_name.replace("\\", "/").rsplit("/", 1)[-1]
    ext = Path(file.filename or "").suffix.lower()
    if ext not in ALLOWED_EXT:
        raise HTTPException(415, f"unsupported file type: {ext or '(none)'}")
    job_id = uuid.uuid4().hex
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True)
    limit, size = MAX_UPLOAD_MB * 1024 * 1024, 0
    digest = hashlib.sha256()
    try:
        with open(job_dir / f"input{ext}", "wb") as out:
            while chunk := file.file.read(1024 * 1024):
                size += len(chunk)
                if size > limit:
                    raise HTTPException(413, f"file exceeds {MAX_UPLOAD_MB} MB")
                digest.update(chunk)
                out.write(chunk)
            out.flush()
            os.fsync(out.fileno())       # 确保文件落盘后再入库，崩溃后不会出现"有记录无文件"
        duration = probe_duration(job_dir / f"input{ext}")
        outcome = store.add(
            job_id, submitted_name or f"input{ext}", language, size, duration,
            digest.hexdigest(), recognition_config_key(), MAX_QUEUE)
        if outcome == "full":
            raise HTTPException(429, "queue is full, try again later")
        if outcome == "reused":
            shutil.rmtree(job_dir, ignore_errors=True)
    except BaseException:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise
    if outcome == "queued":
        wake.set()
    response.headers["Location"] = f"/v1/jobs/{job_id}"
    return describe(store.get(job_id))


@app.post("/v1/jobs/raw", status_code=202, dependencies=[Depends(check_key)])
async def create_raw_job(
        request: Request,
        response: Response,
        language: str = Query(DEFAULT_LANGUAGE),
        original_filename: str = Query("audio.mp3")):
    """Accept a raw audio stream so native clients avoid Base64/FormData bridges."""
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/octet-stream":
        raise HTTPException(415, "content-type must be application/octet-stream")

    submitted_name = original_filename.replace("\\", "/").rsplit("/", 1)[-1].strip()
    ext = Path(submitted_name).suffix.lower()
    if ext not in ALLOWED_EXT:
        raise HTTPException(415, f"unsupported file type: {ext or '(none)'}")

    job_id = uuid.uuid4().hex
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True)
    limit, size = MAX_UPLOAD_MB * 1024 * 1024, 0
    digest = hashlib.sha256()
    try:
        with open(job_dir / f"input{ext}", "wb") as out:
            async for chunk in request.stream():
                if not chunk:
                    continue
                size += len(chunk)
                if size > limit:
                    raise HTTPException(413, f"file exceeds {MAX_UPLOAD_MB} MB")
                digest.update(chunk)
                out.write(chunk)
            if size == 0:
                raise HTTPException(400, "uploaded file is empty")
            out.flush()
            os.fsync(out.fileno())

        duration = probe_duration(job_dir / f"input{ext}")
        outcome = store.add(
            job_id, submitted_name, language, size, duration,
            digest.hexdigest(), recognition_config_key(), MAX_QUEUE)
        if outcome == "full":
            raise HTTPException(429, "queue is full, try again later")
        if outcome == "reused":
            shutil.rmtree(job_dir, ignore_errors=True)
    except BaseException:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise

    if outcome == "queued":
        wake.set()
    response.headers["Location"] = f"/v1/jobs/{job_id}"
    return describe(store.get(job_id))


def get_row(job_id: str, with_segments: bool = False) -> dict:
    row = store.get(job_id, with_segments)
    if row is None:
        raise HTTPException(404, "job not found")
    return row


@app.get("/v1/jobs/{job_id}", dependencies=[Depends(check_key)])
def job_status(job_id: str):
    return describe(get_row(job_id))


def render_result(row: dict, format: str) -> Response:
    if format not in FORMATS:
        raise HTTPException(400, f"format must be one of {FORMATS}")
    if row["status"] == "failed":
        raise HTTPException(422, row["error"] or "job failed")
    if row["status"] != "succeeded" or row["segments"] is None:
        raise HTTPException(409, f"job not finished (status={row['status']})")

    segs = json.loads(row["segments"])
    if format == "json":
        body, mime = json.dumps({"segments": segs}, ensure_ascii=False), "application/json"
    else:
        body = {"lrc": to_lrc, "srt": to_srt, "vtt": to_vtt}[format](segs)
        mime = "text/vtt" if format == "vtt" else "text/plain"
    name = quote(f"{Path(row['filename']).stem}.{format}")
    return Response(body, media_type=f"{mime}; charset=utf-8",
                    headers={"Content-Disposition": f"attachment; filename*=UTF-8''{name}"})


def remove_job(job_id: str) -> None:
    result = store.delete_unless_running(job_id)
    if result == "missing":
        raise HTTPException(404, "job not found")
    if result == "running":
        raise HTTPException(409, "running jobs cannot be deleted")
    shutil.rmtree(JOBS_DIR / job_id, ignore_errors=True)


@app.get("/v1/jobs/{job_id}/result", dependencies=[Depends(check_key)])
def job_result(job_id: str, format: str = "lrc"):
    if format not in FORMATS:
        raise HTTPException(400, f"format must be one of {FORMATS}")
    return render_result(get_row(job_id, with_segments=True), format)


@app.delete("/v1/jobs/{job_id}", status_code=204, dependencies=[Depends(check_key)])
def delete_job(job_id: str):
    remove_job(job_id)
    return Response(status_code=204)


# ---------------- 后台管理（无登录，默认仅限本机） ----------------
ADMIN_HTML = Path(__file__).with_name("admin.html")


def admin_guard(request: Request) -> None:
    if ADMIN_ALLOW_REMOTE:
        return
    host = request.client.host if request.client else ""
    if host not in ("127.0.0.1", "::1", "localhost"):
        raise HTTPException(403, "admin is only available from localhost "
                                 "(set ADMIN_ALLOW_REMOTE=1 to allow remote access)")


def server_state(has_running_job: bool) -> dict:
    alive = WhisperServer.alive()
    owned = server.proc is not None and server.proc.poll() is None
    # 推理繁忙时 server 偶尔会来不及应答探测请求，此时显示"繁忙"而不是"离线"
    state = "up" if alive else ("busy" if owned and has_running_job else "down")
    return {
        "state": state, "managed": owned,
        "pid": server.proc.pid if owned else None, "url": BASE_URL,
        "model": Path(MODEL).name, "vad_model": Path(VAD_MODEL).name,
        "log_tail": server.log_tail() if server.proc is not None else None,
    }


@app.get("/admin", dependencies=[Depends(admin_guard)])
def admin_page():
    return HTMLResponse(ADMIN_HTML.read_text(encoding="utf-8"),
                        headers={"Cache-Control": "no-store"})


@app.get("/admin/api/overview", dependencies=[Depends(admin_guard)])
def admin_overview(status: str = "", limit: int = 100):
    if status not in ("", "queued", "running", "succeeded", "failed"):
        raise HTTPException(400, "invalid status")
    limit = max(1, min(limit, 500))
    counts = {k: 0 for k in ("queued", "running", "succeeded", "failed")}
    counts.update(store.counts())
    running = next(iter(store.list_jobs("running", 1)), None)
    try:
        db_bytes = (DATA_DIR / "jobs.db").stat().st_size
    except OSError:
        db_bytes = None
    return {
        "now": time.time(),
        "service": {
            "uptime": time.time() - START_TIME,
            "worker_alive": bool(worker_thread and worker_thread.is_alive()),
            "shutting_down": shutting_down.is_set(),
            "db_bytes": db_bytes,
            "disk_free_bytes": shutil.disk_usage(DATA_DIR).free,
            "avg_duration": store.avg_duration(),
            "config": {"language": DEFAULT_LANGUAGE, "max_queue": MAX_QUEUE,
                       "max_upload_mb": MAX_UPLOAD_MB, "max_attempts": MAX_ATTEMPTS,
                       "result_ttl_hours": round(RESULT_TTL_SECONDS / 3600, 1),
                       "api_key_enabled": bool(API_KEY)},
        },
        "whisper_server": server_state(running is not None),
        "counts": counts,
        "queued_audio_seconds": store.queued_audio_seconds(),
        "current": describe(running) if running else None,
        "jobs": [describe(r) for r in store.list_jobs(status or None, limit)],
    }


@app.get("/admin/api/jobs/{job_id}/result", dependencies=[Depends(admin_guard)])
def admin_job_result(job_id: str, format: str = "lrc"):
    return render_result(get_row(job_id, with_segments=True), format)


@app.delete("/admin/api/jobs/{job_id}", status_code=204, dependencies=[Depends(admin_guard)])
def admin_delete_job(job_id: str):
    remove_job(job_id)
    return Response(status_code=204)

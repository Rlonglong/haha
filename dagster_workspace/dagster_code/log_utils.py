"""
UI 事件 log 與「明細 log」的分流。

為什麼要分流
------------
Dagster UI 上的 Run log，只要能開啟 Dagster 網頁的人都看得到，沒有再細分權限。
所以那裡只放「狀態」——這一步成功了沒、要不要有人介入——
不放檔案路徑、遠端指令、遠端輸出、筆數這類明細。

明細寫到獨立的 logger，由 rsyslog 收走（那邊有權限控管），需要查問題時再去看。

用法
----
    from dagster_code.log_utils import log_detail

    context.log.info("✅ FTP 下載完成")                       # UI 看得到：只有狀態
    log_detail(context, f"下載路徑: {remote_path} -> {local_path}")   # 只進明細

輸出位置
--------
依環境變數決定，優先序如下：

1. ``DAGSTER_DETAIL_SYSLOG``：``host:port``（例如 ``10.10.1.5:514``）
   → 直接送 rsyslog，最終形態。
2. ``DAGSTER_DETAIL_LOG_PATH``：檔案路徑
   （預設 ``/app/workspace/dagster_home/logs/pipeline_detail.log``）
   → 由 rsyslog 的 imfile 或 log 收集器讀走。
3. 前兩者都不可用時退回 stderr，避免明細靜靜消失。
   ⚠️ 這個 fallback 的內容會被 Dagster 的 compute log 收走、
   出現在 UI 的 stderr 分頁——只是保命用，正式環境請確保 1 或 2 可用。

⚠️ 這個 logger 刻意設定 ``propagate = False``。
往上冒泡會被 dagster 的 root logger 接走並寫進 process 的 stdout／stderr，
那就等於又回到 UI 上看得到，分流就失效了。
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler, SysLogHandler

# 這個名字刻意不要放進 dagster.yaml 的 managed_python_loggers，
# 放進去就會被收進 Dagster 的事件 log，也就是又跑回 UI 上了。
DETAIL_LOGGER_NAME = "dagster_pipeline.detail"

_DEFAULT_LOG_PATH = "/app/workspace/dagster_home/logs/pipeline_detail.log"
_MAX_BYTES = 50 * 1024 * 1024
_BACKUP_COUNT = 5


def _build_handler() -> logging.Handler:
    syslog_addr = os.getenv("DAGSTER_DETAIL_SYSLOG", "").strip()
    if syslog_addr:
        try:
            host, _, port = syslog_addr.partition(":")
            return SysLogHandler(address=(host, int(port or 514)))
        except Exception:
            # syslog 設定有問題時不要讓整個 code location 起不來，往下退到檔案
            pass

    path = os.getenv("DAGSTER_DETAIL_LOG_PATH", _DEFAULT_LOG_PATH)
    try:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        return RotatingFileHandler(
            path, maxBytes=_MAX_BYTES, backupCount=_BACKUP_COUNT, encoding="utf-8"
        )
    except OSError:
        return logging.StreamHandler(sys.stderr)


def _build_logger() -> logging.Logger:
    logger = logging.getLogger(DETAIL_LOGGER_NAME)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False        # ★ 不要冒泡，見模組說明
    handler = _build_handler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    )
    logger.addHandler(handler)
    return logger


_detail = _build_logger()


def _context_prefix(context) -> str:
    """組出 run / 資產 / 分區的識別字，方便之後在 rsyslog 那邊比對回 Dagster 的 Run。"""
    parts = []

    run_id = getattr(context, "run_id", None)
    if run_id:
        parts.append(f"run={run_id}")

    try:
        asset_key = context.asset_key
    except Exception:
        asset_key = None
    if asset_key is not None:
        parts.append(f"asset={asset_key.to_user_string()}")

    try:
        partition = context.partition_key
    except Exception:
        partition = None
    if partition:
        parts.append(f"partition={partition}")

    return f"[{' '.join(parts)}]" if parts else "[-]"


def log_detail(context, message: str) -> None:
    """把明細寫到明細 logger。**不會**出現在 Dagster UI 的事件 log。"""
    _detail.info(f"{_context_prefix(context)} {message}")


def log_detail_warning(context, message: str) -> None:
    """明細層級的警告（UI 上通常會有一則對應的簡短訊息）。"""
    _detail.warning(f"{_context_prefix(context)} {message}")


def log_detail_error(context, message: str) -> None:
    """完整的錯誤內容（stack trace、遠端輸出等）只寫這裡，UI 上只留一句話。"""
    _detail.error(f"{_context_prefix(context)} {message}")

"""RQ job entrypoints: run Prefect deployments for LSM strips and OCT batches."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
import os
from pathlib import Path
from typing import Any

from prefect.deployments import run_deployment
from pydantic import BaseModel, ConfigDict, Field
from redis import Redis
from rq import Queue

from opticstream.flows.lsm.utils import host_lsm_fs_path
from opticstream.events.lsm_events import STRIP_READY
from opticstream.events.lsm_event_emitters import emit_strip_lsm_event
from opticapi.project_state.lsm_models import LSMStripId
from opticapi.naming import ProjectQueueKind, queue_name_for_project
from opticapi.project_state.oct_models import OCTBatchId

logger = logging.getLogger(__name__)

_PROCESS_FN = "opticnode.modules.worker.process"
_PROCESS_BACKLOG_FN = "opticnode.modules.worker.process_backlog"


class WorkerConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    project_name: str = Field(..., min_length=1)
    deployment_name: str = Field(..., min_length=1)
    queue_kind: ProjectQueueKind = Field(
        ...,
        description="Which RQ queue namespace to use (lsm:project:… vs oct:project:…).",
    )
    allowed_window_minutes: float = Field(default=10, ge=0)
    redis_url: str = Field(default="")
    process_cache_dir: str = Field(
        default="/local_mount/space/zircon/6/users/tmp/",
        min_length=1,
        description="Local directory for rclone copy before Prefect LSM processing.",
    )

    @property
    def queue_name(self) -> str:
        return queue_name_for_project(self.project_name, self.queue_kind)

    @property
    def backlog_queue_name(self) -> str:
        return queue_name_for_project(self.project_name, self.queue_kind, backlog=True)


class StripTask(BaseModel):
    lsm_strip_id: LSMStripId
    timestamp: datetime = Field(default_factory=datetime.now)
    strip_path: str
    force_rerun: bool = False


class OctBatchTask(BaseModel):
    batch_id: OCTBatchId
    file_list: list[str]
    timestamp: datetime = Field(default_factory=datetime.now)
    force_rerun: bool = False


def _coerce_timestamp(val: object) -> datetime:
    if isinstance(val, datetime):
        return val
    s = str(val).replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def _maybe_defer_to_backlog(payload: dict[str, Any], config: WorkerConfig) -> bool:
    """Return True if the job was re-queued to backlog and should not run now."""
    if config.allowed_window_minutes <= 0:
        return False
    ts_raw = payload.get("timestamp")
    if ts_raw is None:
        return False
    ts = _coerce_timestamp(ts_raw)
    if ts.tzinfo is not None:
        ts = ts.astimezone().replace(tzinfo=None)
    if ts < datetime.now() - timedelta(minutes=config.allowed_window_minutes):
        logger.info("Job timestamp outside allowed window; sending to backlog")
        conn = Redis.from_url(config.redis_url, decode_responses=False)
        Queue(config.backlog_queue_name, connection=conn).enqueue(
            _PROCESS_BACKLOG_FN, payload, config.model_dump()
        )
        logger.info("Re-queued job to backlog queue %r", config.backlog_queue_name)
        return True
    return False

def _build_rclone_cmd(
    source: str,
    destination:str,
    checkers: int =4,
    transfers:int = 8
    ):
    cmd = [
        "rclone",
        "copy",
        str(source),
        destination,
        "--checkers",
        str(checkers),
        "--transfers",
        str(transfers),
    ]
    return cmd
    pass 
def _run_lsm_deployment(payload: dict[str, Any], config: WorkerConfig) -> None:

    task = StripTask.model_validate(payload)
    # copy the files over
    # should have enough space
    SPACE_AVAILABLE = True
    if SPACE_AVAILABLE:
        source_path = host_lsm_fs_path(payload['strip_path'])
        source_acquisition_folder = source_path.parent.name
        logger.info(
            "LSM source acquisition folder: %s",
            source_acquisition_folder,
        )
        dest_base = config.process_cache_dir.rstrip("/") + "/"
        dest_path = os.path.join(dest_base, os.path.basename(str(source_path).rstrip("/")))
        cmd = _build_rclone_cmd(source_path, dest_path)
    import subprocess

    # Run the rclone command and wait for it to finish
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        logger.error("rclone copy failed: %s", e)
        raise
    task.strip_path = dest_path
    # emit_strip_lsm_event(STRIP_READY, task.lsm_strip_id, extra_payload={"strip_path": str(dest_path)})

    param = {
        "payload": {
            "strip_ident": task.lsm_strip_id.model_dump(),
            "strip_path": task.strip_path,
            "source_acquisition_folder": source_acquisition_folder,
            "force_rerun": task.force_rerun,
        }
    }

    # run_deployment(name="archive-strip-event-flow/local", parameters=param, timeout=0)
    run_deployment(
        name=config.deployment_name,
        parameters=param,
        timeout=0
    )


def _run_oct_deployment(payload: dict[str, Any], deployment_name: str) -> None:
    from opticapi.config.psoct_scan_config import PSOCTScanConfigModel
    from opticstream.config.psoct_scan_config import get_psoct_scan_config

    task = OctBatchTask.model_validate(payload)
    project_name = task.batch_id.project_name
    block = get_psoct_scan_config(project_name)
    scan_config = PSOCTScanConfigModel.model_validate(block.model_dump())
    run_deployment(
        name=deployment_name,
        parameters={
            "batch_id": task.batch_id.model_dump(),
            "config": scan_config.model_dump(mode="json"),
            "file_list": [str(Path(p)) for p in task.file_list],
            "force_rerun": task.force_rerun,
        },
    )


def _run_deployment(payload: dict[str, Any], config: WorkerConfig) -> None:
    if "lsm_strip_id" in payload:
        _run_lsm_deployment(payload, config)
    elif "batch_id" in payload:
        _run_oct_deployment(payload, config.deployment_name)
    else:
        raise ValueError("Payload must include lsm_strip_id or batch_id")


def process(payload: dict[str, Any], config_dict: dict[str, Any]) -> None:
    """Main RQ handler: defer stale jobs to backlog when configured."""
    config = WorkerConfig.model_validate(config_dict)
    if _maybe_defer_to_backlog(payload, config):
        return
    _run_deployment(payload, config)


def process_backlog(payload: dict[str, Any], config_dict: dict[str, Any]) -> None:
    """Backlog RQ handler: always run deployment (no time-window check)."""
    config = WorkerConfig.model_validate(config_dict)
    _run_deployment(payload, config)

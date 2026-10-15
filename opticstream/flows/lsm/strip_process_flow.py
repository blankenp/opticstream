import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Sequence

import dask
import psutil
from niizarr.multizarr import ZarrConfig
from prefect import flow, get_run_logger, task
from prefect.deployments import run_deployment

from opticstream.hooks.publish_hooks import (
    publish_lsm_project_hook,
    publish_lsm_slice_hook,
)
from opticstream.hooks.check_channel_ready_hook import check_channel_ready_hook
from opticapi.config.lsm_scan_config import LSMScanConfigModel
from opticstream.data_processing.qc.convert_image import convert_image
from opticstream.events.lsm_events import STRIP_READY
from opticstream.events.utils import get_event_trigger
from opticstream.state.state_guards import (
    enter_flow_stage,
    force_rerun_from_payload,
    RunDecision,
    enter_milestone_stage,
    should_skip_run,
)
from opticstream.flows.lsm.utils import (
    host_lsm_fs_path,
    load_scan_config_for_payload,
    strip_ident_from_payload,
    strip_mip_output_path,
    strip_zarr_output_path,
)
from opticapi.project_state.lsm_models import LSMStripId
from opticstream.state import LSM_STATE_SERVICE
from opticstream.tasks.slack_notification import (
    send_slack_message,
    upload_multiple_files_to_slack,
)
from opticstream.hooks.slack_notification_hook import slack_notification_hook
from opticstream.utils.filesystem import format_bytes, get_disk_usage_info
from opticstream.utils.zarr_validation import (
    DirManifest,
    ValidationResult,
    get_dir_manifest,
    validate_zarr_directory,
)
from opticstream.flows.lsm.strip_archive_flow import (
    archive_strip,
    check_archive_result,
    remove_archived_source,
    invalid_path,
)
from opticstream.flows.lsm.strip_cleanup_flow import (
    describe_cleanup,
    run_cleanup_tasks,
)


def build_status_message(
    success: bool,
    failure_reasons: List[str],
    slice_id: int,
    strip_id: int,
    camera: Optional[str] = None,
) -> tuple[str, str]:
    """
    Build a status key and human-readable message for Slack notifications.
    """
    camera_part = f" ({camera})" if camera else ""
    if success:
        message = (
            f"Strip {strip_id} of slice {slice_id}{camera_part} processed successfully"
        )
        return "success", message

    base = f"Strip {strip_id} of slice {slice_id}{camera_part} failed validation"
    if failure_reasons:
        base += ": " + ", ".join(failure_reasons)
    return "error", base



def build_strip_usage_details(
    strip_size_bytes: int,
    zarr_size_bytes: int,
    backup_size_bytes: int,
    strip_disk: Dict[str, Any],
    backup_disk: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build the notification/report payload for strip storage usage.
    """
    return {
        "sizes": {
            "raw_strip": {
                "bytes": strip_size_bytes,
                "human": format_bytes(strip_size_bytes),
            },
            "zarr": {
                "bytes": zarr_size_bytes,
                "human": format_bytes(zarr_size_bytes),
            },
            "backup": {
                "bytes": backup_size_bytes,
                "human": format_bytes(backup_size_bytes),
            },
        },
        "disk": {
            "strip": {
                "used_percent": strip_disk["used_percent"],
                "used_human": strip_disk["used_human"],
                "free_human": strip_disk["free_human"],
                "total_human": strip_disk["total_human"],
            },
            "backup": {
                "used_percent": backup_disk["used_percent"],
                "used_human": backup_disk["used_human"],
                "free_human": backup_disk["free_human"],
                "total_human": backup_disk["total_human"],
            },
        },
    }


def format_strip_usage_details(usage: Dict[str, Any]) -> str:
    """
    Render strip usage details into a compact, human-readable Slack message.
    """
    sizes = usage.get("sizes", {})
    disk = usage.get("disk", {})

    raw = sizes.get("raw_strip", {}).get("human", "n/a")
    zarr = sizes.get("zarr", {}).get("human", "n/a")
    backup = sizes.get("backup", {}).get("human", "n/a")

    strip_disk = disk.get("strip", {})
    backup_disk = disk.get("backup", {})

    strip_disk_line = (
        f'{strip_disk.get("used_percent", "n/a")} used '
        f'({strip_disk.get("used_human", "n/a")}/{strip_disk.get("total_human", "n/a")}, '
        f'free {strip_disk.get("free_human", "n/a")})'
    )
    backup_disk_line = (
        f'{backup_disk.get("used_percent", "n/a")} used '
        f'({backup_disk.get("used_human", "n/a")}/{backup_disk.get("total_human", "n/a")}, '
        f'free {backup_disk.get("free_human", "n/a")})'
    )

    return (
        "Storage\n"
        f"- raw strip: {raw}\n"
        f"- zarr: {zarr}\n"
        f"- backup: {backup}\n"
        "Disk\n"
        f"- strip: {strip_disk_line}\n"
        f"- backup: {backup_disk_line}"
    )


@task(task_run_name="compress-{strip_ident}")
def compress_strip(
    strip_ident: LSMStripId,
    strip_path: Path,
    info_file: Path,
    output_path: Path,
    zarr_config: "ZarrConfig",
    num_workers: int = 6,
    cpu_affinity: Optional[List[int]] = None,
    mip_output_path: Optional[Path] = None,
    force_rerun: bool = False,
) -> None:
    """
    Compress a strip of a slice.
    """
    from linc_convert.modalities.lsm import strip

    logger = get_run_logger()
    logger.info(f"Compressing {strip_ident} from {strip_path} to {output_path}")

    strip_view = LSM_STATE_SERVICE.peek_strip(
        strip_ident=strip_ident,
    )
    if (
        enter_milestone_stage(
            item_state_view=strip_view,
            item_ident=strip_ident,
            field_name="compressed",
            force_rerun=force_rerun,
        )
        == RunDecision.SKIPPED
    ):
        return
    if cpu_affinity:
        p = psutil.Process(os.getpid())
        p.cpu_affinity(list(range(*cpu_affinity)))
        logger.info(f"cpu affinity:{p.cpu_affinity()}")

    with dask.config.set(pool=ThreadPoolExecutor(num_workers)):
        strip.convert(
            inp=os.fspath(strip_path),
            info_file=os.fspath(info_file),
            mip_image_output=os.fspath(mip_output_path)
            if mip_output_path is not None
            else None,
            out=os.fspath(output_path),
            zarr_config=zarr_config,
            nii=False,
        )
    logger.info(f"Compressed {strip_ident} to {output_path}")



@task(task_run_name="check-compressed-{strip_ident}")
def check_compressed_result(
    strip_ident: LSMStripId,
    output_path: Optional[str] = None,
    mip_output_path: Optional[str] = None,
    generate_mip: bool = True,
    generate_zarr: bool = True,
    mip_size_threshold: int = 10**6,  # 1MB
    zarr_size_threshold: int = 10**9,  # 1GB
) -> ValidationResult:
    """
    Check if the compressed strip is valid and report its size.
    """
    logger = get_run_logger()
    # check mip
    if generate_mip:
        if invalid_path(mip_output_path):
            logger.error(f"MIP output path is not set for {strip_ident}")
            return ValidationResult(
                ok=False,
                size_bytes=0,
                reason="mip output path is not set",
            )
        if not os.path.exists(mip_output_path):
            logger.error(f"MIP output {mip_output_path} does not exist")
            return ValidationResult(
                ok=False,
                size_bytes=0,
                reason="mip output does not exist",
            )
        if os.path.getsize(mip_output_path) < mip_size_threshold:
            logger.error(f"MIP output {mip_output_path} is smaller than the threshold")
            return ValidationResult(
                ok=False,
                size_bytes=0,
                reason="mip output below size threshold",
            )
    zarr_size_bytes = 0
    if generate_zarr:
        logger.info(f"Checking if the compressed strip {strip_ident} is valid")
        zr = validate_zarr_directory(
            logger,
            output_path,
            zarr_size_threshold,
            context=str(strip_ident),
            missing_reason="compressed output missing",
            empty_reason="compressed output empty",
            below_threshold_reason="compressed output below size threshold",
        )
        if not zr.ok:
            return zr
        zarr_size_bytes = zr.size_bytes

    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as strip_state:
        strip_state.set_compressed(True)
    run_deployment(
        name="upload-strip-to-dandi-event-flow/local",
        parameters={"payload": {"strip_ident": strip_ident.model_dump()}},
        timeout=0,
        as_subflow=False,
    )
    return ValidationResult(ok=True, size_bytes=zarr_size_bytes)



@task(on_failure=[slack_notification_hook])
def strip_mip_qc_notification(
    strip_ident: LSMStripId,
    mip_stitched_path: Optional[str] = None,
    output_mip_preview_window: List[float] = [],
) -> None:
    """
    Send a notification for the strip MIP QC.
    """
    logger = get_run_logger()
    logger.info(f"Sending notification for strip MIP QC: {strip_ident}")
    if mip_stitched_path is None:
        logger.warning(f"MIP stitched path is not set for {strip_ident}")
        return
    window_min = None
    window_max = None
    if output_mip_preview_window:
        if len(output_mip_preview_window) != 2:
            raise ValueError(
                f"Output MIP preview window must be a list of 2 values: {output_mip_preview_window}"
            )
        window_min, window_max = output_mip_preview_window
    preview_path = mip_stitched_path.replace(".tiff", ".jpg")
    convert_image(
        input=mip_stitched_path,
        output=preview_path,
        window_min=window_min,
        window_max=window_max,
        split=2,
    )
    files = [
        Path(preview_path).with_suffix(f".part{i + 1}.jpg") for i in range(2)
    ]
    upload_multiple_files_to_slack(
        filepaths=files, initial_comment=f"MIP QC for {strip_ident}"
    )



@task
def check_strip_timestamp(
    strip_ident: LSMStripId,
    strip_path: str,
) -> None:
    """
    Check if the strip timestamp is valid.
    """
    # TODO: two channels should have similar timestamps
    logger = get_run_logger()
    logger.info(f"Checking strip timestamp for {strip_ident}")

@flow(
    flow_run_name="process-strip-{strip_ident}",
    on_completion=[publish_lsm_slice_hook, publish_lsm_project_hook, check_channel_ready_hook],
    on_failure=[slack_notification_hook],
)
def process_strip(
    strip_ident: LSMStripId,
    strip_path: str,
    scan_config: LSMScanConfigModel,
    force_rerun: bool = False,
    source_acquisition_folder: Optional[str] = None,
) -> None:
    """
    Process a strip of a slice.
    """
    logger = get_run_logger()
    strip_path = host_lsm_fs_path(strip_path)
    logger.info(f"Processing strip path: {strip_path} {strip_ident}")

    if should_skip_run(
        enter_flow_stage(
            LSM_STATE_SERVICE.peek_strip(strip_ident=strip_ident),
            force_rerun=force_rerun,
            skip_if_running=False,
            item_ident=strip_ident,
        )
    ):
        return None

    initial_strip_manifest = (
        get_dir_manifest(os.fspath(strip_path))
        if os.path.exists(strip_path)
        else DirManifest(file_count=0, total_bytes=0, sizes={})
    )

    strip_threshold_bytes = int(scan_config.strip_folder_size_threshold * 10**9)
    if initial_strip_manifest.total_bytes < strip_threshold_bytes:
        msg = (
            f":warning: Strip folder too small for {strip_ident} — "
            f"please check if the acquisition completed correctly.\n"
            f"- Path: `{strip_path}`\n"
            f"- Size: {format_bytes(initial_strip_manifest.total_bytes)} "
            f"(threshold: {format_bytes(strip_threshold_bytes)})"
        )
        send_slack_message(msg)
        raise RuntimeError(
            f"Strip folder {strip_path} is too small: "
            f"{format_bytes(initial_strip_manifest.total_bytes)} < "
            f"{format_bytes(strip_threshold_bytes)}"
        )

    acq = f"camera-{strip_ident.channel_id:02d}"
    logger.info(f"Processing {strip_ident} (acq={acq})")

    archive_path = (
        host_lsm_fs_path(scan_config.archive_path)
        if scan_config.archive_path is not None
        else None
    )
    strip_cleanup_action = scan_config.strip_cleanup_action

    logger.info(
        "LSM archive settings: path=%s, distributed=%s, rate_limit=%s MB/s",
        archive_path,
        scan_config.distribute_archive,
        scan_config.archive_rate_limit,
    )

    zarr_output_path = strip_zarr_output_path(strip_ident, scan_config)
    zarr_output_path = host_lsm_fs_path(zarr_output_path)
    zarr_output_path.parent.mkdir(parents=True, exist_ok=True)
    has_zarr_output = scan_config.output_path is not None

    mip_output_path = None
    if scan_config.generate_mip:
        mip_output_path = strip_mip_output_path(strip_ident, scan_config)
        mip_output_path = host_lsm_fs_path(mip_output_path)
        mip_output_path.parent.mkdir(parents=True, exist_ok=True)
    
    backup_path = None
    check_backup_future = None

    if archive_path is not None:
        if not source_acquisition_folder:
            raise ValueError(
                f"Missing source_acquisition_folder for {strip_ident}. "
                "Cannot safely determine raw archive destination."
            )

        # Ensure the folder name is a single directory component.
        if (
            source_acquisition_folder in (".", "..")
            or Path(source_acquisition_folder).name != source_acquisition_folder
            or "\\" in source_acquisition_folder
        ):
            raise ValueError(
                f"Invalid source_acquisition_folder: {source_acquisition_folder!r}"
            )

        backup_path = (
            archive_path
            / source_acquisition_folder
            / strip_path.name
        )

        logger.info("Raw archive destination: %s", backup_path)

    if archive_path is not None and not scan_config.distribute_archive:
        backup_future = archive_strip.submit(
            strip_ident=strip_ident,
            strip_path=os.fspath(strip_path),
            output_path=os.fspath(backup_path),
            rate_limit_mb=scan_config.archive_rate_limit,
            force_rerun=force_rerun,
        )

        check_backup_future = check_archive_result.submit(
            strip_ident=strip_ident,
            strip_path=os.fspath(strip_path),
            backup_path=os.fspath(backup_path),
            wait_for=[backup_future],
        )

    compress_result = ValidationResult(ok=True, size_bytes=0)
    if scan_config.output_path is not None or scan_config.generate_mip:
        compress_strip(
            strip_ident=strip_ident,
            strip_path=strip_path,
            info_file=host_lsm_fs_path(scan_config.info_file),
            output_path=zarr_output_path,
            zarr_config=scan_config.zarr_config,
            num_workers=scan_config.num_workers,
            cpu_affinity=scan_config.cpu_affinity,
            mip_output_path=mip_output_path,
            force_rerun=force_rerun,
        )
        compress_result = check_compressed_result(
            strip_ident=strip_ident,
            output_path=os.fspath(zarr_output_path),
            mip_output_path=os.fspath(mip_output_path) if mip_output_path else None,
            generate_mip=scan_config.generate_mip,
            generate_zarr=has_zarr_output,
        )
        if scan_config.generate_mip:
            strip_mip_qc_notification(
                strip_ident=strip_ident,
                mip_stitched_path=os.fspath(mip_output_path)
                if mip_output_path
                else None,
                output_mip_preview_window=scan_config.output_mip_preview_window,
            )

    backup_result = ValidationResult(ok=True, size_bytes=0)
    if check_backup_future:
        backup_result = check_backup_future.result(raise_on_failure=True)

    success = compress_result.ok and backup_result.ok

    strip_disk = get_disk_usage_info(os.fspath(strip_path))
    backup_disk = get_disk_usage_info(os.fspath(backup_path) if backup_path else None)

    usage_info = build_strip_usage_details(
        strip_size_bytes=initial_strip_manifest.total_bytes,
        zarr_size_bytes=compress_result.size_bytes,
        backup_size_bytes=backup_result.size_bytes,
        strip_disk=strip_disk,
        backup_disk=backup_disk,
    )

    should_cleanup = True
    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as strip_state:
        if not scan_config.distribute_archive and backup_result.ok and backup_path is not None:
            strip_state.set_archived(True)
        if success:
            strip_state.mark_completed()
        else:
            strip_state.mark_failed()
        # For distributed archive: atomically decide cleanup responsibility.
        # If archive is already done (archived=True), we take cleanup here;
        # otherwise the archive flow will handle it when it finishes.
        if scan_config.distribute_archive and archive_path is not None:
            should_cleanup = strip_state.archived

    failure_reasons: List[str] = []
    if not compress_result.ok and compress_result.reason:
        failure_reasons.append(f"compressed output: {compress_result.reason}")
    if not backup_result.ok and backup_result.reason:
        failure_reasons.append(f"backup: {backup_result.reason}")

    cleanup_note = describe_cleanup(strip_cleanup_action)
    status, message = build_status_message(
        success,
        failure_reasons,
        strip_ident.slice_id,
        strip_ident.strip_id,
        camera=acq,
    )

    usage_summary = format_strip_usage_details(usage_info)
    send_slack_message(f"{message}\n{cleanup_note}\n\n{usage_summary}")

    if not success:
        raise RuntimeError(message)

    archive_validated = (
        check_backup_future is not None
        and backup_result.ok
    )

    # ============================================================
    # CLEANUP ZIRCON STAGING DATA
    # ============================================================

    if should_cleanup and (
        archive_path is None
        or archive_validated
        or (scan_config.distribute_archive and should_cleanup)
    ):
        cleanup_future = run_cleanup_tasks(
            cleanup_action=strip_cleanup_action,
            strip_ident=strip_ident,
            strip_path=os.fspath(strip_path),
        )

        if cleanup_future:
            cleanup_future.wait()

    # ============================================================
    # REMOVE ORIGINAL WILLOW D: DATA
    # ============================================================

    if (
        success
        and archive_validated
        and not scan_config.distribute_archive
        and source_acquisition_folder is not None
    ):
        remove_archived_source(
            strip_ident=strip_ident,
            source_acquisition_folder=source_acquisition_folder,
            strip_folder_name=strip_path.name,
            archive_validated=True,
            processing_complete=True,
            backup_path=os.fspath(backup_path),
            dry_run=True,
        )

    return None

@flow
def process_strip_event(payload: Dict[str, Any]) -> None:
    """
    Process a strip of a slice.

    Payload must include ``strip_ident`` (dict) and ``strip_path``.
    """
    strip_ident = strip_ident_from_payload(payload)
    if "strip_path" not in payload:
        raise KeyError("payload must include strip_path")
    lsm_scan_config = load_scan_config_for_payload(strip_ident.project_name, payload)
    process_strip(
        strip_ident=strip_ident,
        strip_path=payload["strip_path"],
        scan_config=lsm_scan_config,
        force_rerun=force_rerun_from_payload(payload),
        source_acquisition_folder=payload.get("source_acquisition_folder"),
    )


def to_deployment(
    *,
    project_name: Optional[str] = None,
    deployment_name: str = "local",
    extra_tags: Sequence[str] = (),
    concurrent_workers: int = 2,
):
    """
    Create both deployments:
    - manual `process_strip`
    - event-driven `process_strip_event` (triggered by STRIP_READY)
    """
    manual = process_strip.to_deployment(
        name=deployment_name,
        tags=["lsm", "process-strip", *list(extra_tags)],
        concurrency_limit=concurrent_workers,
    )
    event = process_strip_event.to_deployment(
        name=deployment_name,
        tags=["event-driven", "lsm", "process-strip", *list(extra_tags)],
        concurrency_limit=concurrent_workers,
        triggers=[get_event_trigger(STRIP_READY, project_name=project_name)],
    )
    return [manual, event]


if __name__ == "__main__":
    import prefect

    prefect.serve(*to_deployment())

import os
import subprocess
from pathlib import Path
import time
from typing import Any, Dict, Optional, Sequence
from pathlib import PureWindowsPath
import re, base64

from prefect import flow, get_run_logger, task

from opticstream.events import get_event_trigger
from opticstream.events.lsm_events import STRIP_ARCHIVED, STRIP_READY
from opticstream.hooks.publish_hooks import (
    publish_lsm_project_hook,
    publish_lsm_slice_hook,
)
from opticstream.hooks.slack_notification_hook import slack_notification_hook
from opticstream.flows.lsm.utils import (
    host_lsm_fs_path,
    load_scan_config_for_payload,
    strip_ident_from_payload,
)
from opticstream.flows.lsm.strip_cleanup_flow import run_cleanup_tasks
from opticapi.project_state.lsm_models import LSMStripId
from opticstream.state import LSM_STATE_SERVICE
from opticstream.state.milestone_wrappers_lsm import strip_processing_milestone
from opticstream.state.state_guards import RunDecision, enter_milestone_stage, force_rerun_from_payload
from opticstream.utils.zarr_validation import (
    ValidationResult,
    compare_dir_manifests,
    get_dir_manifest,
)


def invalid_path(path: Optional[str]) -> bool:
    return path is None or path in {"/", ".", ""}

def validate_archive_mount(output_path: str) -> None:
    """
    Ensure the archive destination is on a mounted Willow E: or F: drive.
    """
    destination = Path(output_path)

    allowed_mounts = [
        Path("/mnt/willow-e"),
        Path("/mnt/willow-f"),
    ]

    for mount_path in allowed_mounts:
        if destination.is_relative_to(mount_path):
            if not mount_path.is_mount():
                raise RuntimeError(
                    f"Archive drive {mount_path} is not mounted."
                )
            return

    raise ValueError(
        f"Archive destination must be on Willow E: or F:: {output_path}"
    )


@task(task_run_name="archive-{strip_ident}", tags=["lsm-data-archive"])
def archive_strip(
    strip_ident: LSMStripId,
    strip_path: str,
    output_path: str,
    rate_limit_mb: int = 500,
    force_rerun: bool = False,
) -> None:
    """
    Archive a raw strip from Willow D: to Willow F: using Robocopy
    executed remotely over SSH.

    The original files on Willow D: are NOT deleted here.
    """
    logger = get_run_logger()
    logger.info("Archiving %s", strip_ident)

    strip_view = LSM_STATE_SERVICE.peek_strip(strip_ident=strip_ident)

    if (
        enter_milestone_stage(
            item_state_view=strip_view,
            item_ident=strip_ident,
            field_name="archived",
            force_rerun=force_rerun,
        )
        == RunDecision.SKIPPED
    ):
        return

    if invalid_path(output_path):
        raise ValueError(f"Unsafe archive destination: {output_path}")

    # Ensure Willow F: is mounted on Zircon for subsequent validation.
    validate_archive_mount(output_path)

    destination = Path(output_path)

    archive_roots = {
        Path("/mnt/willow-e/archive"): "E:",
        Path("/mnt/willow-f/archive"): "F:",
    }

    for archive_root, windows_drive in archive_roots.items():
        if destination.is_relative_to(archive_root):
            relative_path = destination.relative_to(archive_root)
            break
    else:
        raise ValueError(
            f"Archive destination must be under Willow E: or F: archive: "
            f"{destination}"
        )

    if len(relative_path.parts) != 2 or any(
        part in ("", ".", "..") for part in relative_path.parts
    ):
        raise ValueError(
            f"Expected <acquisition_folder>/<strip_folder>: {relative_path}"
        )

    acquisition_folder, strip_folder = relative_path.parts

    windows_source = str(
        PureWindowsPath("D:/data") / acquisition_folder / strip_folder
    )

    windows_destination = str(
        PureWindowsPath(f"{windows_drive}/archive")
        / acquisition_folder
        / strip_folder
    )

    logger.info(
        "Willow archive: %s -> %s",
        windows_source,
        windows_destination,
    )

    # ============================================================
    # ROBOCOPY STRIP DIRECTORY
    # ============================================================

    if not re.fullmatch(r"Slice\d+(?:_C2)?(?:_\d+)?", strip_folder):
        raise ValueError(
            f"Unexpected strip directory: {strip_folder}"
        )
    expected_camera = (
        2 if re.fullmatch(r"Slice\d+_C2(?:_\d+)?", strip_folder)
        else 1
    )

    if strip_ident.channel_id != expected_camera:
        raise ValueError(
            f"Camera mismatch for {strip_ident}: "
            f"{strip_folder} belongs to camera {expected_camera}"
        )

    for path in (windows_source, windows_destination):
        if any(ch in path for ch in "\"'\r\n&|<>^%!"):
            raise ValueError(
                f"Unsupported character in Windows path: {path}"
            )

    ps_script = rf"""
    $ErrorActionPreference = 'Stop'

    $source = '{windows_source}'
    $destination = '{windows_destination}'

    try {{

        if (-not (Test-Path -LiteralPath $source -PathType Container)) {{
            throw "Source directory does not exist: $source"
        }}

        New-Item -ItemType Directory -Path $destination -Force | Out-Null

        # Rate-limited Robocopy
        & robocopy $source $destination /E /MT:4 /IORATE:600M /R:2 /W:2 /NP /NFL /NDL

        $robocopyExitCode = $LASTEXITCODE

        if ($robocopyExitCode -ge 8) {{
            throw "Robocopy failed with exit code $robocopyExitCode"
        }}

        Write-Output "Robocopy completed successfully (exit code $robocopyExitCode)"
        exit 0
    }}
    catch {{
        [Console]::Error.WriteLine($_.Exception.Message)
        exit 1
    }}
    """

    encoded = base64.b64encode(
        ps_script.encode("utf-16le")
    ).decode("ascii")

    remote_command = (
        "powershell -NoProfile -NonInteractive "
        f"-EncodedCommand {encoded}"
    )

    command = [
        "ssh",
        "-x",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        "monkey@192.168.200.101",
        remote_command,
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        logger.info(result.stdout)

    if result.stderr:
        logger.warning(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Willow Robocopy failed for {strip_ident}: "
            f"{result.stderr}"
        )

    logger.info(
        "Archived %s: %s -> %s",
        strip_ident,
        windows_source,
        windows_destination,
    )


@task(task_run_name="check-backup-{strip_ident}")
def check_archive_result(
    strip_ident: LSMStripId,
    strip_path: str,
    backup_path: Optional[str] = None,
) -> ValidationResult:
    """
    Validate that the archived strip directory matches
    the corresponding Zircon staging directory.

    Camera 1: Slice1
    Camera 2: Slice1_C2
    """

    logger = get_run_logger()

    logger.info(
        "Checking archive for %s",
        strip_ident,
    )

    if backup_path is None:
        return ValidationResult(
            ok=False,
            size_bytes=0,
            reason="backup path not configured",
        )

    if not os.path.isdir(strip_path):
        return ValidationResult(
            ok=False,
            size_bytes=0,
            reason="source strip directory missing",
        )

    if not os.path.isdir(backup_path):
        return ValidationResult(
            ok=False,
            size_bytes=0,
            reason="backup directory missing",
        )

    # ============================================================
    # COMPARE COMPLETE STRIP DIRECTORY MANIFESTS
    # ============================================================

    source_manifest = get_dir_manifest(strip_path)
    backup_manifest = get_dir_manifest(backup_path)

    if source_manifest.file_count == 0:
        return ValidationResult(
            ok=False,
            size_bytes=0,
            reason="source strip directory empty",
        )

    if not compare_dir_manifests(
        source_manifest,
        backup_manifest,
        logger=logger,
    ):
        logger.error(
            "Archive differs from source for %s",
            strip_ident,
        )

        return ValidationResult(
            ok=False,
            size_bytes=backup_manifest.total_bytes,
            reason="backup differs from source",
        )

    # ============================================================
    # MARK STRIP ARCHIVED
    # ============================================================

    with LSM_STATE_SERVICE.open_strip(
        strip_ident=strip_ident
    ) as strip_state:
        strip_state.set_archived(True)

    logger.info(
        "Archive validated for %s: %d files, %.2f GB",
        strip_ident,
        backup_manifest.file_count,
        backup_manifest.total_bytes / 1e9,
    )

    return ValidationResult(
        ok=True,
        size_bytes=backup_manifest.total_bytes,
    )


@task(task_run_name="remove-willow-source-{strip_ident}")
def remove_archived_source(
    strip_ident: LSMStripId,
    source_acquisition_folder: str,
    strip_folder_name: str,
    archive_validated: bool,
    backup_path: str,
    processing_complete: bool,
    dry_run: bool = True,
) -> None:
    """
    Remove one completed strip directory from Willow D: after
    successful processing and archive validation.

    Camera 1: Slice1
    Camera 2: Slice1_C2

    Never deletes the parent acquisition directory.
    """

    logger = get_run_logger()

    # ============================================================
    # VALIDATE PROCESSING / ARCHIVE STATUS
    # ============================================================

    if (
            not source_acquisition_folder
            or source_acquisition_folder in (".", "..")
            or "/" in source_acquisition_folder
            or "\\" in source_acquisition_folder
            or ":" in source_acquisition_folder
        ):
            raise ValueError(
                f"Unsafe acquisition folder: {source_acquisition_folder}"
            )

    if not archive_validated:
        raise RuntimeError(
            f"Refusing deletion for {strip_ident}: "
            "archive has not been validated."
        )

    if not processing_complete:
        raise RuntimeError(
            f"Refusing deletion for {strip_ident}: "
            "processing has not completed successfully."
        )

    validate_archive_mount(backup_path)

    backup = Path(backup_path)

    archive_roots = {
        Path("/mnt/willow-e/archive"): "E:",
        Path("/mnt/willow-f/archive"): "F:",
    }

    for archive_root, windows_drive in archive_roots.items():
        if backup.is_relative_to(archive_root):
            relative_path = backup.relative_to(archive_root)
            break
    else:
        raise ValueError(
            f"Invalid archive destination: {backup_path}"
        )

    if (
        len(relative_path.parts) != 2
        or relative_path.parts != (
            source_acquisition_folder,
            strip_folder_name,
        )
    ):
        raise ValueError(
            f"Archive/source path mismatch: {relative_path}"
        )

    # ============================================================
    # VALIDATE STRIP / CAMERA NAMING
    # ============================================================

    if not re.fullmatch(r"Slice\d+(?:_C2)?(?:_\d+)?", strip_folder_name):
        raise ValueError(
            f"Unexpected strip directory: {strip_folder_name}"
        )

    expected_camera = (
        2 if re.fullmatch(r"Slice\d+_C2(?:_\d+)?", strip_folder_name)
        else 1
    )

    if strip_ident.channel_id != expected_camera:
        raise ValueError(
            f"Camera mismatch for {strip_ident}: "
            f"{strip_folder_name} belongs to camera {expected_camera}"
        )

    windows_source = str(
        PureWindowsPath("D:/data")
        / source_acquisition_folder
        / strip_folder_name
    )

    windows_destination = str(
        PureWindowsPath(f"{windows_drive}/archive")
        / source_acquisition_folder
        / strip_folder_name
    )

    for path in (windows_source, windows_destination):
        if any(ch in path for ch in "\"'\r\n&|<>^%!"):
            raise ValueError(
                f"Unsafe Windows path: {path}"
            )

    if not backup.is_dir():
        raise RuntimeError(
            f"Archive destination does not exist: {backup}"
        )

    # ============================================================
    # CHECK ARCHIVED STATE
    # ============================================================

    strip_view = LSM_STATE_SERVICE.read_strip(
        strip_ident=strip_ident
    )

    if not strip_view or not strip_view.archived:
        raise RuntimeError(
            f"Refusing deletion: {strip_ident} is not marked archived."
        )

    # ============================================================
    # BUILD POWERSHELL SCRIPT
    # ============================================================

    ps_script = rf"""
$ErrorActionPreference = 'Stop'

$source = '{windows_source}'
$destination = '{windows_destination}'
$dryRun = ${str(dry_run).lower()}

try {{

    if (-not (Test-Path -LiteralPath $source -PathType Container)) {{
        throw "Source directory does not exist: $source"
    }}

    if (-not (Test-Path -LiteralPath $destination -PathType Container)) {{
        throw "Archive directory does not exist: $destination"
    }}

    # ========================================================
    # ENUMERATE SOURCE AND ARCHIVE FILES
    # ========================================================

    $sourceFiles = @(
        Get-ChildItem -LiteralPath $source -File -Recurse -Force
    )

    $archiveFiles = @(
        Get-ChildItem -LiteralPath $destination -File -Recurse -Force
    )

    if ($sourceFiles.Count -eq 0) {{
        throw "Source strip directory is empty"
    }}

    if ($sourceFiles.Count -ne $archiveFiles.Count) {{
        throw "Source/archive file count mismatch: source=$($sourceFiles.Count), archive=$($archiveFiles.Count)"
    }}

    # ========================================================
    # BUILD ARCHIVE MANIFEST USING RELATIVE FILE PATHS
    # ========================================================

    $archiveLookup = @{{}}

    foreach ($file in $archiveFiles) {{

        $relative = $file.FullName.Substring(
            $destination.Length
        ).TrimStart('\')

        $archiveLookup[$relative] = $file.Length
    }}

    # ========================================================
    # VALIDATE SOURCE FILES AGAINST ARCHIVE
    # ========================================================

    foreach ($file in $sourceFiles) {{

        $relative = $file.FullName.Substring(
            $source.Length
        ).TrimStart('\')

        if (-not $archiveLookup.ContainsKey($relative)) {{
            throw "Missing archive file: $relative"
        }}

        if ($file.Length -ne $archiveLookup[$relative]) {{
            throw "File size mismatch: $relative"
        }}
    }}

    Write-Output "Archive validation successful"
    Write-Output "Validated $($sourceFiles.Count) files"

    # ========================================================
    # DELETE ONLY THIS STRIP DIRECTORY
    # ========================================================

    if ($dryRun) {{

        Write-Output "[DRY RUN] Would delete strip directory: $source"

    }}
    else {{

        Remove-Item `
            -LiteralPath $source `
            -Recurse `
            -Force `
            -ErrorAction Stop

        if (Test-Path -LiteralPath $source) {{
            throw "Strip directory still exists after deletion"
        }}

        Write-Output "Deleted strip directory: $source"
    }}

    Write-Output "Strip cleanup completed"

    exit 0
}}
catch {{
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}}
"""

    # ============================================================
    # EXECUTE ON WILLOW
    # ============================================================

    encoded = base64.b64encode(
        ps_script.encode("utf-16le")
    ).decode("ascii")

    remote_command = (
        "powershell -NoProfile -NonInteractive "
        f"-EncodedCommand {encoded}"
    )

    command = [
        "ssh",
        "-x",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        "monkey@192.168.200.101",
        remote_command,
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        logger.info(result.stdout)

    if result.stderr:
        logger.warning(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Willow source cleanup failed for {strip_ident}: "
            f"{result.stderr}"
        )

    if dry_run:
        logger.info(
            "[DRY RUN] Willow strip directory validated for %s: %s",
            strip_ident,
            windows_source,
        )
    else:
        logger.info(
            "Deleted Willow strip directory for %s: %s",
            strip_ident,
            windows_source,
        )

@task
def check_disbributed_archive_result(
    strip_ident: LSMStripId,
    timeout: int,
    strip_path: Optional[str] = None,
    backup_path: Optional[str] = None,
) -> ValidationResult:
    """
    Check if the backup strip is valid.
    """
    logger = get_run_logger()
    logger.info(f"Checking if backup for {strip_ident} is valid")
    start_time = time.time()
    while time.time() - start_time < timeout:
        strip_view = LSM_STATE_SERVICE.peek_strip(strip_ident=strip_ident)
        if strip_view.archived:
            size_bytes = 0
            if backup_path and os.path.exists(backup_path):
                size_bytes = get_dir_manifest(backup_path).total_bytes
            return ValidationResult(ok=True, size_bytes=size_bytes)
        time.sleep(1)

    return ValidationResult(ok=False, size_bytes=0, reason="timeout")

@flow(
    flow_run_name="archive-strip-{strip_ident}",
    on_completion=[publish_lsm_slice_hook, publish_lsm_project_hook],
    on_failure=[slack_notification_hook],
)
@strip_processing_milestone(
    field_name="archived",
    success_event=STRIP_ARCHIVED,
)
def archive_strip_flow(
    strip_ident: LSMStripId,
    strip_path: str,
    archive_path: str,
    source_acquisition_folder: str,
    rate_limit_mb: int = 500,
    force_rerun: bool = False,
) -> None:
    """
    Archive a raw strip to Willow F: or E:, then validate the copy.

    Expected destination:
        <archive_path>/<source_acquisition_folder>/<strip_folder>

    Example:
        /mnt/willow-f/archive/
            sub-test-before-m2-start-slice-5-strip-1/
                Slice5_C2_2/
    """

    logger = get_run_logger()

    # ============================================================
    # RESOLVE PATHS
    # ============================================================

    resolved_strip = os.fspath(host_lsm_fs_path(strip_path))
    resolved_archive = os.fspath(host_lsm_fs_path(archive_path))

    strip_folder = Path(resolved_strip).name

    # ============================================================
    # VALIDATE ACQUISITION FOLDER
    # ============================================================

    if (
        not source_acquisition_folder
        or source_acquisition_folder in (".", "..")
        or "/" in source_acquisition_folder
        or "\\" in source_acquisition_folder
        or ":" in source_acquisition_folder
    ):
        raise ValueError(
            f"Unsafe acquisition folder: {source_acquisition_folder}"
        )

    # ============================================================
    # BUILD ARCHIVE DESTINATION
    # ============================================================

    backup_path = os.path.join(
        resolved_archive,
        source_acquisition_folder,
        strip_folder,
    )

    logger.info(
        "Archiving %s: %s -> %s",
        strip_ident,
        resolved_strip,
        backup_path,
    )

    # ============================================================
    # ARCHIVE
    # ============================================================

    backup_future = archive_strip.submit(
        strip_ident=strip_ident,
        strip_path=resolved_strip,
        output_path=backup_path,
        rate_limit_mb=rate_limit_mb,
        force_rerun=force_rerun,
    )

    # ============================================================
    # VALIDATE ARCHIVE
    # ============================================================

    validation_result = check_archive_result.submit(
        strip_ident=strip_ident,
        strip_path=resolved_strip,
        backup_path=backup_path,
        wait_for=[backup_future],
    ).result(raise_on_failure=True)

    if not validation_result.ok:
        raise RuntimeError(
            f"Archive validation failed for {strip_ident}: "
            f"{validation_result.reason}"
        )

    logger.info(
        "Archive completed and validated for %s",
        strip_ident,
    )


@flow
def archive_strip_event_flow(payload: Dict[str, Any]) -> None:
    """
    Event-driven wrapper for ``archive_strip_flow``, triggered by ``STRIP_READY``.

    Payload must include ``strip_ident`` (dict) and ``strip_path``.
    The archive destination is read from the project's scan config
    ``archive_path``; the flow is a no-op when that field is unset.
    """
    strip_ident = strip_ident_from_payload(payload)
    if "strip_path" not in payload:
        raise KeyError("payload must include strip_path")
    cfg = load_scan_config_for_payload(strip_ident.project_name, payload)
    if cfg.archive_path is None:
        get_run_logger().warning(
            f"archive_path not configured for {strip_ident.project_name}, skipping archive"
        )
        return
    source_acquisition_folder = payload.get(
        "source_acquisition_folder"
    )

    if not source_acquisition_folder:
        raise ValueError(
            "Archive event payload missing source_acquisition_folder"
        )

    archive_strip_flow(
        strip_ident=strip_ident,
        strip_path=payload["strip_path"],
        archive_path=os.fspath(cfg.archive_path),
        source_acquisition_folder=source_acquisition_folder,
        rate_limit_mb=cfg.archive_rate_limit,
        force_rerun=force_rerun_from_payload(payload),
    )

    if cfg.distribute_archive:
        # Locked read: archived is already True (set inside archive_strip_flow).
        # If compressed is also True, the process flow already finished and
        # handed cleanup responsibility to us. Otherwise, process will handle
        # cleanup when it completes (it will see archived=True in its open_strip block).
        strip_view = LSM_STATE_SERVICE.read_strip(strip_ident=strip_ident)
        if strip_view and strip_view.compressed:
            cleanup_future = run_cleanup_tasks(
                cleanup_action=cfg.strip_cleanup_action,
                strip_ident=strip_ident,
                strip_path=payload["strip_path"],
            )
            if cleanup_future:
                cleanup_future.wait()


def to_deployment(
    *,
    project_name: Optional[str] = None,
    deployment_name: str = "local",
    extra_tags: Sequence[str] = (),
    concurrent_workers: int = 1,
) -> list:
    """
    Create both deployments:
    - manual ``archive_strip_flow``
    - event-driven ``archive_strip_event_flow`` (triggered by ``STRIP_READY``)
    """
    manual = archive_strip_flow.to_deployment(
        name=deployment_name,
        tags=["lsm", "archive-strip", *list(extra_tags)],
        concurrency_limit=concurrent_workers,
    )
    event = archive_strip_event_flow.to_deployment(
        name=deployment_name,
        tags=["event-driven", "lsm", "archive-strip", *list(extra_tags)],
        concurrency_limit=concurrent_workers,
        triggers=[get_event_trigger(STRIP_READY, project_name=project_name)],
    )
    return [manual, event]


if __name__ == "__main__":
    import prefect

    prefect.serve(*to_deployment())

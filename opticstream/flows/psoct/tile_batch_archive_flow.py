import os
from pathlib import Path

from prefect import flow, get_run_logger, task

from opticstream.hooks.publish_hooks import (
    publish_oct_mosaic_hook,
    publish_oct_project_hook,
)
from opticstream.events.psoct_event_emitters import emit_batch_psoct_event
from opticstream.events.psoct_events import BATCH_ARCHIVED
from opticstream.flows.psoct.tile_file_reference import TileFileReference
from opticstream.state.milestone_wrappers_psoct import oct_batch_processing_milestone
from opticapi.project_state.oct_models import OCTBatchId
from opticstream.state import OCT_STATE_SERVICE
from opticstream.tasks.archive_file import archive_file
from opticstream.hooks.slack_notification_hook import slack_notification_hook


@task(
    on_completion=[publish_oct_mosaic_hook, publish_oct_project_hook],
    on_failure=[slack_notification_hook],
)
@oct_batch_processing_milestone(field_name="archived")
def archive_tile_batch(
    batch_id: OCTBatchId,
    file_reference_list: list[TileFileReference],
    *,
    acquisition_label: str,
    archive_path: Path,
    archive_tile_name_format: str,
    force_rerun: bool = False,
) -> None:
    logger = get_run_logger()
    archive_path.mkdir(parents=True, exist_ok=True)
    with OCT_STATE_SERVICE.open_batch(batch_ident=batch_id) as batch:
        batch.reset_archived()

    archived_file_paths: list[str] = []
    futures = []
    for ref in file_reference_list:
        if ref.spectral_file_path:
            input_path = ref.spectral_file_path
        elif ref.complex_file_path:
            input_path = ref.complex_file_path
        else:
            raise ValueError(f"No input path found for tile {ref.tile_number}, {ref}")
        output_name = archive_tile_name_format.format(
            project_name=batch_id.project_name,
            slice_id=batch_id.slice_id,
            tile_id=batch_id.batch_id,
            acq=acquisition_label,
        )
        output_path = archive_path / output_name
        output_path.parent.mkdir(parents=True, exist_ok=True)
        futures.append(archive_file.submit(input_path, output_path))
        archived_file_paths.append(str(output_path))

    for future in futures:
        future.wait()

    logger.info("Archived %d files for %s", len(archived_file_paths), batch_id)
    files_with_issue = check_archive_result(batch_id, archived_file_paths)
    if files_with_issue:
        raise RuntimeError(
            "Archive validation failed for "
            f"{len(files_with_issue)} file(s): " + " | ".join(files_with_issue)
        )

    emit_batch_psoct_event(
        BATCH_ARCHIVED,
        batch_id,
        extra_payload={"file_list": archived_file_paths},
    )


def check_archive_result(
    batch_id: OCTBatchId,
    archived_file_paths: list[str],
    min_file_size_bytes: int = 200 * 1024 * 1024,
) -> list[str]:
    logger = get_run_logger()
    logger.info("Checking if the archived files are valid for %s", batch_id)
    files_with_issue: list[str] = []
    for archived_file_path in archived_file_paths:
        if not os.path.exists(archived_file_path):
            logger.error("Archived file %s does not exist", archived_file_path)
            files_with_issue.append(f"{archived_file_path} (missing)")
            continue

        file_size = os.path.getsize(archived_file_path)
        if file_size <= min_file_size_bytes:
            logger.error(
                "Archived file %s is too small: %d bytes (threshold: %d bytes)",
                archived_file_path,
                file_size,
                min_file_size_bytes,
            )
            files_with_issue.append(
                f"{archived_file_path} (size={file_size}B, min={min_file_size_bytes}B)"
            )
    return files_with_issue


@flow(
    flow_run_name="archive-tile-batch-{batch_id}",
    on_completion=[publish_oct_mosaic_hook, publish_oct_project_hook],
    on_failure=[slack_notification_hook],
)
def archive_tile_batch_flow(
    batch_id: OCTBatchId,
    file_reference_list: list[TileFileReference],
    *,
    acquisition_label: str,
    archive_path: Path,
    archive_tile_name_format: str,
    force_rerun: bool = False,
) -> None:
    archive_tile_batch(
        batch_id=batch_id,
        file_reference_list=file_reference_list,
        acquisition_label=acquisition_label,
        archive_path=archive_path,
        archive_tile_name_format=archive_tile_name_format,
        force_rerun=force_rerun,
    )

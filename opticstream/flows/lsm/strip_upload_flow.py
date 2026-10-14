import os
import shutil
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from prefect import flow, get_run_logger

from opticstream.hooks.publish_hooks import (
    publish_lsm_project_hook,
    publish_lsm_slice_hook,
)
from opticstream.config.lsm_scan_config import get_lsm_scan_config
from opticstream.events import get_event_trigger
from opticstream.events.lsm_events import STRIP_COMPRESSED, STRIP_UPLOADED
from opticstream.state.milestone_wrappers_lsm import strip_processing_milestone
from opticstream.state.state_guards import force_rerun_from_payload
from opticstream.flows.lsm.utils import (
    host_lsm_fs_path,
    lsm_output_root,
    strip_ident_from_payload,
    strip_zarr_output_path,
)
from opticapi.project_state.lsm_models import LSMStripId
from opticstream.tasks.dandi_upload import upload_to_dandi
from opticstream.hooks.slack_notification_hook import slack_notification_hook


# ============================================================
# CLEANUP HELPER
# ============================================================

def delete_uploaded_zarr(
    output_path: Path,
    expected_path: Path,
    output_root: Path,
) -> None:
    """
    Delete one compressed LSM OME-Zarr directory after successful upload.

    Safety checks:
      - Target must be an existing directory.
      - Target must not be a symbolic link.
      - Target must have an .ome.zarr suffix.
      - Target must match the expected strip path.
      - Target must be directly inside the configured output directory.
    """
    output_path = Path(output_path)
    expected_path = Path(expected_path)
    output_root = Path(output_root)

    if output_path.is_symlink():
        raise ValueError(f"Refusing to delete symbolic link: {output_path}")

    if not output_path.is_dir():
        raise ValueError(f"Expected Zarr directory does not exist: {output_path}")

    if not output_path.name.endswith(".ome.zarr"):
        raise ValueError(f"Refusing to delete non-OME-Zarr directory: {output_path}")

    target = output_path.resolve()
    expected = expected_path.resolve()
    root = output_root.resolve()

    if target != expected:
        raise ValueError(
            f"Output path does not match expected path: {target} != {expected}"
        )

    if not target.is_relative_to(root) or target == root:
        raise ValueError(
            f"Refusing to delete Zarr outside configured output root: {target}"
        )

    shutil.rmtree(output_path)


# ============================================================
# UPLOAD FLOW
# ============================================================

@flow(
    flow_run_name="upload-to-dandi-{strip_ident}",
    on_completion=[publish_lsm_slice_hook, publish_lsm_project_hook],
    on_failure=[slack_notification_hook],
)
@strip_processing_milestone(field_name="uploaded", success_event=STRIP_UPLOADED)
def upload_strip_to_dandi_flow(
    strip_ident: LSMStripId,
    output_path: Path,
    dandi_instance: str = "linc",
    dandi_bin: str = "dandi",
    force_rerun: bool = False,
    delete_compressed_after_upload: bool = False,
    expected_output_path: Optional[Path] = None,
    output_root: Optional[Path] = None,
) -> None:
    """
    Upload the strip to DANDI.

    Optionally delete the local compressed OME-Zarr after a
    successful upload.
    """
    logger = get_run_logger()

    output_path = host_lsm_fs_path(output_path)

    logger.info(f"Output path: {output_path}")
    logger.info(f"Uploading {strip_ident} to DANDI")

    upload_to_dandi(
        os.fspath(output_path),
        dandi_instance=dandi_instance,
        dandi_bin=dandi_bin,
    )

    logger.info(f"Successfully uploaded {strip_ident} to DANDI")

    # --------------------------------------------------------
    # OPTIONAL CLEANUP AFTER SUCCESSFUL UPLOAD
    # --------------------------------------------------------

    if delete_compressed_after_upload:
        try:
            if expected_output_path is None or output_root is None:
                raise ValueError(
                    "Expected output path and output root are required "
                    "when cleanup is enabled."
                )

            delete_uploaded_zarr(
                output_path=output_path,
                expected_path=host_lsm_fs_path(expected_output_path),
                output_root=host_lsm_fs_path(output_root),
            )

            logger.info(f"Deleted uploaded compressed Zarr: {output_path}")

        except Exception:
            logger.exception(
                f"Upload succeeded, but cleanup failed for {output_path}"
            )


# ============================================================
# EVENT-DRIVEN UPLOAD FLOW
# ============================================================

@flow
def upload_strip_to_dandi_event_flow(payload: Dict[str, Any]) -> None:
    """
    Event wrapper flow for uploading the strip to DANDI.

    Payload requires ``strip_ident`` (dict); zarr path is derived from config.
    """
    strip_ident = strip_ident_from_payload(payload)

    cfg = get_lsm_scan_config(
        strip_ident.project_name,
        override_config_name=payload.get("override_config"),
    )

    output_path = strip_zarr_output_path(strip_ident, cfg)

    upload_strip_to_dandi_flow(
        strip_ident=strip_ident,
        output_path=output_path,
        force_rerun=force_rerun_from_payload(payload),
        dandi_instance=payload.get("dandi_instance") or cfg.dandi_instance,
        dandi_bin=payload.get("dandi_bin") or cfg.dandi_bin,
        delete_compressed_after_upload=cfg.delete_compressed_after_upload,
        expected_output_path=output_path,
        output_root=lsm_output_root(cfg),
    )


# ============================================================
# DEPLOYMENTS
# ============================================================

def to_deployment(
    *,
    project_name: Optional[str] = None,
    deployment_name: str = "local",
    extra_tags: Sequence[str] = (),
):
    manual = upload_strip_to_dandi_flow.to_deployment(
        name=deployment_name,
        tags=["lsm", "strip", "upload", *list(extra_tags)],
    )

    event = upload_strip_to_dandi_event_flow.to_deployment(
        name=deployment_name,
        tags=["event-driven", "lsm", "strip", "upload", *list(extra_tags)],
        triggers=[get_event_trigger(STRIP_COMPRESSED, project_name=project_name)],
    )

    return [manual, event]
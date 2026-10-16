from enum import Enum
from typing import List, Optional
from pathlib import Path

from niizarr import ZarrConfig
from pydantic import BaseModel, Field, field_validator

from opticapi.config.utils import with_positions
from opticapi.naming import normalize_project_name


class StripCleanupAction(str, Enum):
    KEEP = "keep"
    RENAME = "rename"
    DELETE = "delete"


@with_positions
class LSMScanConfigModel(BaseModel):
    project_name: str = Field(..., min_length=1)
    project_base_path: Path = Field(
        ..., description="Base filesystem path for project data"
    )
    info_file: Path = Field(..., description="Path to the info .mat file")
    strip_folder_size_threshold: float = Field(
        default=25.0,
        gt=0,
        description="Min total size in GB for raw strip folder when running; if lower than this, the processing will just fail",
    )
    output_path: Path | None = Field(
        default=None, description="Path to the output directory for compressed strips, if not set, no zarr output will be generated"
    )
    output_format: Optional[str] = Field(
        default=(
            "{project_name}_sample-slice{slice_id:03d}_chunk-{strip_id:04d}_acq-{acq}.ome.zarr"
        ),
        min_length=1,
    )
    generate_mip: bool = Field(
        default=True, description="Whether to generate the MIP output. Requires output_path to be set."
    )
    output_mip_format: Optional[str] = Field(
        default=(
            "{project_name}_sample-slice{slice_id:03d}_chunk-{strip_id:04d}_acq-{acq}_proc-mip.tiff"
        ),
        min_length=1,
    )
    output_mip_preview_window: List[float] = Field(
        [0, 1000],
        description="Preview window for the MIP output. If not set, the full range of the MIP will be used.",
    )
    archive_path: None | Path = Field(
        default=None,
        description="Path to the archive directory for backup of compressed strips",
    )
    distribute_archive: bool = Field(
        default=False,
        description="Whether to run the archive process in a distributed manner",
    )
    distribute_archive_timeout: int = Field(
        default=600, ge=0,
        description="Timeout for the archive process in seconds, after compression is complete",
    )
    archive_rate_limit: int = Field(
        default=500,
        ge=1,
        description="Rate limit for the archive process in MB/s",
    )
    delete_compressed_after_upload: bool = Field(
        default=False,
        description=(
            "Delete local compressed LSM OME-Zarr outputs after successful DANDI upload."
        ),
    )
    strip_cleanup_action: StripCleanupAction = Field(
        default=StripCleanupAction.RENAME,
        description="How to handle the raw spooled strip after compression/backup: keep, rename into processed/, or delete",
    )
    strips_per_slice: int = Field(
        default=100, ge=1, description="Number of strips per slice to process"
    )

    zarr_config: ZarrConfig = Field(default_factory=ZarrConfig)

    dandi_bin: str = Field(
        default="dandi",
        min_length=1,
        description="Path to the DANDI CLI binary, useful for installation in a separate conda/venv environment",
    )
    dandi_instance: str = Field(
        default="linc", min_length=1, description="DANDI instance to use for upload"
    )
    dandiset_path: str = Field(
        default="linc://000052/", description="Path to the DANDI set to use for upload"
    )

    # for acquisition host
    cpu_affinity: Optional[List[int]] = Field(
        default=None,
        description="Range of CPU cores to use for processing. If empty, all cores will be used.",
    )
    num_workers: int = Field(
        default=8, ge=1, description="Number of workers to use for compression"
    )

    stitch_volume: bool = Field(
        default=False, description="Whether to stitch the volume from the strips"
    )

    channel_volume_zarr_size_threshold: float = Field(
        default=1.0,
        gt=0,
        description="Min total size in GB for stitched channel volume zarr when validation runs",
    )
    skip_channel_volume_zarr_validation: bool = Field(
        default=True,
        description=(
            "If True, only ensure volume output dir exists after stitch (placeholder-friendly). "
            "Set False when volume stitch writes a real zarr to enforce size threshold."
        ),
    )

    @field_validator("output_mip_preview_window")
    @classmethod
    def validate_output_mip_preview_window(cls, value: List[float]) -> List[float]:
        if len(value) not in (0, 2):
            raise ValueError("output_mip_preview_window must have 0 or 2 values")
        if len(value) == 2 and value[0] >= value[1]:
            raise ValueError(
                "output_mip_preview_window must be [min, max] with min < max"
            )
        return value

    @field_validator("cpu_affinity")
    @classmethod
    def validate_cpu_affinity(cls, value: List[int]) -> List[int]:
        if any(core < 0 for core in value):
            raise ValueError("cpu_affinity values must be >= 0")
        if len(set(value)) != len(value):
            raise ValueError("cpu_affinity values must be unique")
        return value

    @field_validator("archive_path")
    @classmethod
    def validate_archive_path(cls, value: Path | None) -> Path | None:
        if value is not None and len(str(value)) < 1:
            raise ValueError("archive_path must be longer than 1 character")
        return value

    @field_validator("output_path")
    @classmethod
    def validate_output_path(cls, value: Path | None) -> Path | None:
        if value is not None and len(str(value)) < 1:
            raise ValueError("output_path must be longer than 1 character")
        return value


class LSMScanConfigOverrides(BaseModel):
    project_base_path: Path | None = None
    info_file: Path | None = None
    output_path: Path | None = None
    output_format: Optional[str] = None
    generate_mip: Optional[bool] = None
    output_mip_format: Optional[str] = None
    archive_path: Path | None = None
    strip_cleanup_action: Optional[StripCleanupAction] = None
    strips_per_slice: Optional[int] = Field(default=None, ge=1)

    zarr_config: Optional[ZarrConfig] = None

    dandi_bin: Optional[str] = None
    dandi_instance: Optional[str] = None
    dandiset_path: Path | None = None

    cpu_affinity: Optional[List[int]] = Field(default=None)
    num_workers: Optional[int] = Field(default=None, ge=1)
    stitch_volume: Optional[bool] = None
    strip_folder_size_threshold: Optional[float] = Field(default=None, gt=0)
    channel_volume_zarr_size_threshold: Optional[float] = Field(default=None, gt=0)
    skip_channel_volume_zarr_validation: Optional[bool] = None


def get_lsm_scan_config_block_name(project_name: str) -> str:
    return f"{normalize_project_name(project_name)}-lsm-config"

from __future__ import annotations

import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from opticstream.cli.oct import oct_cli
from opticapi.config.psoct_scan_config import (
    PSOCTScanConfigModel,
    TileSavingType,
)
from opticstream.config.psoct_scan_config import get_psoct_scan_config
from opticstream.events import BATCH_READY
from opticstream.events.psoct_event_emitters import emit_batch_psoct_event
from opticstream.flows.psoct.tile_batch_process_flow import process_tile_batch
from opticstream.flows.psoct.utils import oct_batch_ident
from opticstream.flows.psoct.utils import (
    logical_mosaic_from_source_mosaic,
    slice_from_mosaic,
)
from opticstream.state import OCT_STATE_SERVICE
from opticstream.utils.filename_utils import (
    extract_processed_index_from_filename,
    extract_spectral_index_from_filename,
)
from opticstream.utils.polling_watcher import PollingStableWatcher
from opticstream.utils.refresh_disk import RefreshHook, resolve_refresh_hook

logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _configure_logging(verbose: bool) -> None:
    """Force log level on the root logger and ensure a StreamHandler is present.

    basicConfig() is a no-op when any library has already attached a handler,
    so we set the level explicitly here instead of relying on it.
    """
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
        root.addHandler(handler)
    root.setLevel(level)
    # Ensure the opticstream namespace is not filtered out by a library-set level.
    logging.getLogger("opticstream").setLevel(level)


_COMPLEX_RE = re.compile(
    r"^mosaic_(?P<mosaic>\d+)_image_(?P<image>\d+)_processed(?:_cropped)?\.nii$",
    re.IGNORECASE,
)
_SPECTRAL_NII_RE = re.compile(
    r"^mosaic_(?P<mosaic>\d+)_image_(?P<image>\d+)_spectral_(?P<k>\d+)\.nii$",
    re.IGNORECASE,
)
_SPECTRAL_RAW_RE = re.compile(
    r"^mosaic_(?P<mosaic>\d+)_image_(?P<image>\d+)_spectral_(?P<k>\d+)\.raw$",
    re.IGNORECASE,
)
_TILE_NII_RE = re.compile(
    r"^mosaic_(?P<mosaic>\d+)_image_(?P<image>\d+)_\w+\.nii$",
    re.IGNORECASE,
)


def _is_readable_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    if not os.access(path, os.R_OK | os.X_OK):
        return False
    try:
        next(path.iterdir(), None)
    except (OSError, PermissionError):
        return False
    return True


_MIN_FILE_SIZE_BYTES = 100 * 1024  # 100 KB


def _is_readable_file(path: Path) -> bool:
    if not path.is_file():
        return False
    if not os.access(path, os.R_OK):
        return False
    try:
        if path.stat().st_size < _MIN_FILE_SIZE_BYTES:
            return False
    except (OSError, PermissionError):
        return False
    return True


def _all_files_readable(files: tuple[Path, ...]) -> bool:
    return all(_is_readable_file(p) for p in files)


def _batch_fingerprint(
    files: tuple[Path, ...],
    root: Path,
) -> tuple[tuple[str, float, int], ...]:
    items: list[tuple[str, float, int]] = []
    for p in files:
        stat = p.stat()
        items.append((str(p.relative_to(root)), stat.st_mtime, stat.st_size))
    items.sort()
    return tuple(items)


def _parse_mosaic_ranges(mosaic_ranges_str: str) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for range_str in mosaic_ranges_str.split(","):
        parts = range_str.strip().split(":")
        if len(parts) != 2:
            raise ValueError(
                f"Invalid mosaic range format: {range_str!r}. Expected 'min:max'"
            )
        out.append((int(parts[0]), int(parts[1])))
    return out


def _apply_batch_offset(
    *,
    local_batch: int,
    batch_offset: int,
    batches_per_slice: int,
) -> tuple[int, int]:
    """
    Converts local batch numbering within the watched folder into:
      - slice_carry: how many logical slices to advance
      - logical_batch: 1-based batch/strip within the logical slice
    """
    if batches_per_slice < 1:
        raise ValueError(f"batches_per_slice must be >= 1, got {batches_per_slice}")

    zero_based = (local_batch - 1) + batch_offset
    slice_carry = zero_based // batches_per_slice
    logical_batch = (zero_based % batches_per_slice) + 1

    return slice_carry, logical_batch


@dataclass(frozen=True)
class ParsedTileFile:
    path: Path
    source_mosaic_id: int
    image_index: int


@dataclass(frozen=True)
class ParsedProcessedFile:
    path: Path
    source_mosaic_id: int
    tile_number: int


@dataclass(frozen=True)
class OCTBatchCandidate:
    source_slice_id: int
    logical_slice_id: int
    source_mosaic_id: int | None
    logical_mosaic_id: int
    logical_batch: int
    files: tuple[Path, ...]


class OCTWatcherService:
    def __init__(
        self,
        *,
        project_name: str,
        folder_path: Path,
        project_base_path: str,
        mosaic_ranges: list[tuple[int, int]],
        slice_offset: int,
        batch_offset: int,
        batch_size: int,
        scan_config: PSOCTScanConfigModel,
        direct: bool,
        force_resend: bool,
        refresh_hook: RefreshHook | None = None,
        min_complex_file_size_bytes: int = 1,
        prefer_spectral_for_complex_with_spectral: bool = True,
    ) -> None:
        self.project_name = project_name
        self.folder_path = folder_path
        self.project_base_path = project_base_path
        self.mosaic_ranges = mosaic_ranges
        self.slice_offset = slice_offset
        self.batch_offset = batch_offset
        self.batch_size = batch_size
        self.scan_config = scan_config
        self.direct = direct
        self.force_resend = force_resend
        self.refresh_hook = refresh_hook
        self.min_complex_file_size_bytes = min_complex_file_size_bytes
        self.prefer_spectral_for_complex_with_spectral = (
            prefer_spectral_for_complex_with_spectral
        )
        self._discover_cache_key: tuple[float, ...] | None = None
        self._discover_cache_result: list[OCTBatchCandidate] = []
        self._skip_candidates: set[tuple[int, int]] = set()  # candidates confirmed dispatched or state-exists

    def _selected_tile_saving_type(self) -> TileSavingType:
        saving_type = self.scan_config.acquisition.tile_saving_type
        if saving_type is TileSavingType.COMPLEX_WITH_SPECTRAL:
            return (
                TileSavingType.SPECTRAL
                if self.prefer_spectral_for_complex_with_spectral
                else TileSavingType.COMPLEX
            )
        return saving_type

    def discover_candidates(self) -> list[OCTBatchCandidate]:
        if not _is_readable_dir(self.folder_path):
            logger.warning(
                "OCT watch directory is not readable right now: %s", self.folder_path
            )
            return []

        try:
            dir_mtime = self.folder_path.stat().st_mtime
        except OSError:
            return []
        mtimes: list[float] = [dir_mtime]
        if self.scan_config.mosaics_per_slice == 3:
            spectral_dir = self.folder_path / "spectral"
            try:
                if spectral_dir.is_dir():
                    mtimes.append(spectral_dir.stat().st_mtime)
            except OSError:
                pass
        cache_key = tuple(mtimes)

        if cache_key != self._discover_cache_key:
            logger.debug("Scanning folder: %s", self.folder_path)
            if self.scan_config.mosaics_per_slice == 3:
                self._discover_cache_result = self._discover_three_mosaic_candidates()
            else:
                self._discover_cache_result = self._discover_two_mosaic_candidates()
            self._discover_cache_key = cache_key
            logger.debug("discover_candidates: scanned %s candidate(s)", len(self._discover_cache_result))

        if not self.force_resend and self._skip_candidates:
            return [
                c for c in self._discover_cache_result
                if (c.logical_mosaic_id, c.logical_batch) not in self._skip_candidates
            ]
        return self._discover_cache_result

    def _discover_two_mosaic_candidates(self) -> list[OCTBatchCandidate]:
        parsed_files = self._discover_two_mosaic_files()

        files_by_mosaic_and_image: dict[tuple[int, int], list[Path]] = defaultdict(list)
        for parsed in parsed_files:
            files_by_mosaic_and_image[
                (parsed.source_mosaic_id, parsed.image_index)
            ].append(parsed.path)

        tiles_by_source_mosaic: dict[int, set[int]] = defaultdict(set)
        for source_mosaic_id, image_index in files_by_mosaic_and_image:
            tiles_by_source_mosaic[source_mosaic_id].add(image_index)

        out: list[OCTBatchCandidate] = []

        for min_source_mosaic_id, max_source_mosaic_id in self.mosaic_ranges:
            for source_mosaic_id in range(
                min_source_mosaic_id, max_source_mosaic_id + 1
            ):
                tile_indices = tiles_by_source_mosaic.get(source_mosaic_id)
                if not tile_indices:
                    continue

                batches: dict[tuple[int, int], list[int]] = defaultdict(list)
                batches_per_slice = self.scan_config.acquisition.grid_size_x_normal // self.batch_size

                for image_index in sorted(tile_indices):
                    local_batch = ((image_index - 1) // self.batch_size) + 1
                    slice_carry, logical_batch = _apply_batch_offset(
                        local_batch=local_batch,
                        batch_offset=self.batch_offset,
                        batches_per_slice=batches_per_slice,
                    )
                    batches[(slice_carry, logical_batch)].append(image_index)

                for (slice_carry, logical_batch), batch_tile_indices in sorted(batches.items()):
                    if len(batch_tile_indices) < self.batch_size:
                        logger.warning(
                            "Incomplete batch source_mosaic=%s logical_batch=%s (%s tiles, need %s)",
                            source_mosaic_id,
                            logical_batch,
                            len(batch_tile_indices),
                            self.batch_size,
                        )
                        continue

                    batch_files: list[Path] = []
                    for image_index in batch_tile_indices:
                        batch_files.extend(
                            files_by_mosaic_and_image[(source_mosaic_id, image_index)]
                        )

                    candidate_files = tuple(sorted(batch_files))
                    if not _all_files_readable(candidate_files):
                        logger.debug(
                            "Batch files transiently unreadable; will retry "
                            "source_mosaic=%s logical_batch=%s",
                            source_mosaic_id,
                            logical_batch,
                        )
                        continue

                    source_slice_id = slice_from_mosaic(
                        source_mosaic_id,
                        self.scan_config.mosaics_per_slice,
                    )
                    logical_slice_id, logical_mosaic_id = (
                        logical_mosaic_from_source_mosaic(
                            source_mosaic_id,
                            mosaics_per_slice=self.scan_config.mosaics_per_slice,
                            slice_offset=self.slice_offset,
                        )
                    )
                    logical_slice_id += slice_carry
                    logical_mosaic_id += slice_carry * self.scan_config.mosaics_per_slice
                    out.append(
                        OCTBatchCandidate(
                            source_slice_id=source_slice_id,
                            logical_slice_id=logical_slice_id,
                            source_mosaic_id=source_mosaic_id,
                            logical_mosaic_id=logical_mosaic_id,
                            logical_batch=logical_batch,
                            files=candidate_files,
                        )
                    )

        return out

    def _discover_two_mosaic_files(self) -> list[ParsedTileFile]:
        logger.debug("_discover_two_mosaic_files: scanning all tile files")
        out: list[ParsedTileFile] = []

        for path in self.folder_path.iterdir():
            if not path.is_file():
                continue
            if not _is_readable_file(path):
                logger.debug("Skipping unreadable file: %s", path.name)
                continue

            parsed: ParsedTileFile | None = None
            for parser in (
                self._parse_complex_file,
                self._parse_spectral_nii_file,
                self._parse_spectral_raw_file,
                self._parse_tile_nii_file,
            ):
                parsed = parser(path)
                if parsed is not None:
                    break

            if parsed is not None:
                out.append(parsed)
            else:
                logger.debug("File did not match any tile pattern: %s", path.name)

        logger.debug("_discover_two_mosaic_files: matched %s tile file(s)", len(out))
        return out

    def _discover_three_mosaic_candidates(self) -> list[OCTBatchCandidate]:
        parsed_by_mosaic: dict[int, list[ParsedProcessedFile]] = defaultdict(list)
        for path in self._iter_three_mosaic_files():
            parsed = self._parse_three_mosaic_file(path)
            if parsed is None:
                continue
            parsed_by_mosaic[parsed.source_mosaic_id].append(parsed)

        out: list[OCTBatchCandidate] = []
        for source_mosaic_id, parsed_files in sorted(parsed_by_mosaic.items()):
            files_by_tile: dict[int, list[Path]] = defaultdict(list)
            for pf in parsed_files:
                files_by_tile[pf.tile_number].append(pf.path)

            batches: dict[tuple[int, int], list[int]] = defaultdict(list)
            batches_per_slice = self.scan_config.acquisition.grid_size_x_normal // self.batch_size

            for tile_number in sorted(files_by_tile.keys()):
                local_batch = ((tile_number - 1) // self.batch_size) + 1
                slice_carry, logical_batch = _apply_batch_offset(
                    local_batch=local_batch,
                    batch_offset=self.batch_offset,
                    batches_per_slice=batches_per_slice,
                )
                batches[(slice_carry, logical_batch)].append(tile_number)

            for (slice_carry, logical_batch), batch_tile_numbers in sorted(batches.items()):
                if len(batch_tile_numbers) < self.batch_size:
                    logger.warning(
                        "Incomplete batch source_mosaic=%s logical_batch=%s (%s tiles, need %s)",
                        source_mosaic_id,
                        logical_batch,
                        len(batch_tile_numbers),
                        self.batch_size,
                    )
                    continue

                batch_files: list[Path] = []
                for tile_number in batch_tile_numbers:
                    batch_files.extend(files_by_tile[tile_number])

                candidate_files = tuple(sorted(batch_files))
                if not _all_files_readable(candidate_files):
                    logger.debug(
                        "Batch files transiently unreadable; will retry "
                        "source_mosaic=%s logical_batch=%s",
                        source_mosaic_id,
                        logical_batch,
                    )
                    continue

                source_slice_id = slice_from_mosaic(
                    source_mosaic_id,
                    self.scan_config.mosaics_per_slice,
                )
                logical_slice_id, logical_mosaic_id = logical_mosaic_from_source_mosaic(
                    source_mosaic_id,
                    mosaics_per_slice=self.scan_config.mosaics_per_slice,
                    slice_offset=self.slice_offset,
                )
                logical_slice_id += slice_carry
                logical_mosaic_id += slice_carry * self.scan_config.mosaics_per_slice
                out.append(
                    OCTBatchCandidate(
                        source_slice_id=source_slice_id,
                        logical_slice_id=logical_slice_id,
                        source_mosaic_id=source_mosaic_id,
                        logical_mosaic_id=logical_mosaic_id,
                        logical_batch=logical_batch,
                        files=candidate_files,
                    )
                )

        return out

    def _iter_three_mosaic_files(self) -> list[Path]:
        search_dirs = [self.folder_path, self.folder_path / "spectral"]
        out: list[Path] = []
        for directory in search_dirs:
            if not _is_readable_dir(directory):
                continue
            for path in directory.iterdir():
                if not path.is_file():
                    continue
                if not _is_readable_file(path):
                    continue
                out.append(path)
        return out

    def _parse_three_mosaic_file(self, path: Path) -> ParsedProcessedFile | None:
        if path.suffix.lower() != ".nii":
            return None

        i = None
        try:
            i = extract_spectral_index_from_filename(path.name)
        except ValueError:
            pass
        if i is None:
            try:
                i = extract_processed_index_from_filename(path.name)
            except ValueError:
                pass
        if i is None:
            return None

        j = self.scan_config.acquisition.grid_size_x_normal
        if j < 1:
            raise ValueError(f"grid_size_x_normal must be >= 1, got {j}")

        slice_number = (i - 1) // j + 1
        tile_number = (i - 1) % j + 1
        mosaics_per_slice = self.scan_config.mosaics_per_slice
        source_mosaic_id = (slice_number - 1) * mosaics_per_slice + 1
        return ParsedProcessedFile(path=path, source_mosaic_id=source_mosaic_id, tile_number=tile_number)

    def _parse_complex_file(self, path: Path) -> ParsedTileFile | None:
        match = _COMPLEX_RE.match(path.name)
        if match is None:
            return None

        try:
            if path.stat().st_size < self.min_complex_file_size_bytes:
                return None
        except (OSError, PermissionError):
            return None

        return ParsedTileFile(
            path=path,
            source_mosaic_id=int(match.group("mosaic")),
            image_index=int(match.group("image")),
        )

    def _parse_spectral_nii_file(self, path: Path) -> ParsedTileFile | None:
        match = _SPECTRAL_NII_RE.match(path.name)
        if match is None:
            return None
        return ParsedTileFile(
            path=path,
            source_mosaic_id=int(match.group("mosaic")),
            image_index=int(match.group("image")),
        )

    def _parse_spectral_raw_file(self, path: Path) -> ParsedTileFile | None:
        match = _SPECTRAL_RAW_RE.match(path.name)
        if match is None:
            return None
        return ParsedTileFile(
            path=path,
            source_mosaic_id=int(match.group("mosaic")),
            image_index=int(match.group("image")),
        )

    def _parse_tile_nii_file(self, path: Path) -> ParsedTileFile | None:
        match = _TILE_NII_RE.match(path.name)
        if match is None:
            return None
        return ParsedTileFile(
            path=path,
            source_mosaic_id=int(match.group("mosaic")),
            image_index=int(match.group("image")),
        )

    def _image_index_from_file(self, path: Path) -> int:
        for parser in (
            self._parse_complex_file,
            self._parse_spectral_nii_file,
            self._parse_spectral_raw_file,
        ):
            parsed = parser(path)
            if parsed is not None:
                return parsed.image_index
        raise ValueError(f"Could not parse image index from {path.name!r}")

    def candidate_key(self, candidate: OCTBatchCandidate) -> tuple[int, int]:
        return (candidate.logical_mosaic_id, candidate.logical_batch)

    def fingerprint(self, candidate: OCTBatchCandidate) -> object:
        if not _all_files_readable(candidate.files):
            raise OSError("Candidate files are temporarily unreadable")
        return _batch_fingerprint(candidate.files, self.folder_path)

    def process(self, candidate: OCTBatchCandidate) -> int:
        ckey = (candidate.logical_mosaic_id, candidate.logical_batch)

        if not _all_files_readable(candidate.files):
            logger.info(
                "Candidate not ready yet; files unreadable logical_mosaic=%s logical_batch=%s",
                candidate.logical_mosaic_id,
                candidate.logical_batch,
            )
            return 0

        batch_ident = oct_batch_ident(
            self.project_name,
            candidate.logical_mosaic_id,
            candidate.logical_batch,
        )

        existing = OCT_STATE_SERVICE.peek_batch(batch_ident=batch_ident)
        if existing is not None and not self.force_resend:
            self._skip_candidates.add(ckey)
            logger.debug("[SKIP] batch state exists for %s: state=%r", batch_ident, existing)
            logger.info("[SKIP] batch state exists for %s", batch_ident)
            return 0

        if self.direct:
            dispatched = self._process_direct(candidate, batch_ident)
        else:
            dispatched = self._process_event(candidate, batch_ident)

        if dispatched > 0:
            self._skip_candidates.add(ckey)
        return dispatched

    def _process_direct(self, candidate: OCTBatchCandidate, batch_ident: Any) -> int:
        logger.info(
            "Direct-processing OCT source_slice=%s logical_slice=%s source_mosaic=%s "
            "logical_mosaic=%s logical_batch=%s files=%s",
            candidate.source_slice_id,
            candidate.logical_slice_id,
            candidate.source_mosaic_id,
            candidate.logical_mosaic_id,
            candidate.logical_batch,
            len(candidate.files),
        )

        process_tile_batch(
            batch_id=batch_ident,
            config=self.scan_config,
            file_list=list(candidate.files),
            force_rerun=self.force_resend,
        )

        if self.refresh_hook is not None:
            self.refresh_hook()

        return 1

    def _process_event(self, candidate: OCTBatchCandidate, batch_ident: Any) -> int:
        logger.info(
            "Emitting BATCH_READY source_slice=%s logical_slice=%s source_mosaic=%s "
            "logical_mosaic=%s logical_batch=%s files=%s",
            candidate.source_slice_id,
            candidate.logical_slice_id,
            candidate.source_mosaic_id,
            candidate.logical_mosaic_id,
            candidate.logical_batch,
            len(candidate.files),
        )

        extra: dict[str, Any] = {
            "file_list": [str(p) for p in candidate.files],
            "project_base_path": self.project_base_path,
        }
        if self.force_resend:
            extra["force_rerun"] = True

        emit_batch_psoct_event(BATCH_READY, batch_ident, extra_payload=extra)
        with OCT_STATE_SERVICE.open_batch(batch_ident=batch_ident):
            pass
        return 1


def watch_oct(
    *,
    project_name: str,
    folder_path: Path,
    project_base_path: str,
    mosaic_ranges: list[tuple[int, int]],
    slice_offset: int,
    batch_offset: int,
    batch_size: int,
    scan_config: PSOCTScanConfigModel,
    stability_seconds: int = 15,
    poll_interval: int = 5,
    direct: bool = True,
    force_resend: bool = False,
    refresh_hook: RefreshHook | None = None,
    min_complex_file_size_bytes: int = 1,
) -> None:
    service = OCTWatcherService(
        project_name=project_name,
        folder_path=folder_path,
        project_base_path=project_base_path,
        mosaic_ranges=mosaic_ranges,
        slice_offset=slice_offset,
        batch_offset=batch_offset,
        batch_size=batch_size,
        scan_config=scan_config,
        direct=direct,
        force_resend=force_resend,
        refresh_hook=refresh_hook,
        min_complex_file_size_bytes=min_complex_file_size_bytes,
    )

    watcher = PollingStableWatcher[OCTBatchCandidate, tuple[int, int]](
        discover_candidates=service.discover_candidates,
        candidate_key=service.candidate_key,
        fingerprint=service.fingerprint,
        process=service.process,
        poll_interval=poll_interval,
        stability_seconds=stability_seconds,
        running_message=f"OCT polling watcher running ({'direct' if direct else 'event'} mode)",
    )
    watcher.run()


@oct_cli.command
def watch(
    project_name: str,
    folder_path: str,
    mosaic_ranges: str = "1:999999",
    *,
    slice_offset: int = 0,
    batch_offset: int = 0,
    stability_seconds: int = 15,
    poll_interval: int = 5,
    direct: bool = True,
    refresh: str | None = None,
    force_resend: bool = False,
    min_complex_file_size_bytes: int = 1,
    verbose: bool = False,
) -> None:
    """
    Poll for complete OCT batches and dispatch each stable candidate either by:
    - running process_tile_batch directly, or
    - emitting BATCH_READY.

    For mosaics_per_slice == 2:
      - grouping is mosaic/image-based
      - mosaic_ranges applies to source mosaic ids parsed from filenames

    For mosaics_per_slice == 3:
      - discovery is from processed-index files (e.g. spectral/processed_<i>.nii)
      - source mosaic id is derived from processed index and grid_size_x
      - slice_offset controls logical slice numbering via derived source mosaic ids
    """
    _configure_logging(verbose)

    project_config = get_psoct_scan_config(project_name)

    scan_config = PSOCTScanConfigModel.model_validate(project_config.model_dump())
    project_base_path = str(project_config.project_base_path)
    batch_size = project_config.acquisition.grid_size_y
    watch_path = Path(folder_path)

    if not watch_path.exists():
        raise ValueError(f"Watch directory {watch_path} does not exist")
    if not _is_readable_dir(watch_path):
        raise ValueError(f"Watch directory {watch_path} is not readable")

    refresh_hook = resolve_refresh_hook(refresh)

    logger.info("Using batch_size=%s", batch_size)
    logger.info("Using mosaics_per_slice=%s", scan_config.mosaics_per_slice)
    logger.info("Using tile_saving_type=%s", scan_config.acquisition.tile_saving_type)

    watch_oct(
        project_name=project_name,
        folder_path=watch_path,
        project_base_path=project_base_path,
        mosaic_ranges=_parse_mosaic_ranges(mosaic_ranges),
        slice_offset=slice_offset,
        batch_offset=batch_offset,
        batch_size=batch_size,
        scan_config=scan_config,
        stability_seconds=stability_seconds,
        poll_interval=poll_interval,
        direct=direct,
        force_resend=force_resend,
        refresh_hook=refresh_hook,
        min_complex_file_size_bytes=min_complex_file_size_bytes,
    )

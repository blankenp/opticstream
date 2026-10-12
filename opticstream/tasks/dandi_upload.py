"""
DANDI upload tasks.

This module provides a list-capable uploader so higher-level flows can upload
multiple files in one Prefect task invocation.
"""

from __future__ import annotations

import os
import shlex
from typing import Dict, List, Tuple

from prefect import get_run_logger, task
from prefect.blocks.system import Secret
from prefect_shell import ShellOperation

from opticstream.config.constants import (
    DANDI_API_TOKEN_BLOCK_NAME,
    LINC_API_TOKEN_BLOCK_NAME,
    DANDI_INSTANCE,
)

# Default location for DANDI CLI state and logs.
# Keep these off the home filesystem to avoid filling quota usage.
DEFAULT_DANDI_STATE_HOME = "/autofs/space/zircon_005/users/dandi_state"

def build_dandi_upload_command(
    file_list: List[str],
    *,
    dandi_instance: DANDI_INSTANCE = "dandi",
    dandi_bin: str = "dandi",
    realpath: bool = True,
    max_jobs: str = "10:10",
) -> Tuple[str, str, str]:
    """
    Build the shell command to upload a list of files with the DANDI CLI.

    Returns
    -------
    (command, working_dir, api_key_env_var)
    """
    if not file_list:
        raise ValueError("file_list must not be empty")

    file_paths = [os.path.realpath(p) if realpath else p for p in file_list]
    file_paths_str = " ".join(shlex.quote(p) for p in file_paths)

    command = f"{shlex.quote(dandi_bin)} upload "
    if dandi_instance != "dandi":
        command += f"-i {shlex.quote(dandi_instance)} "

    # Match the CLI flags used elsewhere in the codebase.
    command += f"{file_paths_str} -J {shlex.quote(max_jobs)} --allow-any-path"
    if dandi_instance == "dandi" or True:
        command += " --existing overwrite --validation skip"
    else:
        command += " --existing OVERWRITE --validation SKIP"

    # DANDI CLI errors can sometimes be clearer when run from file directory.
    working_dir = os.path.dirname(file_list[0]) if file_list[0] else os.getcwd()

    # Use the instance to decide which API key env var is expected.
    api_key_env_var = "LINC_API_KEY" if dandi_instance == "linc" else "DANDI_API_KEY"
    return command, working_dir, api_key_env_var


def _build_dandi_upload_env(
    *,
    dandi_instance: DANDI_INSTANCE,
) -> Dict[str, str]:
    """
    Build environment variables required by the DANDI/LINC CLI.

    Redirect DANDI CLI state/log files away from the home directory.
    The location can be overridden using OPTICSTREAM_DANDI_STATE_HOME.
    """

    # Determine where DANDI should write its state/log files.
    dandi_state_home = os.environ.get(
        "OPTICSTREAM_DANDI_STATE_HOME",
        DEFAULT_DANDI_STATE_HOME,
    )

    # Ensure the directory exists before launching DANDI.
    os.makedirs(dandi_state_home, exist_ok=True)

    # Environment variables passed to the DANDI subprocess.
    env = {
        "XDG_STATE_HOME": dandi_state_home,
        "DANDI_DEVEL": "1",
    }

    if dandi_instance == "linc":
        api_key = Secret.load(
            LINC_API_TOKEN_BLOCK_NAME,
            validate=False,
        ).get()

        env["LINC_API_KEY"] = api_key
        env["DANDI_API_KEY"] = api_key

    else:
        api_key = Secret.load(
            DANDI_API_TOKEN_BLOCK_NAME,
            validate=False,
        ).get()

        env["DANDI_API_KEY"] = api_key

    return env


@task(tags=["dandi-upload"], retries=1)
def upload_to_dandi_batch(
    file_list: List[str],
    *,
    dandi_instance: DANDI_INSTANCE = "dandi",
    dandi_bin: str = "dandi",
    realpath: bool = True,
    max_jobs: str = "10:10",
) -> None:
    """
    Upload multiple files to DANDI (or optionally to the LINC instance).
    """
    logger = get_run_logger()

    command, working_dir, _ = build_dandi_upload_command(
        file_list,
        dandi_instance=dandi_instance,
        dandi_bin=dandi_bin,
        realpath=realpath,
        max_jobs=max_jobs,
    )

    # Load secrets inside the task (not at import time).
    env = _build_dandi_upload_env(dandi_instance=dandi_instance)

    logger.info("Running DANDI batch upload with %s files", len(file_list))
    logger.info(command)

    with ShellOperation(
        commands=[command],
        env=env,
        working_dir=working_dir,
    ) as upload_operation:
        upload_process = upload_operation.trigger()
        upload_process.wait_for_completion()
        logger.info(upload_process.fetch_result())


@task(tags=["dandi-upload"], retries=1)
def upload_to_dandi(
    file_path: str,
    *,
    dandi_instance: DANDI_INSTANCE = "linc",
    dandi_bin: str = "dandi",
    realpath: bool = True,
    max_jobs: str = "10:10",
) -> None:
    """
    Upload the file to DANDI.
    """
    return upload_to_dandi_batch.fn(
        [file_path],
        dandi_instance=dandi_instance,
        dandi_bin=dandi_bin,
        realpath=realpath,
        max_jobs=max_jobs,
    )


__all__ = [
    "build_dandi_upload_command",
    "upload_to_dandi",
    "upload_to_dandi_batch",
]

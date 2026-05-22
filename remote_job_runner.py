#!/usr/bin/env python3
"""Production-oriented CLI for running a local folder as a remote Linux job."""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime as dt
import fnmatch
import getpass
import hashlib
import json
import logging
import os
import posixpath
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterator, Literal, Optional

import paramiko
import yaml


LOGGER = logging.getLogger("remote_job_runner")
WSL_COMMAND = "wsl"
RSYNC_COMMAND = "rsync"


class RemoteJobRunnerError(Exception):
    """Base class for all expected remote_job_runner errors."""


class ConfigError(RemoteJobRunnerError):
    """Raised when configuration is missing or invalid."""


class ConnectionError(RemoteJobRunnerError):
    """Raised when SSH connection setup fails."""


class TransferError(RemoteJobRunnerError):
    """Raised when local or remote transfer fails."""


class RemoteCommandError(RemoteJobRunnerError):
    """Raised when remote command orchestration fails."""


class VerificationError(RemoteJobRunnerError):
    """Raised when downloaded result verification fails."""


class SafeSwapError(RemoteJobRunnerError):
    """Raised when replacing the source directory fails."""


class LockError(RemoteJobRunnerError):
    """Raised when a source directory lock cannot be acquired."""


@dataclass(frozen=True)
class RemoteConfig:
    host: str
    port: int
    username: str
    remote_base_dir: str


@dataclass(frozen=True)
class AuthConfig:
    key_file: Optional[Path]
    password: Optional[str]
    password_env: Optional[str]


@dataclass(frozen=True)
class ResolvedAuth:
    key_file: Optional[Path]
    password: Optional[str]
    auth_source: Literal["key_file", "password", "password_env", "interactive"]


@dataclass(frozen=True)
class JobConfig:
    source_dir: Path
    keep_backup: bool
    enable_logs: bool
    show_progress: bool
    cleanup_remote_on_success: bool
    cleanup_remote_on_failure: bool
    verify_hash: bool
    skip_symlinks: bool
    max_captured_output_bytes: int


@dataclass(frozen=True)
class TransferConfig:
    method: Literal["sftp", "rsync"]
    sftp_max_workers: int
    include_globs: list[str]
    exclude_globs: list[str]


@dataclass(frozen=True)
class ResultsConfig:
    remote_paths: list[str]
    local_base_dir: Path
    local_base_dir_raw: Optional[str]
    allow_local_base_dir_outside_source: bool
    overwrite: bool
    backup_overwritten: bool
    sync_mode: Literal["merge"]


@dataclass(frozen=True)
class CommandSpec:
    name: str
    cmd: str
    timeout_sec: int = 3600


@dataclass(frozen=True)
class StageSpec:
    name: str
    mode: Literal["sequential", "parallel"]
    commands: list[CommandSpec]
    max_workers: Optional[int] = None


@dataclass(frozen=True)
class ResolvedConfig:
    remote: RemoteConfig
    auth: AuthConfig
    job: JobConfig
    transfer: TransferConfig
    results: ResultsConfig
    stages: list[StageSpec]


@dataclass(frozen=True)
class FileManifestEntry:
    relative_path: str
    kind: Literal["file", "directory"]
    size: int
    sha256: Optional[str]
    mtime_ns: int


@dataclass(frozen=True)
class CommandResult:
    stage_name: str
    command_name: str
    command: str
    start_time: str
    end_time: str
    duration_sec: float
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool
    stdout_bytes: int = 0
    stderr_bytes: int = 0
    stdout_truncated: bool = False
    stderr_truncated: bool = False


@dataclass(frozen=True)
class StageResult:
    stage_name: str
    mode: str
    command_results: list[CommandResult]
    success: bool


@dataclass(frozen=True)
class JobResult:
    job_id: str
    success: bool
    remote_workdir: str
    local_log_dir: Optional[Path]
    backup_dir: Optional[Path]
    stage_results: list[StageResult]


RunnerFunc = Callable[[Any, str, CommandSpec, str, Optional[Path]], CommandResult]


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def generate_job_id() -> str:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"job_{stamp}_{uuid.uuid4().hex[:8]}"


def sanitize_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return cleaned.strip("._") or "unnamed"


def _require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a mapping")
    return value


def _as_bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ConfigError(f"{name} must be a boolean")


def _as_str(value: Any, name: str) -> str:
    if isinstance(value, str) and value.strip():
        return value
    raise ConfigError(f"{name} must be a non-empty string")


def _as_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{name} must be an integer")
    return value


def _string_list(value: Any, name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigError(f"{name} must be a list of strings")
    return list(value)


def _optional_path(value: Any, name: str) -> Optional[Path]:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise ConfigError(f"{name} must be a string path")
    return Path(value).expanduser()


def _optional_str(value: Any, name: str) -> Optional[str]:
    if value in (None, ""):
        return None
    return _as_str(value, name)


def _optional_password(value: Any, name: str) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        if not value:
            raise ConfigError(f"{name} must not be empty")
        return value
    if isinstance(value, bool):
        raise ConfigError(f"{name} must be a string or integer, not a boolean")
    if isinstance(value, int):
        return str(value)
    raise ConfigError(f"{name} must be a string or integer")


def validate_result_remote_path(remote_path: str) -> None:
    if not remote_path or not remote_path.strip():
        raise ConfigError("results.remote_paths entries must not be empty")
    normalized = PurePosixPath(remote_path)
    if remote_path.startswith("/") or normalized.is_absolute():
        raise ConfigError(f"results.remote_paths must be relative to remote_workdir: {remote_path}")
    if any(part == ".." for part in normalized.parts):
        raise ConfigError(f"results.remote_paths must not contain '..': {remote_path}")


def path_is_inside_or_equal(parent: Path, candidate: Path) -> bool:
    try:
        candidate.relative_to(parent)
        return True
    except ValueError:
        return False


def resolve_results_local_base_dir(
    source_dir: Path,
    raw_local_base_dir: Optional[str],
    allow_outside_source: bool,
) -> Path:
    source = source_dir.resolve()
    if raw_local_base_dir is None or raw_local_base_dir == "":
        candidate = source
    else:
        raw_path = Path(raw_local_base_dir).expanduser()
        candidate = raw_path.resolve() if raw_path.is_absolute() else (source / raw_path).resolve()
    if not allow_outside_source and not path_is_inside_or_equal(source, candidate):
        raise ConfigError(f"results.local_base_dir must stay inside source_dir: {raw_local_base_dir}")
    return candidate


def validate_local_base_dir(source_dir: Path, local_base_dir: Path) -> None:
    resolve_results_local_base_dir(source_dir, str(local_base_dir), False)


def _apply_overrides(data: dict[str, Any], overrides: Optional[argparse.Namespace | dict[str, Any]]) -> dict[str, Any]:
    if overrides is None:
        return data
    values = vars(overrides) if isinstance(overrides, argparse.Namespace) else dict(overrides)
    remote = data.setdefault("remote", {})
    auth = data.setdefault("auth", {})
    job = data.setdefault("job", {})

    mapping = {
        "host": (remote, "host"),
        "port": (remote, "port"),
        "username": (remote, "username"),
        "remote_base_dir": (remote, "remote_base_dir"),
        "password_env": (auth, "password_env"),
        "key_file": (auth, "key_file"),
        "source_dir": (job, "source_dir"),
        "keep_backup": (job, "keep_backup"),
        "enable_logs": (job, "enable_logs"),
        "show_progress": (job, "show_progress"),
        "cleanup_remote_on_success": (job, "cleanup_remote_on_success"),
        "cleanup_remote_on_failure": (job, "cleanup_remote_on_failure"),
    }
    for override_name, (section, key) in mapping.items():
        if override_name in values and values[override_name] is not None:
            section[key] = values[override_name]
    return data


def load_config(config_path: Path, overrides: Optional[argparse.Namespace | dict[str, Any]] = None) -> ResolvedConfig:
    try:
        with config_path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except OSError as exc:
        raise ConfigError(f"Cannot read config file {config_path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {config_path}: {exc}") from exc

    if raw is None:
        raw = {}
    data = _require_mapping(raw, "config")
    data = _apply_overrides(data, overrides)

    remote_data = _require_mapping(data.get("remote", {}), "remote")
    auth_data = _require_mapping(data.get("auth", {}), "auth")
    job_section = data.get("job", {})
    if job_section is None:
        raise ConfigError("job.source_dir must be a non-empty string")
    job_data = _require_mapping(job_section, "job")
    transfer_data = _require_mapping(data.get("transfer", {}), "transfer")
    results_data = _require_mapping(data.get("results", {}), "results")
    stages_data = data.get("stages")

    remote = RemoteConfig(
        host=_as_str(remote_data.get("host"), "remote.host"),
        port=_as_int(remote_data.get("port", 22), "remote.port"),
        username=_as_str(remote_data.get("username"), "remote.username"),
        remote_base_dir=_as_str(remote_data.get("remote_base_dir"), "remote.remote_base_dir"),
    )
    if remote.port <= 0 or remote.port > 65535:
        raise ConfigError("remote.port must be between 1 and 65535")

    auth = AuthConfig(
        key_file=_optional_path(auth_data.get("key_file"), "auth.key_file"),
        password=_optional_password(auth_data.get("password"), "auth.password"),
        password_env=_optional_str(auth_data.get("password_env"), "auth.password_env"),
    )

    source_raw = job_data.get("source_dir")
    if not isinstance(source_raw, str) or not source_raw.strip():
        raise ConfigError("job.source_dir must be a non-empty string")
    job = JobConfig(
        source_dir=Path(source_raw).expanduser().resolve(),
        keep_backup=_as_bool(job_data.get("keep_backup", True), "job.keep_backup"),
        enable_logs=_as_bool(job_data.get("enable_logs", True), "job.enable_logs"),
        show_progress=_as_bool(job_data.get("show_progress", True), "job.show_progress"),
        cleanup_remote_on_success=_as_bool(
            job_data.get("cleanup_remote_on_success", True), "job.cleanup_remote_on_success"
        ),
        cleanup_remote_on_failure=_as_bool(
            job_data.get("cleanup_remote_on_failure", False), "job.cleanup_remote_on_failure"
        ),
        verify_hash=_as_bool(job_data.get("verify_hash", True), "job.verify_hash"),
        skip_symlinks=_as_bool(job_data.get("skip_symlinks", False), "job.skip_symlinks"),
        max_captured_output_bytes=_as_int(
            job_data.get("max_captured_output_bytes", 1024 * 1024), "job.max_captured_output_bytes"
        ),
    )
    if job.max_captured_output_bytes < 1024:
        raise ConfigError("job.max_captured_output_bytes must be >= 1024")

    method = _as_str(transfer_data.get("method", "sftp"), "transfer.method")
    if method not in ("sftp", "rsync"):
        raise ConfigError("transfer.method must be sftp or rsync")
    include_globs = _string_list(transfer_data.get("include_globs", ["**/*"]), "transfer.include_globs")
    exclude_globs = _string_list(transfer_data.get("exclude_globs", []), "transfer.exclude_globs")
    sftp_max_workers = _as_int(transfer_data.get("sftp_max_workers", 1), "transfer.sftp_max_workers")
    if sftp_max_workers < 1:
        raise ConfigError("transfer.sftp_max_workers must be >= 1")
    transfer = TransferConfig(
        method=method,  # type: ignore[arg-type]
        sftp_max_workers=sftp_max_workers,
        include_globs=include_globs or ["**/*"],
        exclude_globs=exclude_globs,
    )

    remote_paths = _string_list(results_data.get("remote_paths", []), "results.remote_paths")
    if not remote_paths:
        raise ConfigError("results.remote_paths must contain at least one result path")
    for remote_path in remote_paths:
        validate_result_remote_path(remote_path)
    sync_mode = _as_str(results_data.get("sync_mode", "merge"), "results.sync_mode")
    if sync_mode != "merge":
        raise ConfigError("results.sync_mode currently supports only merge")
    local_base_raw_value = results_data.get("local_base_dir")
    if local_base_raw_value is not None and not isinstance(local_base_raw_value, str):
        raise ConfigError("results.local_base_dir must be a string or null")
    local_base_raw = local_base_raw_value if isinstance(local_base_raw_value, str) else None
    allow_outside_source = _as_bool(
        results_data.get("allow_local_base_dir_outside_source", False),
        "results.allow_local_base_dir_outside_source",
    )
    local_base_dir = resolve_results_local_base_dir(job.source_dir, local_base_raw, allow_outside_source)
    results = ResultsConfig(
        remote_paths=remote_paths,
        local_base_dir=local_base_dir,
        local_base_dir_raw=local_base_raw,
        allow_local_base_dir_outside_source=allow_outside_source,
        overwrite=_as_bool(results_data.get("overwrite", True), "results.overwrite"),
        backup_overwritten=_as_bool(
            results_data.get("backup_overwritten", True), "results.backup_overwritten"
        ),
        sync_mode=sync_mode,  # type: ignore[arg-type]
    )

    if transfer.method == "rsync" and auth.key_file is None:
        if auth.password is None and not auth.password_env:
            raise ConfigError(
                "transfer.method=rsync requires auth.key_file, auth.password, or auth.password_env; "
                "interactive password prompt is not supported for rsync."
            )

    if not isinstance(stages_data, list) or not stages_data:
        raise ConfigError("stages must contain at least one stage")

    stages: list[StageSpec] = []
    for stage_index, stage_raw in enumerate(stages_data):
        stage_data = _require_mapping(stage_raw, f"stages[{stage_index}]")
        name = _as_str(stage_data.get("name"), f"stages[{stage_index}].name")
        mode = _as_str(stage_data.get("mode"), f"stages[{stage_index}].mode")
        if mode not in ("sequential", "parallel"):
            raise ConfigError(f"stage {name!r} mode must be sequential or parallel")
        commands_raw = stage_data.get("commands")
        if not isinstance(commands_raw, list) or not commands_raw:
            raise ConfigError(f"stage {name!r} must contain at least one command")

        commands: list[CommandSpec] = []
        for command_index, command_raw in enumerate(commands_raw):
            command_data = _require_mapping(command_raw, f"stage {name!r} command[{command_index}]")
            command_name = _as_str(command_data.get("name"), f"stage {name!r} command[{command_index}].name")
            command_cmd = _as_str(command_data.get("cmd"), f"stage {name!r} command[{command_index}].cmd")
            timeout_sec = _as_int(command_data.get("timeout_sec", 3600), f"command {command_name!r}.timeout_sec")
            if timeout_sec <= 0:
                raise ConfigError(f"command {command_name!r}.timeout_sec must be > 0")
            commands.append(CommandSpec(name=command_name, cmd=command_cmd, timeout_sec=timeout_sec))

        max_workers: Optional[int] = None
        if mode == "parallel":
            raw_workers = stage_data.get("max_workers", min(4, len(commands)))
            max_workers = _as_int(raw_workers, f"stage {name!r}.max_workers")
            if max_workers < 1:
                raise ConfigError(f"stage {name!r}.max_workers must be >= 1")
        stages.append(StageSpec(name=name, mode=mode, commands=commands, max_workers=max_workers))

    return ResolvedConfig(remote=remote, auth=auth, job=job, transfer=transfer, results=results, stages=stages)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if dataclasses.is_dataclass(value):
        return {key: _json_safe(val) for key, val in dataclasses.asdict(value).items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(val) for key, val in value.items()}
    return value


def resolved_config_to_dict(config: ResolvedConfig) -> dict[str, Any]:
    data = _json_safe(config)
    auth = data.get("auth")
    if isinstance(auth, dict) and auth.get("password") is not None:
        auth["password"] = "<redacted>"
    return data


def dump_resolved_config_yaml(config: ResolvedConfig) -> str:
    return yaml.safe_dump(resolved_config_to_dict(config), sort_keys=False)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(value), indent=2, sort_keys=True), encoding="utf-8")


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def maybe_write_json(enabled: bool, path: Optional[Path], value: Any) -> None:
    if enabled and path is not None:
        write_json(path, value)


def maybe_write_text(enabled: bool, path: Optional[Path], value: str) -> None:
    if enabled and path is not None:
        write_text(path, value)


def progress_print(message: str, *, enabled: bool = True) -> None:
    if enabled:
        print(f"[{utc_now_iso()}] {message}", flush=True)


def progress(config: ResolvedConfig, message: str) -> None:
    progress_print(message, enabled=config.job.show_progress)


@contextlib.contextmanager
def timed_phase(name: str, show_progress: bool) -> Iterator[None]:
    start = time.monotonic()
    progress_print(f"phase started: {name}", enabled=show_progress)
    try:
        yield
    finally:
        duration = time.monotonic() - start
        progress_print(f"phase completed: {name}, duration_sec={duration:.3f}", enabled=show_progress)


def _normalize_relative(path: Path) -> str:
    text = path.as_posix()
    return "." if text == "" else text


def _glob_matches(relative_path: str, patterns: list[str]) -> bool:
    path = PurePosixPath(relative_path)
    for pattern in patterns:
        if pattern == "**/*" and relative_path != ".":
            return True
        if pattern.endswith("/**") and relative_path == pattern[:-3]:
            return True
        if fnmatch.fnmatchcase(relative_path, pattern) or path.match(pattern):
            return True
        if pattern.startswith("**/") and (
            fnmatch.fnmatchcase(relative_path, pattern[3:]) or path.match(pattern[3:])
        ):
            return True
    return False


def _is_included(relative_path: str, include_globs: list[str], exclude_globs: list[str]) -> bool:
    if relative_path == ".":
        return True
    return _glob_matches(relative_path, include_globs) and not _glob_matches(relative_path, exclude_globs)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(
    root_dir: Path,
    include_globs: list[str],
    exclude_globs: list[str],
    verify_hash: bool,
    skip_symlinks: bool,
) -> list[FileManifestEntry]:
    root = root_dir.expanduser()
    if not root.exists() or not root.is_dir():
        raise TransferError(f"source/result directory must exist and be a directory: {root}")

    entries: list[FileManifestEntry] = []
    for current, dir_names, file_names in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        rel_dir_path = current_path.relative_to(root)
        rel_dir = _normalize_relative(rel_dir_path)

        kept_dirs: list[str] = []
        for dirname in dir_names:
            child = current_path / dirname
            rel_child = _normalize_relative(child.relative_to(root))
            if child.is_symlink():
                if skip_symlinks:
                    LOGGER.warning("Skipping symlink directory: %s", child)
                    continue
                raise TransferError(f"Symlink is not supported: {child}")
            if _glob_matches(rel_child, exclude_globs):
                continue
            kept_dirs.append(dirname)
        dir_names[:] = kept_dirs

        if _is_included(rel_dir, include_globs, exclude_globs):
            st = current_path.stat()
            entries.append(
                FileManifestEntry(
                    relative_path=rel_dir,
                    kind="directory",
                    size=0,
                    sha256=None,
                    mtime_ns=st.st_mtime_ns,
                )
            )

        for filename in file_names:
            file_path = current_path / filename
            rel_file = _normalize_relative(file_path.relative_to(root))
            if file_path.is_symlink():
                if skip_symlinks:
                    LOGGER.warning("Skipping symlink file: %s", file_path)
                    continue
                raise TransferError(f"Symlink is not supported: {file_path}")
            if not _is_included(rel_file, include_globs, exclude_globs):
                continue
            st = file_path.stat()
            entries.append(
                FileManifestEntry(
                    relative_path=rel_file,
                    kind="file",
                    size=st.st_size,
                    sha256=sha256_file(file_path) if verify_hash else None,
                    mtime_ns=st.st_mtime_ns,
                )
            )

    entries.sort(key=lambda entry: (entry.relative_path != ".", entry.relative_path))
    return entries


def manifest_to_jsonable(entries: list[FileManifestEntry]) -> list[dict[str, Any]]:
    return [_json_safe(entry) for entry in entries]


def compare_download_manifest(entries: list[FileManifestEntry], verify_hash: bool) -> None:
    seen: set[str] = set()
    for entry in entries:
        key = entry.relative_path
        if key in seen:
            raise VerificationError(f"Duplicate downloaded manifest entry: {key}")
        seen.add(key)
        if entry.kind == "file":
            if entry.size < 0:
                raise VerificationError(f"Downloaded file has invalid size: {key}")
            if verify_hash and not entry.sha256:
                raise VerificationError(f"Downloaded file is missing sha256: {key}")


def remote_join(base: str, relative_path: str) -> str:
    if relative_path == ".":
        return base
    return posixpath.join(base, *Path(relative_path).parts)


def sftp_exists(sftp: paramiko.SFTPClient, remote_path: str) -> bool:
    try:
        sftp.stat(remote_path)
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        if getattr(exc, "errno", None) == 2:
            return False
        raise TransferError(f"Cannot stat remote path {remote_path}: {exc}") from exc


def remote_mkdir_p(sftp: paramiko.SFTPClient, remote_path: str) -> None:
    parts = [part for part in remote_path.split("/") if part]
    current = "/" if remote_path.startswith("/") else ""
    for part in parts:
        current = posixpath.join(current, part) if current else part
        try:
            sftp.stat(current)
        except OSError:
            try:
                sftp.mkdir(current)
            except OSError as exc:
                raise TransferError(f"Cannot create remote directory {current}: {exc}") from exc


def upload_manifest(
    sftp: paramiko.SFTPClient,
    source_dir: Path,
    remote_workdir: str,
    manifest: list[FileManifestEntry],
) -> None:
    if sftp_exists(sftp, remote_workdir):
        raise TransferError(f"Remote working directory already exists: {remote_workdir}")
    remote_mkdir_p(sftp, posixpath.dirname(remote_workdir))
    try:
        sftp.mkdir(remote_workdir)
    except OSError as exc:
        raise TransferError(f"Cannot create remote working directory {remote_workdir}: {exc}") from exc

    for entry in manifest:
        remote_path = remote_join(remote_workdir, entry.relative_path)
        local_path = source_dir / entry.relative_path if entry.relative_path != "." else source_dir
        try:
            if entry.kind == "directory":
                if entry.relative_path != ".":
                    remote_mkdir_p(sftp, remote_path)
            else:
                remote_mkdir_p(sftp, posixpath.dirname(remote_path))
                sftp.put(str(local_path), remote_path)
        except OSError as exc:
            raise TransferError(f"Failed to upload {local_path} to {remote_path}: {exc}") from exc


def upload_with_sftp(
    sftp: paramiko.SFTPClient,
    source_dir: Path,
    remote_workdir: str,
    manifest: list[FileManifestEntry],
    show_progress: bool = True,
) -> None:
    progress_print(f"sftp upload started: files={len([entry for entry in manifest if entry.kind == 'file'])}, workers=1", enabled=show_progress)
    upload_manifest(sftp, source_dir, remote_workdir, manifest)
    progress_print(f"sftp upload completed: files={len([entry for entry in manifest if entry.kind == 'file'])}", enabled=show_progress)


def chunk_entries(entries: list[FileManifestEntry], chunk_count: int) -> list[list[FileManifestEntry]]:
    return [entries[index::chunk_count] for index in range(chunk_count)]


def upload_manifest_parallel_sftp(
    ssh_client: paramiko.SSHClient,
    source_dir: Path,
    remote_workdir: str,
    manifest: list[FileManifestEntry],
    max_workers: int,
    show_progress: bool,
) -> None:
    directories = [entry for entry in manifest if entry.kind == "directory"]
    files = [entry for entry in manifest if entry.kind == "file"]
    setup_sftp: Optional[paramiko.SFTPClient] = None
    start = time.monotonic()
    actual_workers = min(max_workers, len(files)) if files else 0
    progress_print(
        f"sftp upload started: files={len(files)}, workers={actual_workers}",
        enabled=show_progress,
    )
    try:
        setup_sftp = ssh_client.open_sftp()
        if sftp_exists(setup_sftp, remote_workdir):
            raise TransferError(f"Remote working directory already exists: {remote_workdir}")
        remote_mkdir_p(setup_sftp, posixpath.dirname(remote_workdir))
        try:
            setup_sftp.mkdir(remote_workdir)
        except OSError as exc:
            raise TransferError(f"Cannot create remote working directory {remote_workdir}: {exc}") from exc
        for entry in directories:
            if entry.relative_path != ".":
                remote_mkdir_p(setup_sftp, remote_join(remote_workdir, entry.relative_path))
    finally:
        if setup_sftp is not None:
            setup_sftp.close()

    if not files:
        duration = time.monotonic() - start
        progress_print(f"sftp upload completed: files=0, duration_sec={duration:.3f}", enabled=show_progress)
        return

    completed = 0
    last_progress = 0.0

    def upload_chunk(chunk: list[FileManifestEntry]) -> list[str]:
        worker_sftp: Optional[paramiko.SFTPClient] = None
        uploaded: list[str] = []
        try:
            worker_sftp = ssh_client.open_sftp()
            for entry in chunk:
                remote_path = remote_join(remote_workdir, entry.relative_path)
                local_path = source_dir / entry.relative_path
                try:
                    worker_sftp.put(str(local_path), remote_path)
                except OSError as exc:
                    raise TransferError(f"Failed to upload {local_path} to {remote_path}: {exc}") from exc
                uploaded.append(entry.relative_path)
            return uploaded
        finally:
            if worker_sftp is not None:
                worker_sftp.close()

    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        futures = [executor.submit(upload_chunk, chunk) for chunk in chunk_entries(files, actual_workers) if chunk]
        for future in as_completed(futures):
            uploaded_paths = future.result()
            for relative_path in uploaded_paths:
                completed += 1
                now = time.monotonic()
                if completed == len(files) or completed % 50 == 0 or now - last_progress >= 1.0:
                    progress_print(
                        f"sftp upload progress: completed={completed}/{len(files)}, file={relative_path}",
                        enabled=show_progress,
                    )
                    last_progress = now
    duration = time.monotonic() - start
    progress_print(f"sftp upload completed: files={len(files)}, duration_sec={duration:.3f}", enabled=show_progress)


def download_remote_tree(sftp: paramiko.SFTPClient, remote_dir: str, result_dir: Path) -> None:
    result_dir.mkdir(parents=True, exist_ok=True)

    def walk(remote_path: str, local_path: Path) -> None:
        try:
            attrs = sftp.listdir_attr(remote_path)
        except OSError as exc:
            raise TransferError(f"Cannot list remote directory {remote_path}: {exc}") from exc
        local_path.mkdir(parents=True, exist_ok=True)
        for attr in attrs:
            name = attr.filename
            child_remote = posixpath.join(remote_path, name)
            child_local = local_path / name
            mode = attr.st_mode
            if stat.S_ISDIR(mode):
                walk(child_remote, child_local)
            elif stat.S_ISREG(mode):
                try:
                    sftp.get(child_remote, str(child_local))
                except OSError as exc:
                    raise TransferError(f"Cannot download {child_remote}: {exc}") from exc
            else:
                raise TransferError(f"Unsupported remote file type at {child_remote}")

    walk(remote_dir, result_dir)


def ensure_remote_workdir(sftp: paramiko.SFTPClient, remote_workdir: str) -> None:
    if sftp_exists(sftp, remote_workdir):
        raise TransferError(f"Remote working directory already exists: {remote_workdir}")
    remote_mkdir_p(sftp, posixpath.dirname(remote_workdir))
    try:
        sftp.mkdir(remote_workdir)
    except OSError as exc:
        raise TransferError(f"Cannot create remote working directory {remote_workdir}: {exc}") from exc


def windows_path_to_wsl_path(path: Path, wsl_command: str = WSL_COMMAND) -> str:
    try:
        completed = subprocess.run(
            [wsl_command, "wslpath", "-a", str(path)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise TransferError("WSL command not found. Install WSL before using transfer.method=rsync.") from exc
    if completed.returncode != 0:
        raise TransferError(f"wslpath failed for {path}: {completed.stderr.strip()}")
    converted = completed.stdout.strip()
    if not converted:
        raise TransferError(f"wslpath returned an empty path for {path}")
    return converted


def create_rsync_files_from(manifest: list[FileManifestEntry], staging_dir: Path) -> Path:
    staging_dir.mkdir(parents=True, exist_ok=True)
    files_from = staging_dir / "rsync_files_from.txt"
    lines = [entry.relative_path for entry in manifest if entry.relative_path != "."]
    files_from.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return files_from


def sanitize_command_for_display(command: list[str]) -> str:
    sanitized: list[str] = []
    for item in command:
        if "REMOTE_JOB_RUNNER_SSH_PASSWORD=" in item:
            sanitized.append(item.split("=", 1)[0] + "=<redacted>")
        else:
            sanitized.append(item)
    return " ".join(shlex.quote(part) for part in sanitized)


def _redact_secrets(text: str, secret_values: Optional[list[str]] = None) -> str:
    if not secret_values:
        return text
    redacted = text
    for secret in secret_values:
        if secret:
            redacted = redacted.replace(secret, "<redacted>")
    return redacted


def run_streaming_subprocess(
    command: list[str],
    show_progress: bool,
    extra_env: Optional[dict[str, str]] = None,
    display_command: Optional[str] = None,
    secret_values: Optional[list[str]] = None,
) -> None:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    safe_command = display_command or sanitize_command_for_display(command)
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
    except FileNotFoundError as exc:
        raise TransferError(f"Command not found while running: {safe_command}. Install WSL and rsync.") from exc
    assert process.stdout is not None
    output_tail: list[str] = []
    for line in process.stdout:
        output_tail.append(line)
        output_tail = output_tail[-20:]
        if show_progress:
            print(line, end="", flush=True)
    return_code = process.wait()
    if return_code != 0:
        tail = _redact_secrets("".join(output_tail).strip(), secret_values)
        hint = ""
        if extra_env and "SSH_ASKPASS" in extra_env:
            hint = "\nrsync password auth uses SSH_ASKPASS; if this fails, use transfer.method=sftp or SSH key auth."
        raise TransferError(f"Command failed with exit code {return_code}: {safe_command}\n{tail}{hint}")


def resolve_auth_for_method(auth: AuthConfig, method: Literal["sftp", "rsync"]) -> ResolvedAuth:
    if auth.key_file is not None:
        return ResolvedAuth(key_file=auth.key_file, password=None, auth_source="key_file")
    if auth.password is not None:
        return ResolvedAuth(key_file=None, password=auth.password, auth_source="password")
    if auth.password_env:
        if auth.password_env not in os.environ:
            raise ConfigError(f"Password environment variable is not set: {auth.password_env}")
        return ResolvedAuth(key_file=None, password=os.environ[auth.password_env], auth_source="password_env")
    if method == "rsync":
        raise ConfigError(
            "transfer.method=rsync requires auth.key_file, auth.password, or auth.password_env; "
            "interactive password prompt is not supported for rsync."
        )
    return ResolvedAuth(key_file=None, password=getpass.getpass("SSH password: "), auth_source="interactive")


@contextlib.contextmanager
def wsl_askpass_context(password: str, staging_parent: Path) -> Iterator[dict[str, str]]:
    temp_dir = staging_parent / f".remote_job_runner_askpass_{uuid.uuid4().hex[:8]}"
    script_path = temp_dir / "askpass.sh"
    try:
        temp_dir.mkdir(parents=True, exist_ok=False)
        script_path.write_bytes(
            b"#!/bin/sh\nprintf '%s\\n' \"$REMOTE_JOB_RUNNER_SSH_PASSWORD\"\n",
        )
        wsl_script_path = windows_path_to_wsl_path(script_path)
        try:
            completed = subprocess.run(
                [WSL_COMMAND, "chmod", "700", wsl_script_path],
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            raise TransferError("WSL command not found while preparing SSH_ASKPASS helper.") from exc
        if completed.returncode != 0:
            raise TransferError(f"Failed to chmod SSH_ASKPASS helper: {completed.stderr.strip()}")
        yield {
            "SSH_ASKPASS": wsl_script_path,
            "SSH_ASKPASS_REQUIRE": "force",
            "REMOTE_JOB_RUNNER_SSH_PASSWORD": password,
            "DISPLAY": "dummy",
        }
    finally:
        try:
            shutil.rmtree(temp_dir)
        except FileNotFoundError:
            pass
        except OSError as exc:
            LOGGER.warning("Failed to remove SSH_ASKPASS temporary directory %s: %s", temp_dir, exc)


def build_rsync_ssh_command(
    config: ResolvedConfig,
    resolved_auth: ResolvedAuth,
    auto_add_host_key: bool,
) -> str:
    parts = ["ssh", "-p", str(config.remote.port)]
    if auto_add_host_key:
        parts.extend(["-o", "StrictHostKeyChecking=accept-new"])
    if resolved_auth.auth_source == "key_file":
        if resolved_auth.key_file is None:
            raise ConfigError("Resolved key_file auth is missing key_file")
        wsl_key_file = windows_path_to_wsl_path(resolved_auth.key_file)
        parts.extend(["-i", wsl_key_file, "-o", "BatchMode=yes", "-o", "PreferredAuthentications=publickey"])
    else:
        parts.extend(
            [
                "-o",
                "BatchMode=no",
                "-o",
                "NumberOfPasswordPrompts=1",
                "-o",
                "PreferredAuthentications=password,keyboard-interactive",
                "-o",
                "PubkeyAuthentication=no",
            ]
        )
    return " ".join(shlex.quote(part) for part in parts)


def upload_with_rsync(
    config: ResolvedConfig,
    manifest: list[FileManifestEntry],
    remote_workdir: str,
    staging_dir: Path,
    auto_add_host_key: bool = False,
) -> None:
    files_from = create_rsync_files_from(manifest, staging_dir)
    wsl_files_from = windows_path_to_wsl_path(files_from)
    wsl_source_dir = windows_path_to_wsl_path(config.job.source_dir)
    destination = f"{config.remote.username}@{config.remote.host}:{remote_workdir.rstrip('/')}/"
    resolved_auth = resolve_auth_for_method(config.auth, "rsync")
    ssh_command = build_rsync_ssh_command(config, resolved_auth, auto_add_host_key=auto_add_host_key)
    command = [
        WSL_COMMAND,
        RSYNC_COMMAND,
        "-a",
        "--info=progress2",
        f"--files-from={wsl_files_from}",
        "-e",
        ssh_command,
        f"{wsl_source_dir.rstrip('/')}/",
        destination,
    ]
    progress_print("rsync upload started", enabled=config.job.show_progress)
    if resolved_auth.auth_source == "key_file":
        run_streaming_subprocess(command, config.job.show_progress)
    else:
        if resolved_auth.password is None:
            raise ConfigError("Resolved password auth is missing password")
        with wsl_askpass_context(resolved_auth.password, staging_dir) as askpass_env:
            run_streaming_subprocess(
                command,
                config.job.show_progress,
                extra_env=askpass_env,
                secret_values=[resolved_auth.password],
            )
    progress_print("rsync upload completed", enabled=config.job.show_progress)


def remote_path_has_glob(path: str) -> bool:
    return any(ch in path for ch in "*?[")


def list_remote_tree_paths(sftp: paramiko.SFTPClient, remote_root: str, relative_root: str) -> list[str]:
    paths: list[str] = []
    remote_path = remote_join(remote_root, relative_root)
    try:
        attrs = sftp.listdir_attr(remote_path)
    except OSError:
        return [relative_root]
    paths.append(relative_root)
    for attr in attrs:
        child_relative = f"{relative_root.rstrip('/')}/{attr.filename}" if relative_root != "." else attr.filename
        if stat.S_ISDIR(attr.st_mode):
            paths.extend(list_remote_tree_paths(sftp, remote_root, child_relative))
        elif stat.S_ISREG(attr.st_mode):
            paths.append(child_relative)
    return paths


def expand_remote_result_paths(
    ssh_client: paramiko.SSHClient,
    sftp: paramiko.SFTPClient,
    remote_workdir: str,
    patterns: list[str],
) -> list[str]:
    expanded: list[str] = []
    glob_patterns = [pattern for pattern in patterns if remote_path_has_glob(pattern)]
    literal_patterns = [pattern for pattern in patterns if not remote_path_has_glob(pattern)]

    for pattern in literal_patterns:
        try:
            sftp.stat(remote_join(remote_workdir, pattern))
        except OSError as exc:
            raise TransferError(f"Remote result path does not exist: {pattern}") from exc
        expanded.extend(list_remote_tree_paths(sftp, remote_workdir, pattern))

    if glob_patterns:
        quoted_patterns = " ".join(shlex.quote(pattern) for pattern in glob_patterns)
        script = "shopt -s globstar nullglob dotglob; for pat in \"$@\"; do compgen -G \"$pat\"; done"
        command = (
            f"cd {shlex.quote(remote_workdir)} && "
            f"bash -lc {shlex.quote(script)} _ {quoted_patterns}"
        )
        stdin, stdout, stderr = ssh_client.exec_command(command)
        del stdin
        output = stdout.read().decode("utf-8", errors="replace")
        error = stderr.read().decode("utf-8", errors="replace")
        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            raise TransferError(f"Failed to expand remote result paths: {error.strip()}")
        for line in output.splitlines():
            if line:
                expanded.extend(list_remote_tree_paths(sftp, remote_workdir, line))

    unique = sorted(set(path for path in expanded if path and path != "."))
    if not unique:
        raise TransferError(f"No remote result paths matched: {patterns}")
    return unique


def download_results_with_sftp(
    ssh_client: paramiko.SSHClient,
    sftp: paramiko.SFTPClient,
    remote_workdir: str,
    result_patterns: list[str],
    staging_dir: Path,
    show_progress: bool = True,
) -> list[str]:
    relative_paths = expand_remote_result_paths(ssh_client, sftp, remote_workdir, result_patterns)
    progress_print(f"sftp result download started: paths={len(relative_paths)}", enabled=show_progress)
    for relative_path in relative_paths:
        remote_path = remote_join(remote_workdir, relative_path)
        local_path = staging_dir / relative_path
        try:
            attr = sftp.stat(remote_path)
            if stat.S_ISDIR(attr.st_mode):
                local_path.mkdir(parents=True, exist_ok=True)
            elif stat.S_ISREG(attr.st_mode):
                local_path.parent.mkdir(parents=True, exist_ok=True)
                sftp.get(remote_path, str(local_path))
            else:
                raise TransferError(f"Unsupported remote result file type: {relative_path}")
        except OSError as exc:
            raise TransferError(f"Failed to download remote result {relative_path}: {exc}") from exc
    progress_print(f"sftp result download completed: paths={len(relative_paths)}", enabled=show_progress)
    return relative_paths


def download_results_with_rsync(
    config: ResolvedConfig,
    relative_paths: list[str],
    remote_workdir: str,
    staging_dir: Path,
    files_from_dir: Path,
    auto_add_host_key: bool = False,
) -> None:
    files_from = files_from_dir / "rsync_results_from.txt"
    files_from.parent.mkdir(parents=True, exist_ok=True)
    files_from.write_text("\n".join(relative_paths) + "\n", encoding="utf-8")
    wsl_files_from = windows_path_to_wsl_path(files_from)
    wsl_staging_dir = windows_path_to_wsl_path(staging_dir)
    source = f"{config.remote.username}@{config.remote.host}:{remote_workdir.rstrip('/')}/"
    resolved_auth = resolve_auth_for_method(config.auth, "rsync")
    ssh_command = build_rsync_ssh_command(config, resolved_auth, auto_add_host_key=auto_add_host_key)
    command = [
        WSL_COMMAND,
        RSYNC_COMMAND,
        "-a",
        "--info=progress2",
        f"--files-from={wsl_files_from}",
        "-e",
        ssh_command,
        source,
        f"{wsl_staging_dir.rstrip('/')}/",
    ]
    progress_print("rsync result download started", enabled=config.job.show_progress)
    if resolved_auth.auth_source == "key_file":
        run_streaming_subprocess(command, config.job.show_progress)
    else:
        if resolved_auth.password is None:
            raise ConfigError("Resolved password auth is missing password")
        with wsl_askpass_context(resolved_auth.password, files_from_dir) as askpass_env:
            run_streaming_subprocess(
                command,
                config.job.show_progress,
                extra_env=askpass_env,
                secret_values=[resolved_auth.password],
            )
    progress_print("rsync result download completed", enabled=config.job.show_progress)


def remove_remote_tree(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    def remove(path: str) -> None:
        for attr in sftp.listdir_attr(path):
            child = posixpath.join(path, attr.filename)
            if stat.S_ISDIR(attr.st_mode):
                remove(child)
            else:
                sftp.remove(child)
        sftp.rmdir(path)

    remove(remote_dir)


def iter_local_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*") if path.is_file())


def merge_results_to_source(
    staging_dir: Path,
    source_dir: Path,
    results: ResultsConfig,
    backup_dir: Optional[Path],
    show_progress: bool = True,
) -> None:
    target_base = results.local_base_dir.resolve()
    if not results.allow_local_base_dir_outside_source and not path_is_inside_or_equal(source_dir.resolve(), target_base):
        raise ConfigError(f"results.local_base_dir must stay inside source_dir: {target_base}")
    files = iter_local_files(staging_dir)
    progress_print(f"result merge started: files={len(files)}, target_base={target_base}", enabled=show_progress)
    for index, source_file in enumerate(files, start=1):
        relative_path = source_file.relative_to(staging_dir)
        destination = target_base / relative_path
        progress_print(f"result merge progress: completed={index}/{len(files)}, file={relative_path}", enabled=show_progress)
        if destination.exists():
            if not results.overwrite:
                raise TransferError(f"Refusing to overwrite existing result file: {destination}")
            if results.backup_overwritten:
                if backup_dir is None:
                    raise TransferError(f"backup_dir is required before overwriting {destination}")
                backup_path = backup_dir / relative_path
                try:
                    backup_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(destination, backup_path)
                except OSError as exc:
                    raise TransferError(
                        f"Failed to backup overwritten file. source={destination}, backup={backup_path}: {exc}"
                    ) from exc
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            temp_destination = destination.with_name(f".{destination.name}.remote_job_runner_tmp_{uuid.uuid4().hex}")
            shutil.copy2(source_file, temp_destination)
            os.replace(temp_destination, destination)
        except OSError as exc:
            raise TransferError(
                f"Failed to merge result file. source={source_file}, destination={destination}, "
                f"backup_dir={backup_dir}: {exc}"
            ) from exc
    progress_print(f"result merge completed: files={len(files)}", enabled=show_progress)


def resolve_auth(auth: AuthConfig) -> tuple[Optional[str], Optional[str]]:
    resolved = resolve_auth_for_method(auth, "sftp")
    return str(resolved.key_file) if resolved.key_file is not None else None, resolved.password


def connect_ssh(config: ResolvedConfig, auto_add_host_key: bool) -> paramiko.SSHClient:
    resolved_auth = resolve_auth_for_method(config.auth, config.transfer.method)
    key_filename = str(resolved_auth.key_file) if resolved_auth.key_file is not None else None
    password = resolved_auth.password
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    if auto_add_host_key:
        LOGGER.warning("SECURITY WARNING: --auto-add-host-key trusts unknown SSH host keys.")
        print("WARNING: --auto-add-host-key trusts unknown SSH host keys and reduces SSH security.", file=sys.stderr)
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    else:
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect(
            hostname=config.remote.host,
            port=config.remote.port,
            username=config.remote.username,
            key_filename=key_filename,
            password=password,
            timeout=30,
            look_for_keys=False if key_filename or password else True,
        )
    except Exception as exc:
        raise ConnectionError(f"SSH connection failed for {config.remote.username}@{config.remote.host}:{config.remote.port}: {exc}") from exc
    return client


def _read_available(channel: Any, stderr: bool = False) -> bytes:
    chunks: list[bytes] = []
    recv_ready = channel.recv_stderr_ready if stderr else channel.recv_ready
    recv = channel.recv_stderr if stderr else channel.recv
    while recv_ready():
        chunks.append(recv(65536))
    return b"".join(chunks)


def _append_limited(buffer: bytearray, chunk: bytes, limit: int) -> tuple[int, bool]:
    if not chunk:
        return 0, False
    buffer.extend(chunk)
    truncated = False
    if len(buffer) > limit:
        del buffer[: len(buffer) - limit]
        truncated = True
    return len(chunk), truncated


def run_remote_command(
    ssh_client: paramiko.SSHClient,
    remote_workdir: str,
    command_spec: CommandSpec,
    stage_name: str,
    log_dir: Optional[Path],
    max_captured_output_bytes: int = 1024 * 1024,
    enable_logs: bool = True,
    show_progress: bool = True,
) -> CommandResult:
    start_monotonic = time.monotonic()
    start_time = utc_now_iso()
    stdout_buffer = bytearray()
    stderr_buffer = bytearray()
    stdout_bytes = 0
    stderr_bytes = 0
    stdout_truncated = False
    stderr_truncated = False
    timed_out = False
    exit_code = 255
    full_command = f"cd {shlex.quote(remote_workdir)} && bash -lc {shlex.quote(command_spec.cmd)}"
    progress_print(
        f"remote command started: stage={stage_name}, command={command_spec.name}",
        enabled=show_progress,
    )

    try:
        transport = ssh_client.get_transport()
        if transport is None:
            raise RemoteCommandError("SSH transport is not available")
        channel = transport.open_session()
        channel.exec_command(full_command)
        deadline = start_monotonic + command_spec.timeout_sec
        while True:
            out_chunk = _read_available(channel, stderr=False)
            err_chunk = _read_available(channel, stderr=True)
            added, truncated = _append_limited(stdout_buffer, out_chunk, max_captured_output_bytes)
            stdout_bytes += added
            stdout_truncated = stdout_truncated or truncated
            added, truncated = _append_limited(stderr_buffer, err_chunk, max_captured_output_bytes)
            stderr_bytes += added
            stderr_truncated = stderr_truncated or truncated
            if channel.exit_status_ready():
                exit_code = int(channel.recv_exit_status())
                break
            if time.monotonic() >= deadline:
                timed_out = True
                exit_code = 124
                try:
                    channel.close()
                finally:
                    break
            time.sleep(0.05)
        out_chunk = _read_available(channel, stderr=False)
        err_chunk = _read_available(channel, stderr=True)
        added, truncated = _append_limited(stdout_buffer, out_chunk, max_captured_output_bytes)
        stdout_bytes += added
        stdout_truncated = stdout_truncated or truncated
        added, truncated = _append_limited(stderr_buffer, err_chunk, max_captured_output_bytes)
        stderr_bytes += added
        stderr_truncated = stderr_truncated or truncated
    except RemoteCommandError:
        raise
    except Exception as exc:
        raise RemoteCommandError(f"Failed to run remote command {command_spec.name!r}: {exc}") from exc

    end_time = utc_now_iso()
    duration = time.monotonic() - start_monotonic
    status = "failed" if timed_out or exit_code != 0 else "completed"
    progress_print(
        (
            f"remote command {status}: stage={stage_name}, command={command_spec.name}, "
            f"exit_code={exit_code}, duration_sec={duration:.3f}, timed_out={str(timed_out).lower()}"
        ),
        enabled=show_progress,
    )
    result = CommandResult(
        stage_name=stage_name,
        command_name=command_spec.name,
        command=command_spec.cmd,
        start_time=start_time,
        end_time=end_time,
        duration_sec=round(duration, 6),
        exit_code=exit_code,
        stdout=bytes(stdout_buffer).decode("utf-8", errors="replace"),
        stderr=bytes(stderr_buffer).decode("utf-8", errors="replace"),
        timed_out=timed_out,
        stdout_bytes=stdout_bytes,
        stderr_bytes=stderr_bytes,
        stdout_truncated=stdout_truncated,
        stderr_truncated=stderr_truncated,
    )
    write_command_log(log_dir, result, enable_logs)
    return result


def write_command_log(log_dir: Optional[Path], result: CommandResult, enable_logs: bool = True) -> None:
    if not enable_logs or log_dir is None:
        return
    filename = f"{sanitize_filename(result.stage_name)}_{sanitize_filename(result.command_name)}.log"
    body = (
        f"stage: {result.stage_name}\n"
        f"command_name: {result.command_name}\n"
        f"command: {result.command}\n"
        f"start_time: {result.start_time}\n"
        f"end_time: {result.end_time}\n"
        f"duration_sec: {result.duration_sec}\n"
        f"exit_code: {result.exit_code}\n"
        f"timed_out: {result.timed_out}\n"
        f"stdout_bytes: {result.stdout_bytes}\n"
        f"stderr_bytes: {result.stderr_bytes}\n"
        f"stdout_truncated: {result.stdout_truncated}\n"
        f"stderr_truncated: {result.stderr_truncated}\n"
        "\n--- stdout ---\n"
        f"{result.stdout}\n"
        "\n--- stderr ---\n"
        f"{result.stderr}\n"
    )
    write_text(log_dir / "stdout_stderr" / filename, body)


def command_result_summary(result: CommandResult) -> dict[str, Any]:
    return {
        "stage_name": result.stage_name,
        "command_name": result.command_name,
        "command": result.command,
        "start_time": result.start_time,
        "end_time": result.end_time,
        "duration_sec": result.duration_sec,
        "exit_code": result.exit_code,
        "timed_out": result.timed_out,
        "stdout_bytes": result.stdout_bytes,
        "stderr_bytes": result.stderr_bytes,
        "stdout_truncated": result.stdout_truncated,
        "stderr_truncated": result.stderr_truncated,
    }


def run_stage(
    ssh_client: Any,
    remote_workdir: str,
    stage_spec: StageSpec,
    log_dir: Optional[Path],
    runner: RunnerFunc = run_remote_command,
    max_captured_output_bytes: int = 1024 * 1024,
    enable_logs: bool = True,
    show_progress: bool = True,
) -> StageResult:
    LOGGER.info("Running stage %s (%s)", stage_spec.name, stage_spec.mode)
    stage_start = time.monotonic()
    max_workers = stage_spec.max_workers or (min(4, len(stage_spec.commands)) if stage_spec.mode == "parallel" else None)
    progress_print(
        (
            f"stage started: name={stage_spec.name}, mode={stage_spec.mode}, commands={len(stage_spec.commands)}"
            + (f", max_workers={max_workers}" if max_workers is not None else "")
        ),
        enabled=show_progress,
    )
    results: list[CommandResult] = []

    def run_one(command: CommandSpec) -> CommandResult:
        if runner is run_remote_command:
            result = run_remote_command(
                ssh_client,
                remote_workdir,
                command,
                stage_spec.name,
                log_dir,
                max_captured_output_bytes=max_captured_output_bytes,
                enable_logs=enable_logs,
                show_progress=show_progress,
            )
        else:
            result = runner(ssh_client, remote_workdir, command, stage_spec.name, log_dir)
            status = "timeout" if result.timed_out else ("completed" if result.exit_code == 0 else "failed")
            progress_print(
                (
                    f"remote command {status}: stage={stage_spec.name}, command={command.name}, "
                    f"exit_code={result.exit_code}, timed_out={str(result.timed_out).lower()}"
                ),
                enabled=show_progress,
            )
        if runner is not run_remote_command:
            status = "timeout" if result.timed_out else ("ok" if result.exit_code == 0 else "failed")
            LOGGER.debug("Command finished through custom runner: %s/%s status=%s", stage_spec.name, command.name, status)
        return result

    if stage_spec.mode == "sequential":
        for command in stage_spec.commands:
            result = run_one(command)
            results.append(result)
            if result.exit_code != 0 or result.timed_out:
                duration = time.monotonic() - stage_start
                progress_print(
                    f"stage completed: name={stage_spec.name}, success=false, duration_sec={duration:.3f}",
                    enabled=show_progress,
                )
                return StageResult(stage_name=stage_spec.name, mode=stage_spec.mode, command_results=results, success=False)
        duration = time.monotonic() - stage_start
        progress_print(
            f"stage completed: name={stage_spec.name}, success=true, duration_sec={duration:.3f}",
            enabled=show_progress,
        )
        return StageResult(stage_name=stage_spec.name, mode=stage_spec.mode, command_results=results, success=True)

    if stage_spec.mode != "parallel":
        raise ConfigError(f"stage {stage_spec.name!r} mode must be sequential or parallel")
    max_workers = stage_spec.max_workers or min(4, len(stage_spec.commands))
    if max_workers < 1:
        raise ConfigError(f"stage {stage_spec.name!r}.max_workers must be >= 1")

    indexed_results: dict[int, CommandResult] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_one, command): index
            for index, command in enumerate(stage_spec.commands)
        }
        for future in as_completed(futures):
            index = futures[future]
            indexed_results[index] = future.result()
    results = [indexed_results[index] for index in range(len(stage_spec.commands))]
    success = all(result.exit_code == 0 and not result.timed_out for result in results)
    duration = time.monotonic() - stage_start
    progress_print(
        f"stage completed: name={stage_spec.name}, success={str(success).lower()}, duration_sec={duration:.3f}",
        enabled=show_progress,
    )
    return StageResult(stage_name=stage_spec.name, mode=stage_spec.mode, command_results=results, success=success)


def lock_file_path(source_dir: Path) -> Path:
    source = source_dir.expanduser()
    return source.parent / f".remote_job_runner_{source.name}.lock"


@contextlib.contextmanager
def acquire_lock(source_dir: Path) -> Iterator[Path]:
    lock_path = lock_file_path(source_dir)
    payload = {
        "pid": os.getpid(),
        "timestamp": utc_now_iso(),
        "source_dir": str(source_dir),
    }
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise LockError(f"Lock already exists; another job may be running: {lock_path}") from exc
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        yield lock_path
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def safe_replace_source_dir(source_dir: Path, result_dir: Path, backup_dir: Path, keep_backup: bool) -> None:
    if not source_dir.exists() or not source_dir.is_dir():
        raise SafeSwapError(f"source_dir must exist and be a directory: {source_dir}")
    if not result_dir.exists() or not result_dir.is_dir():
        raise SafeSwapError(f"result_dir must exist and be a directory: {result_dir}")
    if backup_dir.exists():
        raise SafeSwapError(f"backup_dir must not already exist: {backup_dir}")

    LOGGER.info("Renaming source directory to backup: %s -> %s", source_dir, backup_dir)
    try:
        source_dir.rename(backup_dir)
    except OSError as exc:
        raise SafeSwapError(f"Failed to rename source_dir to backup_dir: {source_dir} -> {backup_dir}: {exc}") from exc

    try:
        LOGGER.info("Renaming result directory to source: %s -> %s", result_dir, source_dir)
        result_dir.rename(source_dir)
    except OSError as exc:
        rollback_error: Optional[BaseException] = None
        try:
            backup_dir.rename(source_dir)
        except OSError as rollback_exc:
            rollback_error = rollback_exc
        if rollback_error is not None:
            raise SafeSwapError(
                "Safe swap failed and rollback also failed; manual repair is required. "
                f"backup_dir={backup_dir}, result_dir={result_dir}, source_dir={source_dir}. "
                f"replace_error={exc}; rollback_error={rollback_error}"
            ) from rollback_error
        raise SafeSwapError(f"Failed to move result_dir into source_dir; rollback succeeded: {exc}") from exc

    if not keep_backup:
        try:
            LOGGER.info("Deleting backup directory: %s", backup_dir)
            shutil.rmtree(backup_dir)
        except OSError as exc:
            LOGGER.warning("Failed to delete backup directory; backup retained at %s: %s", backup_dir, exc)


def setup_file_logging(log_dir: Path, verbose: bool) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    file_handler = logging.FileHandler(log_dir / "job.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    LOGGER.addHandler(file_handler)


def setup_console_logging(verbose: bool) -> None:
    LOGGER.setLevel(logging.DEBUG if verbose else logging.INFO)
    if not any(isinstance(handler, logging.StreamHandler) for handler in LOGGER.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        LOGGER.addHandler(handler)


def print_execution_summary(config: ResolvedConfig, remote_workdir: str, backup_dir: Path) -> None:
    print("Execution summary:")
    print(f"  source_dir: {config.job.source_dir}")
    print(f"  remote: {config.remote.username}@{config.remote.host}:{config.remote.port}")
    print(f"  remote_workdir: {remote_workdir}")
    print(f"  overwritten_backup_dir: {backup_dir}")
    print(f"  enable_logs: {config.job.enable_logs}")
    print(f"  show_progress: {config.job.show_progress}")
    print(f"  transfer_method: {config.transfer.method}")
    print(f"  results_remote_paths: {config.results.remote_paths}")
    print(f"  results_local_base_dir: {config.results.local_base_dir}")
    print(f"  cleanup_remote_on_success: {config.job.cleanup_remote_on_success}")
    print(f"  cleanup_remote_on_failure: {config.job.cleanup_remote_on_failure}")
    print("  stages:")
    for stage in config.stages:
        print(f"    - {stage.name} ({stage.mode}, commands={len(stage.commands)}, max_workers={stage.max_workers})")


def confirm_proceed() -> bool:
    answer = input("Proceed? [y/N] ").strip().lower()
    return answer in {"y", "yes"}


def infer_auth_source(auth: AuthConfig, method: Literal["sftp", "rsync"]) -> str:
    if auth.key_file is not None:
        return "key_file"
    if auth.password is not None:
        return "password"
    if auth.password_env:
        return "password_env"
    return "interactive" if method == "sftp" else "missing"


def build_dry_run_plan(config: ResolvedConfig, manifest: list[FileManifestEntry]) -> str:
    source = config.job.source_dir
    job_pattern = f"{config.remote.remote_base_dir.rstrip('/')}/job_<timestamp>_<id>"
    log_pattern = source.parent / ".remote_job_runner_logs" / "job_<timestamp>_<id>"
    backup_pattern = source.parent / f"{source.name}.backup_job_<timestamp>_<id>"
    lock_path = lock_file_path(source)
    lines = [
        "Dry-run plan:",
        f"  source_dir: {source}",
        f"  remote_host: {config.remote.host}",
        f"  port: {config.remote.port}",
        f"  username: {config.remote.username}",
        f"  remote_base_dir: {config.remote.remote_base_dir}",
        f"  remote_workdir_pattern: {job_pattern}",
        f"  log_dir_pattern: {log_pattern}",
        f"  backup_dir_pattern: {backup_pattern}",
        f"  lock_file_path: {lock_path} (not created in dry-run)",
        (
            "  cleanup_policy: "
            f"success={config.job.cleanup_remote_on_success}, failure={config.job.cleanup_remote_on_failure}"
        ),
        f"  keep_backup: {config.job.keep_backup}",
        f"  enable_logs: {config.job.enable_logs}",
        f"  show_progress: {config.job.show_progress}",
        f"  max_captured_output_bytes: {config.job.max_captured_output_bytes}",
        f"  verify_hash: {config.job.verify_hash}",
        f"  skip_symlinks: {config.job.skip_symlinks}",
        f"  transfer_method: {config.transfer.method}",
        (
            "  sftp_max_workers: "
            f"{config.transfer.sftp_max_workers if config.transfer.method == 'sftp' else 'not applicable'}"
        ),
        f"  auth_source: {infer_auth_source(config.auth, config.transfer.method)}",
        f"  effective_upload: {config.transfer.method}",
        f"  effective_download: {config.transfer.method}",
        (
            "  askpass: "
            f"{'will be used at runtime' if config.transfer.method == 'rsync' and infer_auth_source(config.auth, config.transfer.method) in ('password', 'password_env') else 'not used'}"
        ),
        f"  internal_wsl_command: [{WSL_COMMAND!r}]",
        f"  internal_rsync_path: {RSYNC_COMMAND!r}",
        f"  results_remote_paths: {config.results.remote_paths}",
        f"  results_local_base_dir_raw: {config.results.local_base_dir_raw}",
        f"  results_local_base_dir_resolved: {config.results.local_base_dir}",
        f"  allow_local_base_dir_outside_source: {config.results.allow_local_base_dir_outside_source}",
        f"  results_overwrite: {config.results.overwrite}",
        f"  results_backup_overwritten: {config.results.backup_overwritten}",
        f"  include_globs: {config.transfer.include_globs}",
        f"  exclude_globs: {config.transfer.exclude_globs}",
        "  upload_plan:",
    ]
    for entry in manifest:
        lines.append(f"    - {entry.kind}: {entry.relative_path} size={entry.size} sha256={entry.sha256}")
    lines.append("  stages:")
    for stage in config.stages:
        lines.append(f"    - stage: {stage.name}")
        lines.append(f"      mode: {stage.mode}")
        lines.append(f"      max_workers: {stage.max_workers}")
        for command in stage.commands:
            lines.append(f"      command: {command.name}")
            lines.append(f"        cmd: {command.cmd}")
            lines.append(f"        timeout_sec: {command.timeout_sec}")
    return "\n".join(lines)


def run_dry_run(config: ResolvedConfig) -> str:
    progress_print("dry-run started", enabled=config.job.show_progress)
    with timed_phase("dry_run_manifest", config.job.show_progress):
        if not config.job.source_dir.exists() or not config.job.source_dir.is_dir():
            manifest: list[FileManifestEntry] = []
            print(f"WARNING: source_dir does not exist; upload_plan is empty: {config.job.source_dir}", file=sys.stderr)
        else:
            manifest = build_manifest(
                config.job.source_dir,
                config.transfer.include_globs,
                config.transfer.exclude_globs,
                config.job.verify_hash,
                config.job.skip_symlinks,
            )
    plan = build_dry_run_plan(config, manifest)
    print(plan)
    progress_print("dry-run completed", enabled=config.job.show_progress)
    return plan


def run_job(config: ResolvedConfig, confirm: bool, verbose: bool, auto_add_host_key: bool) -> JobResult:
    source_dir = config.job.source_dir
    if not source_dir.exists() or not source_dir.is_dir():
        raise TransferError(f"source_dir must exist and be a directory: {source_dir}")

    job_id = generate_job_id()
    remote_workdir = posixpath.join(config.remote.remote_base_dir.rstrip("/"), job_id)
    log_dir: Optional[Path] = source_dir.parent / ".remote_job_runner_logs" / job_id if config.job.enable_logs else None
    backup_dir: Optional[Path] = None
    if config.results.backup_overwritten:
        backup_dir = (
            log_dir / "overwritten_backup"
            if log_dir is not None
            else source_dir.parent / ".remote_job_runner_overwritten_backup" / job_id
        )
    if log_dir is not None:
        setup_file_logging(log_dir, verbose)
        maybe_write_text(config.job.enable_logs, log_dir / "config.resolved.yaml", dump_resolved_config_yaml(config))

    if confirm:
        print_execution_summary(config, remote_workdir, backup_dir or source_dir.parent / ".remote_job_runner_overwritten_backup" / job_id)
    if confirm and not confirm_proceed():
        LOGGER.info("User declined execution before any mutating action")
        return JobResult(
            job_id=job_id,
            success=False,
            remote_workdir=remote_workdir,
            local_log_dir=log_dir,
            backup_dir=None,
            stage_results=[],
        )

    with acquire_lock(source_dir):
        progress_print(f"job started: job_id={job_id}", enabled=config.job.show_progress)
        with timed_phase("build_manifest_before", config.job.show_progress):
            manifest_before = build_manifest(
                source_dir,
                config.transfer.include_globs,
                config.transfer.exclude_globs,
                config.job.verify_hash,
                config.job.skip_symlinks,
            )
        maybe_write_json(config.job.enable_logs, log_dir / "manifest.before_upload.json" if log_dir else None, manifest_to_jsonable(manifest_before))

        ssh_client: Optional[paramiko.SSHClient] = None
        sftp: Optional[paramiko.SFTPClient] = None
        stage_results: list[StageResult] = []
        success = False
        try:
            with timed_phase("connect_ssh", config.job.show_progress):
                ssh_client = connect_ssh(config, auto_add_host_key=auto_add_host_key)
            with timed_phase("open_sftp", config.job.show_progress):
                sftp = ssh_client.open_sftp()
            try:
                with timed_phase("upload", config.job.show_progress):
                    if config.transfer.method == "sftp":
                        if config.transfer.sftp_max_workers <= 1:
                            upload_with_sftp(sftp, source_dir, remote_workdir, manifest_before, config.job.show_progress)
                        else:
                            upload_manifest_parallel_sftp(
                                ssh_client,
                                source_dir,
                                remote_workdir,
                                manifest_before,
                                config.transfer.sftp_max_workers,
                                config.job.show_progress,
                            )
                    elif config.transfer.method == "rsync":
                        ensure_remote_workdir(sftp, remote_workdir)
                        with tempfile.TemporaryDirectory(
                            prefix=".remote_job_runner_rsync_", dir=source_dir.parent
                        ) as rsync_temp:
                            upload_with_rsync(
                                config,
                                manifest_before,
                                remote_workdir,
                                Path(rsync_temp),
                                auto_add_host_key=auto_add_host_key,
                            )
                    else:
                        raise ConfigError(f"Unsupported transfer.method: {config.transfer.method}")
                with timed_phase("run_stages", config.job.show_progress):
                    for stage in config.stages:
                        stage_result = run_stage(
                            ssh_client,
                            remote_workdir,
                            stage,
                            log_dir,
                            max_captured_output_bytes=config.job.max_captured_output_bytes,
                            enable_logs=config.job.enable_logs,
                            show_progress=config.job.show_progress,
                        )
                        stage_results.append(stage_result)
                        summaries = [
                            command_result_summary(result)
                            for stage_result_item in stage_results
                            for result in stage_result_item.command_results
                        ]
                        maybe_write_json(config.job.enable_logs, log_dir / "commands.json" if log_dir else None, summaries)
                        if not stage_result.success:
                            raise RemoteCommandError(f"Stage failed: {stage.name}")

                with tempfile.TemporaryDirectory(
                    prefix=".remote_job_runner_download_", dir=source_dir.parent
                ) as temp_root:
                    result_dir = Path(temp_root)
                    with timed_phase("download_results", config.job.show_progress):
                        relative_paths = expand_remote_result_paths(ssh_client, sftp, remote_workdir, config.results.remote_paths)
                        if config.transfer.method == "sftp":
                            download_results_with_sftp(
                                ssh_client,
                                sftp,
                                remote_workdir,
                                config.results.remote_paths,
                                result_dir,
                                config.job.show_progress,
                            )
                        elif config.transfer.method == "rsync":
                            with tempfile.TemporaryDirectory(
                                prefix=".remote_job_runner_rsync_results_", dir=source_dir.parent
                            ) as rsync_temp:
                                download_results_with_rsync(
                                    config,
                                    relative_paths,
                                    remote_workdir,
                                    result_dir,
                                    Path(rsync_temp),
                                    auto_add_host_key=auto_add_host_key,
                                )
                        else:
                            raise ConfigError(f"Unsupported transfer.method: {config.transfer.method}")
                    manifest_after = build_manifest(result_dir, ["**/*"], [], config.job.verify_hash, config.job.skip_symlinks)
                    compare_download_manifest(manifest_after, config.job.verify_hash)
                    maybe_write_json(config.job.enable_logs, log_dir / "manifest.after_download.json" if log_dir else None, manifest_to_jsonable(manifest_after))
                    with timed_phase("merge_results", config.job.show_progress):
                        merge_results_to_source(result_dir, source_dir, config.results, backup_dir, config.job.show_progress)
                success = True
                if config.job.cleanup_remote_on_success:
                    with timed_phase("cleanup_remote", config.job.show_progress):
                        try:
                            remove_remote_tree(sftp, remote_workdir)
                        except Exception as exc:
                            LOGGER.warning("Failed to cleanup remote working directory after success: %s", exc)
                progress_print(f"job succeeded: job_id={job_id}", enabled=config.job.show_progress)
            finally:
                if sftp is not None:
                    sftp.close()
        except Exception:
            if ssh_client is not None and config.job.cleanup_remote_on_failure:
                with timed_phase("cleanup_remote", config.job.show_progress):
                    try:
                        sftp_fail = ssh_client.open_sftp()
                        try:
                            remove_remote_tree(sftp_fail, remote_workdir)
                        finally:
                            sftp_fail.close()
                    except Exception as cleanup_exc:
                        LOGGER.warning("Failed to cleanup remote working directory after failure: %s", cleanup_exc)
            progress_print(f"job failed: job_id={job_id}", enabled=config.job.show_progress)
            raise
        finally:
            if ssh_client is not None:
                ssh_client.close()

    return JobResult(
        job_id=job_id,
        success=success,
        remote_workdir=remote_workdir,
        local_log_dir=log_dir,
        backup_dir=backup_dir,
        stage_results=stage_results,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Copy a local folder to a remote Linux workstation, run staged commands, and safely sync results back.")
    parser.add_argument("--config", default="config.yaml", help="YAML config file path")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--username")
    parser.add_argument("--password-env")
    parser.add_argument("--key-file")
    parser.add_argument("--source-dir")
    parser.add_argument("--remote-base-dir")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without changing local or remote filesystems")
    parser.add_argument("--verbose", action="store_true", help="Enable DEBUG logging")
    parser.add_argument("--yes", action="store_true", help="Deprecated no-op; execution no longer asks for confirmation by default")
    parser.add_argument("--confirm", action="store_true", help="Ask for Proceed? confirmation before executing")
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument("--enable-logs", dest="enable_logs", action="store_true", default=None)
    log_group.add_argument("--no-enable-logs", dest="enable_logs", action="store_false")
    progress_group = parser.add_mutually_exclusive_group()
    progress_group.add_argument("--show-progress", dest="show_progress", action="store_true", default=None)
    progress_group.add_argument("--no-show-progress", dest="show_progress", action="store_false")
    backup_group = parser.add_mutually_exclusive_group()
    backup_group.add_argument("--keep-backup", dest="keep_backup", action="store_true", default=None)
    backup_group.add_argument("--no-keep-backup", dest="keep_backup", action="store_false")
    success_group = parser.add_mutually_exclusive_group()
    success_group.add_argument("--cleanup-remote-on-success", dest="cleanup_remote_on_success", action="store_true", default=None)
    success_group.add_argument("--no-cleanup-remote-on-success", dest="cleanup_remote_on_success", action="store_false")
    failure_group = parser.add_mutually_exclusive_group()
    failure_group.add_argument("--cleanup-remote-on-failure", dest="cleanup_remote_on_failure", action="store_true", default=None)
    failure_group.add_argument("--no-cleanup-remote-on-failure", dest="cleanup_remote_on_failure", action="store_false")
    parser.add_argument("--auto-add-host-key", action="store_true", help="Trust unknown SSH host keys. WARNING: lowers SSH security.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    setup_console_logging(args.verbose)
    try:
        config = load_config(Path(args.config), args)
        if args.auto_add_host_key:
            print("WARNING: --auto-add-host-key trusts unknown SSH host keys and reduces SSH security.", file=sys.stderr)
        if args.dry_run:
            run_dry_run(config)
            return 0
        result = run_job(config, confirm=args.confirm, verbose=args.verbose, auto_add_host_key=args.auto_add_host_key)
        if result.success:
            print(f"Job succeeded: {result.job_id}")
            if result.local_log_dir is not None:
                print(f"Logs: {result.local_log_dir}")
            return 0
        print(f"Job did not run to completion: {result.job_id}", file=sys.stderr)
        if result.local_log_dir is not None:
            print(f"Logs: {result.local_log_dir}", file=sys.stderr)
        return 2
    except RemoteJobRunnerError as exc:
        LOGGER.error("%s", exc)
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

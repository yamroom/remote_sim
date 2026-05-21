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
class JobConfig:
    source_dir: Path
    keep_backup: bool
    cleanup_remote_on_success: bool
    cleanup_remote_on_failure: bool
    verify_hash: bool
    skip_symlinks: bool


@dataclass(frozen=True)
class TransferConfig:
    include_globs: list[str]
    exclude_globs: list[str]


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
    local_log_dir: Path
    backup_dir: Optional[Path]
    stage_results: list[StageResult]


RunnerFunc = Callable[[Any, str, CommandSpec, str, Path], CommandResult]


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
        cleanup_remote_on_success=_as_bool(
            job_data.get("cleanup_remote_on_success", True), "job.cleanup_remote_on_success"
        ),
        cleanup_remote_on_failure=_as_bool(
            job_data.get("cleanup_remote_on_failure", False), "job.cleanup_remote_on_failure"
        ),
        verify_hash=_as_bool(job_data.get("verify_hash", True), "job.verify_hash"),
        skip_symlinks=_as_bool(job_data.get("skip_symlinks", False), "job.skip_symlinks"),
    )

    include_globs = _string_list(transfer_data.get("include_globs", ["**/*"]), "transfer.include_globs")
    exclude_globs = _string_list(transfer_data.get("exclude_globs", []), "transfer.exclude_globs")
    transfer = TransferConfig(include_globs=include_globs or ["**/*"], exclude_globs=exclude_globs)

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

    return ResolvedConfig(remote=remote, auth=auth, job=job, transfer=transfer, stages=stages)


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


def resolve_auth(auth: AuthConfig) -> tuple[Optional[str], Optional[str]]:
    if auth.key_file is not None:
        return str(auth.key_file), None
    if auth.password is not None:
        return None, auth.password
    if auth.password_env:
        if auth.password_env not in os.environ:
            raise ConfigError(f"Password environment variable is not set: {auth.password_env}")
        return None, os.environ[auth.password_env]
    return None, getpass.getpass("SSH password: ")


def connect_ssh(config: ResolvedConfig, auto_add_host_key: bool) -> paramiko.SSHClient:
    key_filename, password = resolve_auth(config.auth)
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


def _read_available(channel: Any, stderr: bool = False) -> str:
    chunks: list[bytes] = []
    recv_ready = channel.recv_stderr_ready if stderr else channel.recv_ready
    recv = channel.recv_stderr if stderr else channel.recv
    while recv_ready():
        chunks.append(recv(65536))
    return b"".join(chunks).decode("utf-8", errors="replace")


def run_remote_command(
    ssh_client: paramiko.SSHClient,
    remote_workdir: str,
    command_spec: CommandSpec,
    stage_name: str,
    log_dir: Path,
) -> CommandResult:
    start_monotonic = time.monotonic()
    start_time = utc_now_iso()
    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    timed_out = False
    exit_code = 255
    full_command = f"cd {shlex.quote(remote_workdir)} && bash -lc {shlex.quote(command_spec.cmd)}"

    try:
        transport = ssh_client.get_transport()
        if transport is None:
            raise RemoteCommandError("SSH transport is not available")
        channel = transport.open_session()
        channel.exec_command(full_command)
        deadline = start_monotonic + command_spec.timeout_sec
        while True:
            stdout_parts.append(_read_available(channel, stderr=False))
            stderr_parts.append(_read_available(channel, stderr=True))
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
        stdout_parts.append(_read_available(channel, stderr=False))
        stderr_parts.append(_read_available(channel, stderr=True))
    except RemoteCommandError:
        raise
    except Exception as exc:
        raise RemoteCommandError(f"Failed to run remote command {command_spec.name!r}: {exc}") from exc

    end_time = utc_now_iso()
    duration = time.monotonic() - start_monotonic
    result = CommandResult(
        stage_name=stage_name,
        command_name=command_spec.name,
        command=command_spec.cmd,
        start_time=start_time,
        end_time=end_time,
        duration_sec=round(duration, 6),
        exit_code=exit_code,
        stdout="".join(stdout_parts),
        stderr="".join(stderr_parts),
        timed_out=timed_out,
    )
    write_command_log(log_dir, result)
    return result


def write_command_log(log_dir: Path, result: CommandResult) -> None:
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
        "\n--- stdout ---\n"
        f"{result.stdout}\n"
        "\n--- stderr ---\n"
        f"{result.stderr}\n"
    )
    write_text(log_dir / "stdout_stderr" / filename, body)


def run_stage(
    ssh_client: Any,
    remote_workdir: str,
    stage_spec: StageSpec,
    log_dir: Path,
    runner: RunnerFunc = run_remote_command,
) -> StageResult:
    LOGGER.info("Running stage %s (%s)", stage_spec.name, stage_spec.mode)
    results: list[CommandResult] = []

    if stage_spec.mode == "sequential":
        for command in stage_spec.commands:
            result = runner(ssh_client, remote_workdir, command, stage_spec.name, log_dir)
            results.append(result)
            if result.exit_code != 0 or result.timed_out:
                return StageResult(stage_name=stage_spec.name, mode=stage_spec.mode, command_results=results, success=False)
        return StageResult(stage_name=stage_spec.name, mode=stage_spec.mode, command_results=results, success=True)

    if stage_spec.mode != "parallel":
        raise ConfigError(f"stage {stage_spec.name!r} mode must be sequential or parallel")
    max_workers = stage_spec.max_workers or min(4, len(stage_spec.commands))
    if max_workers < 1:
        raise ConfigError(f"stage {stage_spec.name!r}.max_workers must be >= 1")

    indexed_results: dict[int, CommandResult] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(runner, ssh_client, remote_workdir, command, stage_spec.name, log_dir): index
            for index, command in enumerate(stage_spec.commands)
        }
        for future in as_completed(futures):
            index = futures[future]
            indexed_results[index] = future.result()
    results = [indexed_results[index] for index in range(len(stage_spec.commands))]
    success = all(result.exit_code == 0 and not result.timed_out for result in results)
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
    print(f"  backup_dir: {backup_dir}")
    print(f"  keep_backup: {config.job.keep_backup}")
    print(f"  cleanup_remote_on_success: {config.job.cleanup_remote_on_success}")
    print(f"  cleanup_remote_on_failure: {config.job.cleanup_remote_on_failure}")
    print("  stages:")
    for stage in config.stages:
        print(f"    - {stage.name} ({stage.mode}, commands={len(stage.commands)}, max_workers={stage.max_workers})")


def confirm_proceed() -> bool:
    answer = input("Proceed? [y/N] ").strip().lower()
    return answer in {"y", "yes"}


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
        f"  verify_hash: {config.job.verify_hash}",
        f"  skip_symlinks: {config.job.skip_symlinks}",
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
    manifest = build_manifest(
        config.job.source_dir,
        config.transfer.include_globs,
        config.transfer.exclude_globs,
        config.job.verify_hash,
        config.job.skip_symlinks,
    )
    plan = build_dry_run_plan(config, manifest)
    print(plan)
    return plan


def run_job(config: ResolvedConfig, yes: bool, verbose: bool, auto_add_host_key: bool) -> JobResult:
    source_dir = config.job.source_dir
    if not source_dir.exists() or not source_dir.is_dir():
        raise TransferError(f"source_dir must exist and be a directory: {source_dir}")

    job_id = generate_job_id()
    remote_workdir = posixpath.join(config.remote.remote_base_dir.rstrip("/"), job_id)
    log_dir = source_dir.parent / ".remote_job_runner_logs" / job_id
    backup_dir = source_dir.parent / f"{source_dir.name}.backup_{job_id}"
    setup_file_logging(log_dir, verbose)
    write_text(log_dir / "config.resolved.yaml", dump_resolved_config_yaml(config))

    print_execution_summary(config, remote_workdir, backup_dir)
    if not yes and not confirm_proceed():
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
        manifest_before = build_manifest(
            source_dir,
            config.transfer.include_globs,
            config.transfer.exclude_globs,
            config.job.verify_hash,
            config.job.skip_symlinks,
        )
        write_json(log_dir / "manifest.before_upload.json", manifest_to_jsonable(manifest_before))

        ssh_client: Optional[paramiko.SSHClient] = None
        stage_results: list[StageResult] = []
        success = False
        try:
            ssh_client = connect_ssh(config, auto_add_host_key=auto_add_host_key)
            sftp = ssh_client.open_sftp()
            try:
                upload_manifest(sftp, source_dir, remote_workdir, manifest_before)
                for stage in config.stages:
                    stage_result = run_stage(ssh_client, remote_workdir, stage, log_dir)
                    stage_results.append(stage_result)
                    write_json(log_dir / "commands.json", [result for stage_result in stage_results for result in stage_result.command_results])
                    if not stage_result.success:
                        raise RemoteCommandError(f"Stage failed: {stage.name}")

                with tempfile.TemporaryDirectory(prefix="remote_job_runner_result_") as temp_root:
                    result_dir = Path(temp_root) / source_dir.name
                    download_remote_tree(sftp, remote_workdir, result_dir)
                    manifest_after = build_manifest(result_dir, ["**/*"], [], config.job.verify_hash, config.job.skip_symlinks)
                    compare_download_manifest(manifest_after, config.job.verify_hash)
                    write_json(log_dir / "manifest.after_download.json", manifest_to_jsonable(manifest_after))
                    final_result_dir = source_dir.parent / f".remote_job_runner_result_{job_id}"
                    if final_result_dir.exists():
                        raise SafeSwapError(f"temporary final result directory already exists: {final_result_dir}")
                    result_dir.rename(final_result_dir)
                    safe_replace_source_dir(source_dir, final_result_dir, backup_dir, config.job.keep_backup)
                success = True
                if config.job.cleanup_remote_on_success:
                    try:
                        remove_remote_tree(sftp, remote_workdir)
                    except Exception as exc:
                        LOGGER.warning("Failed to cleanup remote working directory after success: %s", exc)
            finally:
                sftp.close()
        except Exception:
            if ssh_client is not None and config.job.cleanup_remote_on_failure:
                try:
                    sftp_fail = ssh_client.open_sftp()
                    try:
                        remove_remote_tree(sftp_fail, remote_workdir)
                    finally:
                        sftp_fail.close()
                except Exception as cleanup_exc:
                    LOGGER.warning("Failed to cleanup remote working directory after failure: %s", cleanup_exc)
            raise
        finally:
            if ssh_client is not None:
                ssh_client.close()

    return JobResult(
        job_id=job_id,
        success=success,
        remote_workdir=remote_workdir,
        local_log_dir=log_dir,
        backup_dir=backup_dir if config.job.keep_backup else None,
        stage_results=stage_results,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Copy a local folder to a remote Linux workstation, run staged commands, and safely sync results back.")
    parser.add_argument("--config", required=True, help="YAML config file path")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--username")
    parser.add_argument("--password-env")
    parser.add_argument("--key-file")
    parser.add_argument("--source-dir")
    parser.add_argument("--remote-base-dir")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without changing local or remote filesystems")
    parser.add_argument("--verbose", action="store_true", help="Enable DEBUG logging")
    parser.add_argument("--yes", action="store_true", help="Skip interactive proceed confirmation")
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
        result = run_job(config, yes=args.yes, verbose=args.verbose, auto_add_host_key=args.auto_add_host_key)
        if result.success:
            print(f"Job succeeded: {result.job_id}")
            print(f"Logs: {result.local_log_dir}")
            return 0
        print(f"Job did not run to completion: {result.job_id}", file=sys.stderr)
        print(f"Logs: {result.local_log_dir}", file=sys.stderr)
        return 2
    except RemoteJobRunnerError as exc:
        LOGGER.error("%s", exc)
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

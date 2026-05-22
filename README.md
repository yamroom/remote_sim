# remote_job_runner

`remote_job_runner` 是 Python 3.10+ CLI 工具，用來把本機 `source_dir` 的檔案上傳到遠端 Linux 工作站的新工作目錄，在遠端依 YAML 設定執行 staged commands，成功後只下載 `results.remote_paths` 指定的結果，並 merge 回本機。

## 安裝

```bash
pip install -r requirements.txt
```

如果使用 `transfer.method: "rsync"`，Windows 端還需要可用的 WSL，且 WSL 裡需要有 `rsync` 與 OpenSSH client。

## 基本使用

```bash
python remote_job_runner.py --config config.yaml
```

Dry-run 不連 SSH、不上傳、不下載、不改動檔案：

```bash
python remote_job_runner.py --config config.example.yaml --dry-run
```

若需要執行前確認：

```bash
python remote_job_runner.py --config config.yaml --confirm
```

`--yes` 只保留相容舊 CLI，目前是 no-op。

## YAML 範例

```yaml
remote:
  host: "192.168.1.100"
  port: 22
  username: "your_user"
  remote_base_dir: "/tmp/remote_job_runner"

auth:
  key_file: null
  password: null
  password_env: "WORKSTATION_PASSWORD"

job:
  source_dir: "/path/to/source"
  enable_logs: true
  show_progress: true
  cleanup_remote_on_success: true
  cleanup_remote_on_failure: false
  verify_hash: false
  skip_symlinks: false
  max_captured_output_bytes: 1048576

transfer:
  method: "sftp"
  sftp_max_workers: 4
  include_globs:
    - "**/*"
  exclude_globs:
    - ".git/**"
    - "__pycache__/**"
    - "*.tmp"
    - ".remote_job_runner_logs/**"

results:
  remote_paths:
    - "outputs/**"
    - "result.csv"
  local_base_dir: null
  allow_local_base_dir_outside_source: false
  overwrite: true
  backup_overwritten: true
  sync_mode: "merge"

stages:
  - name: "run"
    mode: "sequential"
    commands:
      - name: "run_main"
        cmd: "python test.py > /dev/null"
        timeout_sec: 3600

  - name: "parallel_cases"
    mode: "parallel"
    max_workers: 2
    commands:
      - name: "case_a"
        cmd: "python test.py --case A > /dev/null"
        timeout_sec: 3600
      - name: "case_b"
        cmd: "python test.py --case B > /dev/null"
        timeout_sec: 3600
```

## `auth`

認證優先順序：

1. `key_file`
2. `password`
3. `password_env`
4. `transfer.method: "sftp"` 且前三者都沒有時，才互動式詢問 password

`password_env` 是環境變數名稱，不是密碼本身：

```powershell
$env:WORKSTATION_PASSWORD = "12345"
python remote_job_runner.py --config config.yaml
```

不建議把真實密碼 commit 到 git。`config.resolved.yaml` 會把 `auth.password` 顯示為 `<redacted>`。

## `job`

| 欄位 | 預設 | 用途 |
|---|---:|---|
| `source_dir` | 必填 | 本機要上傳的資料夾 |
| `enable_logs` | `true` | 是否建立 `.remote_job_runner_logs` 與檔案 log |
| `show_progress` | `true` | 是否輸出帶時間戳的進度 |
| `cleanup_remote_on_success` | `true` | 成功後是否刪除遠端 working directory |
| `cleanup_remote_on_failure` | `false` | 失敗後是否刪除遠端 working directory |
| `verify_hash` | `true` | manifest 是否計算 sha256；較安全但較慢 |
| `skip_symlinks` | `false` | 是否跳過 symlink；預設遇到 symlink 會失敗 |
| `max_captured_output_bytes` | `1048576` | 每個 command stdout/stderr 在記憶體中保留的尾端 bytes |

`show_progress: true` 時，console 進度會使用 ISO-8601 UTC timestamp：

```text
[2026-05-22T12:34:56.789012+00:00] phase started: upload
[2026-05-22T12:35:20.234567+00:00] stage completed: name=run, success=true, duration_sec=18.420
```

Progress 不會顯示 SSH password。不過 command 字串仍可能寫入 command log，所以不建議在 command 裡直接放 secret。

## `transfer`

| method | 行為 |
|---|---|
| `sftp` | 使用 Paramiko SFTP。支援 SSH key、直接 password、`password_env`，也可在沒有 auth 時互動輸入 password。 |
| `rsync` | 透過 Windows 端 Python 呼叫 `wsl rsync`。支援 SSH key、直接 password、`password_env`；password 模式內部使用 SSH_ASKPASS helper。 |

SFTP 預設 sequential：

```yaml
transfer:
  method: "sftp"
  sftp_max_workers: 1
```

開啟 SFTP parallel upload：

```yaml
transfer:
  method: "sftp"
  sftp_max_workers: 4
```

建議先從 `2` 或 `4` 開始測。很多小檔案通常較有幫助；單一大檔案通常幫助有限。若遇到 `channel open failed` 或 `too many sessions`，請降低 `sftp_max_workers`。

## `results`

`results.remote_paths` 是成功後要從遠端 working directory 下載的結果清單。它必須明確指定；空 list 會被拒絕，工具不會退回下載整個 working directory。

| 欄位 | 用途 |
|---|---|
| `remote_paths` | 遠端 working directory 內要下載的相對 path / glob |
| `local_base_dir` | 結果 merge 到本機哪個資料夾；`null` 或未指定時是 resolved `job.source_dir` |
| `allow_local_base_dir_outside_source` | 是否允許寫到 `source_dir` 外，預設 `false` |
| `overwrite` | 目標檔案已存在時是否覆蓋 |
| `backup_overwritten` | 覆蓋前是否備份舊檔 |
| `sync_mode` | 目前只支援 `merge` |

## `stages`

`stages` 定義遠端 commands 的執行順序。每個 stage 都有 `name`、`mode` 與 `commands`。

`sequential` stage 會依 YAML 順序逐一執行同一 stage 內的 commands。任一 command 失敗或 timeout，就停止該 stage 的後續 commands，也不會繼續執行後續 stage。

```yaml
- name: "run"
  mode: "sequential"
  commands:
    - name: "run_main"
      cmd: "python test.py > /dev/null"
      timeout_sec: 3600
```

`parallel` stage 會平行執行同一 stage 內的 commands，並等待全部完成後再判斷 stage 成功或失敗。若任一 command 失敗或 timeout，整個 stage 視為失敗。

```yaml
- name: "parallel_cases"
  mode: "parallel"
  max_workers: 2
  commands:
    - name: "case_a"
      cmd: "python test.py --case A > /dev/null"
      timeout_sec: 3600
    - name: "case_b"
      cmd: "python test.py --case B > /dev/null"
      timeout_sec: 3600
```

`max_workers` 限制同一個 parallel stage 內最多同時執行幾個 command。若未設定，程式會使用預設值 `min(4, command_count)`。parallel command 的完成順序可能和 YAML 順序不同，但 `commands.json` 與 `StageResult.command_results` 會依 YAML command 順序整理。

請避免多個 parallel commands 同時寫入同一個檔案；這種 race condition 需要由使用者自行避免。

## Logs

`enable_logs: true` 時，每次 job 建立：

```text
<source_parent>/.remote_job_runner_logs/job_<job_id>/
```

內容包含：

- `job.log`
- `config.resolved.yaml`
- `manifest.before_upload.json`
- `manifest.after_download.json`
- `commands.json`
- `stdout_stderr/<stage>_<command>.log`
- `overwritten_backup/`，若覆蓋結果且 `backup_overwritten: true`

## Known Limitations

- `rsync` method 需要 WSL 與 WSL 內的 rsync/OpenSSH。
- `rsync` password auth 依賴 OpenSSH 的 SSH_ASKPASS 行為；若環境不支援，請改用 `sftp` 或 SSH key。
- SFTP 平行上傳受遠端 SSH server session/channel 限制。
- symlink 預設不支援；可用 `skip_symlinks: true` 跳過。
- parallel commands 若同時寫同一個檔案，使用者必須自行避免 race condition。
- 遠端 command 視為 trusted shell command；本工具只負責執行與記錄，不分析 command 本身是否危險。

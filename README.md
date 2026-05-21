# remote_job_runner

`remote_job_runner` 是一個 Python CLI 工具，用來把本機指定資料夾複製到遠端 Linux 工作站的新工作資料夾，在遠端依照設定執行 sequential / parallel stages，成功後再把遠端工作資料夾安全同步回本機原資料夾。

設計重點是安全與可診斷性：

- 遠端 command 失敗時，不覆蓋本機 `source_dir`。
- 下載結果驗證失敗時，不覆蓋本機 `source_dir`。
- 本機原始資料夾不會被直接刪除，會先 rename 成 backup。
- 預設拒絕未知 SSH host key。
- 可在 YAML 直接寫 SSH password，但不建議把含密碼的 config commit 或分享。
- password 不會寫入 resolved config；會以 `<redacted>` 顯示。
- dry-run 不連線、不上傳、不下載、不執行 command、不修改 filesystem。

## 安裝

需要 Python 3.10+。

```bash
pip install -r requirements.txt
```

## 基本使用

```bash
python remote_job_runner.py --config config.yaml
```

也可以用 CLI override 覆蓋 YAML 中的部分設定：

```bash
python remote_job_runner.py \
  --config config.yaml \
  --host 192.168.1.100 \
  --port 22 \
  --username myuser \
  --source-dir /path/to/source \
  --remote-base-dir /tmp/remote_job_runner \
  --dry-run
```

## Dry-run

dry-run 會讀取並驗證 config，掃描本機來源資料夾，列出會上傳的檔案與遠端 command 執行計畫。

dry-run 不會：

- 建立 lock file。
- 連線 SSH。
- 建立遠端資料夾。
- 上傳或下載檔案。
- 執行遠端 command。
- rename 或刪除本機資料夾。

```bash
python remote_job_runner.py --config config.example.yaml --dry-run
```

## YAML 設定檔總覽

`config.example.yaml` 是工具的主要設定檔範例，描述：

1. 要連到哪台遠端 Linux 工作站。
2. 用什麼 SSH 認證方式。
3. 要處理哪個本機資料夾。
4. 哪些檔案要上傳或排除。
5. 遠端要依序或平行執行哪些 commands。
6. 成功或失敗時如何保留備份與清理遠端資料。

完整範例：

```yaml
remote:
  host: "192.168.1.100"
  port: 22
  username: "your_user"
  remote_base_dir: "/tmp/remote_job_runner"

auth:
  key_file: "~/.ssh/id_ed25519"
  password: null
  password_env: null

job:
  source_dir: "."
  keep_backup: true
  cleanup_remote_on_success: true
  cleanup_remote_on_failure: false
  verify_hash: true
  skip_symlinks: false

transfer:
  include_globs:
    - "**/*"
  exclude_globs:
    - ".git/**"
    - "__pycache__/**"
    - "*.tmp"
    - ".remote_job_runner_logs/**"

stages:
  - name: "prepare"
    mode: "sequential"
    commands:
      - name: "show_files"
        cmd: "ls -la"
        timeout_sec: 60

  - name: "run_parallel_jobs"
    mode: "parallel"
    max_workers: 4
    commands:
      - name: "case_a"
        cmd: "python run.py --case A"
        timeout_sec: 3600
      - name: "case_b"
        cmd: "python run.py --case B"
        timeout_sec: 3600
      - name: "case_c"
        cmd: "python run.py --case C"
        timeout_sec: 3600

  - name: "postprocess"
    mode: "sequential"
    commands:
      - name: "collect"
        cmd: "python collect_results.py"
        timeout_sec: 600
```

## `remote` 區塊

```yaml
remote:
  host: "192.168.1.100"
  port: 22
  username: "your_user"
  remote_base_dir: "/tmp/remote_job_runner"
```

這段定義遠端 Linux 工作站連線資訊。

| 欄位 | 意思 | 用途 |
|---|---|---|
| `host` | 遠端主機 IP 或 hostname | SSH 連線目標，例如 `192.168.1.100` |
| `port` | SSH port | 通常是 `22` |
| `username` | SSH 使用者名稱 | 用來登入遠端工作站 |
| `remote_base_dir` | 遠端 job 根目錄 | 工具會在這下面建立唯一工作資料夾 |

實際遠端工作資料夾會長得像：

```text
/tmp/remote_job_runner/job_20260521_153000_ab12cd34
```

## `auth` 區塊

```yaml
auth:
  key_file: "~/.ssh/id_ed25519"
  password: null
  password_env: null
```

這段定義 SSH 認證方式。

| 欄位 | 意思 | 用途 |
|---|---|---|
| `key_file` | SSH private key 路徑 | 優先使用 SSH key 登入 |
| `password` | SSH 密碼本身 | 直接從 YAML 讀取密碼 |
| `password_env` | 存放密碼的環境變數名稱 | 從環境變數讀取密碼 |

認證優先順序：

1. `key_file`
2. `password`
3. `password_env`
4. 互動式 `getpass` 輸入

目前範例使用 SSH key：

```yaml
auth:
  key_file: "~/.ssh/id_ed25519"
  password: null
  password_env: null
```

如果要直接在 YAML 寫密碼：

```yaml
auth:
  key_file: null
  password: "12345"
  password_env: null
```

`password` 是真實 SSH 密碼。若密碼是純數字，建議加引號，例如 `"12345"`。如果不加引號，YAML 會把 `12345` 解析成整數；本工具會把它轉成字串，但像 `00123` 這類有前導零的密碼可能會因 YAML 解析而失真。

如果要改用密碼環境變數：

```yaml
auth:
  key_file: null
  password: null
  password_env: "WORKSTATION_PASSWORD"
```

然後在 shell 設定：

```bash
export WORKSTATION_PASSWORD="your-password"
python remote_job_runner.py --config config.yaml
```

直接把 password 寫進 YAML 比較方便，但安全性較低。config 檔可能被 commit、複製、備份或附在 bug report，造成 secret 外洩。若使用 `auth.password`，工具會在 `config.resolved.yaml` 中以 `<redacted>` 取代真實密碼，但原始 YAML 仍然含有密碼，請妥善保管。

## `job` 區塊

```yaml
job:
  source_dir: "."
  keep_backup: true
  cleanup_remote_on_success: true
  cleanup_remote_on_failure: false
  verify_hash: true
  skip_symlinks: false
```

這段定義本機 job 行為與安全策略。

| 欄位 | 意思 | 用途 |
|---|---|---|
| `source_dir` | 本機來源資料夾 | 要上傳到遠端，最後被安全替換的資料夾 |
| `keep_backup` | 是否保留本機原始資料夾備份 | 預設保留，較安全 |
| `cleanup_remote_on_success` | 成功後是否刪除遠端工作資料夾 | 預設成功後清理遠端 |
| `cleanup_remote_on_failure` | 失敗後是否刪除遠端工作資料夾 | 預設不刪，方便 debug |
| `verify_hash` | 是否計算 sha256 | 用於 manifest 與下載結果驗證 |
| `skip_symlinks` | 是否跳過 symlink | 預設不跳過，遇到 symlink 直接報錯 |

正式使用時通常會把：

```yaml
source_dir: "."
```

改成：

```yaml
source_dir: "/path/to/local/source_folder"
```

`keep_backup: true` 很重要。成功下載結果後，工具不會直接刪除原本資料夾，而是先 rename 成 backup，再把 result rename 回原本路徑。

## `transfer` 區塊

```yaml
transfer:
  include_globs:
    - "**/*"
  exclude_globs:
    - ".git/**"
    - "__pycache__/**"
    - "*.tmp"
    - ".remote_job_runner_logs/**"
```

這段定義哪些檔案會被上傳。

`include_globs` 是納入規則：

```yaml
include_globs:
  - "**/*"
```

代表遞迴包含所有檔案與子資料夾。

`exclude_globs` 是排除規則，而且優先於 include：

| 規則 | 用途 |
|---|---|
| `.git/**` | 不上傳 git metadata |
| `__pycache__/**` | 不上傳 Python bytecode cache |
| `*.tmp` | 不上傳暫存檔 |
| `.remote_job_runner_logs/**` | 不上傳本工具自己的 log |

如果只想上傳 Python 與資料檔，可改成：

```yaml
transfer:
  include_globs:
    - "**/*.py"
    - "**/*.csv"
  exclude_globs:
    - ".git/**"
    - "__pycache__/**"
```

## `stages` 區塊

`stages` 是遠端 commands 的執行流程。每個 stage 有：

| 欄位 | 意思 |
|---|---|
| `name` | stage 名稱，用於 log 與結果識別 |
| `mode` | 執行模式，只能是 `sequential` 或 `parallel` |
| `commands` | 此 stage 內要執行的 command 清單 |

### Sequential stage

```yaml
- name: "prepare"
  mode: "sequential"
  commands:
    - name: "show_files"
      cmd: "ls -la"
      timeout_sec: 60
```

這代表準備階段，commands 依照 YAML 順序執行。

這個 command 會在遠端 working directory 裡執行：

```bash
ls -la
```

最多允許 60 秒。若 command 失敗或 timeout，這個 stage 失敗，後續 commands 與後續 stages 都不會執行。

### Parallel stage

```yaml
- name: "run_parallel_jobs"
  mode: "parallel"
  max_workers: 4
  commands:
    - name: "case_a"
      cmd: "python run.py --case A"
      timeout_sec: 3600
    - name: "case_b"
      cmd: "python run.py --case B"
      timeout_sec: 3600
    - name: "case_c"
      cmd: "python run.py --case C"
      timeout_sec: 3600
```

這段代表同一 stage 內的 commands 平行執行。

| 欄位 | 意思 |
|---|---|
| `mode: "parallel"` | 同一 stage 的 commands 平行跑 |
| `max_workers: 4` | 最多同時跑 4 個 command |
| `timeout_sec: 3600` | 每個 command 最多跑 3600 秒 |

這三個 command 會在遠端 working directory 平行執行：

```bash
python run.py --case A
python run.py --case B
python run.py --case C
```

如果其中任何一個失敗或 timeout，整個 stage 會失敗。工具會等待同 stage 內已啟動的 commands 結束，然後停止後續 stage，不會下載結果，也不會替換本機 `source_dir`。

### Postprocess stage

```yaml
- name: "postprocess"
  mode: "sequential"
  commands:
    - name: "collect"
      cmd: "python collect_results.py"
      timeout_sec: 600
```

這段是後處理階段。因為是 `sequential`，它會在前面的 stages 全部成功後才執行。

常見用途：

- 合併結果。
- 產生報表。
- 整理輸出檔案。
- 清理遠端 working directory 內的中間產物。

## YAML 範例的完整執行流程

依照 `config.example.yaml`，工具會：

1. 掃描本機 `source_dir`。
2. 排除 `.git/**`、`__pycache__/**`、`*.tmp`、`.remote_job_runner_logs/**`。
3. 依照 auth 優先順序使用 SSH key、YAML password、password_env 或互動式輸入連到 `your_user@192.168.1.100:22`。
4. 在 `/tmp/remote_job_runner` 下建立唯一 job 目錄。
5. 上傳符合規則的檔案。
6. 遠端依序執行 `ls -la`。
7. 遠端平行執行 `python run.py --case A`、`python run.py --case B`、`python run.py --case C`。
8. 如果前面都成功，遠端執行 `python collect_results.py`。
9. 全部成功後下載遠端 working directory。
10. 驗證下載結果。
11. 安全替換本機 `source_dir`。
12. 保留本機 backup。
13. 成功後清理遠端 working directory。

若任何 command 失敗，工具不會覆蓋本機 `source_dir`。

## 必填欄位與預設值

必填欄位：

- `remote.host`
- `remote.username`
- `remote.remote_base_dir`
- `job.source_dir`
- 至少一個 stage
- 每個 stage 至少一個 command
- 每個 command 必須有 `name` 與 `cmd`

預設值：

- `remote.port`: `22`
- `job.keep_backup`: `true`
- `job.cleanup_remote_on_success`: `true`
- `job.cleanup_remote_on_failure`: `false`
- `job.verify_hash`: `true`
- `job.skip_symlinks`: `false`
- `transfer.include_globs`: `["**/*"]`
- `transfer.exclude_globs`: `[]`
- command `timeout_sec`: `3600`
- parallel stage `max_workers`: `min(4, command_count)`

## Host key policy

預設行為：

1. 呼叫 `client.load_system_host_keys()`。
2. 使用 Paramiko `RejectPolicy`。
3. 未知 host key 會被拒絕。

只有明確指定以下參數時，才會自動信任未知 host key：

```bash
python remote_job_runner.py --config config.yaml --auto-add-host-key
```

`--auto-add-host-key` 會降低 SSH 安全性，因為第一次連線到惡意主機時也可能自動信任對方。只應在你完全理解風險且環境受控時使用。

## Safe swap

所有遠端 stages 成功後，工具才會下載結果並進入 safe swap。

流程：

1. 下載遠端 working directory 到本機臨時 result directory。
2. 建立 `manifest.after_download.json`。
3. 將原本 `source_dir` rename 成 backup directory。
4. 將 result directory rename 成原本的 `source_dir` path。
5. 如果第 4 步失敗，嘗試 rollback：把 backup rename 回 `source_dir`。
6. `keep_backup: true` 時保留 backup。
7. `keep_backup: false` 時，成功替換後才刪除 backup。

關閉 backup 保留：

```bash
python remote_job_runner.py --config config.yaml --no-keep-backup
```

## 失敗時如何恢復

如果 remote command、transfer、verification 或 safe swap 失敗，本機 `source_dir` 不會被覆蓋。

如果 job 成功，但你想恢復舊內容，先確認沒有其他 job 正在操作同一資料夾，再把 backup rename 回來。

Linux / macOS 範例：

```bash
mv /path/to/source /path/to/source.failed_or_unwanted
mv /path/to/source.backup_job_id /path/to/source
```

Windows 請使用 PowerShell 或檔案總管做等價 rename。

## Logs

每次實際執行 job 會建立：

```text
<source_parent>/.remote_job_runner_logs/job_<job_id>/
```

內容：

- `job.log`: 高層流程 log。
- `config.resolved.yaml`: resolved config；若 YAML 有 `auth.password`，會以 `<redacted>` 顯示。
- `manifest.before_upload.json`: 上傳前本機 manifest。
- `manifest.after_download.json`: 下載後 result manifest。
- `commands.json`: command start/end/duration/exit code/timeout 狀態。
- `stdout_stderr/<stage>_<command>.log`: 每個 command 的 stdout 與 stderr。

## Cleanup

成功時：

- 如果 `job.cleanup_remote_on_success: true`，工具會刪除遠端 working directory。
- 如果遠端 cleanup 失敗，job 仍視為成功，但會在 log 中記錄 warning。

失敗時：

- 預設 `job.cleanup_remote_on_failure: false`。
- 不覆蓋本機 `source_dir`。
- 不刪除本機原始資料。
- 保留遠端 working directory 供 debug。
- 如果設定 `job.cleanup_remote_on_failure: true`，失敗後也會嘗試刪除遠端 working directory。

## Known limitations

- 第一版使用 recursive SFTP transfer，不是 rsync。
- 不支援 interrupted upload resume。
- symlink 預設不支援；可設定 `skip_symlinks: true` 跳過。
- parallel commands 如果同時寫同一個檔案，使用者必須自行避免 race condition。
- 遠端 command 是 trusted shell command，本工具只負責執行與記錄，不分析 command 本身是否危險。

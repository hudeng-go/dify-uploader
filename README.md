# Dify Knowledge Base Sync Tool

将 Git 仓库中的文档文件同步到 Dify 知识库的命令行工具。

## 功能特性

- 支持全量同步和增量同步两种模式
- 基于 Git commit hash 的增量同步，自动追踪已同步状态
- 自动拉取最新代码，保持本地与远程分支同步
- 支持文件的新增、更新、删除操作
- 灵活的文件过滤配置（扩展名、目录、排除规则）
- 支持自定义索引和处理规则

## 安装

```bash
pip install -r requirements.txt
```

## 配置

复制配置文件示例：

```bash
cp config.yaml.example config.yaml
```

编辑 `config.yaml` 填写必要配置：

```yaml
dify:
  api_url: "https://your-dify-instance/v1"
  api_key: "your-api-key"
  dataset_id: "your-dataset-id"

git:
  repo_path: "/path/to/your/wiki"
  remote_branch: "origin/main"
  local_branch: "main"
```

## 使用方法

### 增量同步（默认）

```bash
python upload_to_dify.py -c config.yaml
```

增量同步会：
1. 自动拉取最新代码（如果本地落后远程）
2. 根据配置文件中的 `last_synced_commit` 判断同步范围
3. 如果 `last_synced_commit` 为空，执行全量同步
4. 同步成功后自动更新配置文件中的 commit hash

### 全量同步

```bash
python upload_to_dify.py -c config.yaml --mode full
```

### 预览模式

```bash
python upload_to_dify.py -c config.yaml --dry-run
```

显示将要执行的操作，不实际执行。

### 命令行参数

| 参数 | 说明 |
|------|------|
| `-c, --config` | 配置文件路径（默认：config.yaml）|
| `--mode` | 同步模式：`full`（全量）或 `incremental`（增量）|
| `--dry-run` | 预览模式，不执行实际操作 |
| `--api-url` | 覆盖 Dify API URL |
| `--api-key` | 覆盖 Dify API Key |
| `--dataset-id` | 覆盖 Dify 数据集 ID |
| `--repo-path` | 覆盖 Git 仓库路径 |

## 环境变量

配置项可通过环境变量覆盖：

| 环境变量 | 对应配置项 |
|----------|-----------|
| `DIFY_API_URL` | dify.api_url |
| `DIFY_API_KEY` | dify.api_key |
| `DIFY_DATASET_ID` | dify.dataset_id |
| `GIT_REPO_PATH` | git.repo_path |
| `GIT_REMOTE_BRANCH` | git.remote_branch |
| `UPLOAD_MODE` | upload.mode |

## 配置文件说明

### Dify 配置

| 字段 | 说明 |
|------|------|
| `api_url` | Dify API 基础 URL |
| `api_key` | Dify API Key |
| `dataset_id` | 知识库/数据集 ID |

### Git 配置

| 字段 | 说明 |
|------|------|
| `repo_path` | Git 仓库本地路径 |
| `remote_branch` | 远程分支名称（如 `origin/main`）|
| `local_branch` | 本地分支名称（可选，默认当前分支）|
| `last_synced_commit` | 上次同步的 commit hash（为空时全量同步）|

### 文件过滤配置

| 字段 | 说明 |
|------|------|
| `extensions` | 文件扩展名列表（如 `["*.md", "*.txt"]`）|
| `exclude_patterns` | 排除的文件/目录模式 |
| `include_dirs` | 包含的目录（空列表表示整个仓库）|
| `exclude_dirs` | 排除的目录 |

### 上传配置

| 字段 | 说明 |
|------|------|
| `mode` | 上传模式：`full` 或 `incremental` |
| `indexing_technique` | 索引技术：`high_quality` 或 `economy` |
| `process_rule.mode` | 处理规则：`automatic` 或 `custom` |

## 同步流程

```
┌─────────────────────────────────────────────────────────────┐
│                      增量同步流程                             │
├─────────────────────────────────────────────────────────────┤
│  1. git fetch                                               │
│  2. 检查 local_hash == remote_hash?                         │
│     ├── 否 → git pull 拉取最新代码                           │
│     └── 是 → 继续                                           │
│  3. last_synced_commit 为空?                                │
│     ├── 是 → 全量同步所有文件                                │
│     └── 否 → 增量同步（计算 commit 差异的文件变更）           │
│  4. 执行文件操作（上传/更新/删除）                            │
│  5. 全部成功 → 更新配置文件中的 last_synced_commit           │
└─────────────────────────────────────────────────────────────┘
```

## 日志级别

配置文件中的 `logging.level` 支持以下值：
- `DEBUG` - 详细调试信息（包括 JSON 输出）
- `INFO` - 常规操作信息（默认）
- `WARNING` - 警告信息
- `ERROR` - 错误信息


#!/usr/bin/env python3
"""Dify Knowledge Base File Uploader

Upload files from a Git repository to Dify knowledge base.
Supports both full upload and incremental upload modes.
Supports create, update, and delete operations based on git change types.
"""

import argparse
import fnmatch
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import requests
import yaml


class ChangeType(Enum):
    ADDED = "A"
    MODIFIED = "M"
    DELETED = "D"
    RENAMED = "R"


@dataclass(frozen=True)
class FileChange:
    path: Path
    change_type: ChangeType
    old_path: Optional[Path] = None


@dataclass
class Config:
    dify_api_base_url: str = "https://api.dify.ai/v1"
    dify_api_key: str = ""
    dify_dataset_id: str = ""
    git_repo_path: str = ""
    git_remote_branch: str = "origin/main"
    git_local_branch: str = ""
    last_synced_commit: str = ""
    config_file_path: str = ""
    file_extensions: list = field(default_factory=lambda: ["*.md", "*.txt"])
    exclude_patterns: list = field(default_factory=list)
    include_dirs: list = field(default_factory=list)
    exclude_dirs: list = field(default_factory=lambda: [".git"])
    upload_mode: str = "incremental"
    indexing_technique: str = "high_quality"
    process_rule_mode: str = "automatic"
    process_rule_rules: dict = field(default_factory=dict)
    summary_index_setting: dict = field(default_factory=dict)
    log_level: str = "INFO"
    log_file: str = ""

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "Config":
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        config = cls()
        config.config_file_path = yaml_path

        dify_cfg = data.get("dify", {})
        config.dify_api_base_url = dify_cfg.get("api_url", config.dify_api_base_url)
        config.dify_api_key = dify_cfg.get("api_key", "")
        config.dify_dataset_id = dify_cfg.get("dataset_id", "")

        git_cfg = data.get("git", {})
        config.git_repo_path = git_cfg.get("repo_path", "")
        config.git_remote_branch = git_cfg.get("remote_branch", "origin/main")
        config.git_local_branch = git_cfg.get("local_branch", "")
        config.last_synced_commit = git_cfg.get("last_synced_commit", "")

        filter_cfg = data.get("file_filter", {})
        config.file_extensions = filter_cfg.get("extensions", ["*.md", "*.txt"])
        config.exclude_patterns = filter_cfg.get("exclude_patterns", [])
        config.include_dirs = filter_cfg.get("include_dirs", [])
        config.exclude_dirs = filter_cfg.get("exclude_dirs", [".git"])

        upload_cfg = data.get("upload", {})
        config.upload_mode = upload_cfg.get("mode", "incremental")
        config.indexing_technique = upload_cfg.get("indexing_technique", "high_quality")
        process_rule = upload_cfg.get("process_rule", {})
        config.process_rule_mode = process_rule.get("mode", "automatic")
        config.process_rule_rules = process_rule.get("rules", {})
        config.summary_index_setting = upload_cfg.get("summary_index_setting", {})

        log_cfg = data.get("logging", {})
        config.log_level = log_cfg.get("level", "INFO")
        config.log_file = log_cfg.get("file", "")

        return config

    def apply_env_overrides(self):
        env_mappings = {
            "DIFY_API_URL": "dify_api_base_url",
            "DIFY_API_KEY": "dify_api_key",
            "DIFY_DATASET_ID": "dify_dataset_id",
            "GIT_REPO_PATH": "git_repo_path",
            "GIT_REMOTE_BRANCH": "git_remote_branch",
            "UPLOAD_MODE": "upload_mode",
        }
        for env_var, attr in env_mappings.items():
            value = os.environ.get(env_var)
            if value:
                setattr(self, attr, value)


class DifyUploader:
    def __init__(self, config: Config):
        self.config = config
        self.logger = self._setup_logger()
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"Bearer {config.dify_api_key}"}
        )
        self._document_cache: dict[str, dict] = {}

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger("dify_uploader")
        logger.setLevel(getattr(logging, self.config.log_level.upper(), logging.INFO))

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if self.config.log_file:
            file_handler = logging.FileHandler(self.config.log_file, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    def _is_git_repo(self) -> bool:
        repo_path = Path(self.config.git_repo_path)
        git_dir = repo_path / ".git"
        return git_dir.exists()

    def _run_git_command(self, args: list, cwd: Optional[str] = None) -> str:
        repo_path = cwd or self.config.git_repo_path
        result = subprocess.run(
            ["git"] + args,
            cwd=repo_path,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.logger.warning(f"Git command failed: git {' '.join(args)}")
            self.logger.warning(f"Error: {result.stderr}")
            return ""
        return result.stdout.strip()

    def _get_current_commit_hash(self) -> str:
        return self._run_git_command(["rev-parse", "HEAD"])

    def _check_branch_sync(self) -> tuple[bool, str]:
        self._run_git_command(["fetch"])
        local_branch = self.config.git_local_branch or self._run_git_command(
            ["rev-parse", "--abbrev-ref", "HEAD"]
        )
        remote_branch = self.config.git_remote_branch

        local_hash = self._run_git_command(["rev-parse", local_branch])
        remote_hash = self._run_git_command(["rev-parse", remote_branch])

        if not local_hash or not remote_hash:
            return False, "Cannot determine branch hashes"

        if local_hash != remote_hash:
            self.logger.info(f"Local branch behind remote, pulling latest changes...")
            pull_result = self._run_git_command(["pull"])
            if not pull_result and pull_result != "":
                return False, "Failed to pull latest changes"
            
            local_hash = self._run_git_command(["rev-parse", local_branch])
            if not local_hash:
                return False, "Cannot determine local hash after pull"
            self.logger.info(f"Pulled successfully, now at commit {local_hash[:8]}")

        return True, local_hash

    def _get_changed_files_since_commit(self, from_commit: str) -> list[FileChange]:
        if not from_commit:
            return self._get_all_files()

        diff_output = self._run_git_command(
            ["diff", "--name-status", f"{from_commit}..HEAD"]
        )

        if not diff_output:
            self.logger.info(f"No changed files since commit {from_commit[:8]}")
            return []

        repo_path = Path(self.config.git_repo_path)
        changed_files = []

        for line in diff_output.split("\n"):
            if not line.strip():
                continue

            parts = line.strip().split("\t")
            if not parts:
                continue

            status = parts[0][0]

            if status == "R":
                old_path = repo_path / parts[1]
                new_path = repo_path / parts[2]
                old_name = old_path.name
                new_name = new_path.name

                if old_name != new_name:
                    if self._should_include_file_by_name(old_name):
                        changed_files.append(FileChange(
                            path=old_path,
                            change_type=ChangeType.DELETED
                        ))
                    if new_path.exists() and self._should_include_file(new_path, repo_path):
                        changed_files.append(FileChange(
                            path=new_path,
                            change_type=ChangeType.ADDED
                        ))
            elif status == "D":
                file_path = repo_path / parts[1]
                if self._should_include_file(file_path, repo_path):
                    changed_files.append(FileChange(
                        path=file_path,
                        change_type=ChangeType.DELETED
                    ))
            elif status in ("A", "M"):
                file_path = repo_path / parts[1]
                if file_path.exists() and self._should_include_file(file_path, repo_path):
                    change_type = ChangeType.ADDED if status == "A" else ChangeType.MODIFIED
                    changed_files.append(FileChange(
                        path=file_path,
                        change_type=change_type
                    ))

        return changed_files

    def _update_config_commit_hash(self, commit_hash: str):
        if not self.config.config_file_path:
            self.logger.warning("No config file path set, cannot update commit hash")
            return

        config_path = Path(self.config.config_file_path)
        if not config_path.exists():
            self.logger.warning(f"Config file not found: {config_path}")
            return

        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        if "git" not in data:
            data["git"] = {}
        data["git"]["last_synced_commit"] = commit_hash

        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        self.logger.info(f"Updated last_synced_commit to {commit_hash[:8]}")

    def _get_all_files(self) -> list[FileChange]:
        repo_path = Path(self.config.git_repo_path)
        all_files = []

        for pattern in self.config.file_extensions:
            if pattern == "*":
                for root, dirs, files in os.walk(repo_path):
                    dirs[:] = [d for d in dirs if d not in self.config.exclude_dirs]
                    for f in files:
                        file_path = Path(root) / f
                        if self._should_include_file(file_path, repo_path):
                            all_files.append(FileChange(path=file_path, change_type=ChangeType.ADDED))
            else:
                for file_path in repo_path.rglob(pattern):
                    if self._should_include_file(file_path, repo_path):
                        all_files.append(FileChange(path=file_path, change_type=ChangeType.ADDED))

        return sorted(set(all_files), key=lambda x: str(x.path))

    def _get_changed_files(self) -> list[FileChange]:
        self._run_git_command(["fetch"])
        local_branch = self.config.git_local_branch or self._run_git_command(
            ["rev-parse", "--abbrev-ref", "HEAD"]
        )
        remote_branch = self.config.git_remote_branch

        diff_output = self._run_git_command(
            ["diff", "--name-status", f"{remote_branch}...{local_branch}"]
        )

        if not diff_output:
            self.logger.info("No changed files detected")
            return []

        repo_path = Path(self.config.git_repo_path)
        changed_files = []

        for line in diff_output.split("\n"):
            if not line.strip():
                continue

            parts = line.strip().split("\t")
            if not parts:
                continue

            status = parts[0][0]

            if status == "R":
                old_path = repo_path / parts[1]
                new_path = repo_path / parts[2]
                old_name = old_path.name
                new_name = new_path.name

                if old_name != new_name:
                    if self._should_include_file_by_name(old_name):
                        changed_files.append(FileChange(
                            path=old_path,
                            change_type=ChangeType.DELETED
                        ))
                    if new_path.exists() and self._should_include_file(new_path, repo_path):
                        changed_files.append(FileChange(
                            path=new_path,
                            change_type=ChangeType.ADDED
                        ))
            elif status == "D":
                file_path = repo_path / parts[1]
                if self._should_include_file(file_path, repo_path):
                    changed_files.append(FileChange(
                        path=file_path,
                        change_type=ChangeType.DELETED
                    ))
            elif status in ("A", "M"):
                file_path = repo_path / parts[1]
                if file_path.exists() and self._should_include_file(file_path, repo_path):
                    change_type = ChangeType.ADDED if status == "A" else ChangeType.MODIFIED
                    changed_files.append(FileChange(
                        path=file_path,
                        change_type=change_type
                    ))

        return changed_files

    def _should_include_file(self, file_path: Path, repo_path: Path) -> bool:
        relative_path = file_path.relative_to(repo_path)

        for exclude_dir in self.config.exclude_dirs:
            if exclude_dir in relative_path.parts:
                return False

        for pattern in self.config.exclude_patterns:
            pattern_path = repo_path / pattern.replace("/**", "")
            if fnmatch.fnmatch(str(file_path), str(pattern_path)) or fnmatch.fnmatch(
                str(relative_path), pattern
            ):
                return False

        if self.config.include_dirs:
            included = False
            for include_dir in self.config.include_dirs:
                if include_dir in relative_path.parts or str(relative_path).startswith(
                    include_dir
                ):
                    included = True
                    break
            if not included:
                return False

        for pattern in self.config.file_extensions:
            if pattern == "*":
                return True
            ext = pattern.lstrip("*")
            if str(file_path).endswith(ext) or fnmatch.fnmatch(file_path.name, pattern):
                return True

        return False

    def _should_include_file_by_name(self, file_name: str) -> bool:
        for pattern in self.config.file_extensions:
            if pattern == "*":
                return True
            ext = pattern.lstrip("*")
            if file_name.endswith(ext) or fnmatch.fnmatch(file_name, pattern):
                return True
        return False

    def _build_upload_data(self, file_path: Path) -> str:
        data = {
            "indexing_technique": self.config.indexing_technique,
            "process_rule": {"mode": self.config.process_rule_mode},
            "name": file_path.name,
        }

        if self.config.process_rule_mode == "custom" and self.config.process_rule_rules:
            data["process_rule"]["rules"] = self.config.process_rule_rules

        if self.config.summary_index_setting:
            data["summary_index_setting"] = self.config.summary_index_setting

        return json.dumps(data)

    def _get_documents_list(self, keyword: Optional[str] = None) -> list[dict]:
        url = f"{self.config.dify_api_base_url.rstrip('/')}/datasets/{self.config.dify_dataset_id}/documents"
        params: dict[str, int | str] = {"limit": 100}
        if keyword:
            params["keyword"] = keyword

        response = self.session.get(url, params=params)
        if response.status_code == 200:
            return response.json().get("data", [])
        return []

    def _find_document_by_name(self, file_name: str) -> Optional[dict]:
        if file_name in self._document_cache:
            return self._document_cache[file_name]

        documents = self._get_documents_list(keyword=file_name)
        for doc in documents:
            if doc.get("name") == file_name:
                self._document_cache[file_name] = doc
                return doc
        return None

    def upload_file(self, file_path: Path) -> dict:
        url = f"{self.config.dify_api_base_url.rstrip('/')}/datasets/{self.config.dify_dataset_id}/document/create-by-file"

        data_str = self._build_upload_data(file_path)

        with open(file_path, "rb") as f:
            files = {"file": (file_path.name, f)}
            data = {"data": data_str}

            self.logger.info(f"Uploading: {file_path}")
            response = self.session.post(url, files=files, data=data)

        if response.status_code == 200:
            self.logger.info(f"Successfully uploaded: {file_path.name}")
            doc = response.json().get("document", {})
            if doc.get("name"):
                self._document_cache[doc["name"]] = doc
            return response.json()
        else:
            self.logger.error(
                f"Failed to upload {file_path.name}: {response.status_code} - {response.text}"
            )
            return {"error": response.text, "status_code": response.status_code}

    def update_file(self, file_path: Path) -> dict:
        existing_doc = self._find_document_by_name(file_path.name)
        if not existing_doc:
            self.logger.info(f"Document not found for update, creating new: {file_path.name}")
            return self.upload_file(file_path)

        document_id = existing_doc.get("id")
        url = f"{self.config.dify_api_base_url.rstrip('/')}/datasets/{self.config.dify_dataset_id}/documents/{document_id}/update-by-file"

        data_str = self._build_upload_data(file_path)

        with open(file_path, "rb") as f:
            files = {"file": (file_path.name, f)}
            data = {"data": data_str}

            self.logger.info(f"Updating: {file_path}")
            response = self.session.post(url, files=files, data=data)

        if response.status_code == 200:
            self.logger.info(f"Successfully updated: {file_path.name}")
            doc = response.json().get("document", {})
            if doc.get("name"):
                self._document_cache[doc["name"]] = doc
            return response.json()
        else:
            self.logger.error(
                f"Failed to update {file_path.name}: {response.status_code} - {response.text}"
            )
            return {"error": response.text, "status_code": response.status_code}

    def delete_document(self, file_path: Path) -> dict:
        existing_doc = self._find_document_by_name(file_path.name)
        if not existing_doc:
            self.logger.warning(f"Document not found for deletion: {file_path.name}")
            return {"warning": f"Document not found: {file_path.name}", "skipped": True}

        document_id = existing_doc.get("id")
        url = f"{self.config.dify_api_base_url.rstrip('/')}/datasets/{self.config.dify_dataset_id}/documents/{document_id}"

        self.logger.info(f"Deleting: {file_path.name} (document_id: {document_id})")
        response = self.session.delete(url)

        if response.status_code in (200, 204):
            self.logger.info(f"Successfully deleted: {file_path.name}")
            if file_path.name in self._document_cache:
                del self._document_cache[file_path.name]
            return {"success": True, "document_id": document_id}
        else:
            self.logger.error(
                f"Failed to delete {file_path.name}: {response.status_code} - {response.text}"
            )
            return {"error": response.text, "status_code": response.status_code}

    def run(self, dry_run: bool = False) -> dict:
        if not self.config.dify_api_key:
            raise ValueError("DIFY_API_KEY is required")
        if not self.config.dify_dataset_id:
            raise ValueError("DIFY_DATASET_ID is required")
        if not self.config.git_repo_path:
            raise ValueError("Directory path is required")

        repo_path = Path(self.config.git_repo_path)
        if not repo_path.exists():
            raise FileNotFoundError(f"Directory path not found: {repo_path}")

        is_git_repo = self._is_git_repo()
        current_commit = ""
        should_update_commit = False

        if not is_git_repo:
            changes = self._get_all_files()
            self.logger.info(f"Not a git repository: found {len(changes)} files to upload")
        elif self.config.upload_mode == "full":
            changes = self._get_all_files()
            self.logger.info(f"Full upload mode: found {len(changes)} files")
            if is_git_repo:
                current_commit = self._get_current_commit_hash()
                should_update_commit = True
        else:
            in_sync, sync_result = self._check_branch_sync()
            if not in_sync:
                raise ValueError(f"Branch sync check failed: {sync_result}")

            current_commit = sync_result
            self.logger.info(f"Branches in sync at commit {current_commit[:8]}")

            if not self.config.last_synced_commit:
                self.logger.info("No last_synced_commit found, performing full sync")
                changes = self._get_all_files()
            else:
                self.logger.info(f"Last synced commit: {self.config.last_synced_commit[:8]}")
                changes = self._get_changed_files_since_commit(self.config.last_synced_commit)

            should_update_commit = True
            self.logger.info(f"Incremental upload mode: found {len(changes)} changed files")

        if dry_run:
            self.logger.info("Dry run mode - operations that would be performed:")
            for change in changes:
                op = {
                    ChangeType.ADDED: "UPLOAD (new)",
                    ChangeType.MODIFIED: "UPDATE",
                    ChangeType.DELETED: "DELETE"
                }.get(change.change_type, "UNKNOWN")
                self.logger.info(f"  [{op}] {change.path}")
            return {
                "dry_run": True,
                "files_count": len(changes),
                "files": [{"path": str(c.path), "action": c.change_type.value} for c in changes]
            }

        results: dict[str, Any] = {
            "uploaded": [],
            "updated": [],
            "deleted": [],
            "failed": [],
            "skipped": []
        }

        for change in changes:
            if change.change_type == ChangeType.ADDED:
                result = self.upload_file(change.path)
                if "error" in result:
                    results["failed"].append({
                        "action": "upload",
                        "file": str(change.path),
                        "error": result["error"]
                    })
                else:
                    results["uploaded"].append({"file": str(change.path), "response": result})

            elif change.change_type == ChangeType.MODIFIED:
                result = self.update_file(change.path)
                if "error" in result:
                    results["failed"].append({
                        "action": "update",
                        "file": str(change.path),
                        "error": result["error"]
                    })
                else:
                    results["updated"].append({"file": str(change.path), "response": result})

            elif change.change_type == ChangeType.DELETED:
                result = self.delete_document(change.path)
                if "error" in result:
                    results["failed"].append({
                        "action": "delete",
                        "file": str(change.path),
                        "error": result["error"]
                    })
                elif result.get("skipped"):
                    results["skipped"].append({
                        "action": "delete",
                        "file": str(change.path),
                        "reason": result.get("warning", "Document not found")
                    })
                else:
                    results["deleted"].append({"file": str(change.path), "document_id": result.get("document_id")})

        summary = {
            "uploaded": len(results["uploaded"]),
            "updated": len(results["updated"]),
            "deleted": len(results["deleted"]),
            "failed": len(results["failed"]),
            "skipped": len(results["skipped"])
        }
        self.logger.info(
            f"Upload complete: {summary['uploaded']} uploaded, {summary['updated']} updated, "
            f"{summary['deleted']} deleted, {summary['failed']} failed, {summary['skipped']} skipped"
        )
        results["summary"] = summary

        if should_update_commit and current_commit and results["failed"].__len__() == 0:
            self._update_config_commit_hash(current_commit)
            results["synced_commit"] = current_commit

        return results


def main():
    parser = argparse.ArgumentParser(
        description="Upload files from a Git repository to Dify knowledge base"
    )
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        help="Override upload mode from config",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show files that would be uploaded without actually uploading",
    )
    parser.add_argument(
        "--api-url",
        help="Dify API base URL (overrides config and env)",
    )
    parser.add_argument(
        "--api-key",
        help="Dify API key (overrides config and env)",
    )
    parser.add_argument(
        "--dataset-id",
        help="Dify dataset ID (overrides config and env)",
    )
    parser.add_argument(
        "--repo-path",
        help="Git repository path (overrides config and env)",
    )

    args = parser.parse_args()

    config_path = Path(args.config)
    if config_path.exists():
        config = Config.from_yaml(str(config_path))
    else:
        config = Config()
        logging.warning(f"Config file not found: {config_path}, using defaults and environment")

    config.apply_env_overrides()

    if args.mode:
        config.upload_mode = args.mode
    if args.api_url:
        config.dify_api_base_url = args.api_url
    if args.api_key:
        config.dify_api_key = args.api_key
    if args.dataset_id:
        config.dify_dataset_id = args.dataset_id
    if args.repo_path:
        config.git_repo_path = args.repo_path

    try:
        uploader = DifyUploader(config)
        results = uploader.run(dry_run=args.dry_run)
        uploader.logger.debug(json.dumps(results, indent=2, ensure_ascii=False))
    except Exception as e:
        logging.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
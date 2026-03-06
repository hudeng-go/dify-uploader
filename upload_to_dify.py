#!/usr/bin/env python3
"""Dify Knowledge Base File Uploader

Upload files from a Git repository to Dify knowledge base.
Supports both full upload and incremental upload modes.
"""

import argparse
import fnmatch
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
import yaml


@dataclass
class Config:
    dify_api_base_url: str = "https://api.dify.ai/v1"
    dify_api_key: str = ""
    dify_dataset_id: str = ""
    git_repo_path: str = ""
    git_remote_branch: str = "origin/main"
    git_local_branch: str = ""
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

        dify_cfg = data.get("dify", {})
        config.dify_api_base_url = dify_cfg.get("api_url", config.dify_api_base_url)
        config.dify_api_key = dify_cfg.get("api_key", "")
        config.dify_dataset_id = dify_cfg.get("dataset_id", "")

        git_cfg = data.get("git", {})
        config.git_repo_path = git_cfg.get("repo_path", "")
        config.git_remote_branch = git_cfg.get("remote_branch", "origin/main")
        config.git_local_branch = git_cfg.get("local_branch", "")

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

    def _get_all_files(self) -> list[Path]:
        repo_path = Path(self.config.git_repo_path)
        all_files = []

        for pattern in self.config.file_extensions:
            if pattern == "*":
                for root, dirs, files in os.walk(repo_path):
                    dirs[:] = [d for d in dirs if d not in self.config.exclude_dirs]
                    for f in files:
                        file_path = Path(root) / f
                        if self._should_include_file(file_path, repo_path):
                            all_files.append(file_path)
            else:
                for file_path in repo_path.rglob(pattern):
                    if self._should_include_file(file_path, repo_path):
                        all_files.append(file_path)

        return sorted(set(all_files))

    def _get_changed_files(self) -> list[Path]:
        self._run_git_command(["fetch"])
        local_branch = self.config.git_local_branch or self._run_git_command(
            ["rev-parse", "--abbrev-ref", "HEAD"]
        )
        remote_branch = self.config.git_remote_branch

        diff_output = self._run_git_command(
            ["diff", "--name-only", f"{remote_branch}...{local_branch}"]
        )

        if not diff_output:
            self.logger.info("No changed files detected")
            return []

        repo_path = Path(self.config.git_repo_path)
        changed_files = []

        for line in diff_output.split("\n"):
            if not line.strip():
                continue
            file_path = repo_path / line.strip()
            if file_path.exists() and self._should_include_file(file_path, repo_path):
                changed_files.append(file_path)

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
            return response.json()
        else:
            self.logger.error(
                f"Failed to upload {file_path.name}: {response.status_code} - {response.text}"
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

        if not is_git_repo:
            files = self._get_all_files()
            self.logger.info(f"Not a git repository: found {len(files)} files to upload")
        elif self.config.upload_mode == "full":
            files = self._get_all_files()
            self.logger.info(f"Full upload mode: found {len(files)} files")
        else:
            files = self._get_changed_files()
            self.logger.info(f"Incremental upload mode: found {len(files)} changed files")

        if dry_run:
            self.logger.info("Dry run mode - files that would be uploaded:")
            for f in files:
                self.logger.info(f"  - {f}")
            return {"dry_run": True, "files_count": len(files), "files": [str(f) for f in files]}

        results = {"success": [], "failed": []}
        for file_path in files:
            result = self.upload_file(file_path)
            if "error" in result:
                results["failed"].append({"file": str(file_path), "error": result["error"]})
            else:
                results["success"].append({"file": str(file_path), "response": result})

        self.logger.info(
            f"Upload complete: {len(results['success'])} succeeded, {len(results['failed'])} failed"
        )
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
        print(json.dumps(results, indent=2, ensure_ascii=False))
    except Exception as e:
        logging.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
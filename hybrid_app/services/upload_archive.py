from __future__ import annotations

import os
import shutil
import tarfile
import tempfile
import uuid
import zipfile
from pathlib import Path

from fastapi import UploadFile

ALLOWED_SUFFIXES = (
    ".zip",
    ".tar",
    ".tar.gz",
    ".tgz",
)
MAX_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB
MAX_EXTRACTED_BYTES = 8 * 1024 * 1024 * 1024  # 8 GB
MAX_ARCHIVE_MEMBERS = 100_000


def _safe_join(base: Path, member_name: str) -> Path:
    target = (base / member_name).resolve()
    if not str(target).startswith(str(base.resolve()) + os.sep):
        raise ValueError(f"Unsafe archive member path: {member_name}")
    return target


def _detect_archive_type(filename: str) -> str:
    name = (filename or "").lower()
    if name.endswith(".zip"):
        return "zip"
    if name.endswith(".tar") or name.endswith(".tar.gz") or name.endswith(".tgz"):
        return "tar"
    raise ValueError("Unsupported archive format. Use .zip, .tar, .tar.gz, or .tgz.")


def extract_uploaded_archive(upload: UploadFile) -> tuple[str, str]:
    """
    Save upload to temp dir, safely extract it, and return:
      (extracted_folder_path, display_label_for_run_path)
    """
    filename = (upload.filename or "").strip()
    archive_type = _detect_archive_type(filename)

    run_id = uuid.uuid4().hex
    root = Path(tempfile.gettempdir()) / "trezor_log_uploads" / run_id
    incoming = root / "incoming"
    extracted = root / "extracted"
    incoming.mkdir(parents=True, exist_ok=True)
    extracted.mkdir(parents=True, exist_ok=True)

    archive_path = incoming / (filename or "upload.bin")
    written = 0
    with archive_path.open("wb") as out:
        while True:
            chunk = upload.file.read(1024 * 1024)
            if not chunk:
                break
            written += len(chunk)
            if written > MAX_UPLOAD_BYTES:
                raise ValueError("Uploaded archive is too large.")
            out.write(chunk)

    total_extracted = 0
    members_count = 0

    if archive_type == "zip":
        with zipfile.ZipFile(archive_path, "r") as zf:
            for member in zf.infolist():
                members_count += 1
                if members_count > MAX_ARCHIVE_MEMBERS:
                    raise ValueError("Archive has too many files.")
                if member.is_dir():
                    continue
                target = _safe_join(extracted, member.filename)
                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(member, "r") as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                # copyfileobj returns None; use file size from metadata
                total_extracted += int(member.file_size or 0)
                if total_extracted > MAX_EXTRACTED_BYTES:
                    raise ValueError("Extracted archive size exceeds limit.")
    else:
        with tarfile.open(archive_path, "r:*") as tf:
            for member in tf.getmembers():
                members_count += 1
                if members_count > MAX_ARCHIVE_MEMBERS:
                    raise ValueError("Archive has too many files.")
                if not member.isfile():
                    continue
                target = _safe_join(extracted, member.name)
                target.parent.mkdir(parents=True, exist_ok=True)
                src = tf.extractfile(member)
                if src is None:
                    continue
                with src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                total_extracted += int(member.size or 0)
                if total_extracted > MAX_EXTRACTED_BYTES:
                    raise ValueError("Extracted archive size exceeds limit.")

    display = f"upload://{filename or 'archive'}"
    return str(extracted), display


def cleanup_uploaded_archive(extracted_folder_path: str | None) -> None:
    if not extracted_folder_path:
        return
    try:
        # extracted path is .../<run>/extracted -> remove whole run root
        root = Path(extracted_folder_path).resolve().parent
        if root.name == "extracted":
            root = root.parent
        if root.exists():
            shutil.rmtree(root, ignore_errors=True)
    except Exception:
        pass


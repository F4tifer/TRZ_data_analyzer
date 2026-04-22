import argparse
import os
import re
from pathlib import Path


VERSION_FILE = Path(__file__).parent / "app_version.py"


def _read_version() -> tuple[int, int, int]:
    content = VERSION_FILE.read_text(encoding="utf-8")
    pattern = r'(APP_VERSION\s*=\s*["\'])(\d+)\.(\d+)\.(\d+)(["\'])'
    match = re.search(pattern, content)
    if not match:
        raise RuntimeError("APP_VERSION not found in app_version.py")
    major, minor, patch = map(int, match.groups()[1:4])
    return major, minor, patch


def _write_version(new_version: str) -> None:
    content = VERSION_FILE.read_text(encoding="utf-8")
    pattern = r'(APP_VERSION\s*=\s*["\'])(\d+)\.(\d+)\.(\d+)(["\'])'
    new_content = re.sub(pattern, rf'\1{new_version}\5', content, count=1)
    VERSION_FILE.write_text(new_content, encoding="utf-8")


def bump_version(part: str = "patch") -> str:
    """Increment APP_VERSION in app_version.py and return the new version.

    part: 'major', 'minor' or 'patch' (default 'patch').
    """
    major, minor, patch = _read_version()

    if part == "major":
        major += 1
        minor = 0
        patch = 0
    elif part == "minor":
        minor += 1
        patch = 0
    else:
        # default: patch
        patch += 1

    new_version = f"{major}.{minor}.{patch}"
    _write_version(new_version)
    return new_version


def main() -> None:
    parser = argparse.ArgumentParser(description="Bump APP_VERSION in app_version.py")
    parser.add_argument(
        "--part",
        choices=["major", "minor", "patch"],
        default=os.getenv("APP_BUMP_PART", "patch"),
        help="Which part of version to bump (default: patch or APP_BUMP_PART env).",
    )
    args = parser.parse_args()
    new_ver = bump_version(args.part)
    print(new_ver)


if __name__ == "__main__":
    main()



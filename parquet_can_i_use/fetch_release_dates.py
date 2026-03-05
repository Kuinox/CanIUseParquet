#!/usr/bin/env python3
"""
Developer utility: fetches release dates for all versions of each tested library
from their respective package registries (PyPI, crates.io, NuGet, Maven Central,
Go module proxy).

Use this when adding new versions to versions.json to look up their release dates.
The dates should then be hardcoded into the version_dates map in versions.json.

Release dates are NOT fetched dynamically at CI build time — they are hardcoded in
versions.json so that CI doesn't depend on external package registry availability.

Outputs: release_dates.json  (for reference; dates should be copied into versions.json)

Usage:
    python fetch_release_dates.py [--output PATH]
"""

import argparse
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DEFAULT_OUTPUT = SCRIPT_DIR / "release_dates.json"

# versions.json is the source of truth for which versions to look up
VERSIONS_FILE = SCRIPT_DIR / "versions.json"


def fetch_json(url: str, headers: dict | None = None) -> dict:
    import gzip as _gzip
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        encoding = resp.headers.get("Content-Encoding", "")
        if encoding == "gzip" or raw[:2] == b"\x1f\x8b":
            raw = _gzip.decompress(raw)
        return json.loads(raw)


def fetch_pypi_dates(package: str, versions: list[str]) -> dict[str, str]:
    """Fetch release dates from PyPI for a package."""
    try:
        data = fetch_json(f"https://pypi.org/pypi/{package}/json")
    except Exception as e:
        print(f"  WARNING: PyPI fetch failed for {package}: {e}")
        return {}

    releases = data.get("releases", {})
    result = {}
    for ver in versions:
        files = releases.get(ver, [])
        if files:
            # Take the earliest upload_time among all files for this release
            dates = [f["upload_time"] for f in files if f.get("upload_time")]
            if dates:
                result[ver] = min(dates)[:10]  # keep YYYY-MM-DD only
    return result


def fetch_crates_dates(crate: str, versions: list[str]) -> dict[str, str]:
    """Fetch release dates from crates.io."""
    try:
        data = fetch_json(
            f"https://crates.io/api/v1/crates/{crate}/versions",
            headers={"User-Agent": "CanIUseParquet/1.0 (fetch_release_dates.py)"},
        )
    except Exception as e:
        print(f"  WARNING: crates.io fetch failed for {crate}: {e}")
        return {}

    crate_versions = {v["num"]: v["created_at"][:10] for v in data.get("versions", [])}
    return {ver: crate_versions[ver] for ver in versions if ver in crate_versions}


def fetch_nuget_dates(package_id: str, versions: list[str]) -> dict[str, str]:
    """Fetch release dates from NuGet v3 API."""
    # NuGet registration index
    pkg_lower = package_id.lower()
    try:
        index = fetch_json(
            f"https://api.nuget.org/v3/registration5-gz-semver2/{pkg_lower}/index.json"
        )
    except Exception as e:
        print(f"  WARNING: NuGet fetch failed for {package_id}: {e}")
        return {}

    date_map: dict[str, str] = {}
    for page in index.get("items", []):
        items = page.get("items")
        if items is None:
            # Paged registration; fetch the page
            page_url = page.get("@id", "")
            if page_url:
                try:
                    page_data = fetch_json(page_url)
                    items = page_data.get("items", [])
                except Exception:
                    continue
        if items:
            for item in items:
                entry = item.get("catalogEntry", {})
                ver = entry.get("version", "")
                published = entry.get("published", "")
                if ver and published:
                    date_map[ver] = published[:10]

    return {ver: date_map[ver] for ver in versions if ver in date_map}


def fetch_maven_dates(group_id: str, artifact_id: str, versions: list[str]) -> dict[str, str]:
    """Fetch release dates from Maven Central."""
    import urllib.parse
    query = urllib.parse.quote(f'g:"{group_id}" AND a:"{artifact_id}"')
    try:
        data = fetch_json(
            f"https://search.maven.org/solrsearch/select?q={query}&core=gav&rows=100&wt=json",
            headers={"User-Agent": "CanIUseParquet/1.0"},
        )
    except Exception as e:
        print(f"  WARNING: Maven Central fetch failed for {group_id}:{artifact_id}: {e}")
        return {}

    docs = data.get("response", {}).get("docs", [])
    date_map: dict[str, str] = {}
    for doc in docs:
        ver = doc.get("v", "")
        ts = doc.get("timestamp")
        if ver and ts:
            import datetime
            dt = datetime.datetime.fromtimestamp(ts / 1000, tz=datetime.timezone.utc)
            date_map[ver] = dt.strftime("%Y-%m-%d")

    return {ver: date_map[ver] for ver in versions if ver in date_map}


def fetch_github_release_dates(owner: str, repo: str, versions: list[str],
                                tag_prefix: str = "v") -> dict[str, str]:
    """Fetch release dates from GitHub Releases API."""
    result: dict[str, str] = {}
    page = 1
    date_map: dict[str, str] = {}

    # Build a set for fast lookup; check both "v{ver}" and "{ver}" tags
    remaining = set(versions)

    while remaining:
        try:
            releases = fetch_json(
                f"https://api.github.com/repos/{owner}/{repo}/releases?per_page=100&page={page}",
                headers={"Accept": "application/vnd.github.v3+json",
                         "User-Agent": "CanIUseParquet/1.0"},
            )
        except Exception as e:
            print(f"  WARNING: GitHub releases fetch failed for {owner}/{repo}: {e}")
            break

        if not releases:
            break

        for rel in releases:
            tag = rel.get("tag_name", "")
            published = rel.get("published_at", "") or rel.get("created_at", "")
            # Strip common prefixes to match plain version strings
            stripped = tag.lstrip("v")
            if stripped in remaining:
                date_map[stripped] = published[:10]
                remaining.discard(stripped)
            if tag in remaining:
                date_map[tag] = published[:10]
                remaining.discard(tag)

        if len(releases) < 100:
            break
        page += 1
        time.sleep(0.2)

    return {ver: date_map[ver] for ver in versions if ver in date_map}


def fetch_github_tag_dates(owner: str, repo: str, versions: list[str],
                           tag_prefix: str = "v") -> dict[str, str]:
    """Fetch tag dates from GitHub Tags API (for repos without formal releases)."""
    date_map: dict[str, str] = {}
    remaining = set(versions)
    page = 1

    while remaining:
        try:
            tags = fetch_json(
                f"https://api.github.com/repos/{owner}/{repo}/tags?per_page=100&page={page}",
                headers={"Accept": "application/vnd.github.v3+json",
                         "User-Agent": "CanIUseParquet/1.0"},
            )
        except Exception as e:
            print(f"  WARNING: GitHub tags fetch failed for {owner}/{repo}: {e}")
            break

        if not tags:
            break

        for tag in tags:
            tag_name = tag.get("name", "")
            stripped = tag_name.lstrip("v")
            # We need to fetch the commit date for this tag
            commit_url = tag.get("commit", {}).get("url", "")
            candidate = None
            if stripped in remaining:
                candidate = stripped
            elif tag_name in remaining:
                candidate = tag_name

            if candidate and commit_url:
                try:
                    commit_data = fetch_json(
                        commit_url,
                        headers={"Accept": "application/vnd.github.v3+json",
                                 "User-Agent": "CanIUseParquet/1.0"},
                    )
                    date_str = (
                        commit_data.get("commit", {})
                        .get("committer", {})
                        .get("date", "")
                    )
                    if date_str:
                        date_map[candidate] = date_str[:10]
                        remaining.discard(candidate)
                except Exception:
                    pass
                time.sleep(0.1)

        if len(tags) < 100:
            break
        page += 1
        time.sleep(0.2)

    return {ver: date_map[ver] for ver in versions if ver in date_map}


def fetch_go_module_dates(module: str, versions: list[str]) -> dict[str, str]:
    """Fetch release dates from the Go module proxy."""
    date_map: dict[str, str] = {}
    for ver in versions:
        tag = f"v{ver}"
        try:
            info = fetch_json(f"https://proxy.golang.org/{module}/@v/{tag}.info")
            ts = info.get("Time", "")
            if ts:
                date_map[ver] = ts[:10]
        except Exception as e:
            print(f"  WARNING: Go module proxy fetch failed for {module}@{tag}: {e}")
        time.sleep(0.05)
    return date_map


def load_versions() -> dict:
    with open(VERSIONS_FILE) as f:
        return json.load(f)


def fetch_all(versions_config: dict) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}

    def _fetch(tool_id: str, fetcher):
        print(f"  Fetching dates for {tool_id}...")
        versions = versions_config[tool_id]["versions"]
        dates = fetcher(versions)
        missing = [v for v in versions if v not in dates]
        if missing:
            print(f"    Missing dates for: {missing}")
        result[tool_id] = dates

    # PyPI packages
    for tool_id, pypi_name in [
        ("pyarrow", "pyarrow"),
        ("fastparquet", "fastparquet"),
        ("polars", "polars"),
        ("duckdb", "duckdb"),
        ("spark", "pyspark"),
    ]:
        if tool_id in versions_config:
            _fetch(tool_id, lambda v, pkg=pypi_name: fetch_pypi_dates(pkg, v))

    # crates.io
    if "parquet-rs" in versions_config:
        _fetch("parquet-rs", lambda v: fetch_crates_dates("parquet", v))

    # NuGet
    if "parquet-dotnet" in versions_config:
        _fetch("parquet-dotnet", lambda v: fetch_nuget_dates("Parquet.Net", v))
    if "parquet-sharp" in versions_config:
        _fetch("parquet-sharp", lambda v: fetch_nuget_dates("ParquetSharp", v))

    # Maven Central (parquet-java uses parquet-format artifacts)
    if "parquet-java" in versions_config:
        _fetch("parquet-java",
               lambda v: fetch_maven_dates("org.apache.parquet", "parquet-common", v))

    # Go module proxy
    if "parquet-go" in versions_config:
        _fetch("parquet-go",
               lambda v: fetch_go_module_dates("github.com/parquet-go/parquet-go", v))

    # Trino: fetch from Maven Central (trino-jdbc artifact)
    if "trino" in versions_config:
        _fetch("trino",
               lambda v: fetch_maven_dates("io.trino", "trino-jdbc", v))

    return result


def main():
    parser = argparse.ArgumentParser(description="Fetch release dates for all tool versions")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output JSON file path")
    args = parser.parse_args()

    print("Loading versions.json...")
    versions_config = load_versions()
    # Remove metadata key
    versions_config = {k: v for k, v in versions_config.items() if not k.startswith("_")}

    print("Fetching release dates from package registries...")
    dates = fetch_all(versions_config)

    output_path = Path(args.output)
    with open(output_path, "w") as f:
        json.dump(dates, f, indent=2, sort_keys=True)
    print(f"\nRelease dates written to {output_path}")
    print(f"Tools covered: {list(dates.keys())}")


if __name__ == "__main__":
    main()

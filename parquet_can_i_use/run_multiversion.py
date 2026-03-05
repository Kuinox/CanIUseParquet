#!/usr/bin/env python3
"""
Multi-version test runner for Parquet feature testing.

For each library, installs multiple versions (in separate venvs for Python libraries)
and runs the same CLI against each version. Results are saved per library per version.

Usage:
    python run_multiversion.py [--only TOOL...] [--skip-compiled] [--bisect]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import venv
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
RESULTS_DIR = SCRIPT_DIR / "results"
VERSIONS_FILE = SCRIPT_DIR / "versions.json"


def load_versions():
    with open(VERSIONS_FILE) as f:
        return json.load(f)


def run_python_version(tool_id, version, cli_path, extra_deps, install_template):
    """Test a specific version of a Python library using a virtual environment."""
    venv_dir = SCRIPT_DIR / ".venvs" / f"{tool_id}-{version}"
    print(f"  [{tool_id}] Testing v{version}...", end=" ", flush=True)

    try:
        # Create venv
        venv.create(str(venv_dir), with_pip=True, clear=True)
        pip = str(venv_dir / "bin" / "pip")
        python = str(venv_dir / "bin" / "python")

        # Install deps
        deps_cmd = [pip, "install", "-q"]
        if extra_deps:
            deps_cmd.extend(extra_deps)
        install_spec = install_template.format(version=version)
        deps_cmd.append(install_spec.split()[-1])  # e.g. "pyarrow==0.17.1"

        subprocess.run(deps_cmd, capture_output=True, text=True, check=True, timeout=300)

        # Run CLI
        result = subprocess.run(
            [python, str(cli_path)],
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode != 0:
            # CLI may fail on old versions due to API changes - that's OK
            # Try to parse any JSON output
            try:
                data = json.loads(result.stdout)
                print(f"OK (partial)")
                return data
            except (json.JSONDecodeError, ValueError):
                print(f"FAILED (exit {result.returncode})")
                return None

        data = json.loads(result.stdout)
        print(f"OK (v{data.get('version', version)})")
        return data

    except subprocess.TimeoutExpired:
        print("TIMEOUT")
        return None
    except subprocess.CalledProcessError as e:
        print(f"INSTALL FAILED: {e}")
        return None
    except Exception as e:
        print(f"ERROR: {e}")
        return None


def _set_rust_version(cli_dir, version):
    """Pin parquet and arrow crate major version in Cargo.toml and clear the lock file."""
    major = version.split(".")[0]
    cargo_toml = cli_dir / "Cargo.toml"
    content = cargo_toml.read_text()
    content = re.sub(r'(parquet = \{ version = ")[^"]*(")', rf'\g<1>{major}\2', content)
    content = re.sub(r'(arrow = \{ version = ")[^"]*(")', rf'\g<1>{major}\2', content)
    cargo_toml.write_text(content)
    lock = cli_dir / "Cargo.lock"
    if lock.exists():
        lock.unlink()


def _set_java_version(cli_dir, version):
    """Set the parquet version property in pom.xml."""
    pom = cli_dir / "pom.xml"
    content = pom.read_text()
    content = re.sub(
        r'<parquet\.version>[^<]*</parquet\.version>',
        f'<parquet.version>{version}</parquet.version>',
        content,
    )
    pom.write_text(content)


def _set_trino_version(cli_dir, version):
    """Set the trino version property in pom.xml."""
    pom = cli_dir / "pom.xml"
    content = pom.read_text()
    content = re.sub(
        r'<trino\.version>[^<]*</trino\.version>',
        f'<trino.version>{version}</trino.version>',
        content,
    )
    pom.write_text(content)


def _set_dotnet_version(cli_dir, version, package_name="Parquet.Net"):
    """Set the dotnet package version in the .csproj file."""
    csproj_files = list(cli_dir.glob("*.csproj"))
    if not csproj_files:
        raise FileNotFoundError(f"No .csproj file found in {cli_dir}")
    csproj = csproj_files[0]
    content = csproj.read_text()
    escaped_name = re.escape(package_name)
    content = re.sub(
        rf'(<PackageReference Include="{escaped_name}" Version=")[^"]*(")',
        rf'\g<1>{version}\2',
        content,
    )
    csproj.write_text(content)


def run_compiled_version(tool_id, tool_config, version):
    """Build and run a compiled tool pinned to a specific version."""
    cli_dir = SCRIPT_DIR / tool_config["cli_dir"]
    tool_type = tool_config.get("type", "")
    print(f"  [{tool_id}] Testing v{version}...", end=" ", flush=True)

    try:
        if tool_type == "rust":
            _set_rust_version(cli_dir, version)
            subprocess.run(
                ["cargo", "build", "--release"], cwd=str(cli_dir),
                capture_output=True, text=True, check=True, timeout=300,
            )
            run_cmd = [str(cli_dir / "target" / "release" / "test_parquet_rs")]
        elif tool_type == "go":
            subprocess.run(
                ["go", "get", f"github.com/parquet-go/parquet-go@v{version}"],
                cwd=str(cli_dir), capture_output=True, text=True, check=True, timeout=120,
            )
            subprocess.run(
                ["go", "mod", "tidy"],
                cwd=str(cli_dir), capture_output=True, text=True, check=True, timeout=120,
            )
            subprocess.run(
                ["go", "build", "-o", "test_parquet_go"], cwd=str(cli_dir),
                capture_output=True, text=True, check=True, timeout=300,
            )
            run_cmd = [str(cli_dir / "test_parquet_go")]
        elif tool_type == "java":
            _set_java_version(cli_dir, version)
            subprocess.run(
                ["mvn", "-q", "package", "-DskipTests"], cwd=str(cli_dir),
                capture_output=True, text=True, check=True, timeout=300,
            )
            run_cmd = ["java", "-jar", str(cli_dir / "target" / "test-parquet-java-1.0-SNAPSHOT.jar")]
        elif tool_type == "trino":
            _set_trino_version(cli_dir, version)
            subprocess.run(
                ["mvn", "-q", "package", "-DskipTests"], cwd=str(cli_dir),
                capture_output=True, text=True, check=True, timeout=600,
            )
            run_cmd = ["java", "-jar", str(cli_dir / "target" / "test-trino-1.0-SNAPSHOT.jar")]
        elif tool_type == "dotnet":
            _set_dotnet_version(cli_dir, version, tool_config.get("dotnet_package", "Parquet.Net"))
            subprocess.run(
                ["dotnet", "build", "-c", "Release", "-v", "q"], cwd=str(cli_dir),
                capture_output=True, text=True, check=True, timeout=300,
            )
            run_cmd = ["dotnet", "run", "--project", str(cli_dir), "-c", "Release", "--no-build"]
        else:
            print("UNKNOWN TYPE")
            return None

        result = subprocess.run(run_cmd, capture_output=True, text=True, check=True, timeout=120)
        data = json.loads(result.stdout)
        print(f"OK")
        return data

    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError, subprocess.TimeoutExpired) as e:
        print(f"FAILED: {e}")
        return None


def run_compiled_tool(tool_id, tool_config):
    """Run a compiled language CLI (Rust/Go/Java/.NET/Trino) for the current version only."""
    cli_dir = SCRIPT_DIR / tool_config["cli_dir"]
    version = tool_config["versions"][-1]  # Latest version
    print(f"  [{tool_id}] Testing v{version}...", end=" ", flush=True)

    tool_type = tool_config.get("type", "")

    try:
        # Build
        if tool_type == "rust":
            subprocess.run(["cargo", "build", "--release"], cwd=str(cli_dir),
                         capture_output=True, text=True, check=True, timeout=300)
            run_cmd = [str(cli_dir / "target" / "release" / "test_parquet_rs")]
        elif tool_type == "go":
            subprocess.run(["go", "build", "-o", "test_parquet_go"], cwd=str(cli_dir),
                         capture_output=True, text=True, check=True, timeout=300)
            run_cmd = [str(cli_dir / "test_parquet_go")]
        elif tool_type == "java":
            subprocess.run(["mvn", "-q", "package", "-DskipTests"], cwd=str(cli_dir),
                         capture_output=True, text=True, check=True, timeout=300)
            run_cmd = ["java", "-jar", str(cli_dir / "target" / "test-parquet-java-1.0-SNAPSHOT.jar")]
        elif tool_type == "trino":
            subprocess.run(["mvn", "-q", "package", "-DskipTests"], cwd=str(cli_dir),
                         capture_output=True, text=True, check=True, timeout=600)
            run_cmd = ["java", "-jar", str(cli_dir / "target" / "test-trino-1.0-SNAPSHOT.jar")]
        elif tool_type == "dotnet":
            subprocess.run(["dotnet", "build", "-c", "Release", "-v", "q"], cwd=str(cli_dir),
                         capture_output=True, text=True, check=True, timeout=300)
            run_cmd = ["dotnet", "run", "--project", str(cli_dir), "-c", "Release", "--no-build"]
        else:
            print("UNKNOWN TYPE")
            return None

        # Run
        result = subprocess.run(run_cmd, capture_output=True, text=True, check=True, timeout=120)
        data = json.loads(result.stdout)
        print(f"OK (v{data.get('version', version)})")
        return data

    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
        print(f"FAILED: {e}")
        return None


def flatten_features(data):
    """Return a frozenset of all (category, feature, subfeature) tuples that are True."""
    if not data:
        return frozenset()
    supported = set()
    for cat in ("compression", "logical_types", "nested_types", "advanced_features"):
        for k, v in data.get(cat, {}).items():
            if v is True:
                supported.add((cat, k, None))
    for enc, types in data.get("encoding", {}).items():
        if isinstance(types, dict):
            for ptype, v in types.items():
                if v is True:
                    supported.add(("encoding", enc, ptype))
        elif types is True:
            supported.add(("encoding", enc, None))
    return frozenset(supported)


def bisect_versions(versions, run_func):
    """Use binary search to find version transition points.

    Tests the oldest and newest version first, then recursively bisects any
    range where the set of supported features differs between the endpoints.
    Returns a dict of {index: result_data} for every version that was tested.
    """
    n = len(versions)
    if n == 0:
        return {}

    tested = {}  # index -> result data (None if the run failed)

    def test(idx):
        if idx not in tested:
            tested[idx] = run_func(versions[idx])
        return tested[idx]

    def has_diff(idx_a, idx_b):
        a, b = tested.get(idx_a), tested.get(idx_b)
        # flatten_features returns frozenset() for None, so a failed version
        # (no features) will compare as different from a successful one.
        return flatten_features(a) != flatten_features(b)

    # Always test oldest and newest
    test(0)
    test(n - 1)

    stack = [(0, n - 1)]
    while stack:
        lo, hi = stack.pop()
        if hi - lo <= 1:
            continue
        if not has_diff(lo, hi):
            continue  # Endpoints identical, no transition in this range
        mid = (lo + hi) // 2
        test(mid)
        if has_diff(lo, mid):
            stack.append((lo, mid))
        if has_diff(mid, hi):
            stack.append((mid, hi))

    return tested


def main():
    parser = argparse.ArgumentParser(description="Run multi-version Parquet tests")
    parser.add_argument("--only", nargs="*", help="Only test specific tools")
    parser.add_argument("--skip-compiled", action="store_true", help="Skip compiled language tools")
    parser.add_argument("--bisect", action="store_true",
                        help="Use binary search to find feature transition versions instead of testing all versions")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(exist_ok=True)
    versions_config = load_versions()

    # Filter out internal comment keys
    tools = {k: v for k, v in versions_config.items() if not k.startswith("_")}

    if args.only:
        tools = {k: v for k, v in tools.items() if k in args.only}

    print("Running multi-version Parquet feature tests...")
    print()

    all_results = {}

    for tool_id, config in tools.items():
        tool_type = config.get("type", "python")

        if tool_type != "python" and tool_type not in ("rust", "go", "java", "dotnet", "trino"):
            # Infer type from presence of install key
            if "install" in config:
                tool_type = "python"

        versions = config["versions"]

        if tool_type == "python" or "install" in config:
            # Python library
            cli_path = SCRIPT_DIR / config["cli"]
            extra_deps = config.get("extra_deps", [])
            install_template = config["install"]

            def _run_py(version):
                return run_python_version(tool_id, version, cli_path, extra_deps, install_template)

            if args.bisect:
                print(f"  [{tool_id}] Bisecting {len(versions)} versions...")
                tested = bisect_versions(versions, _run_py)
                indices = sorted(tested.keys())
            else:
                tested = {i: _run_py(versions[i]) for i in range(len(versions))}
                indices = list(range(len(versions)))

            version_results = []
            for idx in indices:
                data = tested[idx]
                if data:
                    data["tested_version"] = versions[idx]
                    version_results.append(data)
                    result_file = RESULTS_DIR / f"{tool_id}-{versions[idx]}.json"
                    with open(result_file, "w") as f:
                        json.dump(data, f, indent=2)

            all_results[tool_id] = version_results
        else:
            if args.skip_compiled:
                print(f"  [{tool_id}] Skipping (compiled)")
                continue

            # Compiled language
            def _run_compiled(version):
                return run_compiled_version(tool_id, config, version)

            if args.bisect:
                print(f"  [{tool_id}] Bisecting {len(versions)} versions...")
                tested = bisect_versions(versions, _run_compiled)
                indices = sorted(tested.keys())
            else:
                tested = {i: _run_compiled(versions[i]) for i in range(len(versions))}
                indices = list(range(len(versions)))

            version_results = []
            for idx in indices:
                data = tested[idx]
                if data:
                    data["tested_version"] = versions[idx]
                    version_results.append(data)
                    result_file = RESULTS_DIR / f"{tool_id}-{versions[idx]}.json"
                    with open(result_file, "w") as f:
                        json.dump(data, f, indent=2)

            all_results[tool_id] = version_results

    # Save combined results
    combined_file = RESULTS_DIR / "all_versions.json"
    with open(combined_file, "w") as f:
        json.dump(all_results, f, indent=2)

    print()
    print(f"Results saved to {RESULTS_DIR}/")
    print(f"Combined results: {combined_file}")


if __name__ == "__main__":
    main()

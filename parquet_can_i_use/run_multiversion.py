#!/usr/bin/env python3
"""
Multi-version test runner for Parquet feature testing.

For each library, installs multiple versions (in separate venvs for Python libraries)
and runs the same CLI against each version. Results are saved per library per version.

Usage:
    python run_multiversion.py [--only TOOL...] [--skip-compiled]
"""

import argparse
import json
import os
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


def run_compiled_tool(tool_id, tool_config):
    """Run a compiled language CLI (Rust/Go/Java/.NET) for the current version only."""
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


def main():
    parser = argparse.ArgumentParser(description="Run multi-version Parquet tests")
    parser.add_argument("--only", nargs="*", help="Only test specific tools")
    parser.add_argument("--skip-compiled", action="store_true", help="Skip compiled language tools")
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

        if tool_type != "python" and tool_type not in ("rust", "go", "java", "dotnet"):
            # Infer type from presence of install key
            if "install" in config:
                tool_type = "python"

        if tool_type == "python" or "install" in config:
            # Python library - test multiple versions
            cli_path = SCRIPT_DIR / config["cli"]
            extra_deps = config.get("extra_deps", [])
            install_template = config["install"]

            version_results = []
            for version in config["versions"]:
                data = run_python_version(tool_id, version, cli_path, extra_deps, install_template)
                if data:
                    data["tested_version"] = version
                    version_results.append(data)

                    # Also save individual result
                    result_file = RESULTS_DIR / f"{tool_id}-{version}.json"
                    with open(result_file, "w") as f:
                        json.dump(data, f, indent=2)

            all_results[tool_id] = version_results
        else:
            if args.skip_compiled:
                print(f"  [{tool_id}] Skipping (compiled)")
                continue

            # Compiled language - test current version only
            data = run_compiled_tool(tool_id, config)
            if data:
                data["tested_version"] = config["versions"][-1]
                all_results[tool_id] = [data]

                result_file = RESULTS_DIR / f"{tool_id}-{config['versions'][-1]}.json"
                with open(result_file, "w") as f:
                    json.dump(data, f, indent=2)

    # Save combined results
    combined_file = RESULTS_DIR / "all_versions.json"
    with open(combined_file, "w") as f:
        json.dump(all_results, f, indent=2)

    print()
    print(f"Results saved to {RESULTS_DIR}/")
    print(f"Combined results: {combined_file}")


if __name__ == "__main__":
    main()

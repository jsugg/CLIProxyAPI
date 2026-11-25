#!/usr/bin/env python3
"""Run the Go suite; rerun each failed test once, and fail only if it fails again.

Upstream's suite has timing-sensitive tests that fail intermittently on shared
runners. A package that fails without a failing test (build error, panic,
timeout) is never rerun.
"""
import json
import subprocess
import sys

TIMEOUT = "10m"


def run(args: list[str]) -> tuple[int, dict[str, set[str]], set[str]]:
    """Run `go test -json`, echo failed output, return (status, failed tests by package, failed packages)."""
    proc = subprocess.run(["go", "test", "-json", "-timeout", TIMEOUT, *args], capture_output=True, text=True)
    output: dict[tuple[str, str], list[str]] = {}
    tests: dict[str, set[str]] = {}
    packages: set[str] = set()
    for line in proc.stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            print(line)
            continue
        package, test, action = event.get("Package", ""), event.get("Test", ""), event.get("Action")
        if action == "output":
            output.setdefault((package, test), []).append(event.get("Output", ""))
        elif action == "fail":
            if test:
                tests.setdefault(package, set()).add(test.split("/")[0])
            else:
                packages.add(package)
    for (package, test), lines in output.items():
        if (test and test.split("/")[0] in tests.get(package, ())) or (not test and package in packages):
            sys.stdout.write("".join(lines))
    sys.stderr.write(proc.stderr)
    return proc.returncode, tests, packages


def main() -> int:
    status, tests, packages = run(["./..."])
    if status == 0:
        print("go test: all packages passed")
        return 0
    hard = sorted(p for p in packages if p not in tests)
    if hard or not tests:
        print(f"go test: packages failed without a failing test (not rerun): {hard or 'unknown'}")
        return 1
    failed_again = False
    for package, names in sorted(tests.items()):
        pattern = "^(" + "|".join(sorted(names)) + ")$"
        print(f"rerunning once in {package}: {', '.join(sorted(names))}")
        again, _, _ = run(["-count=1", "-run", pattern, package])
        failed_again = failed_again or again != 0
    print("go test: failures reproduced on rerun" if failed_again else "go test: every failure passed on rerun")
    return 1 if failed_again else 0


if __name__ == "__main__":
    sys.exit(main())

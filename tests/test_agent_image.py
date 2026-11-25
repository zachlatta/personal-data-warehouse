"""The managed agent image: what its tag hashes, and one build per tag.

The tag is the cache key: `ensure_agent_image` builds only when no image
carries it. So the tag must move for every input baked into the image (the
Dockerfile, the entrypoint, the `pdw` CLI's Go package closure with its
embedded files, go.mod/go.sum, the build flags) and for nothing else, or a
server-only edit rebuilds a 2.5 GB image. And concurrent pytest workers or
overlapping runs that need the same tag must share one build.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading
import time

import pytest

from personal_data_warehouse import agent_runner
from personal_data_warehouse.agent_runner import (
    DEFAULT_AGENT_CLI_SOURCE_DIR,
    DEFAULT_AGENT_DOCKERFILE_PATH,
    agent_image_lock_path,
    default_agent_docker_image,
    ensure_agent_image,
    pdw_cli_source_files,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture
def fake_tree(tmp_path) -> dict[str, Path]:
    """A miniature app/ module shaped like the real one."""

    app = tmp_path / "app"
    files = {
        "dockerfile": write(tmp_path / "docker" / "agent.Dockerfile", "FROM alpine\n"),
        "entrypoint": write(tmp_path / "docker" / "agent-entrypoint.sh", "#!/bin/sh\n"),
        "dockerignore": write(tmp_path / ".dockerignore", ".git/\n"),
        "go_mod": write(app / "go.mod", "module example.com/m\n\ngo 1.26\n"),
        "go_sum": write(app / "go.sum", "example.org/dep v1.0.0 h1:abc=\n"),
        "main": write(
            app / "cmd" / "pdw-cli" / "main.go",
            "package main\n\nimport (\n\t\"fmt\"\n\tcc \"example.com/m/internal/cliclient\"\n"
            "\t_ \"example.com/m/internal/guide\"\n)\n\nfunc main() { fmt.Println(cc.X) }\n",
        ),
        "cliclient": write(
            app / "internal" / "cliclient" / "client.go",
            'package cliclient\n\nimport "example.com/m/internal/shared"\n\nvar X = shared.Y\n',
        ),
        "shared": write(app / "internal" / "shared" / "shared.go", "package shared\n\nvar Y = 1\n"),
        "guide": write(
            app / "internal" / "guide" / "guide.go",
            'package guide\n\nimport "embed"\n\n//go:embed brief.md topics/*.md\nvar files embed.FS\n',
        ),
        "brief": write(app / "internal" / "guide" / "brief.md", "brief\n"),
        "topic": write(app / "internal" / "guide" / "topics" / "sql.md", "sql\n"),
        "server": write(
            app / "internal" / "server" / "server.go",
            'package server\n\nimport "example.com/m/internal/shared"\n\nvar Z = shared.Y\n',
        ),
        "server_cmd": write(
            app / "cmd" / "pdw-mcp" / "main.go",
            'package main\n\nimport _ "example.com/m/internal/server"\n\nfunc main() {}\n',
        ),
    }
    files["root"] = tmp_path
    files["app"] = app
    return files


def tag_of(tree: dict[str, Path]) -> str:
    return default_agent_docker_image(
        repository="pdw-agent",
        dockerfile_path=tree["dockerfile"],
        entrypoint_path=tree["entrypoint"],
        cli_source_dir=tree["app"],
        dockerignore_path=tree["dockerignore"],
    )


@pytest.mark.parametrize(
    "edit",
    [
        "server",  # a package pdw-cli never imports
        "server_cmd",  # another binary
        "server_test",  # a test file in a package that is not even reached
        "cli_test",  # a test file in a reached package
        "unreached_dir",  # a new package nothing imports
    ],
)
def test_edits_outside_the_cli_image_inputs_keep_the_tag(fake_tree, edit) -> None:
    before = tag_of(fake_tree)
    app = fake_tree["app"]
    if edit == "server":
        fake_tree["server"].write_text("package server\n// changed\n", encoding="utf-8")
    elif edit == "server_cmd":
        fake_tree["server_cmd"].write_text("package main\n// changed\nfunc main() {}\n", encoding="utf-8")
    elif edit == "server_test":
        write(app / "internal" / "server" / "server_test.go", "package server\n")
    elif edit == "cli_test":
        write(app / "internal" / "cliclient" / "client_test.go", 'package cliclient\n\nimport "testing"\n')
    elif edit == "unreached_dir":
        write(app / "internal" / "feed" / "feed.go", "package feed\n")

    assert tag_of(fake_tree) == before


@pytest.mark.parametrize(
    "edit",
    [
        "main",  # the CLI's own package
        "cliclient",  # a direct import, reached through an aliased import
        "shared",  # a transitive import
        "guide",  # reached only through a blank import
        "brief",  # a go:embed file
        "topic",  # a go:embed glob match in a subdirectory
        "go_mod",
        "go_sum",
        "dockerfile",
        "entrypoint",
        "dockerignore",
        "new_source_in_reached_package",
        "go_work",  # a workspace file changes which module versions build
        "vendor",  # a vendor tree replaces the module cache
    ],
)
def test_every_image_input_moves_the_tag(fake_tree, edit) -> None:
    before = tag_of(fake_tree)
    if edit == "new_source_in_reached_package":
        write(fake_tree["app"] / "internal" / "shared" / "more.go", "package shared\n")
    elif edit == "go_work":
        write(fake_tree["app"] / "go.work", "go 1.26\n\nuse .\n")
    elif edit == "vendor":
        write(fake_tree["app"] / "vendor" / "modules.txt", "# example.org/dep v1.0.0\n")
    else:
        path = fake_tree[edit]
        path.write_text(path.read_text(encoding="utf-8") + "\n// changed\n", encoding="utf-8")

    assert tag_of(fake_tree) != before


def test_build_flags_are_part_of_the_tag(fake_tree, monkeypatch) -> None:
    before = tag_of(fake_tree)
    monkeypatch.setattr(agent_runner, "AGENT_IMAGE_BUILD_FLAGS", ("--build-arg", "EXAMPLE=1"))

    assert tag_of(fake_tree) != before


def test_renaming_an_input_moves_the_tag(fake_tree) -> None:
    """Paths are hashed relative to the module, not by bare file name."""

    before = tag_of(fake_tree)
    fake_tree["shared"].rename(fake_tree["shared"].with_name("renamed.go"))

    assert tag_of(fake_tree) != before


def go_list_cli_inputs() -> tuple[set[Path], set[Path]]:
    """What the Go toolchain says the CLI build reads, for linux (the image) and darwin."""

    fields = "GoFiles CgoFiles SFiles HFiles CFiles CXXFiles SysoFiles EmbedFiles".split()
    template = "{{.ImportPath}}|{{.Dir}}" + "".join(f"|{{{{join .{field} \",\"}}}}" for field in fields)
    module = "github.com/zachlatta/personal-data-warehouse/app"
    dirs: set[Path] = set()
    files: set[Path] = set()
    for goos in ("linux", "darwin"):
        completed = subprocess.run(
            ["go", "list", "-deps", "-f", template, "./cmd/pdw-cli"],
            cwd=DEFAULT_AGENT_CLI_SOURCE_DIR,
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "GOOS": goos, "CGO_ENABLED": "0"},
        )
        for line in completed.stdout.splitlines():
            import_path, directory, *groups = line.split("|")
            if import_path != module and not import_path.startswith(module + "/"):
                continue
            package_dir = Path(directory)
            dirs.add(package_dir)
            for group in groups:
                files.update(package_dir / name for name in group.split(",") if name)
    return dirs, files


@pytest.mark.skipif(shutil.which("go") is None, reason="the Go suite requires go on PATH; tests/test_go_suite.py fails loudly")
def test_the_real_cli_inputs_cover_everything_go_list_reports_and_leave_out_the_server() -> None:
    """The Python closure is checked against the Go toolchain on the real tree.

    The tag is computed without Go (the Dagster container has none), so this is
    what proves the hand-rolled import walk is a superset of the real build.
    """

    inputs = set(pdw_cli_source_files(DEFAULT_AGENT_CLI_SOURCE_DIR))
    go_dirs, go_files = go_list_cli_inputs()

    missing = sorted(str(path) for path in go_files if path not in inputs)
    assert not missing, f"image inputs miss files the Go build reads: {missing[:10]}"
    input_dirs = {path.parent for path in inputs if path.suffix == ".go"}
    assert go_dirs <= input_dirs
    # The walk ignores build constraints, so it may only over-reach, never miss.
    assert input_dirs == go_dirs, sorted(str(path) for path in input_dirs ^ go_dirs)

    server_dir = DEFAULT_AGENT_CLI_SOURCE_DIR / "internal" / "server"
    assert not any(server_dir in path.parents for path in inputs)
    assert not any(path.name.endswith("_test.go") for path in inputs)
    assert DEFAULT_AGENT_CLI_SOURCE_DIR / "go.mod" in inputs
    assert DEFAULT_AGENT_CLI_SOURCE_DIR / "go.sum" in inputs


class FakeDocker:
    """A docker CLI whose image store is a set, with a slow, optionally failing build."""

    def __init__(self, *, build_seconds: float = 0.2, failures: int = 0) -> None:
        self.images: set[str] = set()
        self.builds = 0
        self.failures = failures
        self.build_seconds = build_seconds
        self.lock = threading.Lock()
        self.concurrent = 0
        self.max_concurrent = 0

    def __call__(self, command, **kwargs):
        if command[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(command, 0 if command[3] in self.images else 1)
        assert command[:2] == ["docker", "build"]
        tag = command[command.index("-t") + 1]
        with self.lock:
            self.builds += 1
            self.concurrent += 1
            self.max_concurrent = max(self.max_concurrent, self.concurrent)
            fail = self.failures > 0
            self.failures -= 1 if fail else 0
        time.sleep(self.build_seconds)
        with self.lock:
            self.concurrent -= 1
            if not fail:
                self.images.add(tag)
        return subprocess.CompletedProcess(command, 1 if fail else 0)


def test_concurrent_callers_share_one_build(fake_tree, tmp_path) -> None:
    docker = FakeDocker()
    results: list[str] = []

    def call() -> None:
        results.append(
            ensure_agent_image(
                dockerfile_path=fake_tree["dockerfile"],
                entrypoint_path=fake_tree["entrypoint"],
                context_dir=fake_tree["root"],
                cli_source_dir=fake_tree["app"],
                dockerignore_path=fake_tree["dockerignore"],
                lock_dir=tmp_path / "locks",
                runner=docker,
            )
        )

    threads = [threading.Thread(target=call) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert docker.builds == 1
    assert docker.max_concurrent == 1
    assert len(set(results)) == 1 and len(results) == 5


def test_a_failed_build_is_retried_by_the_next_caller(fake_tree, tmp_path) -> None:
    docker = FakeDocker(build_seconds=0, failures=1)
    kwargs = dict(
        dockerfile_path=fake_tree["dockerfile"],
        entrypoint_path=fake_tree["entrypoint"],
        context_dir=fake_tree["root"],
        cli_source_dir=fake_tree["app"],
        dockerignore_path=fake_tree["dockerignore"],
        lock_dir=tmp_path / "locks",
        runner=docker,
    )

    with pytest.raises(RuntimeError, match="failed to build"):
        ensure_agent_image(**kwargs)
    image = ensure_agent_image(**kwargs)

    assert docker.builds == 2
    assert image in docker.images


def test_a_builder_that_dies_holding_the_lock_does_not_block_the_next_build(fake_tree, tmp_path) -> None:
    docker = FakeDocker(build_seconds=0)
    image = tag_of(fake_tree).replace("pdw-agent:", "personal-data-warehouse-agent:")
    lock_path = agent_image_lock_path(image, lock_dir=tmp_path / "locks")
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import fcntl, sys, time\n"
            "path = sys.argv[1]\n"
            "import pathlib; pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)\n"
            "fh = open(path, 'a+')\n"
            "fcntl.flock(fh, fcntl.LOCK_EX)\n"
            "print('held', flush=True)\n"
            "time.sleep(60)\n",
            str(lock_path),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "held"
        holder.send_signal(signal.SIGKILL)
        holder.wait(timeout=10)

        built = ensure_agent_image(
            dockerfile_path=fake_tree["dockerfile"],
            entrypoint_path=fake_tree["entrypoint"],
            context_dir=fake_tree["root"],
            cli_source_dir=fake_tree["app"],
            dockerignore_path=fake_tree["dockerignore"],
            lock_dir=tmp_path / "locks",
            runner=docker,
        )
    finally:
        if holder.poll() is None:
            holder.kill()

    assert built == image
    assert docker.builds == 1


def test_the_agent_dockerfile_builds_with_the_legacy_builder() -> None:
    """Production builds the image from the Dagster container, whose Debian
    docker.io 20.10 CLI has no buildx and runs the legacy builder. BuildKit-only
    syntax would build fine here and fail only there, on the first agent run
    after a deploy."""

    dockerfile = DEFAULT_AGENT_DOCKERFILE_PATH.read_text(encoding="utf-8")
    instructions = [line for line in dockerfile.splitlines() if line and not line.startswith("#")]

    assert not any(line.lower().startswith("# syntax") for line in dockerfile.splitlines())
    assert not any("--mount=" in line or "--link" in line or "<<" in line for line in instructions)


def test_the_cli_dependencies_compile_in_a_layer_that_cli_edits_do_not_invalidate() -> None:
    """Almost all of a cold `go build` of pdw is std and third-party code
    (2026-11-25 on mew-dev: 21.7 s cold, 1.6 s after a CLI edit). The build
    stage compiles that from the CLI's third-party package list before it
    copies app/, so a CLI edit recompiles only the module's own packages."""

    dockerfile = DEFAULT_AGENT_DOCKERFILE_PATH.read_text(encoding="utf-8")
    stages = re.split(r"^FROM ", dockerfile, flags=re.MULTILINE)[1:]
    build_stage = next(stage for stage in stages if stage.split()[2:3] == ["pdw-cli"])
    lines = [line for line in build_stage.splitlines() if line.startswith(("COPY", "RUN"))]

    warm = next(index for index, line in enumerate(lines) if "go build" in line and "/deps.txt" in line)
    copy_source = next(index for index, line in enumerate(lines) if line.startswith("COPY app/ "))
    copies_before_warm = [line for line in lines[:warm] if line.startswith("COPY")]

    assert warm < copy_source
    assert all("app/go.mod" in line or "--from=" in line for line in copies_before_warm)
    assert "./cmd/pdw-cli" in lines[-1] and "go build" in lines[-1]

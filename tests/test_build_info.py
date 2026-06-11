from __future__ import annotations

import json

from personal_data_warehouse import build_info


def test_git_sha_prefers_runtime_env(monkeypatch, tmp_path) -> None:
    path = tmp_path / "build-info.json"
    path.write_text(json.dumps({"git_sha": "baked"}), encoding="utf-8")
    monkeypatch.setattr(build_info, "BUILD_INFO_PATH", path)
    monkeypatch.setenv("PDW_GIT_SHA", "runtime")

    assert build_info.git_sha() == "runtime"


def test_git_sha_reads_baked_build_info(monkeypatch, tmp_path) -> None:
    path = tmp_path / "build-info.json"
    path.write_text(json.dumps({"git_sha": "baked"}), encoding="utf-8")
    monkeypatch.setattr(build_info, "BUILD_INFO_PATH", path)
    for name in (
        "PDW_GIT_SHA",
        "SOURCE_COMMIT",
        "GIT_SHA",
        "GIT_COMMIT",
        "COMMIT_SHA",
        "COOLIFY_GIT_COMMIT",
    ):
        monkeypatch.delenv(name, raising=False)

    assert build_info.git_sha() == "baked"

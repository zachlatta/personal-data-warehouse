from __future__ import annotations

from collections.abc import Mapping, Sequence
import base64
import json
import os
from urllib.parse import urlparse
import subprocess
import time
import urllib.error
import urllib.request

from dagster import ConfigurableResource
from pydantic import PrivateAttr


class OllamaResource(ConfigurableResource):
    base_url: str = "http://127.0.0.1:11435"
    request_timeout_seconds: int = 120
    pull_timeout_seconds: int = 900
    startup_timeout_seconds: int = 30
    auto_start: bool = True
    binary_path: str = "ollama"
    models_dir: str = "/app/.ollama/models"

    _server_process: subprocess.Popen | None = PrivateAttr(default=None)

    @property
    def normalized_base_url(self) -> str:
        return self.base_url.rstrip("/")

    def ensure_model(self, model: str, *, pull: bool = True) -> None:
        self.ensure_server()
        if self.model_available(model):
            return
        if not pull:
            raise RuntimeError(f"Ollama model {model!r} is not available")
        self.pull_model(model)

    def ensure_server(self) -> None:
        if self.server_available():
            return
        if not self.auto_start:
            raise RuntimeError(f"Ollama is not reachable at {self.normalized_base_url}")
        self.start_server()
        deadline = time.monotonic() + self.startup_timeout_seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                if self.server_available():
                    return
            except Exception as exc:
                last_error = exc
            time.sleep(0.5)
        raise RuntimeError(f"Ollama did not start at {self.normalized_base_url}: {last_error or 'timeout'}")

    def server_available(self) -> bool:
        try:
            self._get_json("/api/tags", timeout_seconds=self.request_timeout_seconds)
        except Exception:
            return False
        return True

    def start_server(self) -> None:
        if self._server_process is not None and self._server_process.poll() is None:
            return
        env = dict(os.environ)
        env["OLLAMA_HOST"] = ollama_host_from_base_url(self.normalized_base_url)
        if self.models_dir:
            env["OLLAMA_MODELS"] = self.models_dir
            os.makedirs(self.models_dir, exist_ok=True)
        self._server_process = subprocess.Popen(
            [self.binary_path, "serve"],
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def teardown_after_execution(self, _context) -> None:
        if self._server_process is None or self._server_process.poll() is not None:
            return
        self._server_process.terminate()
        try:
            self._server_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._server_process.kill()
            self._server_process.wait(timeout=10)

    def model_available(self, model: str) -> bool:
        payload = self._get_json("/api/tags", timeout_seconds=self.request_timeout_seconds)
        model_names = {
            str(item.get("name", ""))
            for item in payload.get("models", [])
            if isinstance(item, Mapping)
        }
        if model in model_names:
            return True
        model_without_latest = model.removesuffix(":latest")
        return any(name == f"{model_without_latest}:latest" for name in model_names)

    def pull_model(self, model: str) -> None:
        payload = self._post_json(
            "/api/pull",
            payload={
                "model": model,
                "stream": False,
            },
            timeout_seconds=self.pull_timeout_seconds,
        )
        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))

    def generate(
        self,
        *,
        model: str,
        prompt: str,
        images: Sequence[bytes] = (),
        format: str | None = None,
        options: Mapping[str, object] | None = None,
        think: bool = False,
        timeout_seconds: int | None = None,
    ) -> str:
        payload: dict[str, object] = {
            "model": model,
            "prompt": prompt,
            "images": [base64.b64encode(image).decode("ascii") for image in images],
            "stream": False,
            "think": think,
        }
        if format is not None:
            payload["format"] = format
        if options is not None:
            payload["options"] = dict(options)
        response = self._post_json(
            "/api/generate",
            payload=payload,
            timeout_seconds=timeout_seconds or self.request_timeout_seconds,
        )
        return str(response.get("response", ""))

    def _get_json(self, path: str, *, timeout_seconds: int) -> dict[str, object]:
        request = urllib.request.Request(f"{self.normalized_base_url}{path}")
        return self._open_json(request, timeout_seconds=timeout_seconds)

    def _post_json(
        self,
        path: str,
        *,
        payload: Mapping[str, object],
        timeout_seconds: int,
    ) -> dict[str, object]:
        request = urllib.request.Request(
            f"{self.normalized_base_url}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        return self._open_json(request, timeout_seconds=timeout_seconds)

    def _open_json(self, request: urllib.request.Request, *, timeout_seconds: int) -> dict[str, object]:
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                payload = json.loads(response.read())
        except urllib.error.URLError as exc:
            raise RuntimeError(str(exc)) from exc
        if isinstance(payload, dict):
            return payload
        raise RuntimeError("Ollama returned a non-object JSON response")


def ollama_host_from_base_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeError(f"invalid Ollama base URL: {base_url}")
    return parsed.netloc

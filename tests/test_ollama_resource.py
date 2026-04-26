from __future__ import annotations

import io
import json
import subprocess

from personal_data_warehouse.ollama_resource import OllamaResource


class FakeUrlopen:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return FakeHttpResponse(response)


class FakeHttpResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return io.BytesIO(self._body)

    def __exit__(self, exc_type, exc, traceback):
        return False


class FakeProcess:
    def __init__(self) -> None:
        self.terminated = False
        self.waited = False

    def poll(self):
        return None

    def terminate(self) -> None:
        self.terminated = True

    def wait(self, timeout=None) -> None:
        self.waited = True


def test_ollama_resource_ensure_model_skips_pull_when_model_exists(monkeypatch) -> None:
    fake_urlopen = FakeUrlopen(
        [
            {"models": [{"name": "gemma4:e2b"}]},
            {"models": [{"name": "gemma4:e2b"}]},
        ]
    )
    monkeypatch.setattr("personal_data_warehouse.ollama_resource.urllib.request.urlopen", fake_urlopen)

    resource = OllamaResource(base_url="http://ollama.local:11435", request_timeout_seconds=12)

    resource.ensure_model("gemma4:e2b", pull=True)

    assert len(fake_urlopen.requests) == 2
    request, timeout = fake_urlopen.requests[0]
    assert request.full_url == "http://ollama.local:11435/api/tags"
    assert timeout == 12


def test_ollama_resource_ensure_model_pulls_missing_model(monkeypatch) -> None:
    fake_urlopen = FakeUrlopen(
        [
            {"models": []},
            {"models": []},
            {"status": "success"},
        ]
    )
    monkeypatch.setattr("personal_data_warehouse.ollama_resource.urllib.request.urlopen", fake_urlopen)

    resource = OllamaResource(
        base_url="http://ollama.local:11435/",
        request_timeout_seconds=12,
        pull_timeout_seconds=34,
    )

    resource.ensure_model("gemma4:e2b", pull=True)

    assert [request.full_url for request, _timeout in fake_urlopen.requests] == [
        "http://ollama.local:11435/api/tags",
        "http://ollama.local:11435/api/tags",
        "http://ollama.local:11435/api/pull",
    ]
    pull_request, pull_timeout = fake_urlopen.requests[2]
    assert pull_timeout == 34
    assert json.loads(pull_request.data) == {"model": "gemma4:e2b", "stream": False}


def test_ollama_resource_ensure_model_starts_local_server_when_unavailable(monkeypatch) -> None:
    fake_urlopen = FakeUrlopen(
        [
            RuntimeError("connection refused"),
            {"models": [{"name": "gemma4:e2b"}]},
            {"models": [{"name": "gemma4:e2b"}]},
        ]
    )
    process = FakeProcess()
    popen_calls = []

    def fake_popen(args, **kwargs):
        popen_calls.append((args, kwargs))
        return process

    monkeypatch.setattr("personal_data_warehouse.ollama_resource.urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("personal_data_warehouse.ollama_resource.subprocess.Popen", fake_popen)
    monkeypatch.setattr("personal_data_warehouse.ollama_resource.time.sleep", lambda _seconds: None)

    resource = OllamaResource(
        base_url="http://127.0.0.1:11435",
        request_timeout_seconds=12,
        startup_timeout_seconds=2,
        models_dir="/tmp/ollama-models",
    )

    resource.ensure_model("gemma4:e2b", pull=True)
    resource.teardown_after_execution(None)

    assert popen_calls
    args, kwargs = popen_calls[0]
    assert args == ["ollama", "serve"]
    assert kwargs["env"]["OLLAMA_HOST"] == "127.0.0.1:11435"
    assert kwargs["env"]["OLLAMA_MODELS"] == "/tmp/ollama-models"
    assert kwargs["stdout"] == subprocess.DEVNULL
    assert process.terminated is True


def test_ollama_resource_generate_posts_images_prompt_and_options(monkeypatch) -> None:
    fake_urlopen = FakeUrlopen([{"response": "ok"}])
    monkeypatch.setattr("personal_data_warehouse.ollama_resource.urllib.request.urlopen", fake_urlopen)

    resource = OllamaResource(base_url="http://ollama.local:11435", request_timeout_seconds=12)

    response = resource.generate(
        model="gemma4:e2b",
        prompt="describe this",
        images=[b"fake image"],
        options={"temperature": 0},
        think=False,
        timeout_seconds=56,
    )

    assert response == "ok"
    request, timeout = fake_urlopen.requests[0]
    assert request.full_url == "http://ollama.local:11435/api/generate"
    assert timeout == 56
    assert json.loads(request.data) == {
        "model": "gemma4:e2b",
        "prompt": "describe this",
        "images": ["ZmFrZSBpbWFnZQ=="],
        "stream": False,
        "think": False,
        "options": {"temperature": 0},
    }

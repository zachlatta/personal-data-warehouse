from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_PATH = REPO_ROOT / "embeddings" / "docker-compose.yaml"


def _service() -> dict:
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    assert set(compose["services"]) == {"embeddings"}
    return compose["services"]["embeddings"]


def test_embeddings_compose_pins_the_production_image_and_model() -> None:
    service = _service()

    assert service["image"] == (
        "ghcr.io/huggingface/text-embeddings-inference:86-1.8"
        "@sha256:65f792e790f976713a5d2ab2586d93d074203d1f0ec2045e87e60113fbd0e256"
    )
    assert service["command"] == [
        "--model-id",
        "Qwen/Qwen3-Embedding-4B",
        "--auto-truncate",
        "--max-client-batch-size",
        "256",
        "--max-batch-tokens",
        "16384",
    ]


def test_embeddings_compose_preserves_gpu_cache_and_private_port_contract() -> None:
    service = _service()

    assert service["deploy"]["resources"]["reservations"]["devices"] == [
        {"driver": "nvidia", "count": 1, "capabilities": ["gpu"]}
    ]
    assert service["volumes"] == [
        "${EMBEDDINGS_CACHE_PATH:-/opt/pdw-embeddings/hf-cache}:/data"
    ]
    assert service["ports"] == [
        "${EMBEDDINGS_BIND_ADDRESS:?set EMBEDDINGS_BIND_ADDRESS to mew's tailnet IP}:"
        "${EMBEDDINGS_PORT:-8485}:80"
    ]
    assert service["environment"] == {
        "HUGGINGFACE_HUB_CACHE": "/data",
        "NVIDIA_DRIVER_CAPABILITIES": "compute,utility",
        "NVIDIA_VISIBLE_DEVICES": "all",
    }


def test_embeddings_compose_has_a_slow_start_aware_healthcheck() -> None:
    healthcheck = _service()["healthcheck"]

    assert healthcheck["test"] == [
        "CMD",
        "curl",
        "--fail",
        "--silent",
        "--show-error",
        "http://127.0.0.1/health",
    ]
    assert healthcheck["start_period"] == "10m"
    assert healthcheck["interval"] == "30s"
    assert healthcheck["timeout"] == "5s"
    assert healthcheck["retries"] == 5

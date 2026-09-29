"""Model providers: a common interface, typed transport errors, and OpenAI-compatible clients
(Ollama, OpenAI, Azure OpenAI, Foundry Local, LM Studio all speak /v1/chat/completions)."""
from __future__ import annotations

import time
from dataclasses import dataclass


class ProviderError(Exception):
    retryable = False


class ProviderTimeout(ProviderError):
    retryable = True


class ProviderRateLimited(ProviderError):
    retryable = True

    def __init__(self, message: str = "rate limited", retry_after: float = 1.0):
        super().__init__(message)
        self.retry_after = retry_after


class ProviderUnavailable(ProviderError):
    retryable = True


class ProviderBadRequest(ProviderError):
    retryable = False


@dataclass
class ProviderResult:
    text: str
    input_tokens: int
    output_tokens: int
    model: str
    latency_ms: float


class OpenAICompatProvider:
    def __init__(self, name: str, cfg: dict, secrets, timeout_s: float):
        self.name = name
        self.cfg = cfg
        self.secrets = secrets
        self.timeout_s = timeout_s
        self._client = None

    def _get_client(self):
        if self._client is None:
            import openai

            if self.cfg.get("type") == "azure_openai":
                self._client = openai.AzureOpenAI(azure_endpoint=self.secrets.resolve(self.cfg.get("endpoint")),
                                                  api_key=self.secrets.resolve(self.cfg.get("api_key")),
                                                  api_version=self.cfg.get("api_version", "2024-10-21"),
                                                  timeout=self.timeout_s, max_retries=0)
            else:
                self._client = openai.OpenAI(base_url=self.cfg.get("base_url"), api_key=self.secrets.resolve(self.cfg.get("api_key")) or "none",
                                             timeout=self.timeout_s, max_retries=0)
        return self._client

    def complete(self, model_name: str, messages: list[dict], *, temperature: float, max_tokens: int,
                 json_mode: bool, seed: int | None = None) -> ProviderResult:
        import openai

        kwargs = {"model": model_name, "messages": messages, "temperature": temperature, "max_tokens": max_tokens}
        if json_mode and self.cfg.get("json_mode", True):
            kwargs["response_format"] = {"type": "json_object"}
        if seed is not None:
            kwargs["seed"] = seed
        t0 = time.perf_counter()
        try:
            resp = self._get_client().chat.completions.create(**kwargs)
        except openai.APITimeoutError as exc:
            raise ProviderTimeout(str(exc)) from exc
        except openai.RateLimitError as exc:
            raise ProviderRateLimited(str(exc), retry_after=2.0) from exc
        except openai.APIConnectionError as exc:
            raise ProviderUnavailable(f"connection failed: {exc}") from exc
        except openai.APIStatusError as exc:
            if exc.status_code >= 500:
                raise ProviderUnavailable(f"HTTP {exc.status_code}: {exc}") from exc
            raise ProviderBadRequest(f"HTTP {exc.status_code}: {exc}") from exc
        except Exception as exc:  # noqa: BLE001
            raise ProviderUnavailable(f"{type(exc).__name__}: {exc}") from exc
        latency = (time.perf_counter() - t0) * 1000
        text = resp.choices[0].message.content or ""
        usage = getattr(resp, "usage", None)
        return ProviderResult(text, getattr(usage, "prompt_tokens", 0) or 0, getattr(usage, "completion_tokens", 0) or 0,
                              getattr(resp, "model", model_name), latency)


def make_provider(name: str, cfg: dict, svc):
    kind = cfg.get("type")
    if kind == "mock":
        from swarmpipe.llm.mock import MockProvider

        return MockProvider(name, svc)
    if kind in ("openai_compat", "azure_openai"):
        return OpenAICompatProvider(name, cfg, svc.secrets, svc.settings.llm.request_timeout_s)
    raise ValueError(f"unknown provider type {kind!r} for provider {name}")

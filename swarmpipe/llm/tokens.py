"""Token estimation. Real providers report usage; this estimate is used for the simulated models,
for budget pre-checks and for context-window guards. Heuristic by default (chars/4, a common
rule of thumb for English text); set SWARMPIPE_TOKENIZER=tiktoken to use
the cl100k_base tokenizer (slower start-up)."""
from __future__ import annotations

import os

_enc = None
_tried = False


def count_tokens(text: str) -> int:
    global _enc, _tried
    if not text:
        return 0
    if os.environ.get("SWARMPIPE_TOKENIZER") == "tiktoken":
        if not _tried:
            _tried = True
            try:
                import tiktoken

                _enc = tiktoken.get_encoding("cl100k_base")
            except Exception:  # noqa: BLE001
                _enc = None
        if _enc is not None:
            return len(_enc.encode(text))
    return max(1, round(len(text) / 4))


def count_messages(messages: list[dict]) -> int:
    return sum(count_tokens(m.get("content", "")) + 4 for m in messages)

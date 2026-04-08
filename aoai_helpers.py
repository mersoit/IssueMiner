# -*- coding: utf-8 -*-
"""
aoai_helpers  –  Centralized Azure OpenAI helpers
==================================================
Provides:
  • Thread-safe sliding-window rate limiter (RPM + TPM)
  • Automatic retry with exponential back-off on 429 / rate-limit errors
  • Shared AzureOpenAI client factories (nano, mini, GPT-5.2, pro, grok)
  • Shared SQL connection factory
  • Common utility functions used across phases
  • ProWorkerPool: slow-start thread pool for GPT-5 Pro calls

Model tiers:
  Nano     – Phase 1A, 1B-refine, 1C, 2E, 2F
  Mini     – Phase 1B-propose, 4A-pass1, 4A-pass2
  GPT-5.2  – Phase 3-emergent, 4B
  Pro      – Phase 3-leaf, 4C
  Grok 4.1 – Phase 4D
"""

import os
import re
import json
import time
import math
import random
import logging
import hashlib
import threading
import datetime as dt
from collections import deque
from typing import Any, Callable, Dict, List, Optional, Tuple

import pyodbc
from openai import AzureOpenAI

# ─────────────────────────────────────────────────────────
# Configuration defaults  (override via env vars)
# ─────────────────────────────────────────────────────────

# Rate-limit budgets – GPT-5.2 tier  (10k RPM / 1M TPM)
_DEFAULT_GPT52_RPM = int(os.getenv("AOAI_RPM_LIMIT", "9000"))  # keep ~10 % headroom
_DEFAULT_GPT52_TPM = int(os.getenv("AOAI_TPM_LIMIT", "900000"))  # keep ~10 % headroom

# Rate-limit budgets – Nano tier  (5k RPM / 5M TPM)
_DEFAULT_NANO_RPM = int(os.getenv("AOAI_NANO_RPM_LIMIT", "4500"))  # keep ~10 % headroom
_DEFAULT_NANO_TPM = int(os.getenv("AOAI_NANO_TPM_LIMIT", "4500000"))  # keep ~10 % headroom

# Rate-limit budgets – Mini tier  (1k RPM / 1M TPM)
_DEFAULT_MINI_RPM = int(os.getenv("AOAI_MINI_RPM_LIMIT", "900"))  # keep ~10 % headroom
_DEFAULT_MINI_TPM = int(os.getenv("AOAI_MINI_TPM_LIMIT", "900000"))  # keep ~10 % headroom

# Rate-limit budgets – Pro tier  (1.6k RPM / 160k TPM)
_DEFAULT_PRO_RPM = int(os.getenv("AOAI_PRO_RPM_LIMIT", "1400"))  # keep ~10 % headroom
_DEFAULT_PRO_TPM = int(os.getenv("AOAI_PRO_TPM_LIMIT", "144000"))  # keep ~10 % headroom

# Rate-limit budgets – Grok 4.1 tier  (1k RPM / 1M TPM)
_DEFAULT_GROK_RPM = int(os.getenv("AOAI_GROK_RPM_LIMIT", "900"))  # keep ~10 % headroom
_DEFAULT_GROK_TPM = int(os.getenv("AOAI_GROK_TPM_LIMIT", "900000"))  # keep ~10 % headroom

# Legacy aliases (used by get_rate_limiter() for backward compat → GPT-5.2)
_DEFAULT_RPM = _DEFAULT_GPT52_RPM
_DEFAULT_TPM = _DEFAULT_GPT52_TPM

# Retry behaviour
_MAX_RETRIES = int(os.getenv("AOAI_CALL_MAX_RETRIES", "5"))
_RETRY_BASE_DELAY = float(os.getenv("AOAI_RETRY_BASE_DELAY", "2.0"))  # seconds
_RETRY_MAX_DELAY = float(os.getenv("AOAI_RETRY_MAX_DELAY", "120.0"))  # seconds
_RETRY_JITTER = float(os.getenv("AOAI_RETRY_JITTER", "0.5"))  # ±fraction
_RETRY_COOLDOWN_DELAY = float(os.getenv("AOAI_RETRY_COOLDOWN_DELAY", "65.0"))  # final retry after cooldown

# Token estimation (fast approximation: ~4 chars per token)
_CHARS_PER_TOKEN = int(os.getenv("AOAI_CHARS_PER_TOKEN", "4"))

# Pro slow-start defaults
_PRO_INITIAL_WORKERS = int(os.getenv("PRO_INITIAL_WORKERS", "4"))
_PRO_MAX_WORKERS = int(os.getenv("PRO_MAX_WORKERS", "8"))
_PRO_RAMP_INTERVAL = float(os.getenv("PRO_RAMP_INTERVAL_SECS", "70"))  # seconds between ramp-ups
_PRO_RAMP_STEP = int(os.getenv("PRO_RAMP_STEP", "4"))  # add N workers per ramp


# ─────────────────────────────────────────────────────────
# Thread-safe sliding-window Rate Limiter
# ─────────────────────────────────────────────────────────

class RateLimiter:
    """
    Thread-safe RPM / TPM rate limiter with a true 60-second sliding window.

    Uses a deque of (timestamp, token_count) entries. Old entries are evicted
    as they fall outside the 60 s window, freeing budget immediately rather
    than waiting for the entire window to reset.

    Waiting threads are woken via ``threading.Event`` as soon as any entry
    expires, avoiding the stale-sleep problem of the old tumbling-window
    implementation.
    """

    _WINDOW = 60.0  # seconds

    def __init__(self, max_rpm: int = _DEFAULT_RPM, max_tpm: int = _DEFAULT_TPM):
        self.max_rpm = max(1, int(max_rpm))
        self.max_tpm = max(1, int(max_tpm))
        self._lock = threading.Lock()
        self._budget_freed = threading.Event()
        self._entries: deque = deque()  # (monotonic_timestamp, token_count)
        self._req_count = 0
        self._tok_count = 0

    def _evict_expired(self, now: float) -> None:
        cutoff = now - self._WINDOW
        while self._entries and self._entries[0][0] <= cutoff:
            _, tokens = self._entries.popleft()
            self._req_count -= 1
            self._tok_count -= int(tokens)

    def _seconds_until_next_eviction(self, now: float) -> float:
        if not self._entries:
            return 0.0
        oldest_ts = self._entries[0][0]
        return max(0.05, (oldest_ts + self._WINDOW) - now + 0.05)

    def wait_if_needed(self, estimated_tokens: int = 0) -> float:
        total_slept = 0.0
        estimated_tokens = max(0, int(estimated_tokens or 0))

        while True:
            with self._lock:
                now = time.monotonic()
                self._evict_expired(now)

                if (self._req_count + 1 <= self.max_rpm) and (self._tok_count + estimated_tokens <= self.max_tpm):
                    self._req_count += 1
                    self._tok_count += estimated_tokens
                    self._entries.append((now, estimated_tokens))
                    return total_slept

                sleep_for = self._seconds_until_next_eviction(now)

                if total_slept == 0.0:
                    logging.info(
                        "RateLimiter: budget exhausted (rpm=%d/%d, tpm=%d/%d). Waiting ~%.1f s.",
                        self._req_count, self.max_rpm,
                        self._tok_count, self.max_tpm,
                        sleep_for,
                    )

                self._budget_freed.clear()

            self._budget_freed.wait(timeout=sleep_for)
            total_slept += sleep_for

    def notify_budget_freed(self) -> None:
        self._budget_freed.set()


# Module-level singletons (shared by all phases in the same process)
_rate_limiter_gpt52: Optional[RateLimiter] = None
_rate_limiter_nano: Optional[RateLimiter] = None
_rate_limiter_mini: Optional[RateLimiter] = None
_rate_limiter_pro: Optional[RateLimiter] = None
_rate_limiter_grok: Optional[RateLimiter] = None
_rate_limiter_lock = threading.Lock()


def get_rate_limiter(kind: str = "gpt52") -> RateLimiter:
    """Return (or create) the process-wide rate limiter for the given model tier."""
    global _rate_limiter_gpt52, _rate_limiter_nano, _rate_limiter_mini, _rate_limiter_pro, _rate_limiter_grok
    kind = (kind or "gpt52").strip().lower()

    if kind == "nano":
        if _rate_limiter_nano is not None:
            return _rate_limiter_nano
        with _rate_limiter_lock:
            if _rate_limiter_nano is None:
                _rate_limiter_nano = RateLimiter(max_rpm=_DEFAULT_NANO_RPM, max_tpm=_DEFAULT_NANO_TPM)
        return _rate_limiter_nano

    if kind == "mini":
        if _rate_limiter_mini is not None:
            return _rate_limiter_mini
        with _rate_limiter_lock:
            if _rate_limiter_mini is None:
                _rate_limiter_mini = RateLimiter(max_rpm=_DEFAULT_MINI_RPM, max_tpm=_DEFAULT_MINI_TPM)
        return _rate_limiter_mini

    if kind == "pro":
        if _rate_limiter_pro is not None:
            return _rate_limiter_pro
        with _rate_limiter_lock:
            if _rate_limiter_pro is None:
                _rate_limiter_pro = RateLimiter(max_rpm=_DEFAULT_PRO_RPM, max_tpm=_DEFAULT_PRO_TPM)
        return _rate_limiter_pro

    if kind == "grok":
        if _rate_limiter_grok is not None:
            return _rate_limiter_grok
        with _rate_limiter_lock:
            if _rate_limiter_grok is None:
                _rate_limiter_grok = RateLimiter(max_rpm=_DEFAULT_GROK_RPM, max_tpm=_DEFAULT_GROK_TPM)
        return _rate_limiter_grok

    # default: gpt52 / legacy
    if _rate_limiter_gpt52 is not None:
        return _rate_limiter_gpt52
    with _rate_limiter_lock:
        if _rate_limiter_gpt52 is None:
            _rate_limiter_gpt52 = RateLimiter(max_rpm=_DEFAULT_GPT52_RPM, max_tpm=_DEFAULT_GPT52_TPM)
    return _rate_limiter_gpt52


# ─────────────────────────────────────────────────────────
# Token estimation
# ─────────────────────────────────────────────────────────

def estimate_tokens(text: str) -> int:
    """Fast token estimate (chars / 4). Rounds up, minimum 1."""
    return max(1, math.ceil(len(text or "") / _CHARS_PER_TOKEN))


def estimate_prompt_tokens(*parts: str) -> int:
    """Estimate combined token count for multiple text parts."""
    return sum(estimate_tokens(p) for p in parts)


# ─────────────────────────────────────────────────────────
# Error helpers
# ─────────────────────────────────────────────────────────

def _is_rate_limit_error(exc: Exception) -> bool:
    exc_str = str(exc).lower()
    if "429" in exc_str or "rate" in exc_str or "throttl" in exc_str:
        return True
    status = getattr(exc, "status_code", None)
    if status == 429:
        return True
    resp = getattr(exc, "response", None)
    if resp is not None and getattr(resp, "status_code", None) == 429:
        return True
    return False


def _is_transient_error(exc: Exception) -> bool:
    exc_str = str(exc).lower()
    if any(tok in exc_str for tok in ("timeout", "timed out", "connection", "502", "503", "504")):
        return True
    status = getattr(exc, "status_code", None)
    if status is not None and 500 <= int(status) < 600:
        return True
    resp = getattr(exc, "response", None)
    if resp is not None:
        code = getattr(resp, "status_code", None)
        if code is not None and 500 <= int(code) < 600:
            return True
    return False


def _parse_retry_after(exc: Exception) -> Optional[float]:
    resp = getattr(exc, "response", None)
    if resp is None:
        return None
    headers = getattr(resp, "headers", None)
    if headers is None:
        return None

    for hdr in ("retry-after", "Retry-After", "retry-after-ms", "x-ratelimit-reset-requests"):
        val = headers.get(hdr) if hasattr(headers, "get") else None
        if val is None:
            continue
        try:
            v = float(val)
            if "ms" in hdr.lower():
                return v / 1000.0
            return v
        except (ValueError, TypeError):
            pass
    return None


def extract_aoai_error(e: Exception) -> str:
    resp = getattr(e, "response", None)
    if resp is not None:
        try:
            txt = getattr(resp, "text", None)
            if txt:
                return txt
        except Exception:
            pass
        try:
            content = getattr(resp, "content", None)
            if content:
                if isinstance(content, (bytes, bytearray)):
                    return content.decode("utf-8", errors="replace")
                return str(content)
        except Exception:
            pass
    return str(e)


# ─────────────────────────────────────────────────────────
# Retry wrappers
# ─────────────────────────────────────────────────────────

def call_aoai_with_retry(
    client: AzureOpenAI,
    *,
    model: str,
    messages: List[Dict[str, str]],
    response_format: Optional[Dict[str, str]] = None,
    max_completion_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    estimated_prompt_tokens: int = 0,
    rate_limiter: Optional[RateLimiter] = None,
    max_retries: int = _MAX_RETRIES,
    caller_tag: str = "",
) -> Any:
    """
    Call client.chat.completions.create with:
      1) pre-call rate limiting
      2) retry on 429/transient errors

    Intended for tiers that support response_format (e.g., GPT-5.2, nano, mini).
    """
    if rate_limiter is None:
        rate_limiter = get_rate_limiter()

    if estimated_prompt_tokens <= 0:
        total_chars = sum(len(m.get("content", "")) for m in messages)
        estimated_prompt_tokens = max(1, math.ceil(total_chars / _CHARS_PER_TOKEN))

    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        rate_limiter.wait_if_needed(estimated_prompt_tokens)

        try:
            kwargs: Dict[str, Any] = {"model": model, "messages": messages}
            if response_format is not None:
                kwargs["response_format"] = response_format
            if max_completion_tokens is not None:
                kwargs["max_completion_tokens"] = max_completion_tokens
            if temperature is not None:
                kwargs["temperature"] = temperature

            return client.chat.completions.create(**kwargs)

        except Exception as exc:
            last_exc = exc
            is_rate = _is_rate_limit_error(exc)
            is_transient = _is_transient_error(exc)

            if not is_rate and not is_transient:
                raise

            retry_after = _parse_retry_after(exc)
            if retry_after is not None and retry_after > 0:
                delay = min(retry_after + 1.0, _RETRY_MAX_DELAY)
            else:
                base = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
                jitter = random.uniform(-_RETRY_JITTER * base, _RETRY_JITTER * base)
                delay = min(base + jitter, _RETRY_MAX_DELAY)

            if attempt < max_retries:
                logging.warning(
                    "[%s] AOAI %s (attempt %d/%d). Retrying in %.1f s. Error: %s",
                    caller_tag or "aoai",
                    "rate_limit" if is_rate else "transient_error",
                    attempt, max_retries,
                    delay,
                    str(exc)[:300],
                )
                time.sleep(delay)
            else:
                logging.warning(
                    "[%s] AOAI %s – all %d retries exhausted. Final cooldown retry in %.0f s. Error: %s",
                    caller_tag or "aoai",
                    "rate_limit" if is_rate else "transient_error",
                    max_retries,
                    _RETRY_COOLDOWN_DELAY,
                    str(exc)[:500],
                )

    # Final cooldown retry: wait long enough for minute-level rate limits to reset
    if last_exc is not None:
        time.sleep(_RETRY_COOLDOWN_DELAY)
        rate_limiter.wait_if_needed(estimated_prompt_tokens)
        try:
            kwargs_final: Dict[str, Any] = {"model": model, "messages": messages}
            if response_format is not None:
                kwargs_final["response_format"] = response_format
            if max_completion_tokens is not None:
                kwargs_final["max_completion_tokens"] = max_completion_tokens
            if temperature is not None:
                kwargs_final["temperature"] = temperature
            return client.chat.completions.create(**kwargs_final)
        except Exception as final_exc:
            logging.error(
                "[%s] AOAI final cooldown retry also failed. Error: %s",
                caller_tag or "aoai",
                str(final_exc)[:500],
            )
            raise final_exc from last_exc

    raise last_exc  # type: ignore[misc]


def call_aoai_chat_with_retry(
    client: AzureOpenAI,
    *,
    model: str,
    messages: List[Dict[str, str]],
    max_completion_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    estimated_prompt_tokens: int = 0,
    rate_limiter: Optional[RateLimiter] = None,
    max_retries: int = _MAX_RETRIES,
    caller_tag: str = "",
    response_format: Optional[Dict[str, str]] = None,
    allow_response_format: bool = True,
) -> Any:
    """Chat call wrapper with optional suppression of response_format.

    Use this when calling GPT-5 Pro deployments where JSON mode
    (response_format) may not be supported.
    """
    if rate_limiter is None:
        rate_limiter = get_rate_limiter()

    if estimated_prompt_tokens <= 0:
        total_chars = sum(len(m.get("content", "")) for m in messages)
        estimated_prompt_tokens = max(1, math.ceil(total_chars / _CHARS_PER_TOKEN))

    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        rate_limiter.wait_if_needed(estimated_prompt_tokens)

        try:
            kwargs: Dict[str, Any] = {"model": model, "messages": messages}
            if max_completion_tokens is not None:
                kwargs["max_completion_tokens"] = max_completion_tokens
            if temperature is not None:
                kwargs["temperature"] = temperature
            if allow_response_format and response_format is not None:
                kwargs["response_format"] = response_format

            return client.chat.completions.create(**kwargs)

        except Exception as exc:
            last_exc = exc
            is_rate = _is_rate_limit_error(exc)
            is_transient = _is_transient_error(exc)

            if not is_rate and not is_transient:
                raise

            retry_after = _parse_retry_after(exc)
            if retry_after is not None and retry_after > 0:
                delay = min(retry_after + 1.0, _RETRY_MAX_DELAY)
            else:
                base = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
                jitter = random.uniform(-_RETRY_JITTER * base, _RETRY_JITTER * base)
                delay = min(base + jitter, _RETRY_MAX_DELAY)

            if attempt < max_retries:
                logging.warning(
                    "[%s] AOAI %s (attempt %d/%d). Retrying in %.1f s. Error: %s",
                    caller_tag or "aoai",
                    "rate_limit" if is_rate else "transient_error",
                    attempt, max_retries, delay, str(exc)[:300],
                )
                time.sleep(delay)
            else:
                logging.warning(
                    "[%s] AOAI %s – all %d retries exhausted. Final cooldown retry in %.0f s. Error: %s",
                    caller_tag or "aoai",
                    "rate_limit" if is_rate else "transient_error",
                    max_retries, _RETRY_COOLDOWN_DELAY, str(exc)[:500],
                )

    # Final cooldown retry: wait long enough for minute-level rate limits to reset
    if last_exc is not None:
        time.sleep(_RETRY_COOLDOWN_DELAY)
        rate_limiter.wait_if_needed(estimated_prompt_tokens)
        try:
            kwargs_final: Dict[str, Any] = {"model": model, "messages": messages}
            if max_completion_tokens is not None:
                kwargs_final["max_completion_tokens"] = max_completion_tokens
            if temperature is not None:
                kwargs_final["temperature"] = temperature
            if allow_response_format and response_format is not None:
                kwargs_final["response_format"] = response_format
            return client.chat.completions.create(**kwargs_final)
        except Exception as final_exc:
            logging.error(
                "[%s] AOAI final cooldown retry also failed. Error: %s",
                caller_tag or "aoai",
                str(final_exc)[:500],
            )
            raise final_exc from last_exc

    raise last_exc  # type: ignore[misc]


# ─────────────────────────────────────────────────────────
# Shared AzureOpenAI client factories  (thread-safe singletons)
# ─────────────────────────────────────────────────────────

_NANO_CLIENT: Optional[AzureOpenAI] = None
_MINI_CLIENT: Optional[AzureOpenAI] = None
_GPT52_CLIENT: Optional[AzureOpenAI] = None
_PRO_CLIENT: Optional[AzureOpenAI] = None
_GROK_CLIENT: Optional[AzureOpenAI] = None
_client_lock = threading.Lock()


def make_nano_client() -> AzureOpenAI:
    """Return (or create) the singleton AzureOpenAI client for Nano."""
    global _NANO_CLIENT
    if _NANO_CLIENT is not None:
        return _NANO_CLIENT
    with _client_lock:
        if _NANO_CLIENT is None:
            _NANO_CLIENT = AzureOpenAI(
                azure_endpoint=os.environ["AOAI_ENDPOINT"].rstrip("/"),
                api_key=os.environ["AOAI_API_KEY"],
                api_version=os.getenv("AOAI_API_VERSION", "2025-01-01-preview"),
                timeout=float(os.getenv("AOAI_TIMEOUT_SECONDS", "120")),
                max_retries=0,
            )
    return _NANO_CLIENT


def get_nano_deployment() -> str:
    dep = (os.environ.get("AOAI_DEPLOYMENT") or "").strip()
    if not dep:
        raise RuntimeError("Missing AOAI_DEPLOYMENT env var.")
    return dep


def make_mini_client() -> AzureOpenAI:
    """Return (or create) the singleton AzureOpenAI client for Mini."""
    global _MINI_CLIENT
    if _MINI_CLIENT is not None:
        return _MINI_CLIENT
    with _client_lock:
        if _MINI_CLIENT is None:
            endpoint = os.environ.get("AOAI_ENDPOINT_MINI") or os.environ.get("AOAI_ENDPOINT")
            key = os.environ.get("AOAI_API_KEY_MINI") or os.environ.get("AOAI_API_KEY")
            if not endpoint or not key:
                raise RuntimeError(
                    "Missing AOAI endpoint/key for Mini "
                    "(set AOAI_ENDPOINT_MINI / AOAI_API_KEY_MINI or fall back to AOAI_ENDPOINT)"
                )
            _MINI_CLIENT = AzureOpenAI(
                azure_endpoint=endpoint.rstrip("/"),
                api_key=key,
                api_version=os.getenv(
                    "AOAI_API_VERSION_MINI",
                    os.getenv("AOAI_API_VERSION", "2025-01-01-preview"),
                ),
                timeout=float(os.getenv(
                    "AOAI_TIMEOUT_SECONDS_MINI",
                    os.getenv("AOAI_TIMEOUT_SECONDS", "120"),
                )),
                max_retries=0,
            )
    return _MINI_CLIENT


def get_mini_deployment() -> str:
    dep = (os.environ.get("AOAI_DEPLOYMENT_MINI") or "").strip()
    if not dep:
        raise RuntimeError("Missing AOAI_DEPLOYMENT_MINI env var.")
    return dep


def make_gpt52_client() -> AzureOpenAI:
    """Return (or create) the singleton AzureOpenAI client for GPT-5.2."""
    global _GPT52_CLIENT
    if _GPT52_CLIENT is not None:
        return _GPT52_CLIENT
    with _client_lock:
        if _GPT52_CLIENT is None:
            endpoint = os.environ.get("AOAI_ENDPOINT_GPT52") or os.environ.get("AOAI_ENDPOINT")
            key = os.environ.get("AOAI_API_KEY_GPT52") or os.environ.get("AOAI_API_KEY")
            if not endpoint or not key:
                raise RuntimeError(
                    "Missing AOAI endpoint/key for GPT-5.2 "
                    "(set AOAI_ENDPOINT_GPT52 / AOAI_API_KEY_GPT52)"
                )
            _GPT52_CLIENT = AzureOpenAI(
                azure_endpoint=endpoint.rstrip("/"),
                api_key=key,
                api_version=os.getenv("AOAI_API_VERSION", "2025-01-01-preview"),
                timeout=float(os.getenv("AOAI_TIMEOUT_SECONDS", "120")),
                max_retries=0,
            )
    return _GPT52_CLIENT


def get_gpt52_deployment() -> str:
    dep = (os.environ.get("AOAI_DEPLOYMENT_GPT52") or "").strip()
    if not dep:
        raise RuntimeError("Missing AOAI_DEPLOYMENT_GPT52 – refusing fallback.")
    return dep


def make_pro_client() -> AzureOpenAI:
    """Return (or create) the singleton AzureOpenAI client for Pro."""
    global _PRO_CLIENT
    if _PRO_CLIENT is not None:
        return _PRO_CLIENT
    with _client_lock:
        if _PRO_CLIENT is None:
            endpoint = (
                os.environ.get("AOAI_ENDPOINT_PRO")
                or os.environ.get("AOAI_ENDPOINT_GPT5PRO")
                or os.environ.get("AOAI_ENDPOINT_GPT52")
                or os.environ.get("AOAI_ENDPOINT")
            )
            key = (
                os.environ.get("AOAI_API_KEY_PRO")
                or os.environ.get("AOAI_API_KEY_GPT5PRO")
                or os.environ.get("AOAI_API_KEY_GPT52")
                or os.environ.get("AOAI_API_KEY")
            )
            if not endpoint or not key:
                raise RuntimeError(
                    "Missing AOAI endpoint/key for Pro "
                    "(set AOAI_ENDPOINT_PRO / AOAI_API_KEY_PRO)"
                )
            _PRO_CLIENT = AzureOpenAI(
                azure_endpoint=endpoint.rstrip("/"),
                api_key=key,
                api_version=os.getenv(
                    "AOAI_API_VERSION_PRO",
                    os.getenv(
                        "AOAI_API_VERSION_GPT5PRO",
                        os.getenv("AOAI_API_VERSION", "2025-03-01-preview"),
                    ),
                ),
                timeout=float(os.getenv(
                    "AOAI_TIMEOUT_SECONDS_PRO",
                    os.getenv("AOAI_TIMEOUT_SECONDS_GPT5PRO", "300"),
                )),
                max_retries=0,
            )
    return _PRO_CLIENT


def get_pro_deployment() -> str:
    dep = (os.environ.get("AOAI_DEPLOYMENT_PRO") or os.environ.get("AOAI_DEPLOYMENT_GPT5PRO") or "").strip()
    if not dep:
        raise RuntimeError("Missing AOAI_DEPLOYMENT_PRO (or AOAI_DEPLOYMENT_GPT5PRO) env var.")
    return dep


def make_grok_client() -> AzureOpenAI:
    """Return (or create) the singleton AzureOpenAI client for Grok 4.1."""
    global _GROK_CLIENT
    if _GROK_CLIENT is not None:
        return _GROK_CLIENT
    with _client_lock:
        if _GROK_CLIENT is None:
            endpoint = (
                os.environ.get("AOAI_ENDPOINT_GROK")
                or os.environ.get("AOAI_ENDPOINT_GPT52")
                or os.environ.get("AOAI_ENDPOINT")
            )
            key = (
                os.environ.get("AOAI_API_KEY_GROK")
                or os.environ.get("AOAI_API_KEY_GPT52")
                or os.environ.get("AOAI_API_KEY")
            )
            if not endpoint or not key:
                raise RuntimeError(
                    "Missing AOAI endpoint/key for Grok "
                    "(set AOAI_ENDPOINT_GROK / AOAI_API_KEY_GROK)"
                )
            _GROK_CLIENT = AzureOpenAI(
                azure_endpoint=endpoint.rstrip("/"),
                api_key=key,
                api_version=os.getenv(
                    "AOAI_API_VERSION_GROK",
                    os.getenv("AOAI_API_VERSION", "2025-01-01-preview"),
                ),
                timeout=float(os.getenv(
                    "AOAI_TIMEOUT_SECONDS_GROK",
                    os.getenv("AOAI_TIMEOUT_SECONDS", "120"),
                )),
                max_retries=0,
            )
    return _GROK_CLIENT


def get_grok_deployment() -> str:
    dep = (os.environ.get("AOAI_DEPLOYMENT_GROK") or "").strip()
    if not dep:
        raise RuntimeError("Missing AOAI_DEPLOYMENT_GROK env var.")
    return dep

# Add after the Grok client factory section

_ANTHROPIC_CLIENT: Optional[AzureOpenAI] = None

def make_anthropic_client() -> AzureOpenAI:
    """Return (or create) the singleton AzureOpenAI client for Anthropic models (Sonnet/Opus)."""
    global _ANTHROPIC_CLIENT
    if _ANTHROPIC_CLIENT is not None:
        return _ANTHROPIC_CLIENT
    with _client_lock:
        if _ANTHROPIC_CLIENT is None:
            endpoint = os.environ.get("AOAI_ENDPOINT_ANTHROPIC")
            key = os.environ.get("AOAI_API_KEY_ANTHROPIC")
            if not endpoint or not key:
                raise RuntimeError(
                    "Missing AOAI endpoint/key for Anthropic "
                    "(set AOAI_ENDPOINT_ANTHROPIC / AOAI_API_KEY_ANTHROPIC)"
                )
            _ANTHROPIC_CLIENT = AzureOpenAI(
                azure_endpoint=endpoint.rstrip("/"),
                api_key=key,
                api_version=os.getenv("AOAI_API_VERSION_ANTHROPIC", "2025-04-01-preview"),
                timeout=float(os.getenv("AOAI_TIMEOUT_SECONDS_ANTHROPIC", "300")),
                max_retries=0,
            )
    return _ANTHROPIC_CLIENT


def get_opus_deployment() -> str:
    dep = (os.environ.get("AOAI_DEPLOYMENT_OPUS") or "").strip()
    if not dep:
        raise RuntimeError("Missing AOAI_DEPLOYMENT_OPUS env var.")
    return dep


def get_sonnet_deployment() -> str:
    dep = (os.environ.get("AOAI_DEPLOYMENT_SONNET") or "").strip()
    if not dep:
        raise RuntimeError("Missing AOAI_DEPLOYMENT_SONNET env var.")
    return dep


# Rate limiter for Anthropic tier
_rate_limiter_anthropic: Optional[RateLimiter] = None
_DEFAULT_ANTHROPIC_RPM = int(os.getenv("AOAI_ANTHROPIC_RPM_LIMIT", "900"))
_DEFAULT_ANTHROPIC_TPM = int(os.getenv("AOAI_ANTHROPIC_TPM_LIMIT", "900000"))

# ─────────────────────────────────────────────────────────
# Shared SQL connection factory
# ─────────────────────────────────────────────────────────

def sql_connect(autocommit: bool = False) -> pyodbc.Connection:
    cs = os.environ["SQL_CONNECTION_STRING"]
    timeout = int(os.getenv("SQL_CONNECT_TIMEOUT", "30"))
    cnx = pyodbc.connect(cs, timeout=timeout)
    cnx.autocommit = autocommit
    return cnx


# ─────────────────────────────────────────────────────────
# Common utility functions (used across multiple phases)
# ─────────────────────────────────────────────────────────

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        return float(s) if s else default
    except Exception:
        return default


def safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def safe_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    return str(v)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def norm_space(s: str) -> str:
    s = (s or "").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def sha1_hex(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()


def json_dumps_compact(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:80] or "untitled"


def ensure_single_slashes(path: str) -> str:
    p = (path or "").strip().replace("\\", "/")
    while "//" in p:
        p = p.replace("//", "/")
    if not p.startswith("/"):
        p = "/" + p
    if p != "/" and p.endswith("/"):
        p = p[:-1]
    return p


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


# ─────────────────────────────────────────────────────────
# JSON parsing helpers
# ─────────────────────────────────────────────────────────

_JSON_BLOCK_RE = re.compile(r"\{[\s\S]*\}")


def best_effort_parse_json(s: str) -> Tuple[Dict[str, Any], bool, str]:
    if not s:
        return {}, False, "empty_model_output"
    t = s.strip()
    t = re.sub(r"^\s*```(?:json)?\s*", "", t, flags=re.I)
    t = re.sub(r"\s*```\s*$", "", t)
    try:
        obj = json.loads(t)
        if isinstance(obj, dict):
            return obj, True, ""
        return {}, False, "json_not_object"
    except Exception as e1:
        m = _JSON_BLOCK_RE.search(t)
        if m:
            block = m.group(0).strip()
            try:
                obj = json.loads(block)
                if isinstance(obj, dict):
                    return obj, True, ""
                return {}, False, "json_extract_not_object"
            except Exception as e2:
                return {}, False, f"json_extract_parse_failed: {e2}"
        return {}, False, f"json_parse_failed: {e1}"


def get_choice_text(resp: Any) -> str:
    try:
        choice = resp.choices[0]
        msg = getattr(choice, "message", None)
        if msg is not None:
            content = getattr(msg, "content", "")
            if content is None:
                content = ""
            if isinstance(content, str):
                if content.strip():
                    return content.strip()
            elif isinstance(content, list):
                out: List[str] = []
                for part in content:
                    if isinstance(part, dict):
                        out.append((part.get("text") or part.get("content") or "").strip())
                    else:
                        out.append(str(part).strip())
                joined = "\n".join([x for x in out if x]).strip()
                if joined:
                    return joined
        txt = getattr(choice, "text", None)
        if isinstance(txt, str) and txt.strip():
            return txt.strip()
        return ""
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────
# GPT-5 Pro helpers  (uses Responses API, NOT chat.completions)
# ─────────────────────────────────────────────────────────

def _extract_responses_text(resp: Any) -> str:
    """Extract plain text from an OpenAI Responses API result.

    The SDK returns ``resp.output`` which is a list of output items.
    Each item of type ``message`` has a ``.content`` list of parts,
    each part having ``.text``.
    """
    try:
        for item in (resp.output or []):
            if getattr(item, "type", None) != "message":
                continue
            for part in (getattr(item, "content", None) or []):
                if getattr(part, "type", None) == "output_text":
                    txt = getattr(part, "text", None)
                    if isinstance(txt, str) and txt.strip():
                        return txt.strip()
    except Exception:
        pass
    # Fallback: some SDK versions surface .output_text directly
    txt = getattr(resp, "output_text", None)
    if isinstance(txt, str) and txt.strip():
        return txt.strip()
    return ""


def call_pro_with_retry(
    client: AzureOpenAI,
    *,
    model: str,
    instructions: str,
    input_text: str,
    max_output_tokens: Optional[int] = None,
    estimated_prompt_tokens: int = 0,
    rate_limiter: Optional[RateLimiter] = None,
    max_retries: int = _MAX_RETRIES,
    caller_tag: str = "",
) -> Any:
    """Call GPT-5 Pro via the Responses API (client.responses.create).

    GPT-5 Pro does NOT support chat.completions — it requires the
    ``/responses`` endpoint exposed as ``client.responses.create()``.

    Parameters
    ----------
    instructions : str
        System-level instructions (equivalent to system message).
    input_text : str
        The user prompt content.
    max_output_tokens : int | None
        Maps to ``max_output_tokens`` on the Responses API.
    """
    if rate_limiter is None:
        rate_limiter = get_rate_limiter("pro")

    if estimated_prompt_tokens <= 0:
        estimated_prompt_tokens = max(1, math.ceil((len(instructions) + len(input_text)) / _CHARS_PER_TOKEN))

    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        rate_limiter.wait_if_needed(estimated_prompt_tokens)

        try:
            kwargs: Dict[str, Any] = {
                "model": model,
                "instructions": instructions,
                "input": input_text,
            }
            if max_output_tokens is not None:
                kwargs["max_output_tokens"] = max_output_tokens

            return client.responses.create(**kwargs)

        except Exception as exc:
            last_exc = exc
            is_rate = _is_rate_limit_error(exc)
            is_transient = _is_transient_error(exc)

            if not is_rate and not is_transient:
                raise

            retry_after = _parse_retry_after(exc)
            if retry_after is not None and retry_after > 0:
                delay = min(retry_after + 1.0, _RETRY_MAX_DELAY)
            else:
                base = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
                jitter = random.uniform(-_RETRY_JITTER * base, _RETRY_JITTER * base)
                delay = min(base + jitter, _RETRY_MAX_DELAY)

            if attempt < max_retries:
                logging.warning(
                    "[%s] Pro %s (attempt %d/%d). Retrying in %.1f s. Error: %s",
                    caller_tag or "pro",
                    "rate_limit" if is_rate else "transient_error",
                    attempt, max_retries, delay, str(exc)[:300],
                )
                time.sleep(delay)
            else:
                logging.warning(
                    "[%s] Pro %s – all %d retries exhausted. Final cooldown retry in %.0f s. Error: %s",
                    caller_tag or "pro",
                    "rate_limit" if is_rate else "transient_error",
                    max_retries, _RETRY_COOLDOWN_DELAY, str(exc)[:500],
                )

    # Final cooldown retry: wait long enough for minute-level rate limits to reset
    if last_exc is not None:
        time.sleep(_RETRY_COOLDOWN_DELAY)
        rate_limiter.wait_if_needed(estimated_prompt_tokens)
        try:
            kwargs_final: Dict[str, Any] = {
                "model": model,
                "instructions": instructions,
                "input": input_text,
            }
            if max_output_tokens is not None:
                kwargs_final["max_output_tokens"] = max_output_tokens
            return client.responses.create(**kwargs_final)
        except Exception as final_exc:
            logging.error(
                "[%s] Pro final cooldown retry also failed. Error: %s",
                caller_tag or "pro",
                str(final_exc)[:500],
            )
            raise final_exc from last_exc

    raise last_exc  # type: ignore[misc]


def call_pro_json_with_retry(
    client: AzureOpenAI,
    *,
    model: str,
    messages: List[Dict[str, str]],
    max_completion_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    estimated_prompt_tokens: int = 0,
    rate_limiter: Optional[RateLimiter] = None,
    caller_tag: str = "",
) -> Dict[str, Any]:
    """GPT-5 Pro JSON call via the Responses API.

    Converts chat-style messages (system + user) into Responses API
    format (instructions + input), calls ``client.responses.create()``,
    extracts text, and parses JSON best-effort.
    """
    # Split messages into instructions (system) and input (user)
    instructions_parts: List[str] = []
    input_parts: List[str] = []
    for m in messages:
        role = m.get("role", "user")
        content = m.get("content", "")
        if role == "system":
            instructions_parts.append(content)
        else:
            input_parts.append(content)

    instructions = "\n\n".join(instructions_parts)
    input_text = "\n\n".join(input_parts)

    max_output = max_completion_tokens or int(os.getenv("AOAI_MAX_OUTPUT_TOKENS_GPT5PRO", "8000"))

    resp = call_pro_with_retry(
        client,
        model=model,
        instructions=instructions,
        input_text=input_text,
        max_output_tokens=max_output,
        estimated_prompt_tokens=estimated_prompt_tokens,
        rate_limiter=rate_limiter or get_rate_limiter("pro"),
        caller_tag=caller_tag,
    )

    raw = _extract_responses_text(resp)
    if not raw:
        raise RuntimeError("pro_empty_response")

    obj, ok, err = best_effort_parse_json(raw)
    if not ok:
        raise RuntimeError(f"pro_json_parse_failed:{err}")
    return obj


# ─────────────────────────────────────────────────────────
# Pro slow-start worker pool
# ─────────────────────────────────────────────────────────

class ProWorkerPool:
    """Slow-start thread pool for GPT-5 Pro to respect TPM limits.

    Strategy
    --------
    - Start with ``initial_workers`` concurrent slots.
    - Every ``ramp_interval`` seconds, add ``ramp_step`` more slots.
    - Repeat until ``max_workers`` is reached.
    - Finished workers immediately pick up the next queued item
      (work-stealing via a shared Queue).
    - The RateLimiter remains the hard safety net for RPM/TPM.

    Usage
    -----
    ::

        pool = ProWorkerPool(max_workers=20, initial_workers=8)
        results = pool.run(items, process_fn)

    ``process_fn(item) -> result`` is called once per item.
    """

    def __init__(
        self,
        max_workers: int = _PRO_MAX_WORKERS,
        initial_workers: int = _PRO_INITIAL_WORKERS,
        ramp_interval: float = _PRO_RAMP_INTERVAL,
        ramp_step: int = _PRO_RAMP_STEP,
        caller_tag: str = "pro_pool",
    ):
        self.max_workers = max(1, int(max_workers))
        self.initial_workers = max(1, min(int(initial_workers), self.max_workers))
        self.ramp_interval = max(10.0, float(ramp_interval))
        self.ramp_step = max(1, int(ramp_step))
        self.caller_tag = caller_tag

        self._semaphore = threading.Semaphore(self.initial_workers)
        self._current_cap = self.initial_workers
        self._cap_lock = threading.Lock()
        self._start_time = 0.0
        self._stop_ramp = threading.Event()

    def _ramp_loop(self) -> None:
        """Background thread that periodically increases the semaphore cap."""
        while not self._stop_ramp.is_set():
            self._stop_ramp.wait(timeout=self.ramp_interval)
            if self._stop_ramp.is_set():
                break

            with self._cap_lock:
                if self._current_cap >= self.max_workers:
                    break
                added = min(self.ramp_step, self.max_workers - self._current_cap)
                for _ in range(added):
                    self._semaphore.release()
                self._current_cap += added
                elapsed = time.monotonic() - self._start_time
                logging.info(
                    "[%s] Ramped workers: %d → %d (elapsed=%.0f s, max=%d)",
                    self.caller_tag,
                    self._current_cap - added,
                    self._current_cap,
                    elapsed,
                    self.max_workers,
                )

    def run(
        self,
        items: List[Any],
        process_fn: Callable[[Any], Any],
    ) -> List[Any]:
        """Process *items* with slow-start parallelism and work-stealing.

        Workers that finish immediately pick up the next queued item.
        Returns a list of results in completion order (not input order).
        Exceptions from ``process_fn`` are caught and returned as dicts
        with ``{"status": "pool_error", "error": ...}``.
        """
        import queue as _queue

        if not items:
            return []

        self._start_time = time.monotonic()
        self._stop_ramp.clear()
        self._current_cap = self.initial_workers
        self._semaphore = threading.Semaphore(self.initial_workers)

        work_q: _queue.Queue = _queue.Queue()
        for item in items:
            work_q.put(item)

        results: List[Any] = []
        results_lock = threading.Lock()
        total_items = len(items)

        logging.info(
            "[%s] Starting slow-start pool: %d items, initial=%d, max=%d, "
            "ramp_step=%d, ramp_interval=%.0f s",
            self.caller_tag,
            total_items,
            self.initial_workers,
            self.max_workers,
            self.ramp_step,
            self.ramp_interval,
        )

        # Start the ramp-up background thread (only if we need to ramp)
        ramp_thread: Optional[threading.Thread] = None
        if self.initial_workers < self.max_workers:
            ramp_thread = threading.Thread(
                target=self._ramp_loop, daemon=True, name=f"{self.caller_tag}-ramp"
            )
            ramp_thread.start()

        def _worker(worker_id: int) -> None:
            """Acquire permit once → drain queue items → release permit → exit."""
            thread_name = f"{self.caller_tag}-w{worker_id}"
            items_processed = 0

            # Block until a concurrency permit is available.
            # This means only initial_workers threads run at first;
            # the rest wait here until the ramp thread releases more permits.
            logging.debug("[%s] Worker %s waiting for permit", self.caller_tag, thread_name)
            self._semaphore.acquire()
            logging.info("[%s] Worker %s acquired permit, starting work", self.caller_tag, thread_name)

            try:
                while True:
                    try:
                        item = work_q.get_nowait()
                    except _queue.Empty:
                        logging.info(
                            "[%s] Worker %s exiting – queue empty (processed %d items)",
                            self.caller_tag, thread_name, items_processed,
                        )
                        return

                    # Identify the item for logging
                    item_label = "?"
                    if isinstance(item, dict):
                        item_label = str(
                            item.get("cluster_id")
                            or item.get("scenario_id")
                            or item.get("cluster_key")
                            or "?"
                        )

                    logging.info(
                        "[%s] Worker %s picked up item %s (queue remaining ~%d)",
                        self.caller_tag, thread_name, item_label, work_q.qsize(),
                    )

                    t0 = time.monotonic()
                    try:
                        res = process_fn(item)
                    except Exception as e:
                        elapsed_item = time.monotonic() - t0
                        logging.error(
                            "[%s] Worker %s item %s raised exception after %.1f s: %s",
                            self.caller_tag, thread_name, item_label,
                            elapsed_item, str(e)[:300],
                        )
                        res = {"status": "pool_error", "error": str(e)}
                    finally:
                        work_q.task_done()
                        items_processed += 1

                    elapsed_item = time.monotonic() - t0
                    status = res.get("status", "?") if isinstance(res, dict) else "?"
                    logging.info(
                        "[%s] Worker %s finished item %s in %.1f s (status=%s)",
                        self.caller_tag, thread_name, item_label, elapsed_item, status,
                    )

                    with results_lock:
                        results.append(res)
                        done = len(results)

                    if done % 5 == 0 or done == total_items:
                        elapsed = time.monotonic() - self._start_time
                        with self._cap_lock:
                            cap = self._current_cap
                        logging.info(
                            "[%s] Progress: %d/%d done (%.0f s elapsed, cap=%d)",
                            self.caller_tag, done, total_items, elapsed, cap,
                        )
            finally:
                # Release permit so a waiting worker thread can start
                self._semaphore.release()
                logging.debug(
                    "[%s] Worker %s released permit (processed %d items total)",
                    self.caller_tag, thread_name, items_processed,
                )

        # Spin up max_workers threads — the semaphore gates actual concurrency.
        # Threads that can't acquire a permit block at the top of _worker()
        # until the ramp thread releases more permits OR a finishing worker
        # releases its permit (which only happens when the queue is empty
        # for that worker, so permits flow to waiting threads naturally).
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(_worker, i) for i in range(self.max_workers)]
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as exc:
                    logging.error(
                        "[%s] Worker thread raised unhandled exception: %s",
                        self.caller_tag, str(exc)[:300],
                    )

        # Stop the ramp thread
        self._stop_ramp.set()
        if ramp_thread is not None:
            ramp_thread.join(timeout=2.0)

        elapsed = time.monotonic() - self._start_time
        logging.info(
            "[%s] Pool complete: %d results in %.1f s",
            self.caller_tag, len(results), elapsed,
        )
        return results
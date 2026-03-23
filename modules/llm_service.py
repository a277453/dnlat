#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
llm_service.py

Centralised LLM integration layer for the DN Diagnostics Analysis Platform.

Responsibilities:
  - Build and send prompts to the Ollama LLM endpoint
  - Enforce token/character budgets before every call
  - Retry transient failures with exponential back-off
  - Validate and parse structured LLM responses
  - Emit observability metrics (latency, token estimates, model,
    retry count, success/failure) via the project logger so they
    can be consumed by any downstream monitoring tool

All Ollama configuration (host, model, timeouts, parameters) is
read from a single LLMConfig dataclass so nothing is hard-coded
in route handlers.
"""

import json
import time
import re
import httpx
import asyncio

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from modules.logging_config import logger
from modules.processing import LogPreprocessorService


# ═══════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════

@dataclass
class LLMConfig:
    """
    FUNCTION:
        LLMConfig

    DESCRIPTION:
        All tuneable parameters for the LLM service in one place.
        Override any field when constructing LLMService to change
        behaviour without touching call-site code.

    FIELDS:
        ollama_host     (str)   : Base URL of the Ollama server.
        model           (str)   : Model name as registered in Ollama
                                  (must match your Modelfile FROM name).
        timeout_seconds (int)   : HTTP timeout per request in seconds.
        max_retries     (int)   : Number of retry attempts on transient error.
        retry_backoff   (float) : Multiplier for exponential back-off (seconds).
        max_prompt_chars(int)   : Hard cap on prompt length before sending.
                                  Prompts exceeding this are truncated with a
                                  [TRUNCATED] marker so the model always gets
                                  a valid (if partial) input.
        temperature     (float) : Sampling temperature — keep low (0.1-0.3)
                                  for deterministic log analysis.
        top_p           (float) : Nucleus sampling threshold.
        repeat_penalty  (float) : Penalise repeated phrases in output.
    """
    ollama_host:      str   = "http://localhost:11434"
    model:            str   = "dn-atm-analyzer"   # name from your Modelfile
    timeout_seconds:  int   = 120
    max_retries:      int   = 3
    retry_backoff:    float = 2.0
    max_prompt_chars: int   = 12_000
    temperature:      float = 0.2
    top_p:            float = 0.85
    repeat_penalty:   float = 1.1


# ═══════════════════════════════════════════════════════════════
#  Observability record
# ═══════════════════════════════════════════════════════════════

@dataclass
class LLMCallMetrics:
    """
    FUNCTION:
        LLMCallMetrics

    DESCRIPTION:
        Captures per-call observability data emitted after every LLM
        request. Logged as a structured dict so it can be ingested by
        any log aggregator (ELK, Grafana Loki, CloudWatch, etc.).

    FIELDS:
        atm_id           : ATM identifier from the request.
        model            : Model name used.
        prompt_chars     : Character length of the prompt sent.
        prompt_token_est : Estimated input tokens (chars / 4).
        response_chars   : Character length of the raw LLM response.
        response_token_est: Estimated output tokens (chars / 4).
        latency_ms       : Wall-clock time for the Ollama round-trip (ms).
        retries          : Number of retry attempts before success/failure.
        success          : True if a valid response was received.
        error            : Error message if success is False, else None.
        truncated        : True if the prompt was truncated before sending.
        match_method     : How transactions were matched ("uuid"|"timestamp"|"none").
    """
    atm_id:             str  = ""
    model:              str  = ""
    prompt_chars:       int  = 0
    prompt_token_est:   int  = 0
    response_chars:     int  = 0
    response_token_est: int  = 0
    latency_ms:         float= 0.0
    retries:            int  = 0
    success:            bool = False
    error:              Optional[str] = None
    truncated:          bool = False
    match_method:       str  = "none"

    def to_log_dict(self) -> dict:
        return {
            "event":              "llm_call",
            "atm_id":             self.atm_id,
            "model":              self.model,
            "prompt_chars":       self.prompt_chars,
            "prompt_token_est":   self.prompt_token_est,
            "response_chars":     self.response_chars,
            "response_token_est": self.response_token_est,
            "latency_ms":         round(self.latency_ms, 2),
            "retries":            self.retries,
            "success":            self.success,
            "error":              self.error,
            "truncated":          self.truncated,
            "match_method":       self.match_method,
        }


# ═══════════════════════════════════════════════════════════════
#  LLM Service
# ═══════════════════════════════════════════════════════════════

class LLMService:
    """
    FUNCTION:
        LLMService

    DESCRIPTION:
        Centralised service for all LLM interactions in the DN Diagnostics
        Analysis Platform. Accepts merged or single-source transaction
        records, builds a token-efficient prompt, calls Ollama, validates
        the response, and returns a structured result dict.

        All calls emit structured observability logs via the project logger.

    USAGE:
        # Instantiate once at module level in routes.py
        llm_service = LLMService()

        # In your route handler:
        result = await llm_service.analyze(merged_records, atm_id="DN_ATM_2043")

    PARAMETERS:
        config (LLMConfig) : Optional config override. Uses defaults if omitted.

    RETURNS:
        None

    RAISES:
        None  — all exceptions are caught, logged, and returned as error dicts.
    """

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config      = config or LLMConfig()
        self.preprocessor = LogPreprocessorService()
        logger.info(
            f"LLMService initialised | model={self.config.model} "
            f"host={self.config.ollama_host}"
        )

    # ── Public API ─────────────────────────────────────────────

    async def analyze(
        self,
        records:  List[dict],
        atm_id:   str = "",
    ) -> Dict[str, Any]:
        """
        FUNCTION:
            analyze

        DESCRIPTION:
            Main entry point. Accepts a list of transaction records
            (output of TransactionMergerService.merge() or either
            preprocessor individually), builds a prompt, calls the LLM,
            validates the response, and returns a structured result.

        USAGE:
            result = await llm_service.analyze(merged_records, atm_id="DN_ATM_2043")

        PARAMETERS:
            records (List[dict]) : Per-transaction dicts from the preprocessor
                                   or merger. Mixed ej_only / jrn_only / ej+jrn
                                   source labels are handled transparently.
            atm_id  (str)        : ATM identifier embedded in the prompt and
                                   observability logs.

        RETURNS:
            Dict[str, Any] with keys:
              "success"    (bool)        : Whether analysis completed.
              "atm_id"     (str)         : Echo of the input atm_id.
              "analysis"   (str)         : Raw LLM response text.
              "sections"   (dict)        : Parsed response sections (if parseable).
              "metrics"    (dict)        : Observability data for this call.
              "error"      (str | None)  : Error message on failure.

        RAISES:
            None
        """
        metrics = LLMCallMetrics(atm_id=atm_id, model=self.config.model)

        # Determine dominant match method for observability
        sources = [r.get("source", "none") for r in records]
        if any(s == "ej+jrn" for s in sources):
            metrics.match_method = "uuid" if self._any_uuid_matched(records) else "timestamp"
        elif sources:
            metrics.match_method = sources[0]

        # Build prompt
        prompt, truncated = self._build_prompt(records, atm_id)
        metrics.prompt_chars     = len(prompt)
        metrics.prompt_token_est = len(prompt) // 4
        metrics.truncated        = truncated

        logger.info(
            f"LLM analyze | atm_id={atm_id} records={len(records)} "
            f"prompt_chars={metrics.prompt_chars} "
            f"token_est={metrics.prompt_token_est} "
            f"truncated={truncated}"
        )

        # Call LLM with retry
        raw_response, retries, error = await self._call_with_retry(prompt)
        metrics.retries = retries

        if error:
            metrics.success = False
            metrics.error   = error
            logger.error(f"LLM call failed | {json.dumps(metrics.to_log_dict())}")
            return {
                "success":  False,
                "atm_id":   atm_id,
                "analysis": None,
                "sections": {},
                "metrics":  metrics.to_log_dict(),
                "error":    error,
            }

        metrics.response_chars     = len(raw_response)
        metrics.response_token_est = len(raw_response) // 4
        metrics.success            = True

        # Parse response into sections
        sections = self._parse_response(raw_response)

        logger.info(f"LLM call complete | {json.dumps(metrics.to_log_dict())}")

        return {
            "success":  True,
            "atm_id":   atm_id,
            "analysis": raw_response,
            "sections": sections,
            "metrics":  metrics.to_log_dict(),
            "error":    None,
        }

    async def health_check(self) -> Dict[str, Any]:
        """
        FUNCTION:
            health_check

        DESCRIPTION:
            Pings the Ollama server and verifies the configured model is
            available. Safe to call from a /health endpoint in routes.py.

        RETURNS:
            Dict with "reachable" (bool), "model_available" (bool),
            "error" (str | None).
        """
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{self.config.ollama_host}/api/tags")
                if resp.status_code != 200:
                    return {"reachable": False, "model_available": False,
                            "error": f"HTTP {resp.status_code}"}
                models = [m["name"] for m in resp.json().get("models", [])]
                available = any(
                    self.config.model in m for m in models
                )
                return {
                    "reachable":       True,
                    "model_available": available,
                    "model":           self.config.model,
                    "available_models":models,
                    "error":           None if available else
                                       f"Model '{self.config.model}' not found in Ollama.",
                }
        except Exception as e:
            return {"reachable": False, "model_available": False, "error": str(e)}

    # ── Private: prompt ────────────────────────────────────────

    def _build_prompt(
        self,
        records: List[dict],
        atm_id:  str
    ) -> tuple[str, bool]:
        """
        Build the LLM prompt string from transaction records.
        Returns (prompt_string, was_truncated).
        """
        payload   = json.dumps(records, indent=2)
        truncated = False

        if len(payload) > self.config.max_prompt_chars:
            payload   = payload[:self.config.max_prompt_chars] + "\n... [TRUNCATED]"
            truncated = True
            logger.warning(
                f"Prompt truncated to {self.config.max_prompt_chars} chars. "
                f"Consider reducing records or increasing max_prompt_chars."
            )

        atm_line = f"ATM_ID={atm_id}\n" if atm_id else ""
        prompt = (
            f"{atm_line}"
            f"The following is a structured list of transactions extracted from "
            f"the ATM log. Each entry contains only fields relevant to diagnosis.\n\n"
            f"LOG:\n{payload}"
        )
        return prompt, truncated

    # ── Private: HTTP call + retry ─────────────────────────────

    async def _call_with_retry(
        self,
        prompt: str
    ) -> tuple[str, int, Optional[str]]:
        """
        Call Ollama /api/generate with exponential back-off retry.
        Returns (response_text, retry_count, error_message_or_None).
        """
        payload = {
            "model":  self.config.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature":    self.config.temperature,
                "top_p":          self.config.top_p,
                "repeat_penalty": self.config.repeat_penalty,
            },
        }

        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                t0 = time.monotonic()
                async with httpx.AsyncClient(
                    timeout=self.config.timeout_seconds
                ) as client:
                    resp = await client.post(
                        f"{self.config.ollama_host}/api/generate",
                        json=payload,
                    )
                latency_ms = (time.monotonic() - t0) * 1000

                if resp.status_code != 200:
                    raise ValueError(f"Ollama returned HTTP {resp.status_code}: {resp.text}")

                data = resp.json()
                response_text = data.get("response", "").strip()

                if not response_text:
                    raise ValueError("Ollama returned an empty response.")

                logger.debug(
                    f"Ollama call success | attempt={attempt + 1} "
                    f"latency_ms={latency_ms:.1f}"
                )
                return response_text, attempt, None

            except (httpx.TimeoutException, httpx.ConnectError) as e:
                last_error = f"Network error on attempt {attempt + 1}: {e}"
                logger.warning(last_error)

            except Exception as e:
                last_error = f"Unexpected error on attempt {attempt + 1}: {e}"
                logger.warning(last_error)

            # Exponential back-off before retry
            if attempt < self.config.max_retries - 1:
                wait = self.config.retry_backoff ** (attempt + 1)
                logger.info(f"Retrying in {wait:.1f}s...")
                await asyncio.sleep(wait)

        return "", self.config.max_retries - 1, last_error

    # ── Private: response parsing ──────────────────────────────

    def _parse_response(self, raw: str) -> dict:
        """
        FUNCTION:
            _parse_response

        DESCRIPTION:
            Attempts to extract the structured sections from the LLM response
            using the mandatory output format defined in the Modelfile SYSTEM
            prompt. Returns a dict of section_name → content.

            If the response does not match the expected format (e.g. the model
            deviated), returns {"raw": raw} so the caller always gets something
            usable.

        EXPECTED SECTIONS (from Modelfile):
            Transaction Summary, What Happened, Why It Might Have Happened,
            Possible Root Causes, Recommended Actions, Confidence Level

        PARAMETERS:
            raw (str) : Raw LLM response string.

        RETURNS:
            dict : Keyed by section name, value is the section content string.
                   Always includes "raw" key with the full original response.
        """
        sections = {"raw": raw}

        # Section headers as defined in the Modelfile OUTPUT FORMAT
        section_patterns = [
            ("transaction_summary",      r"Transaction Summary\s*:"),
            ("what_happened",            r"What Happened\s*:"),
            ("why_it_happened",          r"Why It Might Have Happened\s*:"),
            ("possible_root_causes",     r"Possible Root Causes\s*:"),
            ("recommended_actions",      r"Recommended Actions\s*:"),
            ("confidence_level",         r"Confidence Level\s*:"),
        ]

        # Build a split pattern from all section headers
        all_headers = "|".join(p for _, p in section_patterns)
        split_re    = re.compile(f"({all_headers})", re.IGNORECASE)
        parts       = split_re.split(raw)

        if len(parts) < 3:
            # Response doesn't match expected format — return raw only
            logger.warning(
                "LLM response did not match expected section format. "
                "Returning raw response only."
            )
            return sections

        # Pair each header with its content
        current_key = None
        for part in parts:
            matched_key = None
            for key, pattern in section_patterns:
                if re.match(pattern, part.strip(), re.IGNORECASE):
                    matched_key = key
                    break
            if matched_key:
                current_key = matched_key
            elif current_key:
                sections[current_key] = part.strip()
                current_key = None

        # Extract confidence level as a clean enum value
        cl = sections.get("confidence_level", "")
        for level in ("High", "Medium", "Low"):
            if level.lower() in cl.lower():
                sections["confidence_level_value"] = level
                break

        # Check for the "No anomaly detected" shorthand
        if "no anomaly detected" in raw.lower():
            sections["anomaly_detected"] = False
        else:
            sections["anomaly_detected"] = True

        return sections

    # ── Private: helpers ───────────────────────────────────────

    @staticmethod
    def _any_uuid_matched(records: List[dict]) -> bool:
        """
        Heuristic: if a merged record has both txn_number and protocol_steps,
        it was almost certainly matched via UUID (JRN enrichment is rich).
        Used only for observability labelling.
        """
        return any(
            r.get("source") == "ej+jrn" and r.get("protocol_steps")
            for r in records
        )
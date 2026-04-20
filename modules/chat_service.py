# modules/chat_service.py
"""
Chat service for DNLAT — handles follow-up Q&A about an analyzed transaction.

Scope enforcement (two-layer guard):
  Layer A — Heuristic (zero latency)
      Hard off-topic signals  -> reject immediately, no Ollama call.
      Hard on-topic signals   -> skip Layer B, go straight to full answer.
      Neither match           -> borderline -> go to Layer B.

  Layer B — LLM self-check (only for borderline questions)
      Minimal Ollama call (num_predict=5) asks YES/NO.
      YES -> proceed. NO -> reject.

Both layers return the same _OUT_OF_SCOPE_REPLY string so the UI renders
it as a normal assistant message.
"""

import re
import time
import hashlib
import ollama
import os

from modules.logging_config import logger

MODEL_NAME = os.getenv("OLLAMA_MODEL", "llama3_log_analyzer")

# ── Ollama client configuration ───────────────────────────────────────────────
# OLLAMA_HOST should be set in the environment for Docker deployments,
# e.g. OLLAMA_HOST=http://ollama:11434 (where 'ollama' is the container name).
# When running locally, the ollama client defaults to http://localhost:11434
# automatically — no env var needed.
# Reads OLLAMA_BASE_URL to match the existing docker-compose environment variable.
# Locally this var is not set, so the client falls back to http://localhost:11434.
_ollama_host = os.getenv("OLLAMA_BASE_URL", "").strip()
_ollama_client = ollama.Client(host=_ollama_host) if _ollama_host else ollama.Client()
logger.info(
    f"chat_service: Ollama client initialised | "
    f"host={'<default localhost:11434>' if not _ollama_host else _ollama_host}"
)

_MAX_EJ_CHARS       = 3000
_MAX_JRN_CHARS      = 2000
_MAX_ANALYSIS_CHARS = 2000

_OUT_OF_SCOPE_REPLY = (
    "This question is outside the scope of the current transaction analysis. "
    "I can only answer questions about this specific transaction's log data, "
    "events, errors, and diagnosis."
)

# ── Layer A: hard off-topic signals ──────────────────────────────────────────
_OFFTOPIC_PATTERNS = re.compile(
    r'\b('
    r'capital\s+of|country|continent|population|president|prime\s+minister|'
    r'history\s+of|who\s+invented|when\s+was\s+.{0,20}born|biography|'
    r'speed\s+of\s+light|theory\s+of\s+relativity|quantum|photosynthesis|'
    r'pythagorean|integral|derivative|calculus|algebra|geometry|'
    r'write\s+a\s+(script|program|function|class)|'
    r'how\s+to\s+(install|configure|deploy|setup)\s+(?!atm|diebold|dn\b)|'
    r'docker|kubernetes|terraform|machine\s+learning|neural\s+network|'
    r'weather|forecast|temperature\s+today|cricket\s+score|football\s+match|'
    r'movie|film\s+review|song\s+lyrics|music\s+video|recipe|cook|restaurant|'
    r'stock\s+price|cryptocurrency|bitcoin|forex|invest\b|'
    r'tell\s+me\s+a\s+joke|funny\s+story|tell\s+me\s+a\s+story|'
    r'who\s+are\s+you|what\s+is\s+your\s+name|how\s+are\s+you|'
    r'what\s+can\s+you\s+do|'
    # Casual greetings / small talk
    r'\bhow\s+r\s+u\b|\bhi\b|\bhello\b|\bhey\b|\bwassup\b|\bwhat.?s\s+up\b|'
    r'good\s+(morning|afternoon|evening|night)|'
    r'how\s+are\s+you|how\s+r\s+u|how\s+do\s+you\s+do|'
    # Generic definition questions — "what is an atm/emv/pin/etc."
    # These are general knowledge, not transaction-specific.
    r'what\s+is\s+(an?\s+)?(atm|bank|card|pin|emv|network|server|database|'
    r'machine|system|protocol|api|queue|cache)|'
    r'define\s+(atm|emv|pin|icc|host|terminal|cassette|dispenser)|'
    r'explain\s+what\s+(an?\s+)?(atm|emv|pin|icc|host)\s+is'
    r')\b',
    re.IGNORECASE,
)

# ── Layer A: hard on-topic signals ───────────────────────────────────────────
_ONTOPIC_PATTERNS = re.compile(
    r'\b('
    r'transaction|txn|ej\b|jrn\b|journal|log|error\s+code|fault|failure|'
    r'end.?state|status\s+code|event\s+code|'
    r'dispense|dispenser|cassette|cash\b|retract|shutter|card\b|pin\b|'
    r'receipt|printer|sensor|cdm\b|presenter|purge|reject\b|'
    r'diebold|nixdorf|\bdn\b|atm\b|terminal\b|tid\b|'
    r'ccprotfw|tdr_|emv\b|icc\b|host\s+(offline|timeout|error|response)|'
    r'authoris|authoriz|'
    r'3[0-9]{3}\b|4[0-9]{4}\b|'
    r'rollback|reversal|time.?out|cancel|abandon|'
    r'root\s+cause|diagnos|anomal|'
    r'why\s+(did|was|is)\s+.{0,30}(fail|error|cancel|timeout|retract)|'
    r'what\s+(caused|happened|went\s+wrong)|'
    r'explain\s+(the\s+)?(error|failure|result|analysis|log)|'
    r'timestamp|start\s+time|end\s+time|sequence|step|'
    r'duration|how\s+long|what\s+time|'
    r'unsuccessful|successful|succeed|fail(ed|ure)?|incomplete|'
    r'in\s+the\s+first\s+place|at\s+this\s+(point|step)|'
    r'what\s+happen(ed|s)|what\s+went|how\s+did\s+it'
    r')\b',
    re.IGNORECASE,
)


def _layer_a_check(question: str) -> str:
    """
    Returns 'reject', 'allow', or 'borderline'.
    """
    if _OFFTOPIC_PATTERNS.search(question):
        return "reject"
    if _ONTOPIC_PATTERNS.search(question):
        return "allow"
    return "borderline"


def _layer_b_check(question: str, analysis_result: str) -> bool:
    """
    LLM self-check for borderline questions.
    Returns True (in scope) or False (out of scope).
    Defaults to True on any Ollama error.
    """
    analysis_snippet = (analysis_result or "")[:400].strip() or "ATM transaction log analysis"

    scope_check_prompt = (
        "You are a strict scope checker for an ATM transaction log analysis tool.\n"
        "Decide if this question requires THIS SPECIFIC transaction log data to answer.\n"
        "Answer YES only if the question asks about events, errors, states, timings, or "
        "hardware behaviour visible in the logs of THIS transaction.\n"
        "Answer NO if the question is a general definition, general knowledge, or conceptual "
        "question that could be answered without any transaction log "
        "(e.g. \'what is an ATM\', \'what is EMV\', \'how does a PIN work\').\n\n"
        f"Transaction context summary:\n{analysis_snippet}\n\n"
        f"User question: {question}\n\n"
        "Does answering this require THIS transaction\'s specific log data?\n"
        "Reply with ONLY YES or NO. No explanation."
    )

    logger.info(
        "SCOPE CHECK (Layer B) | "
        f"question_chars={len(question)} | "
        f"prompt_chars={len(scope_check_prompt)}"
    )

    start = time.perf_counter()
    try:
        response = _ollama_client.chat(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": scope_check_prompt}],
            options={"temperature": 0.0, "num_predict": 5},
        )
        raw = response["message"]["content"].strip().upper()
        duration = round(time.perf_counter() - start, 3)
        in_scope = raw.startswith("YES")
        logger.info(
            f"SCOPE CHECK RESULT | raw='{raw}' | in_scope={in_scope} | time={duration}s"
        )
        return in_scope
    except Exception as exc:
        logger.warning(f"Layer B scope check failed ({exc}) — defaulting to allow")
        return True


def _truncate(text: str, max_chars: int) -> str:
    if not text or len(text) <= max_chars:
        return text or ""
    return text[:max_chars] + f"\n... [truncated at {max_chars} chars]"


def _build_chat_prompt(
    ej_content: str,
    jrn_content: str,
    analysis_result: str,
    history: list,
    question: str,
) -> str:
    ej_block       = _truncate(ej_content,      _MAX_EJ_CHARS)
    jrn_block      = _truncate(jrn_content,     _MAX_JRN_CHARS)
    analysis_block = _truncate(analysis_result, _MAX_ANALYSIS_CHARS)

    parts = [
        # ── Modelfile override ────────────────────────────────────────────
        # The active Modelfile system prompt instructs the model to produce
        # structured analysis output (sections, headers, fixed format).
        # This block explicitly overrides that behaviour for chat turns.
        "IMPORTANT OVERRIDE: You are now in conversational Q&A mode.",
        "Do NOT produce structured analysis output.",
        "Do NOT use section headers, numbered sections, or the analysis report format.",
        "Answer the question below in plain, concise conversational sentences only.",
        "Base your answer STRICTLY on evidence present in the log context below.",
        "Do NOT speculate, suggest actions, give recommendations, or invent details not in the logs.",
        "If the logs do not contain enough information to answer, say so explicitly.",
        "── END OVERRIDE ──",
        "",
        # ── Task identity ─────────────────────────────────────────────────
        "You are answering a follow-up question about an ATM transaction that has already been analyzed.",
        "Answer based ONLY on the context below. Do not speculate or invent details not present in the logs.",
        "",
        "--- EJ LOG (CUSTOMER JOURNAL) ---",
        ej_block if ej_block.strip() else "(not available)",
        "",
        "--- UI JOURNAL (JRN) ---",
        jrn_block if jrn_block.strip() else "(not available)",
        "",
        "--- PRIOR ANALYSIS RESULT ---",
        analysis_block if analysis_block.strip() else "(not available)",
        "",
    ]

    if history:
        parts.append("--- CONVERSATION SO FAR ---")
        for turn in history:
            role_label = "User" if turn["role"] == "user" else "Assistant"
            parts.append(f"{role_label}: {turn['content']}")
        parts.append("")

    parts += ["--- NEW QUESTION ---", question]
    return "\n".join(parts)


def chat_turn(
    ej_content: str,
    jrn_content: str,
    analysis_result: str,
    history: list,
    question: str,
) -> str:
    """
    Runs a single chat turn with two-layer scope enforcement.

    Returns the model reply, or _OUT_OF_SCOPE_REPLY if the question
    is outside the transaction log domain.
    """
    if not question or not question.strip():
        raise ValueError("Question cannot be empty.")

    # Layer A
    layer_a_result = _layer_a_check(question)
    logger.info(
        f"SCOPE CHECK (Layer A) | result={layer_a_result} | question='{question[:80]}'"
    )

    if layer_a_result == "reject":
        logger.info("SCOPE GUARD | Layer A rejected — out-of-scope reply")
        return _OUT_OF_SCOPE_REPLY

    # Layer B (borderline only)
    if layer_a_result == "borderline":
        if not _layer_b_check(question, analysis_result):
            logger.info("SCOPE GUARD | Layer B rejected — out-of-scope reply")
            return _OUT_OF_SCOPE_REPLY

    # Full chat turn
    prompt = _build_chat_prompt(
        ej_content=ej_content,
        jrn_content=jrn_content,
        analysis_result=analysis_result,
        history=history,
        question=question,
    )

    prompt_fingerprint = hashlib.md5(prompt.encode()).hexdigest()[:8]
    logger.info(
        f"CHAT TURN | "
        f"history_turns={len(history)} | "
        f"prompt_chars={len(prompt)} | "
        f"tokens_est={len(prompt) // 4} | "
        f"fingerprint={prompt_fingerprint}"
    )

    start = time.perf_counter()
    response = _ollama_client.chat(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        options={"temperature": 0.2, "num_predict": 300},
    )
    duration = round(time.perf_counter() - start, 3)
    reply = response["message"]["content"].strip()

    actual_prompt_tokens   = response.get("prompt_eval_count", "N/A")
    actual_response_tokens = response.get("eval_count", "N/A")

    logger.info(
        f"CHAT TURN COMPLETE | "
        f"model={MODEL_NAME} | "
        f"time={duration}s | "
        f"prompt_tokens={actual_prompt_tokens} | "
        f"response_tokens={actual_response_tokens} | "
        f"reply_chars={len(reply)}"
    )

    return reply
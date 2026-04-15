# modules/example_store.py
#
# Dynamic few-shot example retrieval for DNLAT LLM pipeline.
#
# Examples are stored in a local JSON file (examples.json) at the project root.
# When DB access is available, replace _load_examples() and add_example() with
# PostgreSQL equivalents — the retrieval logic stays identical.
#
# File format (examples.json):
# [
#   {
#     "id": "ex_001",
#     "tags": ["Cash Deposit", "TA_OVERDRAFT", "OFFLINE", "3680", "3665"],
#     "input": { ...structured log dict... },
#     "output": "---\nTransaction Summary:\n..."
#   },
#   ...
# ]

import json
import os
from pathlib import Path
from modules.logging_config import logger

# ── Config ────────────────────────────────────────────────────────────────────
EXAMPLES_FILE = Path(os.getenv("DNLAT_EXAMPLES_FILE", "examples.json"))
TOP_K_DEFAULT = 2

# ── Signal extractors ─────────────────────────────────────────────────────────
# Each extractor pulls a specific diagnostic signal from the structured
# transaction record. These signals are matched against example tags.
_SIGNAL_EXTRACTORS = {
    "type": lambda t: str(t.get("type", "")),
    "status": lambda t: str(t.get("status", "")),
    "protocol_results": lambda t: [
        s.split("→")[1].split("(")[0].strip()
        for s in t.get("protocol_steps", [])
        if "→" in s
        and not s.endswith("→sent")
        and "PROCEED_NEXT" not in s   # too generic — matches almost every transaction
    ],
    "device_error_names": lambda t: [
        e.split(":")[0].strip()
        for e in t.get("device_errors", [])
    ],
    "has_retract": lambda t: "RETRACT" if t.get("retract_occurred") else None,
    "has_rollback": lambda t: "3680" if any(
        "3680" in str(e) for e in t.get("events", [])
    ) else None,
    "has_money_withdrawn": lambda t: "3665" if any(
        "3665" in str(e) for e in t.get("events", [])
    ) else None,
    "host_offline": lambda t: "OFFLINE" if any(
        "OFFLINE" in str(s) for s in t.get("protocol_steps", [])
    ) else None,
    "host_cancel": lambda t: "CANCEL_TA" if any(
        "CANCEL_TA" in str(s) for s in t.get("protocol_steps", [])
    ) else None,
    "chained": lambda t: "CHAINED" if "transaction_chain" in t else None,
    "host_decline_reason": lambda t: str(t.get("host_decline_reason", "")) or None,
    "app_state_closed":    lambda t: "CLOSED" if "CLOSED" in str(t.get("app_state_end", "")) else None,
    "app_state_txn5_end":  lambda t: "TRANSACTION_5_END" if (
        "TRANSACTION" in str(t.get("app_state_end", "")) and
        "TRANSACTION" not in str(t.get("app_state_start", "TRANSACTION"))[:5]
        or str(t.get("app_state_start","")) == str(t.get("app_state_end",""))
        and "TRANSACTION" in str(t.get("app_state_end",""))
    ) else None,
    "customer_timeout":    lambda t: "3246" if any(
        "[3246]" in str(a) for a in t.get("customer_actions", [])
    ) else None,
    "tc_cryptogram":       lambda t: "3962" if any(
        "3962" in str(e) for e in t.get("chip_decision", [])
    ) else None,
    "dispense_ok_event":   lambda t: "3302" if any(
        "3302" in str(e) or "Dispense OK" in str(e)
        for e in t.get("events", [])
    ) else None,
    "present_timeout_ev":  lambda t: "3319" if any(
        "3319" in str(e) or "Present timeout" in str(e)
        for e in t.get("events", [])
    ) else None,
    "present_failed_ev":   lambda t: "3306" if any(
        "3306" in str(e) or "present failed" in str(e).lower()
        for e in t.get("events", [])
    ) else None,
    "resubmit_auth":       lambda t: "RESUBMIT_AUTHORIZATION" if any(
        "RESUBMIT_AUTHORIZATION" in str(s) for s in t.get("protocol_steps", [])
    ) else None,
    "chained_bi_ga":       lambda t: "CHAINED_BI_GA" if (
        any("(BI)" in s for s in t.get("protocol_steps", [])) and
        any("(GA)" in s for s in t.get("protocol_steps", []))
    ) else None,
    "ga_function":         lambda t: "GA" if any(
        "(GA)" in s for s in t.get("protocol_steps", [])
    ) else None,
}


def _extract_signals(txn: dict) -> set:
    """Extract a flat set of signal strings from a transaction record."""
    signals = set()
    for fn in _SIGNAL_EXTRACTORS.values():
        val = fn(txn)
        if val is None or val == "":
            continue
        if isinstance(val, list):
            signals.update(v for v in val if v)
        else:
            signals.add(val)
    return signals


def _score(example_tags: list, txn_signals: set) -> int:
    """Count how many example tags match the transaction signals."""
    return sum(1 for tag in example_tags if tag in txn_signals)


# ── File I/O ──────────────────────────────────────────────────────────────────

def _load_examples() -> list:
    """Load examples from JSON file. Returns empty list if file missing."""
    if not EXAMPLES_FILE.exists():
        logger.warning(f"Examples file not found at {EXAMPLES_FILE.resolve()} — no few-shot examples will be injected.")
        return []
    try:
        data = json.loads(EXAMPLES_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            logger.error("examples.json must be a JSON array.")
            return []
        logger.info(f"Loaded {len(data)} few-shot examples from {EXAMPLES_FILE.resolve()}")
        return data
    except Exception as e:
        logger.error(f"Failed to load examples.json: {e}")
        return []


def add_example(tags: list, input_record: dict, output_text: str, example_id: str = None) -> bool:
    """
    Add a new verified example to the store.

    PARAMETERS:
        tags         (list) : Signal tags for retrieval matching.
                              e.g. ["Cash Withdrawal", "CANCEL_TA", "UNABLE TO PERFORM REQUEST"]
        input_record (dict) : The structured transaction record sent to the LLM.
        output_text  (str)  : The verified correct LLM output for this transaction.
        example_id   (str)  : Optional ID. Auto-generated if not provided.

    RETURNS:
        bool : True if saved successfully.
    """
    examples = _load_examples()

    new_example = {
        "id": example_id or f"ex_{len(examples) + 1:03d}",
        "tags": tags,
        "input": input_record,
        "output": output_text,
    }

    examples.append(new_example)

    try:
        EXAMPLES_FILE.write_text(
            json.dumps(examples, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        logger.info(f"Added example {new_example['id']} to {EXAMPLES_FILE.resolve()}")
        return True
    except Exception as e:
        logger.error(f"Failed to save example: {e}")
        return False


# ── Retrieval ─────────────────────────────────────────────────────────────────

# Signals that indicate a complex transaction requiring few-shot guidance.
# Simple transactions (customer cancel, timeout, clean success) are excluded
# to avoid injecting examples for cases the model handles reliably on its own.
_COMPLEX_SIGNALS = {
    "OFFLINE", "CANCEL_TA", "RETRACT", "3680", "3665",
    "CHAINED", "NOT_OPERATIONAL", "RESUBMIT", "RESUBMIT_AUTHORIZATION",
    "ERROR", "UNABLE TO PERFORM REQUEST", "CLOSED", "TRANSACTION_5_END",
    "3962", "3302", "3319", "3306", "CHAINED_BI_GA", "GA",
}


def needs_examples(txn: dict) -> bool:
    """
    Returns True if the transaction has complex signals that benefit from
    few-shot example injection. Simple transactions return False.

    PARAMETERS:
        txn (dict) : Structured transaction record (EJ record format).

    RETURNS:
        bool : True if examples should be injected for this transaction.
    """
    signals = _extract_signals(txn)
    matched = signals & _COMPLEX_SIGNALS
    if matched:
        logger.info(f"Complex signals detected {matched} — examples will be injected")
        return True
    logger.info("No complex signals — skipping example injection")
    return False


def fetch_relevant_examples(txn: dict, top_k: int = TOP_K_DEFAULT) -> list:
    """
    Retrieve the top_k most relevant few-shot examples for a given transaction.

    Scores each example by counting tag matches against signals extracted
    from the transaction record. Returns only examples with score > 0,
    sorted by descending score.

    PARAMETERS:
        txn   (dict) : Structured transaction record (EJ record format).
        top_k (int)  : Maximum number of examples to return.

    RETURNS:
        list : List of matched example dicts [{"id", "tags", "input", "output"}].
               Empty list if no matches or no examples file found.
    """
    examples = _load_examples()
    if not examples:
        return []

    txn_signals = _extract_signals(txn)
    logger.info(f"Transaction signals for few-shot retrieval: {txn_signals}")

    scored = [
        (_score(ex.get("tags", []), txn_signals), ex)
        for ex in examples
    ]
    scored.sort(key=lambda x: x[0], reverse=True)

    matched = [(score, ex) for score, ex in scored if score > 0]

    if not matched:
        logger.info("No matching few-shot examples found for this transaction.")
        return []

    top = [ex for _, ex in matched[:top_k]]
    logger.info(
        f"Fetched {len(top)} few-shot example(s): "
        + ", ".join(f"{ex['id']}(score={s})" for s, ex in matched[:top_k])
    )
    return top


def build_example_block(examples: list) -> str:
    """
    Format retrieved examples into a prompt block for injection.

    PARAMETERS:
        examples (list) : Output of fetch_relevant_examples().

    RETURNS:
        str : Formatted example block, or empty string if no examples.
    """
    if not examples:
        return ""

    lines = [
        "The following are verified analysis examples.",
        "Study the output format and reasoning style only.",
        "Do NOT repeat or echo these examples in your response.",
        "After the examples you will receive a new transaction to analyze.\n"
    ]
    for i, ex in enumerate(examples, 1):
        lines.append(f"Example {i} Input:\n{json.dumps(ex['input'], indent=2, ensure_ascii=False)}")
        lines.append(f"\nExample {i} Output:\n{ex['output']}\n")

    return "\n".join(lines)
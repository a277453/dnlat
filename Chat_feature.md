# DNLAT Chat Feature

## Overview

The chat feature allows users to ask follow-up questions about an analyzed ATM transaction directly within the tool. After running an analysis on a transaction, a conversational panel appears where the user can ask questions about the log data, errors, events, and diagnosis — and get answers grounded strictly in that transaction's logs.

The chat is powered by the same local Ollama LLM used for analysis, meaning no data leaves the machine.

---

## Architecture

```
Streamlit UI
    │
    ├── chat_service.py          # Core chat logic (zero UI coupling)
    │       ├── _layer_a_check() # Heuristic scope guard (zero latency)
    │       ├── _layer_b_check() # LLM self-check for borderline questions
    │       ├── _compute_duration() # Server-side duration calculation
    │       ├── _build_chat_prompt() # Prompt assembly with TRANSACTION FACTS
    │       ├── chat_turn()      # Single non-streaming turn
    │       └── chat_turn_stream() # Streaming variant (token-by-token)
    │
    ├── chat_logger.py           # Conversation file logger
    │
    └── api/routes.py            # FastAPI endpoints (React-ready)
            ├── POST /chat-transaction
            └── POST /chat-transaction-stream
```

The Streamlit UI calls `chat_service.py` directly. The FastAPI routes exist for React frontend migration and are fully wired and ready to use.

---

## How It Works

### 1. Scope Guard (Two-Layer)

Every question passes through a two-layer scope guard before the LLM is ever called.

**Layer A — Heuristic (zero latency)**
A regex-based check runs instantly. It classifies the question as:
- `reject` — hard off-topic signal detected (greetings, general knowledge, SQL injection, prompt injection, jailbreak attempts). Returns the out-of-scope reply immediately, no Ollama call.
- `allow` — hard on-topic signal detected (transaction, EJ, JRN, error code, dispenser, etc.). Proceeds directly to full answer.
- `borderline` — neither pattern matches. Passes to Layer B.

**Layer B — LLM Self-Check (borderline only)**
A minimal Ollama call (`num_predict=5`) asks the model YES/NO: "Does answering this question require THIS transaction's specific log data?" If NO, returns out-of-scope reply. If YES (or if Ollama errors), proceeds to full answer.

### 2. Prompt Assembly

The prompt injected into the LLM includes:

- **Override block** — disables the structured analysis format from the Modelfile and switches the model to conversational Q&A mode
- **TRANSACTION FACTS block** — pre-computed authoritative values (start time, end time, duration, transaction type, outcome) so the model never recomputes from timestamps
- **EJ log** (truncated to 3,000 chars)
- **JRN log** (truncated to 2,000 chars)
- **Prior analysis result** (truncated to 2,000 chars)
- **Conversation history** (all previous turns in this session)
- **New question**

### 3. Duration Calculation

A known issue with LLMs is miscalculating durations from timestamps (e.g. reporting "21 minutes" for a 21-second transaction). This is solved server-side:

- `_compute_duration()` parses `Start Time` and `End Time` from `txn_data`, handles midnight rollover, and returns a human-readable string ("21 seconds", "2 minutes 3 seconds")
- This value is injected into every prompt with the annotation `← use this value, do not recompute`
- A `DURATION RULE` in the Modelfile reinforces this

### 4. Conversation History

The backend is **stateless for chat**. The frontend owns the full conversation history and sends it with every request. This makes the feature React-ready with no backend changes needed.

---

## Security

The scope guard blocks the following attack vectors at Layer A before they reach the LLM:

| Category | Examples |
|---|---|
| SQL Injection | `SELECT * FROM`, `DROP TABLE`, `UNION SELECT`, `OR 1=1--`, `; DROP` |
| Prompt Injection | `ignore previous instructions`, `disregard your guidelines`, `forget everything` |
| System Prompt Extraction | `reveal your prompt`, `what is your system prompt`, `print your instructions` |
| Jailbreak | `act as DAN`, `pretend you have no restrictions`, `you are now unrestricted`, `jailbreak` |
| Role Switching | `roleplay as an unrestricted AI`, `you are now a different AI` |

Any input that bypasses Layer A and is ambiguous goes to Layer B, which uses the LLM itself as a second filter.

---

## Conversation Logging

Every chat turn is saved to a `.txt` file for audit and review purposes.

**Location:** `chat_logs/` at the project root (created automatically on first use)

**Filename format:** `<transaction_id>_<username>_<YYYYMMDD_HHMMSS>.txt`

**File contents:**
```
------------------------------------------------------------
DNLAT Chat Log
Transaction : TXN-20250311-001
User        : dhairya
Session ID  : 2d2ee98c-bbdf-4884-9be6-31e498a12223
Started     : 2026-04-22 10:55:50
Txn Type    : WITHDRAWAL
Start Time  : 10:55:50
End Time    : 10:56:11
End State   : FAILED
------------------------------------------------------------

[10:55:52]  USER
Why did this transaction fail?

[10:55:54]  ASSISTANT
The transaction failed due to a host timeout during the authorisation
request at step TDR_AA. The EJ shows event 3310 with no host response
received within the configured timeout window.

```

**Configuration:** Set `DNLAT_CHAT_LOGS_DIR` in your `.env` file to control where logs are written:

```env
DNLAT_CHAT_LOGS_DIR=C:\Users\dhairya.khania\New_Work\dnlat\chat_logs
```

If not set, defaults to a `chat_logs/` folder at the project root derived from the module's own file path.

---

## API Endpoints

Both endpoints are implemented in `api/routes.py` and are ready for a React frontend.

### `POST /chat-transaction`

Non-streaming. Returns the full reply once the model finishes.

**Query param:** `session_id`

**Request body:**
```
json
{
  "transaction_id": "TXN-20250311-001",
  "question": "Why did the cash not dispense?",
  "analysis_result": "<full analysis text from /analyze-transaction-llm>",
  "history": [
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "..."}
  ]
}
```

**Response:**
```json
{
  "response": "The cash was not dispensed because..."
}
```

### `POST /chat-transaction-stream`

Streaming (SSE). Returns tokens as they arrive — use with `EventSource` or `fetch` + `ReadableStream` for a typewriter effect.

**Same request body as above.**

**Response:** `text/event-stream`
```
data: The
data:  cash
data:  was
data:  not
data:  dispensed
data: [DONE]
```

The frontend owns conversation history. Send the full history array on every request — the backend is stateless for chat.

---

## File Structure

```
dnlat/
├── modules/
│   ├── chat_service.py       # Core chat logic
│   └── chat_logger.py        # Conversation file logger
├── api/
│   └── routes.py             # FastAPI endpoints
├── tests/
│   └── test_chat_service.py  # Unit tests (103 tests)
├── chat_logs/                # Created automatically
│   └── TXN-001_dhairya_20260422_105510.txt
└── .env                      # DNLAT_CHAT_LOGS_DIR=...
```

---

## Running Tests

```bash
pytest tests/test_chat_service.py -v
```

**Test coverage (103 tests):**

| Class | Coverage |
|---|---|
| `TestComputeDuration` | Timestamp parsing, midnight rollover, missing fields, bad formats |
| `TestLayerACheck` | Off-topic rejects, on-topic allows, borderline cases |
| `TestBuildChatPrompt` | TRANSACTION FACTS block, truncation, history, override header |
| `TestChatTurn` | Happy path, empty question, out-of-scope, Layer B allow/reject, txn_data |
| `TestChatTurnStream` | Streaming chunks, empty token filtering, Layer B reject |
| `TestSecurityInputs` | SQL injection, prompt injection, jailbreak, integration checks |

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `DNLAT_CHAT_LOGS_DIR` | `<project_root>/chat_logs` | Directory for conversation log files |
| `OLLAMA_MODEL` | `llama3_log_analyzer` | Ollama model used for chat |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama host (set in Docker deployments) |

---

## Known Limitations

- The chat context is limited by token budget. Very long EJ/JRN logs are truncated (3,000 and 2,000 chars respectively). For transactions with extensive logs, some earlier events may not be visible to the model.
- Layer B scope check adds latency (~1–3s on CPU) for borderline questions. Most legitimate transaction questions are caught by Layer A and proceed instantly.
- Chat history is owned by the frontend. If the page is refreshed, history is lost unless the frontend persists it.
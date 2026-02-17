# WCM Revenue Agent — Complete Project Walkthrough

A comprehensive guide covering every concept, design decision, and implementation detail. Use this to explain the project to anyone — interviewers, teammates, or reviewers.

---

## Table of Contents

1. [What This Project Does](#1-what-this-project-does)
2. [The Data Model](#2-the-data-model)
3. [The Historical Records Problem](#3-the-historical-records-problem)
4. [Architecture & Pipeline](#4-architecture--pipeline)
5. [SQL Safety & Security](#5-sql-safety--security)
6. [Error Handling & Resilience](#6-error-handling--resilience)
7. [Production Hardening](#7-production-hardening)
8. [Testing Strategy](#8-testing-strategy)
9. [Code Structure](#9-code-structure)
10. [Scalability & Future Improvements](#10-scalability--future-improvements)
11. [Common Interview Questions](#11-common-interview-questions)

---

## 1. What This Project Does

This is a **Text-to-SQL agent** for a music publishing company (WCM). It takes natural language questions like *"What is the total revenue for Alex Park?"* and:

1. Translates the question into a SQL query using an LLM (GPT-4o-mini)
2. Validates the SQL for safety (no destructive commands)
3. Executes it against an in-memory SQLite database loaded from CSV files
4. Returns a human-readable answer: **"$4,644.75"**

**Why is this useful?** Business users can ask revenue questions in plain English without knowing SQL. The LLM acts as a translator, not a data store — it never sees the raw data, only the schema.

---

## 2. The Data Model

The database has **3 tables** loaded from CSV files:

```
┌──────────────┐       ┌──────────────┐       ┌──────────────────┐
│  dim_writer   │       │   dim_song   │       │  fact_royalties   │
├──────────────┤       ├──────────────┤       ├──────────────────┤
│ writer_id PK │──1:N─→│ song_id      │←─N:1──│ transaction_id PK│
│ writer_name  │       │ title        │       │ song_id FK       │
│              │       │ writer_id FK │       │ amount_usd       │
│              │       │ etl_date     │       │                  │
└──────────────┘       └──────────────┘       └──────────────────┘
```

**Key stats:**
- **5 writers** (Alex Park, Jane Miller, Sarah Stone, Kevin Webb, Leyla Ademi)
- **20 unique songs** (but 22 rows in `dim_song` — explained below)
- **100 royalty transactions**

**What is a "dimension" vs "fact" table?**
- **Dimension tables** (`dim_writer`, `dim_song`) describe entities — who the writers are, what the songs are
- **Fact tables** (`fact_royalties`) record events — each row is a royalty payment

This is a standard **star schema** pattern used in data warehousing.

---

## 3. The Historical Records Problem

This is the **most important technical challenge** in the project.

### The Problem

`dim_song` has **historical records**. When a song's title changes, a new row is added instead of updating the old one:

| song_id | title | etl_date |
|---|---|---|
| 1 | Starlight (Draft) | 1/1/2023 |
| 1 | Starlight | 6/1/2023 |
| 6 | Static Dreams (Original) | 1/10/2023 |
| 6 | Static Dreams | 5/20/2023 |

Songs 1 and 6 each have **two rows**. If you join `dim_song` directly to `fact_royalties`:

```sql
-- ❌ WRONG: double-counts revenue for songs 1 and 6
SELECT SUM(fr.amount_usd)
FROM fact_royalties fr
JOIN dim_song ds ON fr.song_id = ds.song_id
JOIN dim_writer dw ON ds.writer_id = dw.writer_id
WHERE dw.writer_name = 'Alex Park';
-- Returns: $6,308.00 (WRONG)
```

Each transaction for song 1 matches **both** rows in `dim_song`, so the revenue gets counted twice.

### The Solution: `current_songs` View

I created a SQL **VIEW** that deduplicates by keeping only the latest record per song:

```sql
CREATE VIEW current_songs AS
SELECT song_id, title, writer_id
FROM dim_song
WHERE rowid IN (
    SELECT rowid FROM dim_song d1
    WHERE d1.etl_date = (
        SELECT MAX(d2.etl_date) FROM dim_song d2
        WHERE d2.song_id = d1.song_id
    )
);
```

Now the correct query:

```sql
-- ✅ CORRECT: uses deduplicated view
SELECT ROUND(SUM(fr.amount_usd), 2) AS total_revenue
FROM fact_royalties fr
JOIN current_songs cs ON fr.song_id = cs.song_id
JOIN dim_writer dw ON cs.writer_id = dw.writer_id
WHERE dw.writer_name = 'Alex Park';
-- Returns: $4,644.75 (CORRECT)
```

### Why Not Let the LLM Handle It?

I could have added instructions like *"Be careful about duplicates in dim_song"* to the prompt. **I chose not to.** Here's why:

| Approach | Pros | Cons |
|---|---|---|
| **Prompt instruction** | No code changes needed | LLM might forget; non-deterministic; hard to test |
| **SQL View (my approach)** | Deterministic; testable; LLM can't get it wrong | Requires code-level solution |

**Principle: Use logic, not inference, for correctness-critical operations.** The deduplication is SQL (testable, deterministic). The LLM's job is simplified to query generation against clean tables.

---

## 4. Architecture & Pipeline

### Why a Two-Stage Pipeline?

The agent makes **two LLM calls**, not one:

```
┌─────────┐     ┌───────────────┐     ┌──────────┐     ┌─────────────┐     ┌───────────┐
│ Question │────→│ LLM Call #1   │────→│ Validate │────→│ Execute SQL │────→│ LLM Call #2│
│          │     │ Generate SQL  │     │ SQL      │     │ on SQLite   │     │ Format     │
└─────────┘     └───────────────┘     └──────────┘     └─────────────┘     │ Answer     │
                                                                           └───────────┘
```

**Call #1 — SQL Generation:**
- Input: Database schema + user question
- Output: A raw SQL query
- Temperature: 0.0 (deterministic)

**Call #2 — Answer Formatting:**
- Input: Raw query results + original question
- Output: Human-readable sentence
- Fallback: If this call fails, a **deterministic formatter** generates the answer from the raw data

### Why Not a Single Prompt?

Separating the two concerns (SQL generation vs. answer formatting) means:
- If the SQL is wrong, you can see it in the logs **before** it gets paraphrased
- Each prompt is focused and easier to debug
- The fallback formatter can guarantee the answer even if the second LLM call fails

### Why Not Use LangChain / Agent Frameworks?

The pipeline is **linear and fixed** — there's no decision-making about which tool to use. The agent doesn't need to:
- Choose between multiple tools
- Decide when to stop
- Handle multi-step reasoning

A framework adds complexity without benefit here. Two function calls with error handling is clearer, more testable, and easier to explain.

### The Schema-Only Approach

The LLM receives the **schema description** (table names, columns, relationships), not the actual data:

```python
SCHEMA_DESCRIPTION = """
TABLE: dim_writer
- writer_id (INTEGER, PRIMARY KEY)
- writer_name (TEXT)

VIEW: current_songs
- USE THIS VIEW instead of dim_song for revenue calculations
...
"""
```

**Why?**
- The LLM generates the query; the **database** does the heavy lifting
- Works the same whether the table has 100 rows or 10 million
- No risk of sending sensitive data to the LLM API

---

## 5. SQL Safety & Security

### The Threat Model

The LLM generates SQL. If it generates `DROP TABLE dim_writer`, we've lost our data. Even with a well-crafted prompt, LLMs can be unpredictable. We need **defense-in-depth**.

### Four-Layer Defense

Implemented in [`safety.py`](wcm_agent/safety.py):

**Layer 1 — Comment Stripping:**
```python
cleaned = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
```
Prevents attacks hidden in SQL comments like `SELECT 1 -- ; DROP TABLE x`.

**Layer 2 — SELECT Whitelist:**
```python
if not cleaned.upper().startswith("SELECT"):
    return False, "Only SELECT queries are allowed."
```
Any query that doesn't start with SELECT is rejected outright.

**Layer 3 — Word-Boundary Blocklist:**
```python
blocked = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE", "EXEC"]
for keyword in blocked:
    if re.search(rf"\b{keyword}\b", cleaned, re.IGNORECASE):
        return False, f"Contains '{keyword}'"
```
The `\b` word boundary is critical — it means:
- `UPDATE dim_writer` → **blocked** ✅
- `SELECT updated_at FROM table` → **allowed** ✅ (no false positive)

**Layer 4 — Multi-Statement Blocking:**
```python
if re.search(r";\s*\S", cleaned):
    return False, "Multiple statements not allowed."
```
Prevents `SELECT 1; DROP TABLE x` piggybacking.

### Additional Safety Measures

- **Input sanitisation**: User questions truncated to 500 chars, control characters stripped
- **Auto-LIMIT**: `LIMIT 1000` appended if not present, preventing accidental full-table dumps
- **Query timeouts**: SQLite busy_timeout set to prevent runaway queries

### What Would Be Added in Production?

- **Read-only database connection** — the strongest guarantee (enforced at DB level)
- **Parameterised output limits** — always cap result size
- **Query cost estimation** — reject queries that would be too expensive

---

## 6. Error Handling & Resilience

### Exponential Backoff

API calls can fail due to rate limits, network issues, or service outages. Instead of immediate retry:

```python
for attempt in range(max_retries + 1):
    try:
        response = client.chat.completions.create(...)
        break
    except Exception as e:
        backoff = INITIAL_BACKOFF_SECONDS * (2 ** attempt)
        # Wait: 1s → 2s → 4s
        time.sleep(backoff)
```

**Why exponential backoff?** If the API is rate-limiting you, retrying immediately makes it worse. Increasing delays give the service time to recover.

### Deterministic Fallback

If the second LLM call (answer formatting) fails, we don't lose the answer:

```python
try:
    llm_answer = client.chat.completions.create(...)  # format nicely
except Exception:
    llm_answer = format_result_deterministic(question, result_data)
    # Still returns "$4,644.75" — just without natural language wrapping
```

The `format_result_deterministic()` function formats results without any LLM call:
- Single numeric value → `total_revenue: $4,644.75`
- Multiple rows → pipe-separated table

### Startup Validation

Before the agent even runs, `validate_config()` checks:
1. Is `OPENAI_API_KEY` set?
2. Do all 3 CSV data files exist?

Fails fast with actionable error messages instead of crashing mid-pipeline.

---

## 7. Production Hardening

### Structured Logging

All `print()` statements replaced with Python's `logging` module:

```python
logger.info("Generated SQL: %s", generated_sql)
logger.warning("API error (attempt %d), retrying in %.1fs", attempt, backoff)
logger.error("SQL rejected: %s", reason)
```

**Two output handlers:**
- **Console**: INFO level, human-readable timestamps
- **Rotating File**: DEBUG level, 10MB max, 5 backup files → `logs/wcm_agent.log`

### Modular Architecture

```
wcm_agent/
  config.py          — All constants and configuration in one place
  db.py              — Database setup (isolated, testable)
  safety.py          — SQL validation (isolated, testable)
  agent.py           — LLM pipeline (the only module that talks to OpenAI)
  formatters.py      — Output formatting (no dependencies)
  logging_config.py  — Logging setup (called once at startup)
```

**Why modularise?**
- Each module can be **unit tested independently**
- Team members can work on different modules without conflicts
- Easier to swap components (e.g., replace OpenAI with Anthropic — only `agent.py` changes)

### Pinned Dependencies

```
openai>=1.0.0,<2.0.0
python-dotenv>=1.0.0,<2.0.0
pytest>=7.0.0,<9.0.0
```

Version ranges prevent breaking changes from upstream while allowing patch updates.

---

## 8. Testing Strategy

### Test Suite Overview

**51 tests** across 4 files, all run **without an API key** (LLM calls mocked):

```bash
python -m pytest tests/ -v  # runs in < 1 second
```

### Test Categories

**SQL Safety Tests (`test_safety.py` — 23 tests):**
- Valid SELECTs pass ✅
- Every destructive keyword blocked (DROP, DELETE, INSERT, UPDATE, ALTER, CREATE, TRUNCATE)
- Comment injection blocked
- Multi-statement injection blocked
- Column names with keywords (e.g., `updated_at`) don't trigger false positives
- Input sanitisation (truncation, control char removal)
- Auto-LIMIT enforcement

**Formatter Tests (`test_formatters.py` — 7 tests):**
- Single currency result → `$4,644.75`
- Multi-row results → pipe-separated table
- Empty/null results → "No results found."

**Database Tests (`test_db.py` — 12 tests):**
- All 3 tables created with correct row counts
- `current_songs` view exists and deduplicates correctly
- **Critical**: Alex Park revenue = $4,644.75 via view ✅
- **Critical**: Naive join gives $6,308.00 (proving the view is necessary) ✅
- All writers have at least one song in the view

**Integration Tests (`test_agent.py` — 9 tests):**
- Full pipeline with mocked LLM → correct result
- Unsafe SQL → safety rejection
- LLM formatting failure → deterministic fallback
- API failure → retry with backoff
- All retries exhausted → error message
- Missing API key → clear error
- Empty question → handled gracefully
- Markdown code fences → cleaned
- No results → "No results found."

### Mocking Strategy

The OpenAI client is mocked using `unittest.mock.patch`:

```python
@patch("wcm_agent.agent.OpenAI")
def test_full_pipeline(self, mock_openai_cls, db_conn):
    mock_client = MagicMock()
    mock_openai_cls.return_value = mock_client

    # Mock returns known SQL
    mock_client.chat.completions.create.side_effect = [
        mock_sql_response,     # First call: SQL generation
        mock_answer_response,  # Second call: formatting
    ]

    result = ask_database("What is Alex Park's revenue?", db_conn)
    assert "4,644.75" in result
```

This allows testing the entire pipeline — including SQL execution against the real SQLite database — without making any API calls.

---

## 9. Code Structure

### How a Question Flows Through the Code

```
main.py
  ↓ calls validate_config()          → config.py
  ↓ calls setup_logging()            → logging_config.py
  ↓ calls init_database()            → db.py (loads CSVs into SQLite)
  ↓ calls create_current_songs_view() → db.py (deduplication view)
  ↓ calls ask_database(question)     → agent.py
       ↓ sanitize_input(question)    → safety.py (trim, clean)
       ↓ LLM Call #1: generate SQL   → OpenAI API
       ↓ validate_sql(sql)           → safety.py (4-layer check)
       ↓ enforce_limit(sql)          → safety.py (add LIMIT)
       ↓ conn.execute(sql)           → SQLite (run query)
       ↓ format_result_deterministic() → formatters.py (fallback)
       ↓ LLM Call #2: format answer   → OpenAI API
       ↓ return answer
```

### Key Design Principle: Separation of Concerns

| Module | Responsibility | Dependencies |
|---|---|---|
| `config.py` | Constants & validation | None (only `os`, `logging`) |
| `db.py` | Data loading | `config.py` |
| `safety.py` | Input/output protection | `config.py` |
| `formatters.py` | Display formatting | None |
| `agent.py` | LLM orchestration | `config`, `safety`, `formatters` |
| `logging_config.py` | Log infrastructure | `config.py` |
| `main.py` | Entry point | All modules |

---

## 10. Scalability & Future Improvements

### What Happens at 10x Data Volume?

The architecture **scales well** because:
- The LLM never sees raw data — it generates SQL regardless of table size
- SQLite handles the query execution
- At scale, you'd add an **index** on `dim_song(song_id, etl_date)` for the view

### What If the Data Format Changes?

- New columns → update `SCHEMA_DESCRIPTION` in `config.py` (single point of change)
- New tables → add to the schema description and CSV loading
- Date format change → may need to adjust the `MAX(etl_date)` logic in the view

### Production Deployment Options

| Option | Best For | What to Add |
|---|---|---|
| **CLI tool** | Internal team use | Already works as-is |
| **REST API** | Web/mobile integration | Wrap in FastAPI, add rate limiting |
| **Slack bot** | Team chat integration | Add Slack SDK, webhook handler |
| **Scheduled reports** | Automated insights | Add cron job, email/Slack output |

---

## 11. Common Interview Questions

### Q: "Why didn't you use LangChain?"

The pipeline is linear and fixed — question → SQL → validate → execute → format. There's no tool selection, no multi-step reasoning, no agent loop. LangChain adds abstraction layers that would make this harder to debug, test, and explain without adding any capability. Two LLM calls with error handling is the right level of complexity for this problem.

### Q: "How do you prevent SQL injection?"

Four layers of defense: comment stripping, SELECT whitelist, word-boundary keyword blocklist, and multi-statement blocking. Plus input sanitisation and auto-LIMIT. In production, this would be paired with a read-only database connection for defense-in-depth.

### Q: "What if the LLM generates incorrect SQL?"

The SQL is validated before execution (safety check). If it executes but returns wrong results, the deterministic formatter shows the raw data so you can debug. All generated SQL is logged for audit. The temperature is set to 0.0 for maximum consistency.

### Q: "How would you handle a new writer being added?"

Just add them to `dim_writer.csv`. The database is rebuilt from CSVs on each run. No code changes needed. In a production system with a persistent database, this would be an INSERT into the dimension table.

### Q: "Why in-memory SQLite instead of a persistent database?"

The prompt specified an in-memory database. For production, you'd use PostgreSQL or similar with:
- Persistent storage
- Read-only connection for the agent
- Connection pooling
- Query cost estimation

### Q: "How do you ensure the $4,644.75 answer is correct?"

Three ways:
1. **The test suite** — `test_alex_park_revenue_correct` executes the query and asserts `== 4644.75`
2. **Negative test** — `test_alex_park_revenue_naive_is_wrong` proves the naive join gives $6,308.00
3. **Deterministic fallback** — even if the LLM formatting fails, the raw result is preserved

### Q: "What's the difference between your view approach and adding dedup logic to the prompt?"

The view is **deterministic** — it's SQL, it's testable, it always works. A prompt instruction is **probabilistic** — the LLM might forget, misinterpret, or handle it differently on different runs. For correctness-critical business logic (revenue calculations), determinism wins.

### Q: "How does exponential backoff work?"

On each retry, the wait time doubles: 1s → 2s → 4s. This prevents overwhelming an already-struggling API. If all 3 retries fail, the agent returns a clear error message instead of crashing.

### Q: "How would you monitor this in production?"

- **Structured logs** — every SQL query, execution result, and error is logged to a rotating file
- **Metrics** — track success/failure rate, latency per LLM call, retry count
- **Alerting** — alert on elevated error rates or latency spikes
- **Audit trail** — log file preserves every query for compliance review

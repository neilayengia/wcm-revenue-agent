# WCM Revenue Insights Agent

A Text-to-SQL agent that translates natural language questions into SQL queries against a music publishing royalties database, handling historical data deduplication to ensure accurate revenue calculations.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set up your API key
cp .env.example .env
# Edit .env and add your OpenAI API key

# Run the agent
python main.py
```

## Test Output

**Question:** "What is the total revenue for Alex Park?"
**Answer:** The total revenue for Alex Park is **$4,644.75**.

The agent generates the following SQL:
```sql
SELECT ROUND(SUM(fr.amount_usd), 2) AS total_revenue
FROM fact_royalties fr
JOIN current_songs cs ON fr.song_id = cs.song_id
JOIN dim_writer dw ON cs.writer_id = dw.writer_id
WHERE dw.writer_name = 'Alex Park';
```

## Design Decisions

### 1. Process Orchestration

The agent uses a **two-stage LLM pipeline**, not a single prompt or autonomous agent:

1. **SQL Generation** — The LLM receives the database schema (not the data) and the user's question. It generates a SQL query. Temperature is set to 0.0 for deterministic, consistent output.
2. **Answer Formatting** — The raw query result is sent back to the LLM to generate a human-readable response.

**Why this approach over alternatives:**

- **Why not a single prompt?** Separating SQL generation from answer formatting keeps each prompt focused and debuggable. If the SQL is wrong, I can see it in the logs without the answer layer masking the issue.
- **Why not a full agent framework (e.g., LangChain)?** The task has a fixed, linear pipeline — there's no need for autonomous tool selection or multi-step reasoning. A simple function with two LLM calls is easier to maintain, test, and explain. Adding a framework would be over-engineering for this use case.
- **Why not let the LLM see the raw data?** Sending the schema instead of the data means the solution scales to millions of rows. The LLM generates the query; the database does the heavy lifting.

### 2. Logic vs. Inference — The Historical Records Problem

The key challenge in this dataset is `dim_song`: songs 1 and 6 have multiple rows due to title changes over time. A naive JOIN between `dim_song` and `fact_royalties` would double-count revenue for these songs.

**My approach: Solve it with logic, not inference.**

I created a SQL VIEW called `current_songs` that deduplicates `dim_song` by selecting only the row with the latest `etl_date` per `song_id`. This is a **deterministic, code-level solution** — not something I ask the LLM to figure out.

The LLM is then instructed to use `current_songs` instead of `dim_song` via the schema description. This way:
- The deduplication logic is **reliable and testable** (it's SQL, not a prompt)
- The LLM's job is simplified to query generation against clean tables
- There's zero risk of the LLM "forgetting" to deduplicate on a future question

**Verification:**
| Approach | Alex Park Revenue | Correct? |
|---|---|---|
| With `current_songs` view | $4,644.75 | ✓ |
| Naive join through `dim_song` | $6,308.00 | ✗ (double-counts song 1) |

### 3. Safety — Preventing Destructive Commands

The `validate_sql()` function implements a keyword blocklist that rejects any query containing `DROP`, `DELETE`, `INSERT`, `UPDATE`, `ALTER`, `CREATE`, or `TRUNCATE`. It also requires that the query starts with `SELECT`.

**For a production deployment, I would add:**
- A **read-only database connection** (SQLite's `file:db?mode=ro` or a read-only PostgreSQL role) — this is the strongest guarantee, as it's enforced at the database level regardless of what SQL is generated.
- **Query execution timeouts** to prevent runaway queries from expensive JOINs.
- **Parameterized output limits** (e.g., always append `LIMIT 1000`) to prevent accidental full-table dumps.

The keyword blocklist is a defense-in-depth layer, not the sole protection.

### 4. Scalability & Reliability

**At 10x volume (1,000 transactions → 10,000+):**
- The architecture scales well because the LLM never sees the raw data — it only generates SQL. Whether the table has 100 rows or 10 million, the LLM's job is the same.
- The `current_songs` view would benefit from an index on `(song_id, etl_date)` for faster deduplication at scale.

**If the data format shifted:**
- New columns in `dim_song` or `fact_royalties` would require updating the `SCHEMA_DESCRIPTION` string. This is the single point of configuration — the LLM adapts its SQL generation based on whatever schema it's given.
- If the date format in `etl_date` changed, the `current_songs` view logic (which uses `MAX(etl_date)`) might need adjustment depending on the new format's sort behavior.

**Reliability improvements for production:**
- **Retry logic** with exponential backoff for API failures.
- **Query result caching** for frequently asked questions.
- **Logging** of every generated SQL query for audit and debugging.
- **Fallback responses** when the LLM generates invalid SQL after multiple attempts.

## Project Structure

```
wcm-revenue-agent/
  main.py              — Entry point with all logic
  requirements.txt     — Python dependencies
  .env.example         — API key template
  .gitignore           — Excludes .env and caches
  data/
    dim_writer.csv     — Writer dimension table
    dim_song.csv       — Song dimension table (with historical records)
    fact_royalties.csv — Royalty transactions
  output/
    alex_park_result.txt — Test output
```

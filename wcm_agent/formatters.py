"""
Result Formatters
=================
Deterministic formatting of query results, used as both the
primary fallback and the authoritative answer format.
"""

import logging

logger = logging.getLogger(__name__)


def format_result_deterministic(question, result_data):
    """
    Format query results without using the LLM.

    This is the fallback and the authoritative answer format.
    - Single numeric result → currency format ($X,XXX.XX)
    - Single non-numeric result → key: value
    - Multi-row result → pipe-separated table
    """
    if not result_data:
        return "No results found."

    # Single-value result (e.g., total revenue)
    if len(result_data) == 1 and len(result_data[0]) == 1:
        key = list(result_data[0].keys())[0]
        value = result_data[0][key]
        if isinstance(value, (int, float)):
            return f"{key}: ${value:,.2f}"
        return f"{key}: {value}"

    # Multi-row result
    lines = []
    for row in result_data:
        parts = []
        for k, v in row.items():
            if isinstance(v, float):
                parts.append(f"{k}: ${v:,.2f}")
            else:
                parts.append(f"{k}: {v}")
        lines.append(" | ".join(parts))
    return "\n".join(lines)

"""
n8n webhook client.

Used by the Streamlit review page to pull candidate competitor matches
from n8n's "Competitor Analysis" data table. We use a webhook-based fetch
endpoint instead of n8n's REST API because API access requires a paid
plan; webhooks work on all plan tiers.

The fetch webhook is workflow `Competitor Analysis Fetch` in n8n
(workflowId: qCIIZXoBTCaAxW4X). It returns all rows in the data table
as a JSON array, ordered by createdAt DESC.

Env override:
  N8N_FETCH_WEBHOOK_URL — defaults to the production URL below.
"""

from __future__ import annotations

import os
from typing import Any, Optional

import requests


DEFAULT_FETCH_URL = "https://topas.app.n8n.cloud/webhook/competitor-analysis-fetch"


class N8nFetchError(RuntimeError):
    """Raised when the fetch webhook fails or returns an unexpected payload."""


def get_fetch_url() -> str:
    """Resolve the fetch webhook URL. Override via env if needed."""
    return os.getenv("N8N_FETCH_WEBHOOK_URL", DEFAULT_FETCH_URL).strip()


def fetch_candidates(timeout: int = 30, url: Optional[str] = None) -> list[dict[str, Any]]:
    """Pull all rows from n8n's Competitor Analysis data table.

    Returns a list of dicts, one per row, with keys matching n8n's column
    names plus n8n's bookkeeping fields (id, createdAt, updatedAt).

    Raises N8nFetchError if the webhook is unreachable or returns
    something that isn't a JSON array.
    """
    fetch_url = (url or get_fetch_url())
    try:
        r = requests.post(fetch_url, json={}, timeout=timeout)
    except requests.RequestException as exc:
        raise N8nFetchError(f"Could not reach fetch webhook: {exc}") from exc

    if r.status_code != 200:
        raise N8nFetchError(
            f"Fetch webhook returned HTTP {r.status_code}: {r.text[:300]}"
        )

    try:
        payload = r.json()
    except ValueError as exc:
        raise N8nFetchError(f"Fetch webhook returned non-JSON: {r.text[:200]}") from exc

    if not isinstance(payload, list):
        raise N8nFetchError(
            f"Expected a JSON array, got {type(payload).__name__}: {str(payload)[:200]}"
        )

    return payload

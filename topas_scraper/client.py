"""
Thin wrapper around the Firecrawl Python SDK.

Two reasons this exists rather than calling firecrawl directly from the CLI:
1. Centralised error handling — all callers see the same ScrapeResult dataclass.
2. Easy to mock in tests — swap the client, not the SDK.

v0.8 update: scrape() now optionally accepts a JSON-extraction schema. When
provided, Firecrawl renders the page and uses an LLM internally to populate
the schema, returning structured data alongside markdown. This replaces 9
operator-specific regex parsers with one schema-driven pipeline.
"""

import os
import time
from dataclasses import dataclass, field
from typing import Optional

from firecrawl import Firecrawl

from .config import SCRAPE_FORMATS, SCRAPE_ONLY_MAIN, SCRAPE_TIMEOUT_MS


@dataclass
class ScrapeResult:
    """What every scraper call returns. Always — never raises."""

    url: str
    success: bool
    markdown: Optional[str] = None
    html: Optional[str] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
    title: Optional[str] = None
    # Populated when scrape() is called with a schema. Contains the LLM-extracted
    # structured data conforming to the schema (typically {duration_days,
    # from_price_dkk, departures: [...]}).
    extracted: Optional[dict] = None


class FirecrawlClient:
    """Wraps Firecrawl. Provides scrape() that always returns ScrapeResult."""

    def __init__(self, api_key: Optional[str] = None):
        key = api_key or os.environ.get("FIRECRAWL_API_KEY")
        if not key:
            raise RuntimeError(
                "FIRECRAWL_API_KEY not set. Copy .env.example to .env and add your key."
            )
        self._client = Firecrawl(api_key=key)

    def scrape(
        self,
        url: str,
        max_retries: int = 2,
        overrides: Optional[dict] = None,
        schema: Optional[dict] = None,
    ) -> ScrapeResult:
        """Scrape a URL with the configured options. Retries on transient errors.

        `overrides` lets the caller override defaults like only_main_content or
        add Firecrawl options like wait_for / actions for tricky sites.

        `schema` enables LLM-powered structured extraction. When provided,
        Firecrawl returns both markdown AND the populated schema in result.extracted.
        Use this for tour data extraction — replaces operator-specific regex parsers.
        """
        # Merge defaults with per-target overrides
        formats: list = list(SCRAPE_FORMATS)
        if schema is not None:
            # Add JSON extraction format alongside markdown — get both in one call
            formats = ["markdown", {"type": "json", "schema": schema}]

        opts = {
            "formats": formats,
            "only_main_content": SCRAPE_ONLY_MAIN,
            "timeout": SCRAPE_TIMEOUT_MS,
        }
        if overrides:
            opts.update(overrides)
            # Don't let overrides clobber formats when schema is in play
            if schema is not None:
                opts["formats"] = formats
                # only_main_content=False is usually needed for schema extraction
                # because departure tables often live outside the "main" content area
                opts.setdefault("only_main_content", False)
                opts["only_main_content"] = False

        last_err = None
        for attempt in range(max_retries + 1):
            try:
                doc = self._client.scrape(url, **opts)
                # Firecrawl v4 returns a Document object with attribute access.
                # Defensive: handle dict response too in case SDK shape changes.
                markdown = _get_attr_or_key(doc, "markdown")
                html = _get_attr_or_key(doc, "html") or _get_attr_or_key(doc, "rawHtml")
                metadata = _get_attr_or_key(doc, "metadata") or {}
                title = _get_attr_or_key(metadata, "title")
                status_code = _get_attr_or_key(metadata, "statusCode") or _get_attr_or_key(metadata, "status_code")

                # JSON extraction result lives at .json (Firecrawl v4 SDK shape)
                extracted = _get_attr_or_key(doc, "json") or _get_attr_or_key(doc, "extract")

                if not markdown and not html and not extracted:
                    last_err = "Empty response — no markdown, html, or extracted data"
                    if attempt < max_retries:
                        time.sleep(1.5 * (attempt + 1))
                        continue

                return ScrapeResult(
                    url=url,
                    success=bool(markdown or html or extracted),
                    markdown=markdown,
                    html=html,
                    title=title,
                    status_code=status_code,
                    extracted=extracted if isinstance(extracted, dict) else None,
                )

            except Exception as e:
                last_err = str(e)
                if attempt < max_retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue

        return ScrapeResult(url=url, success=False, error=last_err)


def _get_attr_or_key(obj, name):
    """Safely access either an attribute on an object or a key on a dict-like.

    The Firecrawl SDK has changed shape between versions — sometimes returns
    Document objects, sometimes dicts. This helper papers over the difference.
    """
    if obj is None:
        return None
    if hasattr(obj, name):
        return getattr(obj, name, None)
    if isinstance(obj, dict):
        return obj.get(name)
    return None

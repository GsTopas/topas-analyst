# Topas Tour Taxonomy

**Status:** Draft v0.9 — Catalog-discovery shifted from auto-scrape to manual seed-list. The /faellesrejser/ scroll-actions approach hit a structural ceiling (10→13→17 tours across 4 attempts, never 49). User maintains a curated seed-list of 49 URLs in `topas_catalog.py`; "Add new tour" UI for ongoing maintenance. AI-discovery scope is now **per-tour** (find competitors for ONE Topas tour), not whole-catalog. Discovery runs through user's n8n workflow with Firecrawl + Claude tools — Streamlit triggers via webhook, n8n writes results to its "Competitor Analysis" table. Streamlit reads back for review. Section 2.12 simplified to reflect that catalog-population is no longer code-mediated.
**Owner:** Pricing analyst team.
**Companion document:** `methodology.md` (matching rules, normalization, comparison principles).

---

## 1. Purpose

A structured way to describe every Topas tour so that:

1. Two Topas tours can be compared to each other (internal substitution).
2. Any Topas tour can be matched to competitor tours at the right tier of competition.
3. The web tool can filter, group, score, and rank tours algorithmically.

A tour is **not** a label like "8-day Italian hiking trip." A tour is a vector of values across the dimensions below. Two tours compete to the degree their vectors overlap. Comparison happens by checking how many dimensions align, and how closely.

The taxonomy is a tool for honest pricing analysis. It must not be biased to flatter Topas. Structural differences between Topas and competitors are recorded faithfully — including the cases where Topas has no real advantage, and including the cases where competitors hold structural assets that Topas does not.

### 1.1 Target end-state — what this enables

The taxonomy is the schema for an automated production system. The end-state goal:

1. **Weekly scrape of every Topas tour** captures current departures, prices, and availability.
2. **Per-Topas-tour competitor lookup** finds matching competitor products that pass eligibility, classifies them by tier, and pulls their current departures and prices.
3. **Snapshot diff against previous week** flags price moves, availability shifts, and new sell-outs on both sides.
4. **Pricing dashboard** surfaces the tours where something changed, ranked by what the head of agency needs to act on.

The taxonomy must be machine-fillable from scraped pages and stable enough that competitor diffs week-over-week are meaningful. Every field added in this document should pass a "can the scraper fill this without human judgment" check, or be flagged as analyst-fill.

---

## 2. The dimensions

### 2.1 Identity

| Field | Description | Example | Fill source |
|---|---|---|---|
| `tour_code` | Stable internal Topas code (turkode). Primary key for Topas. | `ITTO` | Topas page |
| `competitor_id` | For competitor tours: `{operator_slug}:{url_slug}` | `viktorsfarmor:kultur-og-vandring-i-nepal` | Derived |
| `tour_name` | Display name | `La Fontanella – En ægte perle i Toscana` | Page H1 |
| `url_slug` | Web path | `italien-la-fontanella-en-aegte-perle-i-toscana` | URL |
| `operator` | Operator brand (not holding company — see 2.11) | `Topas`, `Viktors Farmor`, `Stjernegaard` | Source |
| `status` | Active / discontinued / new / pioneer | Active | Page state |

`tour_code` is stable for Topas. URL slugs change. For competitors that don't expose an internal code, `competitor_id` (operator + url_slug) is the primary key, with fallback fuzzy matching on tour name + duration + first departure date when slugs change.

### 2.2 Geography

| Field | Description | Example |
|---|---|---|
| `country` | ISO country code | `IT` |
| `region` | Topas-defined region within country | `Toscana-Garfagnana` |
| `sub_region` | More specific area / tour footprint | `Apuanske Alper, Cinque Terre` |
| `multi_country` | Boolean — tour spans more than one country | `false` (true for Patagonia AR+CL) |
| `itinerary_landmarks` | List of named places visited (cities, villages, peaks, parks) | `["Pokhara", "Ghandruk", "Tadapani", "Jhinu Danda"]` |

**Rule:** Competitors are matched at **region** level by default, not country. A Toscana tour and a Sicilien tour do not compete just because both are "Italy." See `methodology.md` for the geographic matching tiers.

`itinerary_landmarks` is new in v0.5. It supports the `itinerary_overlap_score` open question and is also useful for the scraper: extracting named places from a dagsprogram is more reliable than free-text region matching across operators.

### 2.3 Structure

| Field | Description | Values |
|---|---|---|
| `tour_format` | The product type | `Fællesrejse`, `Individuel`, `Privat gruppe` |
| `audience_segment` | Who the tour is sold to | `Åben`, `30–50 år`, `Familie`, `Andet` |
| `departure_model` | How dates work | `Fast afgang`, `Fleksibel`, `På forespørgsel` |
| `base_model` | The shape of the trip on the ground | `Single-base`, `Multi-base`, `Punkt-til-punkt` |

**Rule:** Fællesrejse and Individuel are different products. They compete with different competitor offerings. Pair comparisons in this system run **only within `tour_format = Fællesrejse`** — see `methodology.md` for the comparison-eligibility rule.

`base_model` matters because La Fontanella (single-base, daily walks) is structurally different from Dolomitterne hut-to-hut, even though both are 8-day Italian vandreture.

#### `tour_format` is a category label, not a brand term

`Fællesrejse` is our internal canonical name for the category. The defining characteristics — **all five must be present** — are:

1. **Fixed departure date** published on the operator's site
2. **Pre-published price per departure** (not "fra X kr." quoted on inquiry)
3. **Danish-speaking guide** travels with the group from Denmark
4. **Group travels together from Denmark** as a fixed cohort
5. **Set itinerary** that does not change per booking

If a product fails any one of these tests, it is **not** a Fællesrejse, regardless of what the operator calls it. Critically: products that have a Danish guide and a set itinerary but lack pre-published dates and prices ("fra Kontakt os", "ingen planlagte afgange p.t.") fail the test. They are scheduled-but-on-request group products and operationally behave more like flexible-departure individuel products. Flag them via `departure_visibility = On-request only` (see 2.9) and exclude from pair comparisons.

**Important nuance on criterion 2 (pre-published price):** A departure with a fixed date but a temporary "Afventer pris" / "Pris kommer snart" status still counts as eligible *for the tour record* — the product is structurally a Fællesrejse and the price will be filled in. Per-departure, that specific departure is excluded from price comparisons until the price is published, but the tour as a whole stays in the eligible set. See `availability_status` enum in 2.9.

Competitors use different vocabulary for the same product category. The labels in the table below all map to `Fællesrejse` in the taxonomy *if and only if* the product passes all five tests above. The full mapping with the eligibility-test logic lives in `methodology.md` section 5.5.

| Operator label seen in the wild | Maps to (if eligible) |
|---|---|
| Fællesrejse (Topas) | Fællesrejse |
| Grupperejse (Viktors Farmor, Kipling) | Fællesrejse — verify pre-published dates + prices |
| Aktive grupperejser (Gjøa) | Fællesrejse |
| Aktive rejser med dansk rejseleder (Jysk Rejsebureau) | Fællesrejse |
| Rundrejse med dansk rejseleder (Stjernegaard) | Fællesrejse — verify pre-published dates + prices |
| Fællestur (Vagabond Tours) | Fællesrejse — verify pre-published dates + prices |

The same logic applies to the other format values:

- `Individuel` covers: Individuel rejse (Topas), Skræddersyet rejse, Rejseforslag, På egen hånd-rejse, Tilpasset rejse, daily-departure private treks (e.g. Kipling Annapurna Base Camp), Tilkøb udflugter m/ dansktalende rejseleder (Stjernegaard's individuel-format products).
- `Privat gruppe` covers: Privat tur, Privatrejse, Lukket gruppe.

The scraper must do this normalization at ingest time so that competitor data lands with a single canonical value in the `tour_format` field. The scraper must also flag the eligibility-test result for each product (Fællesrejse-eligible / fails-test-on-request / Individuel / Privat gruppe).

### 2.4 Duration

| Field | Description | Example |
|---|---|---|
| `duration_days` | Total days incl. travel | `8` |
| `duration_band` | Categorical band | `Short` |

Suggested bands, derived from observed Topas portfolio clustering:

- **Short:** 6–9 days — European base/region tours
- **Medium:** 10–15 days — multi-region, Marokko, Sydvietnam
- **Long:** 16–22 days — full-country rundrejse, Nepal trekking, Peru
- **Expedition:** 23+ days — Everest BC + Cho La, etc.

Competitor tours are expected to cluster similarly. Bands let us match without forcing exact day-count parity.

### 2.5 Activity mix

| Field | Description |
|---|---|
| `primary_activity` | The dominant activity |
| `secondary_activities` | List, ordered by prominence |
| `activity_intensity_share` | Approximate % of days spent on the primary activity |

**Activity values** (from observed Topas portfolio):

- Vandring (day-walks from a base)
- Højrute / hut-to-hut trekking
- Trekking (multi-day backcountry)
- Cykling (road or hybrid)
- Mountainbike
- Sejlads + vandring
- Sejlads + cykling
- Yoga + vandring
- Langrend / ski
- Bjergbestigning / topbestigning
- Multi-aktivitet rundrejse (no single dominant activity)
- Kulturrundrejse (sightseeing-led, light walking, no trekking)

**Rule:** Multi-region rundrejser (e.g., Vietnam syd til nord) deliberately combine cycling, walking, kayaking, and culture. Forcing them into a single activity bucket misrepresents them. Use `Multi-aktivitet rundrejse` and rely on `secondary_activities` for nuance.

**Rule:** A culturally-focused rundrejse with sightseeing and short walks (Viktors Farmor's Peru, Stjernegaard's Nepal) is distinct from a multi-activity rundrejse where the activity content is itself the product (Topas's Vietnam). The `Kulturrundrejse` value lets us mark this difference instead of collapsing them.

### 2.6 Difficulty

| Field | Description |
|---|---|
| `topas_difficulty` | Topas's own scale (1–5, sometimes a range like 1–3) |
| `normalized_difficulty` | Standardized scale for cross-operator comparison |
| `max_altitude_m` | If trekking/bestigning, max elevation reached |
| `daily_hours_active` | Typical active hours per day |

Topas's difficulty grading is internal. Competitors use their own scales (often easy/medium/hard, 1–4, symbol-based like Viktors Farmor's 1–4 støvler, or letter-based like Kipling's A/B/C/D). The normalized field is what comparisons run against. The mapping rules live in `methodology.md`.

### 2.7 Inclusions

A separate sub-record. This is where "apples vs. oranges" gets exposed in competitor comparisons.

| Field | Description |
|---|---|
| `flights_included` | Boolean |
| `flight_origin` | `CPH` (default), `Multi-airport`, or specific |
| `internal_flights_count` | Integer |
| `internal_transport_included` | Boolean |
| `meals_breakfast_count` | Number of breakfasts included |
| `meals_lunch_count` | Number of lunches included |
| `meals_dinner_count` | Number of dinners included |
| `meals_completeness_score` | Calculated: covered meals / possible meals |
| `board_phrase_raw` | Original phrasing from source ("halvpension", "fuld pension", "B&B") — kept for audit |
| `accommodation_type` | `Hotel`, `Lodge`, `Tehus`, `Hytter`, `Båd`, `Telt`, `Mixed` |
| `accommodation_tier` | `Budget`, `Mid`, `Premium`, `Mixed` |
| `single_supplement_dkk` | Amount |
| `guide_language` | `Dansk`, `Engelsk`, `Lokal-only` |
| `guide_count` | 1, 2, or more |
| `guide_named` | Boolean — is the rejseleder named on the departure? |
| `local_guides_included` | Boolean |
| `local_guide_language` | `Dansk`, `Engelsk`, `Lokal-only` |
| `permits_fees_included` | Boolean (matters for trekking/national parks) |

**Rule:** Headline price comparison without inclusions is misleading. The web tool must always render inclusions side-by-side when displaying competitor comparisons. Phrases like "halvpension" must be parsed into the structured meal counts before comparison — see `methodology.md` for the parsing convention.

`guide_named` is new in v0.5. Operators like Viktors Farmor name the specific rejseleder per departure (e.g., "Victor Rolighed" on the Oct 2026 Nepal departure). This is a mild quality signal worth tracking — named guides usually mean the operator is committing to that person, which is a stronger signal than generic "Dansk rejseleder."

### 2.8 Group dynamics

| Field | Description |
|---|---|
| `min_participants` | Threshold for departure to run |
| `max_participants` | Cap |
| `typical_size` | Observed average if known |

### 2.9 Pricing — separate from the tour record

A tour has many departures. Pricing data lives in its own table.

**Departures table:**

| Field | Description |
|---|---|
| `tour_code` | FK to tour |
| `departure_code` | Topas tripCode (e.g., `ITTO2604`) — or competitor equivalent |
| `start_date` | |
| `end_date` | |
| `price_dkk` | Headline price for that departure (nullable — see status below) |
| `availability_status` | See enum below |
| `season_band` | `Low`, `Shoulder`, `High` (we define) |
| `rejseleder_name` | Named rejseleder if exposed |

**`availability_status` enum** (expanded in v0.5):

| Value | Meaning | Used for price comparison? |
|---|---|---|
| `Garanteret afgang` | Departure confirmed running, often near-full | Yes |
| `Få pladser` | Limited spots remaining | Yes |
| `Udsolgt` | Sold out | Yes (for sell-out signal — see methodology 13 for note on competitor-side sell-out as observable demand signal, new in v0.6) |
| `Åben for booking` | Booking open, status not signalled | Yes |
| `Ledig` | Viktors Farmor / Stjernegaard equivalent of Åben | Yes |
| `Afventer pris` | Date fixed but price not yet published | **No** — exclude from price comparison until priced; keep as planned-departure signal |
| `Garanteret` | Stjernegaard equivalent of Garanteret afgang | Yes |

**Tour-level departure visibility (on the tour record, not per departure):**

| Field | Description |
|---|---|
| `departure_visibility` | `Full` / `Partial` / `Single-departure` / `On-request only` |
| `pricing_slope` | Computed from per-departure prices across calendar. Values: `Flat`, `Climbing` (later afgange more expensive), `Declining` (later afgange cheaper), `Peaked-summer`, `Peaked-Christmas`, `Inconsistent`. New in v0.6. |

This is also a **Fællesrejse-eligibility flag**: a product with `departure_visibility = On-request only` cannot be classified as Fællesrejse for comparison purposes (per 2.3 defining test). It must be excluded from pair comparisons even if the operator calls it a Grupperejse, because the lack of pre-published dates and prices makes price comparison structurally unsound.

**Why `pricing_slope` is a tour-level dimension (new in v0.6):**

Two operators can sit at the same headline price band yet pursue very different strategies across their calendar. When VNSN and PTMD comparison data was first analysed, the headline-price comparison (Topas "fra X" vs competitor "fra Y") missed the bigger story: how each operator prices departure-to-departure across the year. Smilrejser climbs into the future (locks in early bookings at lower 2026 prices, raises 2027); Topas PTMD declines into the future (later afgange cheaper, suggesting either aggressive early-bird or capacity that needs filling). Where a Topas tour's slope mismatches its closest Tier 1 competitor's slope, that mismatch is itself a methodology signal worth investigating — see methodology section 14.bis.

The slope is computed from the `departures` table per tour. Update weekly in production. The slope can be a fixed pattern (peaked-summer is structural to outdoor destinations) or a strategy choice (climbing reflects price-discovery confidence; declining reflects fill-the-calendar pressure).

**Snapshots table** — the time-series layer:

| Field |
|---|
| `departure_code` |
| `observed_at` |
| `price_dkk` |
| `availability_status` |
| `scraper_run_id` |

The snapshot layer is what makes the system intelligent over time. Without it the system answers "what is the price now." With it the system answers "what is the price trend, when do departures sell out, at what price points do competitors hold or move."

`scraper_run_id` is new in v0.5. It links each snapshot row to the weekly scraper run that produced it. This makes diff queries simple ("what changed between run N and run N+1") and lets us audit individual scraper runs if a diff looks suspicious.

### 2.10 Differentiators (two distinct kinds)

The earlier draft of this section lumped all "Topas-distinctive" features together. That introduced a flattering bias: features that good operators all offer were being credited to Topas as if they were structural advantages. The revised model separates them.

#### 2.10.1 Structural assets

Things an operator owns or has locked-in access to, that competitors **cannot** replicate even if they wanted to. These are real and they affect comparison.

**Topas's structural assets observed so far:**

| Field | Description |
|---|---|
| `structural_assets` | List. Examples: `Topas Ecolodge` (owned property in Sapa), `Coletti family / Roggio pensionat` (30-year exclusive partnership), `Self-designed Cordillera Urubamba trek` (Topas's own routed multi-day trek not sold by direct competitors) |

**Competitors also have structural assets.** This is important — the system must record competitor structural assets too, because they affect how we read pricing differentials. Examples observed so far:

- **Kipling Begnas Camp** — Kipling-operated permanent safari-style tented camp on Sal Danda ridge above Begnas Tal, established October 2018, 8 tents at 34 m² each with private bathrooms. Per Kipling's own positioning: "den eneste af sin art i Nepal." No other Danish operator can stay there. This is a structural asset for Kipling on any Nepal product where Begnas Camp is used.

**Rule:** When a tour has a structural asset, competitor matching is flagged: comparison is possible on the broader frame but never apples-to-apples on the asset itself. The asset is a legitimate justification for *some* price premium — but only some.

**Rule (anti-flattery):** A competitor's structural asset is recorded with the same seriousness as Topas's. If a Kipling product has Begnas Camp and Topas does not have an equivalent in the same region, that is a real Kipling advantage — and the system must say so rather than burying it.

#### 2.10.2 Category-standard features

Things Topas does that *good operators in the same category also do*. These are **not** competitive advantages. Listing them as differentiators is a recipe for over-pricing.

| Field | Description |
|---|---|
| `category_standard_features` | List, captured for honesty. Examples: `Danish-speaking guide`, `Multi-guide split on harder days`, `Fixed-departure CPH flight included`, `Half-board accommodation`, `Local guides on top of Danish turleder`, `Nationalpark-tilladelser inkluderet for trekking-produkter` |

**Rule:** Category-standard features are recorded but **never** used to justify a price premium in recommendations. They equalize between Topas and competent competitors; they do not separate.

**Examples of the distinction:**

- Danish guide → category-standard. Most serious Danish operators have one. Not a Topas advantage.
- Multi-guide split → category-standard. Confirmed via Gjøa Corfino tour, which uses Danish turleder + local guide for the same purpose.
- Topas Ecolodge in Sapa → structural asset. Topas owns the property. No competitor can stay there.
- 30-year exclusive use of the Coletti family pensionat in Roggio → structural asset. The relationship and village logistics are Topas's.
- Half-board ("halvpension") → category-standard. Gjøa includes it too.
- Sherpa-guides + bærere on Nepal-trekking → category-standard. Topas, Kipling, Jysk all use them.

#### 2.10.3 Soft factors

Genuinely soft signals that may or may not affect competitive position. Recorded for context, never used as price justification.

| Field | Description |
|---|---|
| `meal_culture_focus` | Boolean — is local cuisine a feature, not just sustenance? |
| `unique_logistics` | Free text — anything structurally distinctive |

### 2.11 Operator and holding-company structure (new in v0.5)

Several Danish operators are part of larger holding companies. The taxonomy tracks the operating brand (`operator`), not the holding company, because the operating brand is what determines the product, the audience, and the booking flow. Holding-company affiliation is a separate dimension.

| Field | Description |
|---|---|
| `operator` | The brand the customer books under. Primary lookup key. |
| `holding_company` | Parent group, if any. |

**Why this matters:** Aller Leisure A/S owns at least eight Danish operating brands. Treating "Aller" as a single competitor would conflate very different products with very different audiences. Smilrejser (kulturrejser, Europe-only) and Stjernegaard (rundrejser w/ dansk leder, global) compete with completely different parts of Topas's portfolio.

**Aller Leisure brands observed so far:**

| Brand | Focus | Relevant to Topas? |
|---|---|---|
| Smilrejser (formerly Kulturrejser Europa, rebranded 2025) | Kulturrejser, Europe-only | Yes for Topas Europe products |
| Stjernegaard Rejser | Rundrejser w/ dansk leder, global | Yes — Tier 2 competitor for Nepal, likely also Peru, Vietnam, etc. |
| Nilles & Gislev Rejser | Bus + fly grupperejser, broad | Likely overlap with Topas on some destinations — not yet evaluated |
| Nyhavn Rejser | Luxury / skræddersyet | Different segment, lower overlap |
| NYATI Safari | Africa-only | Possibly overlaps with Topas Tanzania/Kenya products |
| Gaia Travel | Unknown | Not yet evaluated |
| Let's do Cruise | Krydstogt | No overlap |
| Aller Travel | Norwegian market | No overlap (DK product map only) |
| ALIVE | Unknown | Not yet evaluated |

This map is incomplete. As the production scraper rolls out, each Aller Leisure brand needs its own portfolio assessment — same way we'd assess any independent operator.

### 2.12 Scraper-implementation notes (rewritten v0.8 — universal LLM extraction)

This section captures known per-operator scraping behaviors so the production system handles them correctly. It is the bridge between this taxonomy and the scraper code.

**v0.8 architectural shift:** the scraper no longer has 9 operator-specific regex parsers. It has ONE universal pipeline:
1. **Tier 1 — Firecrawl + JSON schema extraction** (the LLM populates `extraction_schema.py` from the rendered page). Default for every operator.
2. **Tier 2 — Sitemap-variant discovery** (per `OPERATOR_VARIANT_PATTERNS` registry in `sitemap_discovery.py`). Auto-runs for operators registered in the pattern table.
3. **Tier 3 — Claude vision fallback** (per `vision_extractor.py`). Auto-fires when Tier 1 returns 0 departures from a target marked `vision_fallback=True`.

The per-operator notes below describe what each operator's pages look like and which combination of tiers handles them. The scraper code itself is operator-agnostic — the operator-specific knowledge lives in this table, not in code.

| Operator | Page rendering | Departure data location | Strategy (per methodology 7.bis–7.ter) |
|---|---|---|---|
| Topas | Server-rendered HTML | tripCode-tagged departure list, fully visible to LLM | Tier 1 (LLM extraction) |
| Smilrejser | Server-rendered HTML | Per-departure table with afrejsedato + airport + status visible in markdown. Multi-airport (CPH+AAB) — captured per-departure via `flight_origin` field. | Tier 1 (LLM extraction) |
| Jysk Rejsebureau | Server-rendered HTML | Sparse — many products show "Nye priser på vej". LLM correctly returns 0 departures for those (Afventer pris not eligible). | Tier 1 (LLM extraction) |
| Viktors Farmor | JS-rendered (handled by Firecrawl wait_for) | Departure dates render client-side; Firecrawl with `wait_for=4000` captures them; LLM extracts named rejseledere correctly. | Tier 1 (LLM extraction) |
| Ruby Rejser | Cookie-walled, server-rendered after cookie accept | Travel Code "VAG-NNN" used as departure_code-equivalent. `wait_for=3000` + `only_main_content=False`. | Tier 1 (LLM extraction) |
| Nilles & Gislev | Same Aller Leisure backend as Smilrejser | When publishing departures: clean per-departure data. When showing "Nye afgange paa vej": LLM correctly returns 0 (Afventer pris). | Tier 1 (LLM extraction) |
| Best Travel | Stena Line backend, JS-rendered | `wait_for=4000` + LLM extraction handles it. Madeira product is kulturrejse, NOT vandreferie — LLM correctly extracts duration but eligibility check downstream filters it out (T3/borderline ineligible for PTMD). | Tier 1 (LLM extraction) |
| Stjernegaard | JS-rendered `/priser-og-datoer/` subpages | Per-departure table (date + return + days + price + status) fully visible after JS executes. Danish month-abbreviated dates ("14. mar. 2027") and `kr.`-prefixed prices — LLM handles both correctly per schema description. | Tier 1 (LLM extraction) — verified working v0.8 |
| Albatros Travel | React-rendered, departure tabs only show one date at a time | Tier 1 only sees the default-selected departure (typically the first). Other dates exist as URL-variants in `albatros.dk/sitemap.xml` (`?variant=YYYYMMDD`). Sitemap-discovery (Section 7.ter) fetches sitemap, extracts variant dates, merges with Tier 1 base scrape. Result: complete departure list (e.g. NPAP returns both 2026-10-29 and 2027-03-04). | Tier 1 + Tier 2 (sitemap-variant discovery via OPERATOR_VARIANT_PATTERNS registry) — verified working v0.8 |
| Kipling Travel | Server-rendered HTML; group-size selector on Individuel products | Detect group-size selector → mark as Individuel (eligibility fail at the methodology layer). For grupperejse products with fixed dates, Tier 1 works. | Tier 1 (LLM extraction) |
| Vagabond Tours | Server-rendered HTML | Nepal/Asia products use "lokal turleder" not Danish guide — `guide_language` field on the LLM-extracted record drives eligibility check. | Tier 1 (LLM extraction) |

**v0.8 reality check:**

All 19 production targets work end-to-end as of v0.8. The Albatros NPAP target was the canary case — it failed in every previous version (v0.5 missed completely, v0.6 returned 1 of 2 departures, v0.7 with vision still returned 1 of 2). v0.8 with sitemap-variant discovery returns 2 of 2 departures, matching browser-visible data.

This is the first version where Section 0.2 (data acquisition principle — tool matches analyst's reach) is fully satisfied across all configured operators.

**Adding a new operator in v0.8:**

The cost of adding a new operator dropped substantially. v0.7 required writing a new regex parser file (~150 lines). v0.8 requires:

1. Add a `TourTarget` entry in `config.py` with the URL and metadata (~10 lines)
2. If the operator uses URL-variants in sitemap (Albatros pattern), add an entry to `OPERATOR_VARIANT_PATTERNS` (~5 lines)
3. Done. Tier 1 (LLM extraction) handles the rest automatically.

If LLM extraction returns thin data on the new operator, Tier 3 vision fallback fires automatically. No code change needed.

---

## 3. Worked examples

### Example A — La Fontanella (ITTO)

| Dimension | Value |
|---|---|
| tour_code | ITTO |
| operator | Topas |
| country / region / sub_region | IT / Toscana-Garfagnana / Apuanske Alper, Cinque Terre |
| tour_format | Fællesrejse |
| audience_segment | Åben |
| departure_model | Fast afgang |
| departure_visibility | Full (34 departures across 2026–2027) |
| base_model | Single-base (Roggio) |
| duration_days / band | 8 / Short |
| primary_activity | Vandring (day-walks) |
| secondary_activities | Kultur (Lucca, Cinque Terre) |
| topas_difficulty | 1–3 (split-group on harder days) |
| flights_included | Yes (CPH–Pisa) |
| meals_completeness_score | High |
| board_phrase_raw | Itemized; not sold as "halvpension" — closer to half-board with some lunch picnics |
| single_supplement_dkk | 2.200 |
| guide_language | Dansk |
| guide_count | 2 (on splittable days) |
| max_participants | 28 |
| structural_assets | Coletti family / Roggio pensionat (30-year relationship) |
| category_standard_features | Danish guide, multi-guide split, half-board, CPH flight included |
| price range across departures | 9.470 – 11.970 (26.5% spread, peak mid-July) |

### Example B — Vietnam syd til nord (VNSN)

| Dimension | Value |
|---|---|
| tour_code | VNSN |
| operator | Topas |
| country / region | VN / Hele landet (Mekong, Hoi An, Ninh Binh, Halong, Hanoi, Sapa) |
| tour_format | Fællesrejse |
| audience_segment | Åben |
| departure_model | Fast afgang |
| departure_visibility | Full |
| base_model | Punkt-til-punkt |
| duration_days / band | 19 / Long |
| primary_activity | Multi-aktivitet rundrejse |
| secondary_activities | Cykling, vandring, kajak, kultur |
| topas_difficulty | 2–3 |
| flights_included | Yes (CPH–VN + 2 internal) |
| single_supplement_dkk | 6.500 |
| guide_language | Dansk + lokale |
| max_participants | 22 |
| structural_assets | Topas Ecolodge (Topas-owned property in Sapa) |
| category_standard_features | Danish guide, local guides, internal flights included |
| price range across departures | 26.970 – 28.970 |

### Example C — Peru: Titicaca, Cusco og trekking ved Machu Picchu (PEPB)

| Dimension | Value |
|---|---|
| tour_code | PEPB |
| operator | Topas |
| country / region | PE / Peru-syd (Lima, Arequipa, Colca, Titicaca, Cusco, Den Hellige Dal, Cordillera Urubamba, Machu Picchu) |
| tour_format | Fællesrejse |
| audience_segment | Åben (also has 30–50 år version) |
| departure_model | Fast afgang |
| base_model | Punkt-til-punkt |
| duration_days / band | 21 / Long |
| primary_activity | Trekking (5-dages Cordillera Urubamba trek) + multi-aktivitet rundrejse |
| secondary_activities | Vandring, kultur, sejlads (Titicaca) |
| topas_difficulty | 3 |
| max_altitude_m | 4.678 (Pachacutec pass) |
| flights_included | Yes (CPH–Lima + 2 indenrigsfly) |
| meals_completeness_score | High on trek (full forplejning), partial elsewhere |
| board_phrase_raw | Itemized per day |
| single_supplement_dkk | 4.900 |
| guide_language | Dansk + lokal guide |
| max_participants | 19 |
| structural_assets | Self-designed Cordillera Urubamba trek with own logistics (kokke, telte, muldyr) |
| category_standard_features | Danish guide, local guides, internal flights included, half-board |
| price range across departures | 34.970 – 37.970 |

### Example D — Annapurna-region vandreferie (NPAP)

| Dimension | Value |
|---|---|
| tour_code | NPAP |
| operator | Topas |
| country / region / sub_region | NP / Annapurna-region / Pokhara, Nayapul, Ghorepani, Poon Hill, Swanta, Jhinu Danda, Ghandruk, Tolka |
| itinerary_landmarks | Pokhara, Nayapul, Ghorepani, Poon Hill, Swanta, Jhinu Danda, Ghandruk, Tolka |
| tour_format | Fællesrejse |
| audience_segment | Åben (also has 30–50 år version) |
| departure_model | Fast afgang |
| departure_visibility | Partial (3 departures visible across 2026–2027) |
| base_model | Punkt-til-punkt (10-dages tehus-trek) |
| duration_days / band | 17 / Long |
| primary_activity | Trekking (10-dages tehus-trek) |
| secondary_activities | Kultur (Kathmandu, Pokhara) |
| topas_difficulty | 2–3 |
| max_altitude_m | 3.193 (Poon Hill) |
| flights_included | Yes (CPH–KTM + indenrigsfly Pokhara–KTM) |
| meals_completeness_score | Medium-low (limited inkluderede måltider; mad på trek cirka 250 DKK/dag) |
| board_phrase_raw | Itemized per day; M/F/A markeret |
| accommodation_type | Mixed: hotel + tehus |
| single_supplement_dkk | 1.600 (kun Kathmandu og Pokhara) |
| guide_language | Dansk + lokale sherpaguider |
| guide_count | 1 dansk + sidarguide + assistentguider + bærere |
| max_participants | 24 |
| structural_assets | (none specific to this tour) |
| category_standard_features | Danish guide, sherpa-guides + bærere, CPH flight, internal flight, nationalpark-tilladelser |
| price across departures | 22.970 (3 visible departures, no spread) |

### Example E — Viktors Farmor: Kultur og vandring i Nepal (new in v0.5)

A worked competitor example, to anchor how a non-Topas tour fills the schema.

| Dimension | Value |
|---|---|
| competitor_id | viktorsfarmor:kultur-og-vandring-i-nepal |
| operator | Viktors Farmor |
| holding_company | Independent (familieejet) |
| country / region / sub_region | NP / Annapurna-region / Kathmandu, Pokhara, Ghandruk, Tadapani, Jhinu Danda, Tolka, Pothana, Dhampus, Lwang |
| itinerary_landmarks | Kathmandu, Pokhara, Ghandruk, Tadapani, Jhinu Danda, Tolka, Dhampus, Lwang |
| tour_format | Fællesrejse |
| audience_segment | Åben |
| departure_model | Fast afgang |
| departure_visibility | Full (2 visible departures: 15. okt. 2026 priced + Garanteret; 6. apr. 2027 date-fixed, Afventer pris) |
| base_model | Punkt-til-punkt with hotel + tehus + homestay mix |
| duration_days / band | 14 / Medium |
| primary_activity | Vandring + Kultur (operator self-describes as "Kultur og vandring") |
| secondary_activities | Kultur (Kathmandu, Patan, Bhaktapur, Boudhanath) |
| normalized_difficulty | 3 (3 støvler — Viktors Farmor scale) |
| max_altitude_m | 2.650 |
| flights_included | Yes (CPH–KTM + indenrigsfly Pokhara t/r) |
| meals_completeness_score | High (helpension on most days; halvpension dag 10 og 12; kvartpension dag 11) |
| accommodation_type | Mixed: hotel + tehus + homestay |
| single_supplement_dkk | 2.200+ |
| guide_language | Dansk + engelsktalende lokal guide + lokal vandreguide + bærere |
| guide_named | Yes (Victor Rolighed for Oct 2026, Hans-Jørgen Thougaard for Apr 2027) |
| max_participants | 16 |
| structural_assets | None observed |
| category_standard_features | Danish guide, lokal vandreguide + bærere, drikkepenge inkluderet |
| price | 25.990 DKK (Oct 2026); Apr 2027 Afventer pris |

This is a Tier 2 competitor for NPAP — same region, same Fællesrejse format, same general audience, but lighter intensity (no multi-day tehus-trek as the central product, max altitude 2.650m vs 3.193m, 14 days vs 17 days). Per `methodology.md`, customers self-select between this kind of kultur-led product and Topas's trek-led product.

### Example F — Stjernegaard: Et eventyr for alle (new in v0.5)

| Dimension | Value |
|---|---|
| competitor_id | stjernegaard:et-eventyr-for-alle |
| operator | Stjernegaard Rejser |
| holding_company | Aller Leisure A/S |
| country / region / sub_region | NP / Nepal bredt (Kathmandu, Chitwan, Pokhara, Ghandruk) |
| itinerary_landmarks | Kathmandu, Chitwan, Pokhara, Ghandruk |
| tour_format | Fællesrejse |
| audience_segment | Åben |
| departure_model | Fast afgang |
| departure_visibility | Single-departure (14. mar. 2027 visible) |
| base_model | Multi-base (hotel-based throughout) |
| duration_days / band | 14 / Medium |
| primary_activity | Kulturrundrejse (with light vandring at Ghandruk) |
| secondary_activities | Safari (Chitwan), vandring |
| normalized_difficulty | 1–2 |
| max_altitude_m | ~2.100 (Ghandruk) |
| flights_included | Yes |
| meals_completeness_score | Medium |
| accommodation_type | Hotel (turistklasse) throughout |
| guide_language | Dansk + lokale |
| max_participants | ~16–18 |
| price | 24.990 DKK |

Tier 2 competitor for NPAP — same country, same format, but kultur-led with safari rather than trek-led. Different product framing, different buyer profile, but the same Annapurna-region footprint (Pokhara, Ghandruk) means there is real overlap on what a customer might consider when researching "guidet rejse til Nepal."

These examples show that two independent Tier 2 competitors exist for NPAP, both priced higher than Topas at 22.970 for less-intense versions of similar trips. That is the kind of pricing signal the production system needs to surface automatically.

### Example G — Madeira: Majestætiske tinder og levadavandring (PTMD) (new in v0.6)

| Dimension | Value |
|---|---|
| tour_code | PTMD |
| operator | Topas |
| country / region / sub_region | PT / Madeira / Funchal, Santa Cruz, Porto da Cruz, Pico Ruivo, Rabaçal, Boca da Corrida |
| itinerary_landmarks | Santa Cruz, Porto da Cruz, Sao Lourenco halvøen, Praia da Machico, Pico Ruivo, Funchal, Fanal, Cabo Girão, Nonnernes Dal |
| tour_format | Fællesrejse |
| audience_segment | Åben |
| departure_model | Fast afgang |
| departure_visibility | Full (11 visible afgange across 2026 + spring 2027) |
| pricing_slope | **Inconsistent** — declining slope into 2027 (summer 2026 highest at 13.970 → spring 2027 lowest at 9.970), inverse to closest Tier 1 competitor Smilrejser, which climbs into 2027 |
| base_model | Multi-base (3 nætter Santa Cruz + 4 nætter Funchal) |
| duration_days / band | 8 / Short |
| primary_activity | Vandring |
| secondary_activities | Kultur (Funchal byvandring), bestigning (Pico Ruivo) |
| topas_difficulty | 2-3 (5 vandredage, 4-6 t pr. dag) |
| max_altitude_m | 1.862 (Pico Ruivo) |
| flights_included | Yes (CPH–Madeira t/r) |
| accommodation_type | Hotel |
| accommodation_tier | Mid |
| single_supplement_dkk | 1.500 |
| guide_language | Dansk |
| guide_count | 1 |
| max_participants | 22 |
| structural_assets | None observed |
| category_standard_features | Danish guide, CPH flight included, transfer included, all transport included |
| price across departures | 9.970 – 13.970 (40% spread, lowest in spring 2027) |

PTMD is methodologically interesting as the first Topas tour observed where `pricing_slope = Inconsistent` against its closest Tier 1 competitor. See methodology section 14.bis for how the production system should flag this pattern.

### Example H — Smilrejser: Vandreferie på Madeira (new in v0.6)

| Dimension | Value |
|---|---|
| competitor_id | smilrejser:vandreferie-paa-madeira |
| operator | Smilrejser |
| holding_company | Aller Leisure A/S |
| country / region / sub_region | PT / Madeira / Funchal, Marocos, Canical, Rabaçal, Sao Lourenco |
| itinerary_landmarks | Levada do Canical (Marocos-Canical), Levada do Alecrim, Funchal, Ponta de São Lourenço, Pico Ruivo (tilkøb), bananplantage Madalena do Mar |
| tour_format | Fællesrejse |
| audience_segment | Åben |
| departure_model | Fast afgang |
| departure_visibility | Full (12 visible afgange across 2026 + 2027) |
| pricing_slope | **Climbing** — 12.995 baseline 2026, climbs to 13.495–13.995 across spring 2027 (price-discovery pattern) |
| base_model | Single-base (Pestana Bay, Funchal-Lido) — all 7 nætter same hotel |
| duration_days / band | 8 / Short |
| primary_activity | Vandring (4 vandredage, lighter intensity) |
| secondary_activities | Funchal byvandring, hotel-AI fritid |
| normalized_difficulty | 1-2 (let til middel, "ingen vandreerfaring kræves") |
| max_altitude_m | 1.862 (Pico Ruivo — but only as tilkøb) |
| flights_included | Yes (CPH or Aalborg t/r) |
| flight_origin | Multi-airport (København + Aalborg) |
| accommodation_type | Hotel (4-star All Inclusive — Pestana Bay) |
| accommodation_tier | Premium |
| meals_completeness_score | High (full AI: M/F/A all 7 dage + 3 lokale frokoster) |
| board_phrase_raw | "All Inclusive på hotellet (morgenmad, frokost og middag) samt fri bar 11-23" + "3 x lokal frokost" |
| guide_language | Dansk |
| guide_named | No (rejseleder not named per departure) |
| max_participants | Not specified (large-scale tour-operator product) |
| pricing_model | per-person fast-pris per departure |
| structural_assets | Pestana Bay AI hotel (preferred-rate rather than exclusive — not a true structural asset) |
| category_standard_features | Danish guide, CPH/AAB flight included, transfer included, dansk-talende rejseleder |
| price | 12.995 – 13.995 (8% spread); first observed sell-out: 16. maj 2026 marked **Udsolgt** |

Tier 1 competitor for PTMD — same destination, same duration, same Fællesrejse format, same audience, similar landmarks. Differs on activity intensity (4 vs 5 vandredage, Pico Ruivo as tilkøb vs included), accommodation model (4-star AI vs hotel + halvpension), and group scale (Aller-distribution vs Topas-niche). Inclusion delta favors Smilrejser on meals (full AI is a meaningful inclusion advantage); favors Topas on activity content. **The PTMD ↔ Smilrejser pair is the first observation of a Tier 1 competitor with `Udsolgt` status visible from external scraping** — see methodology section 13 for treatment.

---

## 4. Open questions for v1.0

These need team input before the taxonomy is locked:

1. **Multi-activity rundrejse vs. kulturrundrejse.** v0.3 introduced `Kulturrundrejse` as a separate value to handle Viktors Farmor-style culturally-led tours. Is this distinction stable enough to lock in, or do we need finer sub-types (light-active / adventure / cultural-active)?
2. **Difficulty normalization scale.** Topas uses 1–5. Most competitors use easy/medium/hard, 1–4, or symbol scales (Viktors Farmor's 1–4 støvler, Kipling's A/B/C/D). Adopt a 1–5 normalized scale, or simpler easy/medium/hard/expedition?
3. **De facto audience tagging.** Topas formally tags Open and 30–50 år. Should we also tag tours where audience *de facto* skews (e.g., Nepal Everest skewing 50+, Viktors Farmor's "rejser med god tid" skewing senior)?
4. **Default geographic granularity for matching.** Country / region / sub-region. The Greenland example argues for region as the default. Do we apply this consistently?
5. **Individuel / skræddersyet rejser.** No fixed price, no fixed departure. Probably modeled with a price-from range and seasonality, no departure layer. Confirm — and confirm whether they belong in the same database at all if they're never used in pair comparisons.
6. **Itinerary overlap as a new dimension?** ITTO and Gjøa Corfino visit overlapping villages and trails. PEPB and Viktors Farmor's Peru rundrejse share most highlights but not the multi-day trek. NPAP and Viktors Farmor's Kultur og vandring share Pokhara, Ghandruk, Tadapani, Jhinu Danda, Tolka. NPAP and Stjernegaard's Et eventyr share Pokhara, Ghandruk. An `itinerary_overlap_score` (computed from shared landmarks, now structurally captured in `itinerary_landmarks`) might strengthen Tier 1 vs. Tier 2 classification beyond same-region. Worth adding for v1.0?
7. **Structural-asset boundary cases.** The Coletti partnership is contractual but not legally exclusive. The Cordillera Urubamba trek is Topas-designed but not a fenced-off route. Kipling's Begnas Camp is operator-owned infrastructure but not legally exclusive. Where is the boundary between "structural asset" and "strong but not truly locked-in advantage"?
8. **Competitor portfolio mapping.** After NPAP sweep: Kipling's Nepal portfolio is mostly Individuel-format products with daily availability, Jysk's Nepal is skræddersyet, Vagabond's Nepal is individuel, Ruby and Smilrejser have no Nepal products. The genuine Fællesrejse competitive landscape for NPAP comprises Topas + Viktors Farmor + Stjernegaard, and that's it. A `competitor-portfolio.md` file mapping per-operator / per-country which products pass eligibility is needed before the production scraper rolls out at scale.
9. **Production prioritization (new).** When the scraper runs weekly, which Topas tours need to be checked most often? Current departures with imminent dates? Tours where price snapshots show recent volatility? Tours where competitor activity is high? The methodology should define a "watch priority" derived from the data, not a fixed schedule.
10. **Pricing-slope vocabulary (new in v0.6).** v0.6 introduces `pricing_slope` with values `Flat / Climbing / Declining / Peaked-summer / Peaked-Christmas / Inconsistent`. The PTMD finding suggests `Inconsistent` is a flag — the question is whether to compute slope mismatch as a continuous metric (vector cosine of slope curves) or to keep it categorical. Categorical is simpler and matches how a head of agency thinks about the calendar; continuous is more precise. Decision needed before v1.0 launch.
11. **Competitive intensity per destination (new in v0.6).** Different destinations have different numbers of eligible Tier 1 / Tier 2 competitors (NPAP: 0 Tier 1, 2 Tier 2; VNSN: 0 Tier 1, 4+ Tier 2; PTMD: 3 Tier 1, several Tier 2). Pricing power is a function of pair-pool size — high-intensity destinations need closer-to-market pricing; low-intensity destinations allow more structural-asset premium. Should `competitive_intensity` be a computed field on each Topas tour (low/medium/high based on eligible-Fællesrejse count in the pair-pool), and should it directly modulate the asset-premium ceiling in methodology section 10?
12. **Sibling-brand correlation (new in v0.6, deferred from v0.5).** Vietnam observation: Stjernegaard (Aller Leisure) has 5 eligible Vietnam Fællesrejser. Madeira observation: Smilrejser (Aller Leisure) is the closest Tier 1 competitor for PTMD, and Nilles & Gislev (Aller Leisure) also has Madeira products. When two or more Aller Leisure brands appear in the same eligible-set, are they independent signals or correlated signals? Methodology should not double-count Aller pricing as if they were two independent operators when in fact they share corporate strategy. Suggested heuristic: in tier-classification scoring, sibling brands count as one signal weighted at 1.5× rather than two signals at 1.0× each.

---

## 5. Change log

| Version | Date | Change |
|---|---|---|
| 0.1 | 2026-05 | Initial draft from portfolio review of fællesrejser and Vietnam tours. |
| 0.2 | 2026-05 | Split section 2.10 into structural assets vs. category-standard features after ITTO ↔ Gjøa Corfino comparison revealed multi-guide split is category-standard, not Topas-unique. Added `departure_visibility`, `board_phrase_raw`. Added open questions on itinerary overlap and structural-asset boundary cases. |
| 0.3 | 2026-05 | After PEPB ↔ Viktors Farmor Peru comparison: clarified that `tour_format` is a category label, not a brand term — `Fællesrejse` covers competitors' "Grupperejse", "Aktive grupperejser", "Aktive rejser med dansk rejseleder". Added vocabulary mapping table. Added `Kulturrundrejse` as primary_activity value distinct from `Multi-aktivitet rundrejse`. Added `local_guide_language` field. Added Peru worked example (Example C). |
| 0.4 | 2026-05 | After Topas NPAP ↔ Kipling Begnas (pair 1) and Topas NPAP ↔ Kipling ABC (pair 2): tightened Fællesrejse defining test to require all five criteria, including pre-published dates AND prices — products with `departure_visibility = On-request only` fail the test even if they have Danish guide + group + set itinerary. Added Kipling Begnas Camp as competitor structural asset (Topas is not the only operator with structural assets — anti-flattery rule applies). Added NPAP worked example (Example D). Added open question 8 on competitor portfolio mapping. Added `Tehus`/`Telt` to accommodation_type values. Updated tour_format vocabulary mapping table to flag "verify pre-published dates + prices" requirement. |
| 0.5 | 2026-05 | After NPAP full-sweep across all competitors: corrected Viktors Farmor eligibility (page uses dynamic JS rendering — departures *are* published; earlier scraper miss) — product is Tier 2 with named rejseledere per departure (15. okt. 2026 priced 25.990 + Garanteret; 6. apr. 2027 date-fixed Afventer pris). Added Stjernegaard Et eventyr for alle as second Tier 2 NPAP competitor (24.990, 14 dage, kultur-led). Added section 1.1 "Target end-state" — articulates production-scraping aspiration so downstream methodology and scraper development point toward the same goal. Added section 2.11 "Operator and holding-company structure" — Aller Leisure is a holding company, not a single competitor; Smilrejser, Stjernegaard, Nilles & Gislev, Nyhavn, NYATI, ALIVE all owned by Aller Leisure but compete in different segments. Added section 2.12 "Scraper-implementation notes" — captures known per-operator scraping issues (Viktors Farmor JS rendering, Kipling group-size pricing, Jysk default-skræddersyet, Vagabond lokal-turleder mismatch). Added `itinerary_landmarks` field (2.2) — supports itinerary-overlap scoring and is more reliably scrapable than free-text region descriptions. Added `availability_status` enum expansion (2.9) including `Afventer pris`, `Ledig`, `Garanteret` — tracks Viktors Farmor / Stjernegaard vocabulary. Added `guide_named` field (2.7) — operators that name specific rejseledere per departure are committing more strongly to the product. Added `scraper_run_id` to snapshots table — supports week-over-week diff queries. Added Example E (Viktors Farmor) and Example F (Stjernegaard) as worked competitor entries. Added open question 9 on production prioritization. |
| 0.6 | 2026-05 | After VNSN ↔ Stjernegaard pair (Vietnam) and PTMD ↔ Smilrejser pair (Madeira): added `pricing_slope` tour-level field (section 2.9) — captures whether a tour's prices climb / decline / are flat / inconsistent across the calendar. PTMD is the first observed case of `pricing_slope = Inconsistent` against its closest Tier 1 anchor. Annotated `Udsolgt` enum value (section 2.9) to flag that competitor-side sell-out signals are externally observable from scraping (Smilrejser 16. maj 2026 = Udsolgt) and useful as a parallel-source demand signal until Topas-internal sell-out data ingestion is built. Updated Viktors Farmor scraper note (section 2.12) — JS-rendering issue confirmed across both Nepal and Madeira; this is a structural site pattern, not a per-product fluke. Added Smilrejser to scraper notes (section 2.12) — clean per-departure data, multi-airport (CPH+AAB), `Udsolgt` status visible. Added Example G (Topas PTMD with full per-departure pricing showing inconsistent slope) and Example H (Smilrejser Vandreferie på Madeira as the first Tier 1 competitor worked example). Added open questions 10, 11, 12 on pricing-slope vocabulary, competitive intensity per destination, and sibling-brand correlation across Aller Leisure brands. |
| 0.7 | 2026-05 | After production-deployment debugging exposed coverage gaps on Albatros and Stjernegaard: rewrote section 2.12 "Scraper-implementation notes" with explicit `data_acquisition_strategy` column per operator, mapping each operator to its tier-ladder choice from methodology section 7.bis. Added Albatros entry (was missing in v0.6) — heavy React rendering, Tier 3 (Claude vision) needed for per-departure data, Tier 1 captures only headline. Updated Stjernegaard entry — `priser-og-datoer/` subpages are JavaScript-only; Firecrawl with `wait_for=4000` returns navigation chrome only. Workaround at Tier 1: scrape main URL, scope parser to first card in "Andre spændende rejser" section. Tier 3 fallback when workaround returns 0. Tier 4 investigation pending (find Stjernegaard's internal JSON API endpoint). v0.7 explicitly notes that as of publication, only Tier 1 is implemented — coverage gaps on Albatros and Stjernegaard are documented violations of methodology section 0.2 data acquisition principle, on the v1.0 roadmap, not acceptable steady state. Albatros URL bug fixed in production config: was pointing to `/destinationer/asien/nepal/rejser` (category page, no products listed) — corrected to actual product URL `/rejser/albatros-nepal`. |
| 0.8 | 2026-05 | Companion to methodology v0.8. Section 2.12 fully rewritten to reflect the new universal AI-extraction architecture. The 9 operator-specific regex parsers from v0.7 are deprecated — `parsers/__init__.py` now dispatches every parser_key to the same `parsers/generic_ai.py`, which consumes Firecrawl's LLM-extracted JSON (per `extraction_schema.py`). Per-operator knowledge moved from regex code to the table in this section: page-rendering type, departure-data location, and which tier combination handles it. Albatros NPAP is the worked example of Tier 1 + Tier 2 (sitemap-variant discovery) — first verified working extraction returning both 2026-10-29 and 2027-03-04 departures, matching browser-visible data. The `OPERATOR_VARIANT_PATTERNS` registry in `sitemap_discovery.py` is the new place to add operators with URL-variant patterns; ~5 lines per operator. v0.8 documents the cost reduction: ~$8/year for the 19-target weekly scrape, down from v0.7's projected $45/year, because LLM extraction handles ~95% of cases first-pass and Tier 3 vision rarely needs to fire. The lesson encoded throughout: adopt agent-style techniques (multi-source fetching, schema-driven extraction) without adopting agent autonomy — see methodology Section 0.3. |
| 0.9 | 2026-05 | **Architecture pivot: Topas catalog discovery moved from auto-scrape to manual seed-list.** After 4 attempts (sitemap.xml — 49 URLs but mixed with categories/junk; /search.html?filter-type=53094 with no scroll — 7 tours; with 8× scroll — 10; with 20× scroll — 13; with 15× JS-injection scroll-to-bottom — 17), the conclusion is that Firecrawl's single-call architecture has a structural ceiling that prevents capturing all 49 Topas Fællesrejser-med-turleder. Rather than escalate to multi-call orchestration (Playwright, n8n agent loops), we accept that Topas's portfolio changes slowly (~2-5 tours/year) and adopt a manual seed-list approach. `TOPAS_SEED_URLS` in `topas_catalog.py` lists all 49 URLs; per-product light-scrape extracts metadata (~$0.005/URL); "Tilføj ny tur" UI in Streamlit lets analysts add new URLs as Topas's portfolio evolves. **Scope of AI-discovery refined: per-tour, not whole-catalog.** The original "find all Topas's competitors" framing was wrong — it bundled portfolio-discovery with competitor-discovery. They have different operational rhythms: Topas-portfolio is stable (manual maintenance is fine), competitor-mapping is the value-creating layer (AI-agent discovery per Topas tour). User has built the n8n discovery workflow with Firecrawl + Claude tools. Streamlit Tour-detalje page now triggers it via webhook with country/region pre-filled from catalog metadata; n8n writes results to its "Competitor Analysis" data-table; Streamlit reads back for review. **Sidebar reduced from 5 pages to 2** (Tour-detalje, Topas-katalog) — Tour-overblik and Konkurrent-overblik archived to `pages/_archived/`. **Tour-detalje single-dropdown:** previously had a primary picker (showing only scraped tours) + a secondary scrape-target picker (showing TARGETS-configured codes). Now one dropdown shows all 49 catalog tours with state indicators (🟢 = has scraped data, ⚪ = no competitors mapped). For ⚪-tours, the n8n screening section auto-expands as the next-step call-to-action. **Smilrejser '+N pladser' status mapping fix:** schema description and parser safety-net now map '+8 pladser' (Smilrejser's at-least-N-spots-available convention) to `Åben`, not `Få pladser`. Earlier behavior was inverting the demand signal — Smilrejser rows incorrectly showed as Få pladser (yellow scarcity) when the reality was Åben (red availability). |

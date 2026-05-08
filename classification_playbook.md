# Klassificerings-playbook

**Status:** v0.1 — startindhold. Vokser over tid via review-feedback.
**Læses af:** `classifier.py` ved hver Claude API-kald
**Redigeret af:** Mennesker, baseret på `pattern_observations` fra decision-log

---

## Formål

Denne fil er den voksende "hjerne" der hjælper AI-klassificeringen blive mere præcis over tid. Hver gang du afviser eller overruler et AI-forslag, fanger systemet det. Når der findes et mønster i overrulings, foreslår systemet en ny regel her. Du redigerer reglen, og AI bruger den fra næste klassificering.

Reglerne herunder er **bindende for AI**. Hvis en regel siger "tour X er ikke Fællesrejse", skal AI klassificere den sådan, selv hvis det generelle mønster siger noget andet.

## Operatør-specifikke regler

### Albatros Travel
*(ingen regler endnu — udfyldes når vi har scrapet og reviewet deres katalog)*

### Jysk Rejsebureau
- Mange tours markedsføres som "Aktive rejser med dansk rejseleder", men har faktisk ingen publicerede afgange — kun "Nye priser på vej" eller "Indhent tilbud". Disse er **skræddersyet** (Individuel), ikke Fællesrejse, regardless of marketing-label.
- "Pr. pers v. 2 pers." pricing-mønster signalerer skræddersyet, ikke Fællesrejse.

### Kipling Travel
- "Aktive rejser med dansk rejseleder" med "Der er pt. ingen planlagte afgange — Kontakt os" status fejler Fællesrejse-eligibility-testen (mangler både fast dato og publiceret pris).
- Daglige-afgange-produkter (fx "Annapurna Base Camp daglige afgange") er **Individuel**, ikke Fællesrejse.

### Vagabond Tours
- Mange Asien-produkter har dansktalende salgsmedarbejder hjemme, men *engelsktalende lokal turleder* på rejsen. Dette fejler kriterie 3 i Fællesrejse-testen (dansk-talende guide). Verificér altid `guide_language` på selve produktsiden.

### Smilrejser (rebrand fra Kulturrejser Europa, 2025)
- Tidligere kendt som "Kulturrejser Europa". Begge navne kan optræde i ældre kilder — det er den samme operatør.
- Fokus er kulturrejser i Europa (kun Europa).

### Stjernegaard Rejser
- Aller Leisure-brand. Rundrejser med dansk leder — globalt udvalg.
- Generelt rene Fællesrejser med fast dato + pris + dansk leder. Få undtagelser.

### Nilles & Gislev
- Aller Leisure-brand. Bus + fly grupperejser.
- Mange tours mærker som "Nye afgange på vej" når et nyt sæsonprogram udvikles. Disse er midlertidigt ineligible men forventes at blive eligible når dato/pris kommer på.

## Generelle regler

### Fællesrejse-eligibility-testen
Alle fem kriterier skal være opfyldt:

1. **Fast afgangsdato** publiceret på operatørens side (ikke "kontakt os")
2. **Pre-publiceret pris** vises pr. afgang (ikke "fra X kr." udregnet ved booking)
3. **Dansk-talende guide** rejser med gruppen fra Danmark
4. **Gruppen rejser sammen fra Danmark** som en fast kohorte (ikke individuelle samles in-country)
5. **Sat itinerar** der ikke ændres pr. booking

Hvis bare ét kriterium fejler, er det IKKE Fællesrejse. Det er en hård regel.

### Edge case — "Afventer pris"
Et produkt med fast dato men midlertidig "Afventer pris"-status er stadig **tour-niveau eligible** for Fællesrejse — men den specifikke afgang ekskluderes fra prissammenligning indtil pris publiceres. Andre afgange i samme tour kan være eligible.

### Tier-klassificering (når vi laver match-forslag)
- **Tier 1**: Samme land + region + tour_format + primær_aktivitet + duration_band + difficulty (±1 trin) + audience
- **Tier 2**: Samme land + region + tour_format, men ≥1 dimension afviger (intensitet, varighed, aktivitets-vægt)
- **Tier 3**: Samme land + tour_format, anden region, eller multi-land
- **Tier 4**: Andet land, samme bredt aktivitet (kun til portefølje-kontekst, aldrig direkte anker)

### Aktivitets-klassificering — vigtige sondringer
- `Vandring` = day-walks fra en base
- `Trekking` = multi-day backcountry, evt. tehus eller telt
- `Højrute / hut-to-hut` = trekking med variation i overnatninger pr. dag
- `Multi-aktivitet rundrejse` = bevidst kombination af cykling + vandring + kajak + kultur (fx Topas Vietnam syd-til-nord)
- `Kulturrundrejse` = sightseeing-led, lette gåture, ingen trek-kerne (fx Viktors Farmor Peru)
- Forskellen mellem Multi-aktivitet og Kultur er afgørende for tier-matching — disse kunder vælger forskelligt.

### Difficulty-normalisering
- Topas: 1-5 → behold direkte
- Viktors Farmor: 1-4 støvler → 1→1, 2→2, 3→3, 4→4-5
- Kipling: A/B/C/D → A→1, B→2, C→3, D→4
- Jysk: descriptive → "let"→1, "moderat"→2, "krævende"→3, "ekspeditionsniveau"→4-5
- Vagabond: 1-5 → behold direkte
- Når i tvivl, brug `max_altitude_m` og `daily_hours_active` som tværgående kontrol

## Mønstre fra decision-log

*(Tom — udfyldes automatisk når der findes mønstre i den menneskelige review-aktivitet)*

---

## Versions-log

| Version | Dato | Ændring |
|---|---|---|
| v0.1 | 2026-05 | Startindhold baseret på taxonomy.md v0.6 og methodology.md v0.6. Operatør-specifikke regler fra de første scraping-runder. |

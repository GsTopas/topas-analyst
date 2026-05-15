"""Engangs-inspect af Turomkostninger 2026.xls — print kolonne-mapping pr. fane.

Bruges til at verificere hvilke kolonner B/I/M maps til pr. måned før vi bygger
sync_forecast.py. Ingen writes — pure read.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


EXCEL_PATH = Path(r"K:\OFFICE\Operations\Turregnskab\Opfølgning\Turomkostninger 2026.xls")
MONTHS = [
    "Januar", "Februar", "Marts", "April", "Maj", "Juni",
    "Juli", "August", "September", "Oktober", "November", "December",
]


def main() -> int:
    if not EXCEL_PATH.exists():
        print(f"FAIL: {EXCEL_PATH} findes ikke")
        return 1

    # xlrd 2.0+ supports .xls (gammel format)
    xls = pd.ExcelFile(EXCEL_PATH, engine="xlrd")
    print(f"Faner i filen ({len(xls.sheet_names)}):")
    for s in xls.sheet_names:
        marker = "✓" if s in MONTHS else " "
        print(f"  {marker} {s!r}")
    print()

    # Inspicer Januar-fanen som reference
    if "Januar" not in xls.sheet_names:
        print("FAIL: Ingen 'Januar'-fane fundet")
        return 1

    df = pd.read_excel(EXCEL_PATH, sheet_name="Januar", engine="xlrd", header=None)
    print(f"Januar: {df.shape[0]} rækker × {df.shape[1]} kolonner")
    print()

    # Vis række 1-15 alle kolonner så vi kan se hvad B/I/M er
    print("Første 15 rækker, alle kolonner:")
    print("-" * 100)
    for idx in range(min(15, len(df))):
        row_repr = []
        for col in range(min(df.shape[1], 16)):
            val = df.iat[idx, col]
            if pd.isna(val):
                cell = "—"
            else:
                cell = str(val)[:18]
            row_repr.append(f"{chr(65 + col)}={cell}")
        print(f"  R{idx+1:2d}: {' | '.join(row_repr)}")
    print()

    # Find rækker hvor B + I + M alle har værdi (efter række 5 = header)
    print("Rækker hvor B + I + M alle har værdi (data-rækker):")
    print("-" * 100)
    cnt = 0
    for idx in range(5, len(df)):
        b = df.iat[idx, 1] if df.shape[1] > 1 else None  # B = col 1
        i = df.iat[idx, 8] if df.shape[1] > 8 else None  # I = col 8
        m = df.iat[idx, 12] if df.shape[1] > 12 else None  # M = col 12
        if pd.notna(b) and pd.notna(i) and pd.notna(m):
            print(f"  R{idx+1:2d}: B={b!r:25s}  I={i!r:15s}  M={m!r}")
            cnt += 1
            if cnt >= 20:
                print(f"  ... (truncated efter 20)")
                break
    print()
    print(f"Total qualifying rows i Januar: {cnt}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

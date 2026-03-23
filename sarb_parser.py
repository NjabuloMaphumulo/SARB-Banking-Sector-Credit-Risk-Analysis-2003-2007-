"""
SARB DI500 Credit Risk BA Return Parser
========================================
Parses monthly BA Return CSV files (Total Banks, DI500-CREDIT RISK)
from the South African Reserve Bank into clean, analysis-ready tables.

Output tables:
  1. credit_risk          - Table 01: Loan classifications by type and risk grade
  2. provisions           - Table 02: Specific and general provisions by loan type
  3. asset_quality        - Table 03: Repossessed/bought-in assets
  4. asset_profitability  - Table 04: Asset distribution by profitability
  5. sectoral_exposure    - Table 05a: Sectoral distribution of advances (quarterly)
  6. geographic_exposure  - Table 05b/06: Geographic distribution of advances

Usage:
  python sarb_parser.py --input ./data --output ./output
  python sarb_parser.py --input ./data --output ./output --format csv
  python sarb_parser.py --input ./data --output ./output --format sqlite
"""

import csv
import os
import re
import sys
import sqlite3
import argparse
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Table 01 - Risk grades (columns 3-8 in the CSV, index 2-7)
RISK_GRADES = [
    "standard_or_current",
    "special_mention",
    "sub_standard",
    "doubtful",
    "loss",
    "total",
]

# Table 01 - Loan type section headers and the item number ranges they own
LOAN_TYPE_MAP = {
    "MORTGAGE LOANS":                                        (1,  13),
    "INSTALMENT SALES AND LEASES":                          (14, 26),
    "CREDIT CARDS":                                         (27, 39),
    "OTHER LOANS AND ADVANCES":                             (40, 52),
    "INTERBANK ADVANCES,NCDS,INVESTMENTS AND ALL OTHERASSETS": (53, 65),
    "OFFBALANCE SHEET ITEMS":                               (66, 76),
}

# Table 01 - Key item numbers and their descriptions (for clean labelling)
TABLE01_ITEMS = {
    "001": "gross_beginning_of_month",
    "002": "less_reclassified",
    "003": "written_off",
    "004": "payments_received",
    "005": "classified_reclassified",
    "006": "recovered",
    "007": "finance_charges",
    "008": "gross_end_of_month",
    "009": "market_value_security",
    "010": "net_end_of_month_before_provisions",
    "011": "provisions",
    "014": "gross_beginning_of_month",
    "015": "less_reclassified",
    "016": "written_off",
    "017": "payments_received",
    "018": "classified_reclassified",
    "019": "recovered",
    "020": "finance_charges",
    "021": "gross_end_of_month",
    "022": "market_value_security",
    "023": "net_end_of_month_before_provisions",
    "024": "provisions",
    "027": "gross_beginning_of_month",
    "028": "less_reclassified",
    "029": "written_off",
    "030": "payments_received",
    "031": "classified_reclassified",
    "032": "recovered",
    "033": "finance_charges",
    "034": "gross_end_of_month",
    "035": "market_value_security",
    "036": "net_end_of_month_before_provisions",
    "037": "provisions",
    "040": "gross_beginning_of_month",
    "041": "less_reclassified",
    "042": "written_off",
    "043": "payments_received",
    "044": "classified_reclassified",
    "045": "recovered",
    "046": "finance_charges",
    "047": "gross_end_of_month",
    "048": "market_value_security",
    "049": "net_end_of_month_before_provisions",
    "050": "provisions",
    "053": "gross_beginning_of_month",
    "054": "less_reclassified",
    "055": "written_off",
    "056": "payments_received",
    "057": "classified_reclassified",
    "058": "recovered",
    "059": "finance_charges",
    "060": "gross_end_of_month",
    "061": "market_value_security",
    "062": "net_end_of_month_before_provisions",
    "063": "provisions",
    "066": "gross_beginning_of_month",
    "067": "less_reclassified",
    "068": "other",
    "069": "classified_reclassified",
    "070": "other",
    "071": "gross_end_of_month",
    "072": "market_value_security",
    "073": "net_end_of_month_before_provisions",
    "074": "provisions",
}

# Table 02 - Provisions items
TABLE02_ITEMS = {
    "077": "opening_balance",
    "078": "amounts_written_off",
    "079": "recoveries",
    "080": "provisions_raised",
    "081": "interest_in_suspense",
    "082": "other_adjustments",
    "083": "closing_balance",
    "084": "general_debt_provision_gross",
}

TABLE02_LOAN_TYPES = [
    "mortgage_loans",
    "instalment_sales_and_leases",
    "credit_cards",
    "other_loans_and_advances",
    "investments_and_other_assets",
    "off_balance_sheet",
    "total",
]

# Table 03 - Asset quality items
TABLE03_ITEMS = {
    "085": "total_repossessed_unsold",
    "086": "companies_acquired",
    "087": "fixed_property_total",
    "088": "fixed_property_private_dwelling",
    "089": "fixed_property_commercial_industrial",
    "090": "vehicles_and_equipment",
    "091": "other",
}

TABLE03_COLS = [
    "historic_cost",
    "market_value",
    "cumulative_written_off",
    "liabilities_to_settle",
]

# Table 04 - Asset profitability items
TABLE04_ITEMS = {
    "092": "distribution_r000",
    "093": "percentage",
}

TABLE04_COLS = [
    "money",
    "advances_reasonable_return",
    "advances_some_return",
    "advances_no_return",
    "investments_earning",
    "investments_non_earning",
    "non_financial_assets",
    "other_assets",
    "total",
]

# Table 05a - Sectoral exposure
TABLE05A_ITEMS = {
    "094": "distribution_r000",
    "095": "residents_r000",
    "096": "non_residents_r000",
    "097": "number_of_clients",
    "098": "residents_clients",
    "099": "non_residents_clients",
    "100": "specific_provisions_r000",
}

TABLE05A_SECTORS = [
    "agriculture_forestry_fishing",
    "mining",
    "manufacturing",
    "construction",
    "electricity_gas_water",
    "trade_and_accommodation",
    "transport_communication",
    "finance_insurance",
    "real_estate_business_services",
    "community_social_personal",
    "individuals",
    "other",
    "total",
]

# Table 06 - Geographic exposure
TABLE06_ITEMS = {
    "101": "distribution_r000",
    "102": "number_of_clients",
    "103": "specific_provisions_r000",
}

TABLE06_REGIONS = [
    "south_africa",
    "other_african_countries",
    "europe",
    "asia",
    "russian_federation_ussr",
    "americas_north",
    "americas_south",
    "oceania_and_other",
    "total",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_date_from_header(date_str: str) -> str:
    """Convert 'April 2003' -> '2003-04-01'."""
    try:
        dt = datetime.strptime(date_str.strip(), "%B %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date_str.strip()


def clean_numeric(value: str):
    """Return int/float or None for empty/non-numeric cells."""
    v = value.strip().replace(",", "")
    if v in ("", "-", "N/A"):
        return None
    try:
        return int(v)
    except ValueError:
        try:
            return float(v)
        except ValueError:
            return None


def get_loan_type(item_num: str) -> str:
    """Map a 3-digit item number string to a loan type name."""
    n = int(item_num)
    for name, (start, end) in LOAN_TYPE_MAP.items():
        if start <= n <= end:
            # Normalise to snake_case
            clean = name.lower()
            clean = re.sub(r"[^a-z0-9]+", "_", clean).strip("_")
            return clean
    return "unknown"


def read_csv_rows(filepath: str) -> list:
    """Read all rows from a CSV, returning list of lists."""
    rows = []
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            rows.append(row)
    return rows


def extract_period(rows: list) -> str:
    """Pull the date from row 0: ['Date', 'April 2003'] -> '2003-04-01'."""
    for row in rows[:5]:
        if row and row[0].strip().lower() == "date" and len(row) > 1:
            return parse_date_from_header(row[1])
    return "unknown"


def find_table_start(rows: list, table_label: str) -> int:
    """Return the row index of a table header label, e.g. 'Table 01'."""
    for i, row in enumerate(rows):
        if row and row[0].strip() == table_label:
            return i
    return -1


# ---------------------------------------------------------------------------
# Table parsers
# ---------------------------------------------------------------------------

def parse_table01(rows: list, period: str) -> list:
    """
    Parse Table 01 - Credit Risk Classifications.
    Returns list of dicts with columns:
      period, loan_type, item_number, metric, standard_or_current,
      special_mention, sub_standard, doubtful, loss, total
    """
    records = []
    start = find_table_start(rows, "Table 01")
    end = find_table_start(rows, "Table 02")
    if start == -1:
        return records

    # Determine current loan type as we scan rows
    current_loan_type = None

    for row in rows[start:end]:
        if not row or not row[0].strip():
            continue

        desc = row[0].strip().strip('"')

        # Check if this row is a loan type section header
        desc_upper = re.sub(r"\d+$", "", desc).strip().upper()
        matched_type = None
        for lt in LOAN_TYPE_MAP:
            if desc_upper == lt or desc_upper == lt.rstrip("1").strip():
                matched_type = lt
                break
        if matched_type:
            current_loan_type = re.sub(r"[^a-z0-9]+", "_",
                                       matched_type.lower()).strip("_")
            continue

        # Data row: must have an item number in column 1
        if len(row) < 2 or not row[1].strip():
            continue
        item_num = row[1].strip().zfill(3)

        # Only process known Table 01 items
        if item_num not in TABLE01_ITEMS:
            continue
        if current_loan_type is None:
            continue

        metric = TABLE01_ITEMS[item_num]
        values = (row + [""] * 8)[2:8]  # pad to 6 columns

        record = {
            "period": period,
            "loan_type": current_loan_type,
            "item_number": item_num,
            "metric": metric,
        }
        for grade, val in zip(RISK_GRADES, values):
            record[grade] = clean_numeric(val)

        records.append(record)

    return records


def parse_table02(rows: list, period: str) -> list:
    """
    Parse Table 02 - Provisions.
    Returns list of dicts with columns:
      period, item_number, metric, mortgage_loans, instalment_sales_and_leases,
      credit_cards, other_loans_and_advances, investments_and_other_assets,
      off_balance_sheet, total
    """
    records = []
    start = find_table_start(rows, "Table 02")
    end = find_table_start(rows, "Table 03")
    if start == -1:
        return records

    for row in rows[start:end]:
        if len(row) < 2 or not row[1].strip():
            continue
        item_num = row[1].strip().zfill(3)
        if item_num not in TABLE02_ITEMS:
            continue

        metric = TABLE02_ITEMS[item_num]
        values = (row + [""] * 9)[2:9]  # 7 columns

        record = {
            "period": period,
            "item_number": item_num,
            "metric": metric,
        }
        for col, val in zip(TABLE02_LOAN_TYPES, values):
            record[col] = clean_numeric(val)

        records.append(record)

    return records


def parse_table03(rows: list, period: str) -> list:
    """Parse Table 03 - Asset Quality (Repossessed Assets)."""
    records = []
    start = find_table_start(rows, "Table 03")
    end = find_table_start(rows, "Table 04")
    if start == -1:
        return records

    for row in rows[start:end]:
        if len(row) < 2 or not row[1].strip():
            continue
        item_num = row[1].strip().zfill(3)
        if item_num not in TABLE03_ITEMS:
            continue

        metric = TABLE03_ITEMS[item_num]
        values = (row + [""] * 6)[2:6]

        record = {
            "period": period,
            "item_number": item_num,
            "metric": metric,
        }
        for col, val in zip(TABLE03_COLS, values):
            record[col] = clean_numeric(val)

        records.append(record)

    return records


def parse_table04(rows: list, period: str) -> list:
    """Parse Table 04 - Asset Profitability Distribution."""
    records = []
    start = find_table_start(rows, "Table 04")
    end = find_table_start(rows, "Table 05")
    if start == -1:
        return records

    for row in rows[start:end]:
        if len(row) < 2 or not row[1].strip():
            continue
        item_num = row[1].strip().zfill(3)
        if item_num not in TABLE04_ITEMS:
            continue

        metric = TABLE04_ITEMS[item_num]
        values = (row + [""] * 11)[2:11]

        record = {
            "period": period,
            "item_number": item_num,
            "metric": metric,
        }
        for col, val in zip(TABLE04_COLS, values):
            record[col] = clean_numeric(val)

        records.append(record)

    return records


def parse_table05a(rows: list, period: str) -> list:
    """Parse Table 05a - Sectoral Distribution of Advances."""
    records = []
    start = find_table_start(rows, "Table 05")
    end = find_table_start(rows, "Table 06")
    if start == -1:
        return records

    for row in rows[start:end]:
        if len(row) < 2 or not row[1].strip():
            continue
        item_num = row[1].strip().zfill(3)
        if item_num not in TABLE05A_ITEMS:
            continue

        metric = TABLE05A_ITEMS[item_num]
        values = (row + [""] * 15)[2:15]

        record = {
            "period": period,
            "item_number": item_num,
            "metric": metric,
        }
        for col, val in zip(TABLE05A_SECTORS, values):
            record[col] = clean_numeric(val)

        records.append(record)

    return records


def parse_table06(rows: list, period: str) -> list:
    """Parse Table 06 - Geographic Distribution of Advances."""
    records = []
    start = find_table_start(rows, "Table 06")
    end = find_table_start(rows, "Table 07")
    if start == -1:
        return records

    for row in rows[start:end]:
        if len(row) < 2 or not row[1].strip():
            continue
        item_num = row[1].strip().zfill(3)
        if item_num not in TABLE06_ITEMS:
            continue

        metric = TABLE06_ITEMS[item_num]
        values = (row + [""] * 11)[2:11]

        record = {
            "period": period,
            "item_number": item_num,
            "metric": metric,
        }
        for col, val in zip(TABLE06_REGIONS, values):
            record[col] = clean_numeric(val)

        records.append(record)

    return records


# ---------------------------------------------------------------------------
# File processor
# ---------------------------------------------------------------------------

def process_file(filepath: str) -> dict:
    """Parse a single BA Return CSV. Returns dict of table_name -> [records]."""
    rows = read_csv_rows(filepath)
    period = extract_period(rows)

    return {
        "credit_risk":         parse_table01(rows, period),
        "provisions":          parse_table02(rows, period),
        "asset_quality":       parse_table03(rows, period),
        "asset_profitability": parse_table04(rows, period),
        "sectoral_exposure":   parse_table05a(rows, period),
        "geographic_exposure": parse_table06(rows, period),
    }


def process_directory(input_dir: str) -> dict:
    """Process all I9999999_*.csv files in a directory."""
    all_tables = {
        "credit_risk": [],
        "provisions": [],
        "asset_quality": [],
        "asset_profitability": [],
        "sectoral_exposure": [],
        "geographic_exposure": [],
    }

    files = sorted(Path(input_dir).glob("I9999999_*.csv"))
    if not files:
        print(f"  No matching files found in {input_dir}")
        return all_tables

    for fp in files:
        print(f"  Parsing: {fp.name}")
        result = process_file(str(fp))
        for table, records in result.items():
            all_tables[table].extend(records)

    return all_tables


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_csv_outputs(tables: dict, output_dir: str):
    """Write each table to a separate CSV file."""
    os.makedirs(output_dir, exist_ok=True)

    for table_name, records in tables.items():
        if not records:
            print(f"  [SKIP] {table_name} — no records")
            continue

        out_path = os.path.join(output_dir, f"{table_name}.csv")
        fieldnames = list(records[0].keys())

        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        print(f"  [OK]   {table_name}.csv  ({len(records)} rows)")


def write_sqlite_output(tables: dict, output_dir: str):
    """Write all tables into a single SQLite database."""
    os.makedirs(output_dir, exist_ok=True)
    db_path = os.path.join(output_dir, "sarb_di500.db")

    conn = sqlite3.connect(db_path)

    for table_name, records in tables.items():
        if not records:
            print(f"  [SKIP] {table_name} — no records")
            continue

        # Infer column types
        cols = list(records[0].keys())
        col_defs = []
        for col in cols:
            if col in ("period", "loan_type", "metric", "item_number"):
                col_defs.append(f'"{col}" TEXT')
            else:
                col_defs.append(f'"{col}" REAL')

        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(
            f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
        )

        placeholders = ", ".join(["?"] * len(cols))
        for rec in records:
            values = [rec.get(c) for c in cols]
            conn.execute(
                f"INSERT INTO {table_name} VALUES ({placeholders})", values
            )

        conn.commit()
        print(f"  [OK]   {table_name}  ({len(records)} rows)")

    conn.close()
    print(f"\n  Database saved to: {db_path}")
    return db_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Parse SARB BA Return DI500 CSV files into analysis-ready tables."
    )
    parser.add_argument(
        "--input", "-i", required=True,
        help="Directory containing I9999999_*.csv files"
    )
    parser.add_argument(
        "--output", "-o", default="./output",
        help="Output directory (default: ./output)"
    )
    parser.add_argument(
        "--format", "-f", choices=["csv", "sqlite", "both"], default="both",
        help="Output format: csv, sqlite, or both (default: both)"
    )
    args = parser.parse_args()

    print(f"\nSARB DI500 Parser")
    print(f"  Input  : {args.input}")
    print(f"  Output : {args.output}")
    print(f"  Format : {args.format}")
    print()

    print("Processing files...")
    tables = process_directory(args.input)

    total_records = sum(len(r) for r in tables.values())
    print(f"\nTotal records extracted: {total_records}")
    print()

    if args.format in ("csv", "both"):
        print("Writing CSV files...")
        write_csv_outputs(tables, args.output)
        print()

    if args.format in ("sqlite", "both"):
        print("Writing SQLite database...")
        write_sqlite_output(tables, args.output)

    print("\nDone.")


if __name__ == "__main__":
    main()

import re
from datetime import datetime
from pathlib import Path
import sqlite3

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# -----------------------------
# Config (lab)
# -----------------------------
URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
EXCHANGE_RATE_CSV_PATH = "exchange_rate.csv"
OUTPUT_CSV_PATH = "./Largest_banks_data.csv"
DB_NAME = "Banks.db"
TABLE_NAME = "Largest_banks"
LOG_FILE = "code_log.txt"


# -----------------------------
# Logging
# -----------------------------
def log_progress(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} : {message}\n")


# -----------------------------
# Extract (robust, no read_html)
# -----------------------------
def extract(url, table_attribs):
    """
    Extract top 10 banks from the 'By market capitalization' table.
    Returns DataFrame with columns: Name, MC_USD_Billion
    """
    html = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}).text
    soup = BeautifulSoup(html, "lxml")

    # Find the correct section heading
    heading = soup.find("span", {"id": "By_market_capitalization"})
    if heading is None:
        raise ValueError("Could not find 'By market capitalization' section.")

    h2 = heading.find_parent("h2")
    if h2 is None:
        raise ValueError("Could not locate the heading container for 'By market capitalization'.")

    # The market-cap table is the first wikitable after that heading
    table = h2.find_next("table", {"class": "wikitable"})
    if table is None:
        raise ValueError("Could not find the market capitalization table after the heading.")

    rows = table.find_all("tr")
    data = []

    # Helper to get bank name from the bank cell (handles flags/country links)
    def get_bank_name(bank_td):
        # Gather all <a> tags with visible text
        links = bank_td.find_all("a")
        texts = [a.get_text(strip=True) for a in links if a.get_text(strip=True)]

        # In this table, the last meaningful link text is usually the bank name.
        # (First links are often country/flag related.)
        if texts:
            return texts[-1]

        # Fallback to raw text if no links
        raw = bank_td.get_text(" ", strip=True)
        raw = re.sub(r"\s+", " ", raw).strip()
        return raw

    for r in rows[1:]:  # skip header row
        cols = r.find_all("td")
        if len(cols) < 3:
            continue

        name = get_bank_name(cols[1])

        mc_text = cols[2].get_text(strip=True)
        mc_text = re.sub(r"\[.*?\]", "", mc_text)      # remove [1] footnotes
        mc_text = re.sub(r"[^\d.]", "", mc_text)       # keep only digits and dot
        if mc_text == "":
            continue

        mc = float(mc_text)

        # skip bad names (extra safety)
        if not name or name.strip() == "":
            continue

        data.append([name.strip(), mc])

        if len(data) == 10:
            break

    df = pd.DataFrame(data, columns=table_attribs)

    # Final sanity check
    if df.empty or df["Name"].isna().any():
        raise ValueError("Extraction failed: got empty bank names. Check table parsing.")

    return df


# -----------------------------
# Transform
# -----------------------------
def transform(df, csv_path):
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"Missing required file: {csv_path}")

    rates_df = pd.read_csv(csv_path)
    if not {"Currency", "Rate"}.issubset(rates_df.columns):
        raise ValueError("exchange_rate.csv must contain Currency and Rate columns")

    rate_map = dict(zip(rates_df["Currency"], rates_df["Rate"]))

    for cur in ["GBP", "EUR", "INR"]:
        if cur not in rate_map:
            raise ValueError(f"exchange_rate.csv missing {cur} rate")

    gbp = float(rate_map["GBP"])
    eur = float(rate_map["EUR"])
    inr = float(rate_map["INR"])

    out = df.copy()
    out["MC_GBP_Billion"] = np.round(out["MC_USD_Billion"] * gbp, 2)
    out["MC_EUR_Billion"] = np.round(out["MC_USD_Billion"] * eur, 2)
    out["MC_INR_Billion"] = np.round(out["MC_USD_Billion"] * inr, 2)

    return out


# -----------------------------
# Loaders
# -----------------------------
def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement, sql_connection):
    log_progress(f"Executing query: {query_statement}")
    print("\nQuery:")
    print(query_statement)
    print(pd.read_sql(query_statement, sql_connection))


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    log_progress("Preliminaries complete. Initiating ETL process")

    table_attribs = ["Name", "MC_USD_Billion"]

    df = extract(URL, table_attribs)
    print(df)
    log_progress("Data extraction complete. Initiating Transformation process")

    df = transform(df, EXCHANGE_RATE_CSV_PATH)
    print(df)
    log_progress("Data transformation complete. Initiating Loading process")

    load_to_csv(df, OUTPUT_CSV_PATH)
    log_progress("Data saved to CSV file")

    conn = sqlite3.connect(DB_NAME)
    log_progress("SQL Connection initiated")

    try:
        load_to_db(df, conn, TABLE_NAME)
        log_progress("Data loaded to Database as a table, Executing queries")

        run_query(f"SELECT * FROM {TABLE_NAME}", conn)
        run_query(f"SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}", conn)
        run_query(f"SELECT Name FROM {TABLE_NAME} LIMIT 5", conn)

        log_progress("Process Complete")
    finally:
        conn.close()
        log_progress("Server Connection closed")
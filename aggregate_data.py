import pandas as pd
import sqlite3

from lib.constants import *

# Connect to SQLite database (or any other database you use)
conn = sqlite3.connect("emissions_aggregates.db")
cursor = conn.cursor()

# Create tables if they don't exist
def create_tables():
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {DAILY_TABLE} (
            business_facility TEXT,
            date TEXT,
            total_emissions REAL,
            PRIMARY KEY (business_facility, date)
        )
    """)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {MONTHLY_TABLE} (
            business_facility TEXT,
            year INTEGER,
            month INTEGER,
            total_emissions REAL,
            PRIMARY KEY (business_facility, year, month)
        )
    """)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {YEARLY_TABLE} (
            business_facility TEXT,
            year INTEGER,
            total_emissions REAL,
            PRIMARY KEY (business_facility, year)
        )
    """)

    conn.commit()

# Load and preprocess data
def load_data():
    data = pd.read_csv(CSV_FILE_PATH)
    data[CO2_COL] = pd.to_numeric(data[CO2_COL], errors='coerce')
    data[DATE_COL] = pd.to_datetime(data[DATE_COL], format='%d/%m/%y', errors='coerce')
    return data.dropna(subset=[DATE_COL, CO2_COL])

# Aggregate and update tables
def update_daily_aggregates(data):
    daily_aggregates = (
        data.groupby([BUSINESS_COL, data[DATE_COL].dt.date])[CO2_COL]
        .sum()
        .reset_index()
        .rename(columns={DATE_COL: "date", CO2_COL: "total_emissions", BUSINESS_COL: "business_facility"})
    )

    for _, row in daily_aggregates.iterrows():
        cursor.execute(f"""
            INSERT INTO {DAILY_TABLE} (business_facility, date, total_emissions)
            VALUES (?, ?, ?)
            ON CONFLICT(business_facility, date) DO UPDATE SET total_emissions=excluded.total_emissions
        """, (row["business_facility"], row["date"], row["total_emissions"]))

    conn.commit()

def update_monthly_aggregates(data):
    monthly_aggregates = (
        data.groupby([BUSINESS_COL, data[DATE_COL].dt.year.rename("year"), data[DATE_COL].dt.month.rename("month")])[CO2_COL]
        .sum()
        .reset_index()
        .rename(columns={CO2_COL: "total_emissions", BUSINESS_COL: "business_facility"})
    )

    for _, row in monthly_aggregates.iterrows():
        cursor.execute(f"""
            INSERT INTO {MONTHLY_TABLE} (business_facility, year, month, total_emissions)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(business_facility, year, month) DO UPDATE SET total_emissions=excluded.total_emissions
        """, (row["business_facility"], row["year"], row["month"], row["total_emissions"]))

    conn.commit()


def update_yearly_aggregates(data):
    yearly_aggregates = (
        data.groupby([BUSINESS_COL, data[DATE_COL].dt.year.rename("year")])[CO2_COL]
        .sum()
        .reset_index()
        .rename(columns={CO2_COL: "total_emissions", BUSINESS_COL: "business_facility"})
    )

    for _, row in yearly_aggregates.iterrows():
        cursor.execute(f"""
            INSERT INTO {YEARLY_TABLE} (business_facility, year, total_emissions)
            VALUES (?, ?, ?)
            ON CONFLICT(business_facility, year) DO UPDATE SET total_emissions=excluded.total_emissions
        """, (row["business_facility"], row["year"], row["total_emissions"]))

    conn.commit()

# Main function to run the update process
def main():
    create_tables()
    data = load_data()
    update_daily_aggregates(data)
    update_monthly_aggregates(data)
    update_yearly_aggregates(data)
    print("Aggregated tables updated successfully.")

if __name__ == "__main__":
    main()
from flask import jsonify, request
from datetime import datetime
import sqlite3

from lib.constants import *  # Ensure all constants like table names are defined here

# Database connection function
def get_db_connection():
    return sqlite3.connect("emissions_aggregates.db")

# Handler function
def total_emissions_db(payload):
    try:
        start_date = datetime.strptime(payload[START_DATE_REQ], '%d/%m/%y')
        end_date = datetime.strptime(payload[END_DATE_REQ], '%d/%m/%y')
        business_facilities = payload[BUSINESS_REQ]

        # Convert dates to the standard format (YYYY-MM-DD)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        # Extract year, month, and day for range calculations
        start_year, start_month, start_day = start_date.year, start_date.month, start_date.day
        end_year, end_month, end_day = end_date.year, end_date.month, end_date.day

        conn = get_db_connection()
        cursor = conn.cursor()

        # Query daily table for partial months at the start and end
        daily_query = f"""
            SELECT business_facility, SUM(total_emissions) as total_emissions
            FROM {DAILY_TABLE}
            WHERE (date BETWEEN ? AND ?)
            AND business_facility IN ({','.join(['?'] * len(business_facilities))})
            GROUP BY business_facility
        """
        cursor.execute(daily_query, [start_date_str, f"{start_year}-{start_month:02}-31", *business_facilities])
        daily_start_rows = cursor.fetchall()

        cursor.execute(daily_query, [f"{end_year}-{end_month:02}-01", end_date_str, *business_facilities])
        daily_end_rows = cursor.fetchall()

        # Query monthly table for full months within the range
        monthly_query = f"""
            SELECT business_facility, SUM(total_emissions) as total_emissions
            FROM {MONTHLY_TABLE}
            WHERE (
                (year > ? OR (year = ? AND month > ?))
                AND (year < ? OR (year = ? AND month < ?))
            )
            AND business_facility IN ({','.join(['?'] * len(business_facilities))})
            GROUP BY business_facility
        """
        cursor.execute(monthly_query, [
            start_year, start_year, start_month,
            end_year, end_year, end_month,
            *business_facilities
        ])
        monthly_rows = cursor.fetchall()

        # Query yearly table for full years within the range
        yearly_query = f"""
            SELECT business_facility, SUM(total_emissions) as total_emissions
            FROM {YEARLY_TABLE}
            WHERE (year > ? AND year < ?)
            AND business_facility IN ({','.join(['?'] * len(business_facilities))})
            GROUP BY business_facility
        """
        cursor.execute(yearly_query, [start_year, end_year, *business_facilities])
        yearly_rows = cursor.fetchall()

        conn.close()

        # Combine results from all queries
        combined_results = {}

        # Helper function to aggregate rows
        def aggregate_rows(rows):
            for row in rows:
                facility, emissions = row
                if facility not in combined_results:
                    combined_results[facility] = 0
                combined_results[facility] += emissions

        aggregate_rows(daily_start_rows)
        aggregate_rows(daily_end_rows)
        aggregate_rows(monthly_rows)
        aggregate_rows(yearly_rows)

        # Format the combined results into the desired response format
        response = [
            {"business_facility": facility, "total_emissions": emissions}
            for facility, emissions in combined_results.items()
        ]

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400

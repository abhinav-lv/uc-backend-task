from flask import jsonify
from datetime import datetime

from lib.constants import *

def total_emissions_vanilla(data, payload):
    try:
        # Parse JSON input
        start_date = datetime.strptime(payload[START_DATE_REQ], '%d/%m/%y')
        end_date = datetime.strptime(payload[END_DATE_REQ], '%d/%m/%y')
        business_facilities = payload[BUSINESS_REQ]

        # Filter data based on the date range
        filtered_data = data[(data[DATE_COL] >= start_date) & (data[DATE_COL] <= end_date)]

        # Further filter data by the business facilities
        filtered_data = filtered_data[filtered_data[BUSINESS_COL].isin(business_facilities)]

        # Group by business facility and calculate total emissions
        result = (
            filtered_data.groupby(BUSINESS_COL)[CO2_COL]
            .sum()
            .reset_index()
            .rename(columns={CO2_COL: 'total_emissions', BUSINESS_COL: BUSINESS_REQ})
        )

        # Convert the result to a dictionary format
        response = result.to_dict(orient='records')

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400

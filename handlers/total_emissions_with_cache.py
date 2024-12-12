import json
import redis
from flask import jsonify
from lib.constants import *
from datetime import datetime

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

def generate_cache_key(start_date, end_date, business_facilities):
    business_facilities = sorted(business_facilities)  # Sort for consistent keys
    facilities_str = json.dumps(business_facilities)
    return f"emissions:{start_date}:{end_date}:{facilities_str}"

def total_emissions_with_cache(data, payload):
    try:
        # Parse JSON input
        start_date = payload[START_DATE_REQ]
        end_date = payload[END_DATE_REQ]
        business_facilities = payload[BUSINESS_REQ]

        # Generate cache key
        cache_key = generate_cache_key(start_date, end_date, business_facilities)

        # Check for cached results
        cached_result = redis_client.get(cache_key)
        if cached_result:
            return jsonify(json.loads(cached_result)), 200

        # Filter data based on the date range and facilities
        filtered_data = data[(data[DATE_COL] >= datetime.strptime(start_date, '%d/%m/%y')) &
                             (data[DATE_COL] <= datetime.strptime(end_date, '%d/%m/%y')) &
                             (data[BUSINESS_COL].isin(business_facilities))]

        # Group by business facility and calculate total emissions
        result = (
            filtered_data.groupby(BUSINESS_COL)[CO2_COL]
            .sum()
            .reset_index()
            .rename(columns={CO2_COL: 'total_emissions', BUSINESS_COL: BUSINESS_REQ})
        )

        response = result.to_dict(orient='records')

        # Cache the result
        redis_client.setex(cache_key, CACHE_TTL, json.dumps(response))

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400

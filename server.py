from flask import Flask, request
import pandas as pd
from handlers.total_emissions_db import total_emissions_db
from handlers.total_emissions_with_cache import total_emissions_with_cache
from handlers.total_emissions_vanilla import total_emissions_vanilla
from lib.constants import CSV_FILE_PATH, CO2_COL, DATE_COL 

app = Flask(__name__)

# Load the CSV data
data = pd.read_csv(CSV_FILE_PATH)
data[CO2_COL] = pd.to_numeric(data[CO2_COL], errors='coerce')
data[DATE_COL] = pd.to_datetime(data[DATE_COL], format='%d/%m/%y', errors='coerce')

@app.route('/api/total_emissions', methods=['POST'])
def total_emissions_vanilla_handler():
    payload = request.get_json()  
    return total_emissions_vanilla(data, payload)  

@app.route('/api/total_emissions_with_cache', methods=['POST'])
def total_emissions_with_cache_handler():
    payload = request.get_json()  
    return total_emissions_with_cache(data, payload)  

@app.route('/api/total_emissions_from_db', methods=['POST'])
def total_emissions_db_handler():
    payload = request.get_json()
    return total_emissions_db(payload)


if __name__ == '__main__':
    app.run(debug=True)

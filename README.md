# UC Backend Engineer Assignment - Abhinav LV

## Get started

1. Create a python environment and install dependencies:

   ```python
   pip install -r requirements.txt
   ```

2. You will need a `redis` server running on `localhost` at port `6379`.

3. You will need to paste the csv file `uc_results_gf.csv` in the root directory, as I have not included that in this repository.

4. After you have the redis server running and the csv file, you can start the server:

   ```bash
   python server.py
   ```

## API Endpoints

1. <b>http://127.0.0.1:5000/api/total_emissions</b>

   This is the endpoint which process the request without any optimisations - no cache is used, and no database is used.

2. <b>http://127.0.0.1:5000/api/total_emissions_with_cache</b>

   This endpoint uses a basic Redis cache to store the exact request and the data returned for it. I wasn't able to design a cache that supported overlapping data in the cache.

3. <b>http://127.0.0.1:5000/api/total_emissions_from_db</b>

   This endpoint uses a sqlite db that stores pre-aggregated data day-wise, month-wise and year-wise in 3 tables. This is used to optimise the request through SQL queries and use the appropriate tables for the time frame in the payload. The code used for building this db is given in the `aggregate_data.py` file. This only needed to be run once, and I've included the db in the repo, so you need not run it.

## API Payload Format

1. This is the expected payload format:

   ```json
   {
     "start_date": "8/6/21",
     "end_date": "24/12/21",
     "business_facility": ["GreenEat Changi", "GreenEat Fusionopolis"]
   }
   ```

   You can give any amount of business facilities in an array.

2. This is the response format:
   ```json
   [
     {
       "business_facility": "GreenEat Changi",
       "total_emissions": 20977.682745606
     },
     {
       "business_facility": "GreenEat Fusionopolis",
       "total_emissions": 42419.962935537
     }
   ]
   ```

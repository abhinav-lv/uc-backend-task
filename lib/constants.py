# CSV file
CSV_FILE_PATH = "uc_results_gf.csv"

# Column names
DATE_COL = 'TRANSACTION DATE'
CO2_COL = 'CO2_ITEM'
BUSINESS_COL = 'Business Facility'

# Request fields
BUSINESS_REQ = 'business_facility'
START_DATE_REQ = 'start_date'
END_DATE_REQ = 'end_date'

# Redis cache
CACHE_TTL = 3600

# Database tables
DAILY_TABLE = "daily_emissions"
MONTHLY_TABLE = "monthly_emissions"
YEARLY_TABLE = "yearly_emissions"
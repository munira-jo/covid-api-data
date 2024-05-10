from datetime import date, timedelta
from helpers import extract_data_from_api_and_load_to_database
import pytz
from schedule import every, run_pending
import time

# Daily scheduled task to pull data for one day ago
date_to_pull_data_for = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
every().day.at("22:00", "Africa/Johannesburg").do(
    extract_data_from_api_and_load_to_database, date=date_to_pull_data_for
)

while True:
    run_pending()
    time.sleep(5)

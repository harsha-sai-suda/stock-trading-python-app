import schedule
import time
from script import run_stock_job

from datetime import datetime

def basic_job():
    print("Job started at:", datetime.now())

# Run every minute
schedule.every().minute.do(basic_job)
# Run every minute
schedule.every().minute.do(run_stock_job)

# Run every day at 9:00 AM
# Uncomment the line below to run the stock job daily at 9:00 AM
# schedule.every().day.at("09:00").do(run_stock_job)

while True:
    schedule. run_pending()
    time.sleep(1)
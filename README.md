# Stock Trading Data Pipeline — Polygon → Snowflake

A clean and automated data pipeline that extracts live stock ticker data from [Polygon.io](https://polygon.io), stores it locally for reference, and loads it into Snowflake for analysis. 
It allows for easy scheduling, secure configuration through environment variables, and clean data ingestion workflows.

- **End-to-End Automation:** From data extraction to Snowflake loading — handled seamlessly.  
- **Schema-Aware Loading:** Automatically creates the Snowflake table structure from API response.
- **Local Data Backup:** Maintains CSV export for quick validation and data lineage.  
- **Simple Scheduling:** Enables automated job execution using simple time-based triggers.  
- **Secure Configuration:** Credentials managed safely via `.env` file.

# Getting started

```bash
# Create and activate environment
python -m venv pythonenv
source pythonenv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
```

Update the .env file with your Polygon and Snowflake credentials.

Run the project:
```bash
python script.py
```


---

## Metrics Extraction and Automation

This repository automates the extraction and analysis of metrics from a database and generates insights on search and click data. The pipeline calculates **Click-Through Rate (CTR)**, identifies **top-performing queries**, and detects **low-performing queries**, aiming to provide meaningful business insights.

## Features

- **Average CTR Calculation**: Automatically calculates the daily average Click-Through Rate (CTR) for search queries.
- **Top Search Queries**: Identifies the top 5 search queries with the highest CTR over a given period.
- **Low-Performance Queries Detection**: Detects queries with high impressions but low clicks, which can serve as optimization candidates.
- **Automation**: The pipeline is fully automated and can be scheduled to run at specific intervals (e.g., daily) to keep the metrics up-to-date.

## Setup Instructions

Follow the steps below to set up and run the project locally.

### Step 1: Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/JANNATHA-MANISH/metrics-extraction-automation.git
cd metrics-extraction-automation
```

### Step 2: Create a Virtual Environment

Create a virtual environment in your project directory to manage dependencies.

```bash
python -m venv env
```

Activate the virtual environment:

- **Windows**: 
    ```bash
    .\env\Scripts\activate
    ```

- **Linux/MacOS**: 
    ```bash
    source env/bin/activate
    ```

### Step 3: Install Dependencies

Install the required dependencies using `pip`.

```bash
pip install -r requirements.txt
```

### Step 4: Configure the Database Connection

Create a `.env` file in the root of the project directory and add your database connection details. Example:

```bash
DATABASE_URL=your_database_url
```

Make sure to add any additional environment variables needed for your setup (e.g., API keys).

> **Note:** This project is designed to work with **Supabase** as the backend SQL database. If you're using Supabase, ensure you have created a project and obtained your database URL and credentials. [Supabase Setup Documentation](https://supabase.com/docs/guides)

### Step 5: Provide Your Own Data (Using `dumpdata.py`)

The `dumpdata.py` script allows you to import your own sample data into the database. This is useful for testing or simulating the pipeline without needing real-world data initially. You can use this script to populate the `search_clicks` table with sample data.

To run the `dumpdata.py` script:

1. Open the `dumpdata.py` file located in the root directory.
2. Modify the script to match the structure of your dataset, if needed. The script uses the `psycopg2` library to connect to the database and execute SQL queries. Example:

```python
import psycopg2
from datetime import datetime

    # Sample data to insert into the database (modify as needed)
    data = [
        ("2024-12-01", "query1", 1000, 200),  # Date, Query, Impressions, Clicks
        ("2024-12-01", "query2", 1500, 300),
        ("2024-12-01", "query3", 1200, 150),
        ("2024-12-02", "query1", 1100, 250),
        ("2024-12-02", "query2", 1400, 180),
    ]
    
    #repalce your data
   
```

3. Run the script to insert the sample data into your database:

```bash
python dumpdata.py
```

This will populate the `search_clicks` table with sample data that can be used to execute the pipeline and generate insights.

### Step 6: Run the Pipeline

Once the environment is set up and the data is in place, you can manually run the pipeline by executing the following command:

```bash
python run_pipeline.py
```

This will run the pipeline, calculate the metrics, and store the insights in the database.

### Step 7: Automate the Pipeline (Scheduler)

To automate the execution of the pipeline (e.g., on a daily basis), you can use **Task Scheduler** (for Windows), **cron jobs** (for Linux/MacOS), or a task scheduler library like **APScheduler** or **Celery**.

#### Example: Automating with APScheduler (Python Scheduler)
You can use **APScheduler** to run the pipeline automatically at a fixed interval (e.g., daily).

```python
from apscheduler.schedulers.blocking import BlockingScheduler
from run_pipeline import run_pipeline

scheduler = BlockingScheduler()
# Schedule the pipeline to run every day at 2:00 AM
scheduler.add_job(run_pipeline, 'cron', hour=2, minute=0)

scheduler.start()
```

You can include this scheduler in a Python script, and it will execute the pipeline every day.

#### Example: Automating with Task Scheduler (Windows)

For **Windows**, you can use **Task Scheduler** to automate the script. Here's a basic guide:

1. Open **Task Scheduler** and click on **Create Task**.
2. Set the **trigger** to daily at your preferred time.
3. Set the **action** to run the following command in the program/script section:
   ```bash
   python C:\path\to\your\project\run_pipeline.py
   ```

### Step 8: Check Results

The results of the analysis will be saved in the `search_insights` table in your database. The following columns will be available for review:

- `insight_date`: The date when the insights were generated.
- `average_ctr`: The average Click-Through Rate for that date.
- `top_queries`: The top 5 search queries with the highest CTR (stored as JSON).
- `low_performance_queries`: The queries with high impressions but low clicks (stored as JSON).

---

### Final Notes

- Make sure you have **PostgreSQL** or another relational database set up with a table (`search_clicks`) that contains the search data.
- You can modify the pipeline logic to suit your exact use case (e.g., change thresholds for low-performance queries).
- This project can be further extended to include more metrics or integrate with other services.
- **Supabase** is recommended for handling the database and executing SQL commands as it integrates seamlessly with Python using `psycopg2`. For setup details, visit [Supabase Documentation](https://supabase.com/docs/guides).

---

### SAMPLE IMAGE AI GENERATED 

![ctr_analysis](https://github.com/user-attachments/assets/f63d23a3-7dce-4413-a4f5-aa7895e4fd88)

---

By following these steps, you'll be able to set up the **Metrics Extraction and Automation** system, run it locally or automatically, and check your insights via the database or API.

---

This guide should help users easily set up and automate the pipeline.

---

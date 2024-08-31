# ETL Pipeline for NYC Airbnb Data

## Setup
1. Ensure Apache Airflow and PostgreSQL are installed.
2. Place the `AB_NYC_2019.csv` file in the `raw` directory.
3. Set up the PostgreSQL database and table as described above.

## Running the DAG
1. Place `nyc_airbnb_etl.py` in the Airflow DAGs folder.
2. Trigger the DAG from the Airflow UI or CLI.
3. Monitor the pipeline and check logs for errors in `airflow_failures.log`.

## Parameters
- Update the `POSTGRES_CONN_ID` with your PostgreSQL connection ID.
- Adjust file paths in the DAG script as needed.

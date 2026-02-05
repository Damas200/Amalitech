from airflow import DAG
from airflow.decorators import task
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd

with DAG(
    dag_id="flight_csv_to_mysql",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["phase2", "staging"]
):

    @task
    def load_csv_to_mysql():
        # Load CSV
        df = pd.read_csv(
            "/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv"
        )

        # Rename columns (CSV → DB)
        df = df.rename(columns={
            "Airline": "airline",
            "Source": "source",
            "Source Name": "source_name",
            "Destination": "destination",
            "Destination Name": "destination_name",
            "Departure Date & Time": "departure_datetime",
            "Arrival Date & Time": "arrival_datetime",
            "Duration (hrs)": "duration_hrs",
            "Stopovers": "stopovers",
            "Aircraft Type": "aircraft_type",
            "Class": "class",
            "Booking Source": "booking_source",
            "Base Fare (BDT)": "base_fare_bdt",
            "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
            "Total Fare (BDT)": "total_fare_bdt",
            "Seasonality": "seasonality",
            "Days Before Departure": "days_before_departure"
        })

        # Convert datetime columns
        df["departure_datetime"] = pd.to_datetime(df["departure_datetime"])
        df["arrival_datetime"] = pd.to_datetime(df["arrival_datetime"])

        # Load into MySQL
        mysql_hook = MySqlHook(mysql_conn_id="mysql_staging")
        engine = mysql_hook.get_sqlalchemy_engine()

        df.to_sql(
            name="flight_prices_raw",
            con=engine,
            if_exists="replace",
            index=False
        )

    load_csv_to_mysql()

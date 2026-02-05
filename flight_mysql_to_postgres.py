from airflow import DAG
from airflow.decorators import task
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd

with DAG(
    dag_id="flight_mysql_to_postgres",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["phase3", "analytics"]
):

    @task
    def extract_from_mysql():
        mysql_hook = MySqlHook(mysql_conn_id="mysql_staging")
        engine = mysql_hook.get_sqlalchemy_engine()
        df = pd.read_sql("SELECT * FROM flight_prices_raw", engine)
        return df

    @task
    def validate_and_transform(df: pd.DataFrame):
        # Drop rows with missing critical fields
        df = df.dropna(subset=[
            "airline", "source", "destination",
            "base_fare_bdt", "tax_surcharge_bdt"
        ])

        # Remove invalid fares
        df = df[(df["base_fare_bdt"] >= 0) & (df["tax_surcharge_bdt"] >= 0)]

        # Recalculate total fare
        df["total_fare_bdt"] = df["base_fare_bdt"] + df["tax_surcharge_bdt"]

        # Create route
        df["route"] = df["source"] + " → " + df["destination"]

        # Define Peak vs Non-Peak seasons
        peak_seasons = ["Eid", "Winter Holidays"]
        df["season_type"] = df["seasonality"].apply(
            lambda x: "Peak" if x in peak_seasons else "Non-Peak"
        )

        return df

    @task
    def load_clean_data(df: pd.DataFrame):
        pg_hook = PostgresHook(postgres_conn_id="postgres_analytics")
        engine = pg_hook.get_sqlalchemy_engine()

        # Load cleaned flight data
        df[[
            "airline", "source", "destination",
            "departure_datetime", "arrival_datetime",
            "duration_hrs", "stopovers", "class",
            "booking_source", "base_fare_bdt",
            "tax_surcharge_bdt", "total_fare_bdt",
            "seasonality", "days_before_departure"
        ]].to_sql(
            "flight_prices_clean",
            engine,
            if_exists="replace",
            index=False
        )

        # KPI 1: Average fare by airline
        df.groupby("airline")["total_fare_bdt"].mean().reset_index() \
            .rename(columns={"total_fare_bdt": "avg_total_fare_bdt"}) \
            .to_sql("kpi_avg_fare_by_airline", engine, if_exists="replace", index=False)

        # KPI 2: Booking count by airline
        df.groupby("airline").size().reset_index(name="booking_count") \
            .to_sql("kpi_booking_count_by_airline", engine, if_exists="replace", index=False)

        # KPI 3: Most popular routes
        df.groupby("route").size().reset_index(name="booking_count") \
            .sort_values("booking_count", ascending=False) \
            .to_sql("kpi_popular_routes", engine, if_exists="replace", index=False)

        # KPI 4a: Average fare by season (existing)
        df.groupby("seasonality")["total_fare_bdt"].mean().reset_index() \
            .rename(columns={"total_fare_bdt": "avg_total_fare_bdt"}) \
            .to_sql("kpi_seasonal_fares", engine, if_exists="replace", index=False)

        # KPI 4b: Peak vs Non-Peak comparison (NEW – REQUIRED)
        df.groupby("season_type")["total_fare_bdt"].mean().reset_index() \
            .rename(columns={"total_fare_bdt": "avg_total_fare_bdt"}) \
            .to_sql("kpi_peak_vs_non_peak_fares", engine, if_exists="replace", index=False)

    load_clean_data(
        validate_and_transform(
            extract_from_mysql()
        )
    )

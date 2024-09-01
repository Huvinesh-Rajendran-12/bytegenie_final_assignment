from dotenv import load_dotenv
import psycopg2
import os
import time

import polars as pl

from problem_2 import query_data

load_dotenv()


def main():
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")
    DATABASE = os.getenv("DB")
    USERNAME = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")

    conn = psycopg2.connect(
        dbname=DATABASE, user=USERNAME, password=PASSWORD, host=HOST, port=PORT
    )

    filter_arguments = [
        ["event_city", "includes", ["Singapore", "Tokyo"]],
        ["event_start_date", "greater-than-equal-to", "2025-01-01"],
        ["company_industry", "includes", ["Software", "IT"]],
    ]

    output_columns = ['event_city', 'event_name', 'event_country', 'company_industry', 'company_name', 'company_url', 'person_first_name', 'person_last_name', 'person_seniority']

    result_df = query_data(conn, filter_arguments, output_columns)
    print(result_df)

    conn.close()


if __name__ == "__main__":
    main()

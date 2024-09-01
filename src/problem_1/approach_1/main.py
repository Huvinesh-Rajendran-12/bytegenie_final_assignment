from problem_1 import Dataset, DataframeFilter
import time

import polars as pl


def main():
    dataset = Dataset()
    dataframes = dataset.get()

    conditions = {
        "events": {
            "event_city": ["Singapore", "New York"],
            "event_start_date": {"start": "2023-06-01", "end": "2023-12-31"},
        },
        "companies": {
            "company_industry": ["Tech"],
            "company_revenue": {"min": 1500000, "max": 3000000},
        },
    }

    past_employments_df = pl.DataFrame({
            "person_id": [f"person-{i}" for i in range(1, 51) for _ in range(2)],
            "company_url": [f"company-{i}" for i in range(1, 101)],
            "employment_start_date": [pl.date(2010 + i // 10, 1, 1) for i in range(100)],
            "employment_end_date": [pl.date(2015 + i // 10, 12, 31) for i in range(100)]
        })

    dataframes["past_employments"] = past_employments_df
    filter_system = DataframeFilter(dataframes)

    start_time = time.time()
    filtered_dataframes = filter_system.filter_dataframes(conditions)
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"\nFilter execution time: {execution_time:.4f} seconds")

    for name, df in filtered_dataframes.items():
        print(f"\n{name} DataFrame:")
        print(df)


if __name__ == "__main__":
    main()

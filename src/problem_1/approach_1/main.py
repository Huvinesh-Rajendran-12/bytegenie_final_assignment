from problem_1 import Dataset, DataframeFilter

import polars as pl


def main():
    dataset = Dataset()
    dataframes = dataset.get()
    filter_system = DataframeFilter(dataframes)

    conditions = {
        "events": {
            "event_city": ["Singapore", "New York"],
            "event_start_date": {"start": "2023-06-01", "end": "2023-12-31"}
        },
        "companies": {
            "company_industry": ["Tech"],
            "company_revenue": {"min": 1500000, "max": 3000000}
        },
        "event_attendees": {"company_relation_to_event": ["Sponsor"]},
        "company_employees": {"person_seniority": ["Director", "C-Level"]}
    }

    filtered_dataframes = filter_system.filter_dataframes(conditions)

    for name, df in filtered_dataframes.items():
        print(f"\n{name} DataFrame:")
        print(df)


if __name__ == "__main__":
    main()

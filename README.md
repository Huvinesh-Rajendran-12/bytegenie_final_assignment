# ByteGenie Final Assignment

This project implements efficient data filtering and querying mechanisms for interconnected datasets related to events, companies, and people.

## Project Structure

```
├── README.md
├── pyproject.toml
├── requirements-dev.lock
├── requirements.lock
└── src
    ├── problem_1
    │   ├── __init__.py
    │   ├── data
    │   │   ├── __init__.py
    │   │   └── main.py
    │   └── solution
    │       ├── __init__.py
    │       ├── filter.py
    │       └── main.py
    └── problem_2
        ├── __init__.py
        ├── data
        │   └── data.sql
        ├── main.py
        └── query.py
```

## Problem 1: DataFrame Filtering

The first part of the project focuses on filtering interconnected dataframes efficiently.

### Key Components:

- `src/problem_1/data/main.py`: Contains the `Dataset` class for generating sample data.
- `src/problem_1/solution/filter.py`: Implements the `DataframeFilter` class for efficient filtering of interconnected dataframes.
- `src/problem_1/solution/main.py`: Demonstrates the usage of the filtering system.

### Features:

- Handles multiple interconnected dataframes (events, companies, employees, etc.)
- Supports various filter conditions (list-based, range-based, date range)
- Propagates filters across related dataframes
- Ensures consistency across all filtered dataframes

### Usage:

```python
from problem_1 import Dataset, DataframeFilter

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
    }
}

filtered_dataframes = filter_system.filter_dataframes(conditions)
```

## Problem 2: SQL Querying

The second part of the project focuses on querying data from a PostgreSQL database with a specific schema.

### Key Components:

- `src/problem_2/query.py`: Implements the `query_data` function for flexible SQL querying.
- `src/problem_2/main.py`: Demonstrates the usage of the querying system.

### Features:

- Supports querying across multiple related tables (events, companies, people)
- Handles various filter conditions (includes, greater-than-equal-to, less-than-equal-to)
- Returns results as a Polars DataFrame

### Usage:

```python
from problem_2 import query_data
import psycopg2

conn = psycopg2.connect(...)  # Add your database connection details

filter_arguments = [
    ["event_city", "includes", ["Singapore", "Tokyo"]],
    ["event_start_date", "greater-than-equal-to", "2025-01-01"],
    ["company_industry", "includes", ["Software", "IT"]]
]

output_columns = [
    "event_city", "event_name", "event_country", "company_industry",
    "company_name", "company_url", "person_first_name", "person_last_name",
    "person_seniority"
]

result_df = query_data(conn, filter_arguments, output_columns)
print(result_df)
```

## Setup and Installation

1. Clone the repository
2. Install the required packages:
   ```
   pip install -r requirements.lock
   ```
3. For development, use the requirements in `requirements-dev.lock`

## Running the Project

- For Problem 1:
  ```
  python -m src.problem_1.solution.main
  ```
- For Problem 2:
  ```
  python -m src.problem_2.main
  ```

Make sure to set up the necessary environment variables for database connection in Problem 2.

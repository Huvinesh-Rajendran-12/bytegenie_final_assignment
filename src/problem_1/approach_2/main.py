from problem_1 import Dataset, DataGraph
from datetime import date


def main():
    # Create the Dataset instance
    dataset = Dataset()

    # Create the DataGraph instance
    data_graph = DataGraph(dataset)

    # Example 1: Find events attended by directors of energy companies
    filtered_data = data_graph.filter(
        company_industry=["energy"], person_seniority=["director"]
    )

    for df_name, df in filtered_data.items():
        print(f"\n{df_name}:")
        print(df)


if __name__ == "__main__":
    main()

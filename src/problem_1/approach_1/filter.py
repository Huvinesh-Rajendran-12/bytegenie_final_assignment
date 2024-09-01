import polars as pl
from typing import Dict, List, Tuple, Union
from collections import deque
from datetime import datetime


class DataframeFilter:
    def __init__(self, dataframes: Dict[str, pl.DataFrame]):
        self.dataframes = dataframes
        self.relationships = self._find_relationships()

    def _find_relationships(self) -> Dict[Tuple[str, str], str]:
        relationships = {}
        df_names = list(self.dataframes.keys())
        for i in range(len(df_names)):
            for j in range(i + 1, len(df_names)):
                df1_name, df2_name = df_names[i], df_names[j]
                common_columns = set(self.dataframes[df1_name].columns) & set(
                    self.dataframes[df2_name].columns
                )
                for col in common_columns:
                    relationships[(df1_name, df2_name)] = col
                    relationships[(df2_name, df1_name)] = col
        return relationships

    def filter_dataframes(
        self, conditions: Dict[str, Dict[str, Union[List, Dict]]]
    ) -> Dict[str, pl.DataFrame]:
        filtered_dfs = {name: df.clone() for name, df in self.dataframes.items()}
        filter_queue = deque(conditions.items())

        while filter_queue:
            df_name, condition = filter_queue.popleft()
            initial_df = filtered_dfs[df_name].clone()
            filtered_dfs[df_name] = self._apply_filter(filtered_dfs[df_name], condition)

            if not filtered_dfs[df_name].equals(initial_df):
                self._propagate_filter(
                    df_name, filtered_dfs[df_name], filtered_dfs, filter_queue
                )

        filtered_dfs = self._ensure_consistency(filtered_dfs)

        return filtered_dfs

    def _apply_filter(
        self, df: pl.DataFrame, condition: Dict[str, Union[List, Dict]]
    ) -> pl.DataFrame:
        for column, filter_value in condition.items():
            if isinstance(filter_value, list):
                df = df.filter(pl.col(column).is_in(filter_value))
            elif isinstance(filter_value, dict):
                if "min" in filter_value or "max" in filter_value:
                    df = self._apply_range_filter(df, column, filter_value)
                elif "start" in filter_value or "end" in filter_value:
                    df = self._apply_date_range_filter(df, column, filter_value)
        return df

    def _apply_range_filter(
        self, df: pl.DataFrame, column: str, range_dict: Dict[str, Union[int, float]]
    ) -> pl.DataFrame:
        min_val = range_dict.get("min", float("-inf"))
        max_val = range_dict.get("max", float("inf"))
        return df.filter((pl.col(column) >= min_val) & (pl.col(column) <= max_val))

    def _apply_date_range_filter(
        self, df: pl.DataFrame, column: str, date_range: Dict[str, str]
    ) -> pl.DataFrame:
        start_date = datetime.strptime(
            date_range.get("start", "1900-01-01"), "%Y-%m-%d"
        ).date()
        end_date = datetime.strptime(
            date_range.get("end", "9999-12-31"), "%Y-%m-%d"
        ).date()
        return df.filter((pl.col(column) >= start_date) & (pl.col(column) <= end_date))

    def _propagate_filter(
        self,
        source_df_name: str,
        filtered_df: pl.DataFrame,
        filtered_dfs: Dict[str, pl.DataFrame],
        filter_queue: deque,
    ):
        for (df1, df2), common_col in self.relationships.items():
            if df1 == source_df_name:
                related_values = filtered_df[common_col].unique().to_list()
                initial_df = filtered_dfs[df2].clone()
                filtered_dfs[df2] = filtered_dfs[df2].filter(
                    pl.col(common_col).is_in(related_values)
                )

                if not filtered_dfs[df2].equals(initial_df):
                    filter_queue.append((df2, {common_col: related_values}))

    def _ensure_consistency(
        self, filtered_dfs: Dict[str, pl.DataFrame]
    ) -> Dict[str, pl.DataFrame]:
        for (df1_name, df2_name), common_col in self.relationships.items():
            df1_values = set(filtered_dfs[df1_name][common_col].unique().to_list())
            df2_values = set(filtered_dfs[df2_name][common_col].unique().to_list())
            common_values = df1_values.intersection(df2_values)

            filtered_dfs[df1_name] = filtered_dfs[df1_name].filter(
                pl.col(common_col).is_in(common_values)
            )
            filtered_dfs[df2_name] = filtered_dfs[df2_name].filter(
                pl.col(common_col).is_in(common_values)
            )

        return filtered_dfs

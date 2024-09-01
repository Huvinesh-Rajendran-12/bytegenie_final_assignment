import polars as pl
import networkx as nx
from typing import Dict, List, Union, Any, Tuple
from collections import defaultdict
from itertools import combinations
from datetime import datetime, date

class GraphFilter:
    def __init__(self, dataframes: Dict[str, pl.DataFrame]):
        self.dataframes = dataframes
        self.relationships = self._find_relationships()
        self.filtered_dataframes = {name: df.clone() for name, df in dataframes.items()}

    def _find_relationships(self) -> Dict[Tuple[str, str], str]:
        relationships = {}
        df_names = list(self.dataframes.keys())
        for i in range(len(df_names)):
            for j in range(i + 1, len(df_names)):
                df1_name, df2_name = df_names[i], df_names[j]
                common_columns = set(self.dataframes[df1_name].columns) & set(self.dataframes[df2_name].columns)
                for col in common_columns:
                    relationships[(df1_name, df2_name)] = col
                    relationships[(df2_name, df1_name)] = col
        return relationships

    def add_dataframe(self, name: str, df: pl.DataFrame):
        self.dataframes[name] = df
        self.filtered_dataframes[name] = df.clone()
        self.relationships = self._find_relationships()

    def filter_graph(self, conditions: Dict[str, Dict[str, Union[List, Dict]]]) -> Dict[str, pl.DataFrame]:
        for df_name, condition in conditions.items():
            self._apply_filter(df_name, condition)

        self._ensure_consistency()
        return self.filtered_dataframes

    def _apply_filter(self, df_name: str, condition: Dict[str, Union[List, Dict]]):
        df = self.filtered_dataframes[df_name]
        for column, filter_value in condition.items():
            if isinstance(filter_value, list):
                df = df.filter(pl.col(column).is_in(filter_value))
            elif isinstance(filter_value, dict):
                if 'min' in filter_value or 'max' in filter_value:
                    df = self._apply_range_filter(df, column, filter_value)
                elif 'start' in filter_value or 'end' in filter_value:
                    df = self._apply_date_range_filter(df, column, filter_value)
        self.filtered_dataframes[df_name] = df

    def _apply_range_filter(self, df: pl.DataFrame, column: str, range_dict: Dict[str, Union[int, float]]) -> pl.DataFrame:
        min_val = range_dict.get('min', float('-inf'))
        max_val = range_dict.get('max', float('inf'))
        return df.filter((pl.col(column) >= min_val) & (pl.col(column) <= max_val))

    def _apply_date_range_filter(self, df: pl.DataFrame, column: str, date_range: Dict[str, str]) -> pl.DataFrame:
        start_date = pl.lit(date_range.get('start', '1900-01-01')).str.strptime(pl.Date, '%Y-%m-%d')
        end_date = pl.lit(date_range.get('end', '9999-12-31')).str.strptime(pl.Date, '%Y-%m-%d')
        return df.filter((pl.col(column) >= start_date) & (pl.col(column) <= end_date))

    def _ensure_consistency(self):
        for (df1_name, df2_name), common_col in self.relationships.items():
            df1 = self.filtered_dataframes[df1_name]
            df2 = self.filtered_dataframes[df2_name]

            common_values = set(df1[common_col].unique()) & set(df2[common_col].unique())

            self.filtered_dataframes[df1_name] = df1.filter(pl.col(common_col).is_in(common_values))
            self.filtered_dataframes[df2_name] = df2.filter(pl.col(common_col).is_in(common_values))

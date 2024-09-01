import networkx as nx
import polars as pl
from typing import Dict, List, Any, Optional, Set
import time
from problem_1 import DataGraph, Dataset

class FilterEngine:
    def __init__(self, dataset: Dataset):
        self.data_graph = DataGraph(dataset)
        self.graph = self.data_graph.graph
        self.data_frames = self.data_graph.data_frames
        self.relationships = dataset.relationships

    def filter(self, **kwargs: Any) -> Dict[str, pl.DataFrame]:
        start_time = time.time()

        # Step 1: Find initial matching nodes based on criteria
        matching_nodes = self._find_matching_nodes(kwargs)

        # Step 2: Expand to include all connected nodes based on relationships
        expanded_nodes = self._expand_nodes(matching_nodes)

        # Step 3: Organize results by entity type
        results = self._organize_results(expanded_nodes)

        execution_time = time.time() - start_time
        self._display_filtered_results(results, kwargs, execution_time)
        return results

    def _find_matching_nodes(self, kwargs: Any) -> Set[Any]:
        matching_nodes = set()
        for node, data in self.graph.nodes(data=True):
            if all(self._matches_criteria(data.get(k), v) for k, v in kwargs.items()):
                matching_nodes.add(node)
        return matching_nodes

    def _expand_nodes(self, initial_nodes: Set[Any]) -> Set[Any]:
        expanded_nodes = set(initial_nodes)
        to_explore = list(initial_nodes)
        while to_explore:
            node = to_explore.pop(0)
            for neighbor in self.graph.neighbors(node):
                if neighbor not in expanded_nodes:
                    expanded_nodes.add(neighbor)
                    to_explore.append(neighbor)
        return expanded_nodes

    def _organize_results(self, filtered_nodes: Set[Any]) -> Dict[str, pl.DataFrame]:
        results = {}
        for node in filtered_nodes:
            node_data = self.graph.nodes[node]
            entity_type = node_data['entity_type']
            if entity_type not in results:
                results[entity_type] = []
            results[entity_type].append(node_data)

        # Convert lists of dictionaries to Polars DataFrames
        for entity_type, entities in results.items():
            results[entity_type] = pl.DataFrame(entities)

        return results

    def _display_filtered_results(self, results: Dict[str, pl.DataFrame], filters: Dict[str, Any], execution_time: float):
        print("\n--- Filter Criteria ---")
        for key, value in filters.items():
            print(f"{key}: {value}")

        print(f"\n--- Results Summary (Execution Time: {execution_time:.4f} seconds) ---")
        total_items = sum(df.height for df in results.values())
        print(f"Total items found: {total_items}")
        for entity_type, df in results.items():
            print(f"{entity_type.capitalize()}: {df.height} items")

        for entity_type, df in results.items():
            print(f"\n--- {entity_type.capitalize()} Results ---")
            if not df.is_empty():
                print(df)
                print(f"\nShape: {df.shape}")
            else:
                print("No results found.")

        print("\n" + "="*50 + "\n")

    def _get_id_column(self, df: pl.DataFrame) -> str:
        id_columns = ["event_url", "company_url", "person_id", "person_company_url", "past_company_url"]
        for col in id_columns:
            if col in df.columns:
                return col
        raise ValueError("No identifier column found in DataFrame")

    def _matches_criteria(self, value: Any, criteria: Any) -> bool:
        if value is None:
            return False
        if isinstance(criteria, list):
            return value in criteria
        elif callable(criteria):
            try:
                return criteria(value)
            except AttributeError:
                return False
        else:
            return value == criteria

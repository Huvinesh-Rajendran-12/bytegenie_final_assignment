import polars as pl
import networkx as nx
from typing import Dict, Any, Set
from datetime import date

class DataGraph:
    def __init__(self, dataset):
        self.graph = nx.Graph()
        self.dataset = dataset
        self.dataframes = dataset.get()
        self.build_graph()

    def build_graph(self):
        for df_name, df in self.dataframes.items():
            id_column = self.get_id_column(df_name)

            for row in df.iter_rows(named=True):
                node_id = f"{df_name}_{row[id_column]}"
                self.graph.add_node(node_id, df_name=df_name, **row)

        self.add_edges('event_attendees', 'event_url', 'events')
        self.add_edges('event_attendees', 'company_url', 'companies')
        self.add_edges('company_employees', 'company_url', 'companies')
        self.add_edges('past_employments', 'company_url', 'companies')
        self.add_edges('past_employments', 'person_id', 'company_employees')
        self.add_edges('company_contact', 'company_url', 'companies')

    def get_id_column(self, df_name):
        id_columns = {
            'events': 'event_url',
            'companies': 'company_url',
            'company_employees': 'person_id',
            'past_employments': 'person_id',
            'event_attendees': 'event_url',
            'company_contact': 'company_url'
        }
        return id_columns.get(df_name, None)

    def add_edges(self, from_df: str, join_col: str, to_df: str):
        from_df_data = self.dataframes[from_df]
        to_df_data = self.dataframes[to_df]

        for row in from_df_data.iter_rows(named=True):
            from_id = f"{from_df}_{row[self.get_id_column(from_df)]}"
            to_id = f"{to_df}_{row[join_col]}"
            if to_id.split('_')[1] in to_df_data[self.get_id_column(to_df)]:
                self.graph.add_edge(from_id, to_id)

    def filter(self, **kwargs) -> Dict[str, pl.DataFrame]:
        filtered_nodes = set()

        # Start with companies
        company_conditions = {k: v for k, v in kwargs.items() if k.startswith('company_')}
        if company_conditions:
            filtered_companies = self.filter_nodes('companies', company_conditions)
        else:
            filtered_companies = set(node for node in self.graph.nodes() if self.graph.nodes[node]['df_name'] == 'companies')

        # Filter employees
        employee_conditions = {k: v for k, v in kwargs.items() if k.startswith('person_')}
        if employee_conditions:
            filtered_employees = self.filter_nodes('company_employees', employee_conditions)
            filtered_companies &= set(self.get_connected_nodes(filtered_employees, 'companies'))

        # Get events for filtered companies
        filtered_events = self.get_connected_nodes(filtered_companies, 'events')

        # Apply event conditions if any
        event_conditions = {k: v for k, v in kwargs.items() if k.startswith('event_')}
        if event_conditions:
            filtered_events &= self.filter_nodes('events', event_conditions)

        # Get all related nodes
        filtered_nodes = filtered_companies | filtered_events
        filtered_nodes |= self.get_connected_nodes(filtered_companies, 'company_employees')
        filtered_nodes |= self.get_connected_nodes(filtered_companies, 'company_contact')
        filtered_nodes |= self.get_connected_nodes(filtered_companies, 'past_employments')
        filtered_nodes |= self.get_connected_nodes(filtered_events, 'event_attendees')

        result_graph = nx.Graph(self.graph.subgraph(filtered_nodes))
        return self.graph_to_dataframes(result_graph)

    def filter_nodes(self, df_name: str, conditions: Dict[str, Any]) -> Set[str]:
        nodes = set(node for node in self.graph.nodes() if self.graph.nodes[node]['df_name'] == df_name)
        for attribute, value in conditions.items():
            nodes = set(node for node in nodes if self.apply_condition(self.graph.nodes[node].get(attribute), value))
        return nodes

    def get_connected_nodes(self, nodes: Set[str], target_df: str) -> Set[str]:
        connected_nodes = set()
        for node in nodes:
            neighbors = set(self.graph.neighbors(node))
            connected_nodes |= set(n for n in neighbors if self.graph.nodes[n]['df_name'] == target_df)
        return connected_nodes

    def apply_condition(self, node_value: Any, filter_value: Any) -> bool:
        if node_value is None:
            return False

        try:
            if isinstance(filter_value, list):
                return node_value in filter_value
            elif isinstance(filter_value, tuple) and len(filter_value) == 2:
                operator, value = filter_value
                if operator == 'gte':
                    return node_value >= value
                elif operator == 'lte':
                    return node_value <= value
                elif operator == 'gt':
                    return node_value > value
                elif operator == 'lt':
                    return node_value < value
                elif operator == 'ne':
                    return node_value != value
            elif isinstance(node_value, date) and isinstance(filter_value, date):
                return node_value == filter_value
            else:
                return node_value == filter_value
        except TypeError:
            print(f"TypeError: Unable to compare {node_value} with {filter_value}")
            return False

    def graph_to_dataframes(self, graph: nx.Graph) -> Dict[str, pl.DataFrame]:
        result = {}
        for df_name in set(nx.get_node_attributes(graph, 'df_name').values()):
            nodes = [node for node in graph.nodes() if graph.nodes[node]['df_name'] == df_name]
            df = pl.DataFrame([self.graph.nodes[node] for node in nodes])
            if not df.is_empty():
                result[df_name] = df
        return result

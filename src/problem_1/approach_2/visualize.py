import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D


def visualize_graph(G):
    # Set up colors for different node types
    color_map = {"events": "red", "companies": "green", "company_employees": "blue"}
    node_colors = [
        color_map.get(G.nodes[node]["entity_type"], "gray") for node in G.nodes()
    ]

    # Create the plot
    plt.figure(figsize=(20, 20))
    pos = nx.spring_layout(G, k=0.5, iterations=50)
    nx.draw(
        G,
        pos,
        node_color=node_colors,
        with_labels=True,
        node_size=1000,
        font_size=8,
        font_weight="bold",
    )

    # Add edge labels
    edge_labels = nx.get_edge_attributes(G, "relationship")
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=6)

    # Add a legend
    legend_elements = [
        Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            label=key.capitalize(),
            markerfacecolor=value,
            markersize=10,
        )
        for key, value in color_map.items()
    ]
    plt.legend(handles=legend_elements, loc="upper right")

    plt.title("Graph Visualization", fontsize=20)
    plt.axis("off")
    plt.tight_layout()
    plt.show()

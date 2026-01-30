import argparse
import logging
import random

import matplotlib.pyplot as plt
import networkx as nx

logger = logging.getLogger(__name__)

fh = logging.FileHandler("/app/cliques/clique_app.log", mode="a")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)

logger.addHandler(fh)

parser = argparse.ArgumentParser()
parser.add_argument(
    "-n",
    "--nodes",
    type=str,
    help="number of nodes",
)


def generate_random_graph(node_count):
    edge_probability = random.uniform(0.01, 0.99)
    return (
        nx.erdos_renyi_graph(n=node_count, p=edge_probability),
        edge_probability,
    )


def generate_cliques(random_graph):
    maximal_cliques_iterator = nx.find_cliques(random_graph)
    maximal_cliques = list(maximal_cliques_iterator)
    return sorted(maximal_cliques, key=lambda item: len(item), reverse=True)


def save_statistics(sorted_cliques):
    if sorted_cliques:
        statistics_msgs = (
            f"Size of the largest maximal clique: {len(sorted_cliques[0])}"
        )
    else:
        statistics_msgs = statistics_msgs + "No maximal cliques were found."
    return statistics_msgs


def save_plot(random_graph, n_index, plot_caption):
    plt.figure(figsize=(10, 10))
    nx.draw(random_graph, node_size=10, width=0.1)
    plt.suptitle(plot_caption)
    plt.savefig(f"/app/cliques/my_plot_{n_index}.png")


if __name__ == "__main__":
    args = parser.parse_args()
    if args.nodes:
        total_nodes = args.nodes.split(",")
    else:
        total_nodes = ["100"]
    for n_index, node_count_str in enumerate(total_nodes):
        node_count = int(node_count_str)
        random_graph, edge_probability = generate_random_graph(node_count)
        plot_title = f"Generated an Erdős-Rényi graph with {node_count} nodes and edge probability {edge_probability}."
        logger.info(plot_title)
        sorted_cliques = generate_cliques(random_graph)
        clique_count_msg = f"Found {len(sorted_cliques)} cliques." + save_statistics(
            sorted_cliques
        )
        logger.info(clique_count_msg)
        plot_caption = "\n".join([plot_title, clique_count_msg])
        save_plot(random_graph, n_index, plot_caption)

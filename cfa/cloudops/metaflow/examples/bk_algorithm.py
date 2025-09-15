import argparse
import logging
import random
import string

import matplotlib.pyplot as plt
import networkx as nx


def generate_random_string(length):
    """Generates a random string of specified length using letters and digits."""
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.choice(characters) for _ in range(length))
    return random_string


SUFFIX = generate_random_string(5)

fh = logging.FileHandler(f"cliques/clique_app_{SUFFIX}.log")
fh.setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.addHandler(fh)

parser = argparse.ArgumentParser()
parser.add_argument(
    "-n",
    "--nodes",
    type=int,
    help="number of nodes",
)


def generate_random_graph(total_nodes):
    edge_probability = random.uniform(0.01, 0.99)
    return (
        nx.erdos_renyi_graph(n=total_nodes, p=edge_probability),
        edge_probability,
    )


def generate_cliques(random_graph, edge_probability):
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


def save_plot(random_graph, plot_caption):
    plt.figure(figsize=(10, 10))
    nx.draw(random_graph, node_size=10, width=0.1)
    plt.suptitle(plot_caption)
    plt.savefig(f"cliques/my_plot_{SUFFIX}.png")


if __name__ == "__main__":
    args = parser.parse_args()
    total_nodes = args.nodes if args.nodes else 100
    random_graph, edge_probability = generate_random_graph(total_nodes)
    plot_title = f"Generated an Erdős-Rényi graph with {total_nodes} nodes and edge probability {edge_probability}."
    logger.info(plot_title)
    sorted_cliques = generate_cliques(random_graph, edge_probability)
    clique_count_msg = (
        f"Found {len(sorted_cliques)} maximal cliques."
        + save_statistics(sorted_cliques)
    )
    logger.info(clique_count_msg)
    plot_caption = "\n".join([plot_title, clique_count_msg])
    save_plot(random_graph, plot_caption)

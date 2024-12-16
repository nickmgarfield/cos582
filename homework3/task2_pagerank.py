from pyspark import SparkConf, SparkContext
import os.path


def spark_rdd_page_rank(
    graph_fp:str,
    context: SparkContext,
    k = 10
):

    # Input graph represents a directed edge from a vertex to another vertex
    # We can obtain the set of vertices and the number of neighbor nodes from these edges
    edges = context.textFile(graph_fp).map(lambda x: x.split()).map(lambda x: [int(dx) for dx in x])
    vertices = edges.flatMap(lambda vertex: vertex).distinct()
    neighbors = edges.map(lambda edge: (edge[0], 1)).reduceByKey(lambda a, b: a + b)

    # Add edges for nodes with no outgoing edges to every other node in the input graph
    collected_vertices = vertices.collect()
    dangling_nodes = vertices.subtract(neighbors.map(lambda x: x[0]))
    dangling_edges = dangling_nodes.flatMap(lambda node: [(node, v) for v in collected_vertices])
    edges = edges.union(dangling_edges)

    # Recompute neighbors and initialize each vertex to rank 1.
    neighbors = edges.map(lambda edge: (edge[0], 1)).reduceByKey(lambda a, b: a + b).collectAsMap()
    ranks = vertices.map(lambda v: (v, 1.0)) # (vertex, neighbor count, rank)

    # For each iteration:
    #   Each vertex (v0) contributes rank = rank(v0)/neighbors(v0) to its neighboring vertices [v1]
    #   Not all vertices will receive values for contributions using the groupByKey() method
    #   To fix this, we LeftOuterJoin with ranks which will produce a kv pair: (vertex, (rank_i-1, rank_i))
    #   Where we are only interested in keeping rank_i. 
    for _ in range(k):
        collected_ranks = ranks.collectAsMap()
        contributions = edges.map(lambda edge: (edge[1], collected_ranks[edge[0]] / neighbors[edge[0]]))
        contributions = contributions.groupByKey().mapValues(sum).mapValues(lambda contribution: 0.15 + 0.85*contribution)
        ranks = ranks.leftOuterJoin(contributions).mapValues(lambda x: x[1] if x[1] is not None else 0.15)

    n_vertices = vertices.count()
    ranks = ranks.mapValues(lambda rank: rank/n_vertices)
    return ranks


if __name__ == "__main__":
    conf = SparkConf().setAppName("PageRank")
    context = SparkContext(conf=conf)

    ranks = spark_rdd_page_rank(
        graph_fp = os.path.join(os.path.dirname(__file__), "page_rank_dataset.txt"),
        context = context
    )

    ranks.coalesce(1).map(lambda x: str(x[0]) + " " + str(x[1])).saveAsTextFile(
        path= os.path.join(os.path.dirname(__file__), "page_rank_dataset_ranks.txt")
    )

    context.stop()

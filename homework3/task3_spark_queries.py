from pyspark.sql import SparkSession
import os.path

if __name__ == "__main__":

    session = SparkSession.builder.appName("Task3").getOrCreate()

    #a) Spark data frame from the ranks RDD
    rank_fp = os.path.join(os.path.dirname(__file__), "page_ranks.txt")
    ranks = session.sparkContext.textFile(rank_fp).map(lambda x: x.split()).map(lambda x: (int(x[0]), float(x[1])))
    print("Rank DataFrame:")
    rank_df = ranks.toDF(["vertex", "rank"])
    rank_df.createOrReplaceTempView("rank_df")
    rank_df.show()

    #b) Spark sql query to find page rank of vertex 2
    print("\nRank of Vertex 2:")
    vertex_2_rank = session.sql("""
        SELECT vertex, rank
        FROM rank_df
        WHERE vertex = 2
    """)
    vertex_2_rank.show()


    #c) Spark sql query to find highest page rank
    print("\nVertex with Maximum Page Rank:")
    max_rank_df = session.sql("""
        SELECT vertex, rank
        FROM rank_df
        WHERE rank = (SELECT MAX(rank) FROM rank_df)
    """)
    max_rank_df.show()


    #d) Create a dataframe from the vertex meanings dataset
    print("\nVertex Meanings DataFrame:")
    vertex_meanings_fp = os.path.join(os.path.dirname(__file__), "page_vertices.txt")
    vertex_meanings = session.sparkContext.textFile(vertex_meanings_fp).map(lambda x: x.split()).map(lambda x: (int(x[0]), str(x[1])))
    vertex_meanings_df = vertex_meanings.toDF(["vertex", "person"])
    vertex_meanings_df.createOrReplaceTempView("vertex_meanings_df")
    vertex_meanings_df.show()


    #e) SQL query to join vertex_meanings and rank dataframes
    print("\Joined Ranks and Meanings DataFrame:")
    joined_df = session.sql("""
        SELECT m.vertex, m.person, r.rank
        FROM vertex_meanings_df m
        JOIN rank_df r ON m.vertex = r.vertex
    """)
    joined_df.show()
    joined_df.coalesce(1).write.csv(os.path.join(os.path.dirname(__file__), "joined_page_ranks.csv"), header=True, mode="overwrite")
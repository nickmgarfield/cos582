import psycopg2
import csv

def find_best_movies_in_years(
    connection,
    table_name,
    start_year,
    end_year,
    limit,
    file = "best_movies_in_years.csv"
):
    
    cursor = connection.cursor()
    
    # The sql query uses a subquery containing a window function to assign a unique number to each row within a partition for a given year based on the 'rank' attribute, stored as 'row_num'.
    # A lower 'row_num' is associated with a higher ranking movie for the year (the partition)
    # NULL values for 'rank' are ignored
    # 
    # The main query takes the subquery table and filters by a given limit for row_num, which is a proxy for the rank of a movie in integer form from best (1) to worst for each year. 
    # # This yields the top movies for a given range of years
    
    try:
        cursor.execute(
            f"""
SELECT id, name, year, rank
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY year ORDER BY rank DESC) AS row_num
    FROM {table_name}
    WHERE year BETWEEN {start_year} AND {end_year}
    AND rank IS NOT NULL
    ORDER BY row_num DESC
) AS best_{table_name}_in_years
WHERE row_num <= {limit}
ORDER BY year ASC, rank DESC;
            """
        )

    except Exception as exc:
        connection.rollback()
        print(type(exc))
        print(exc)
        return
    
    data = cursor.fetchall()
    
    with open(file, "w", newline='') as file:
        writer = csv.writer(file, delimiter=";")
        writer.writerow(("id", "name", "year", "rank"))
        writer.writerows(data)
        
    cursor.close()
    connection.close()
    
    
if __name__ == "__main__":
    
    
    connection = psycopg2.connect(
        database="moviesdb", user="postgres", password="password"
    )
    
    find_best_movies_in_years(
        connection,
        "Movie",
        1995,
        2004,
        20,
        "top_20_movies_95-04.csv"
    )
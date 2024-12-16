## COS 582 12/14/24
## Homework 3 Part 1
## Nick Garfield 
## nicholas.garfield@maine.edu

import pymongo
import os.path 
import re
import pandas as pd

# 
# Raw data is preprocessed with pandas to aggregate Movie, Actor, Director relationships quickly
# Initially a pure MongoDb approach was attempted that queried movie documents to insert actors and directors.
# This proved to be unrealistically slow. Precomputing these relationships with pandas made it feasible to
# connect all of the movie, actor, director documents by identifiers. Ideally, they would have been connected by ObjectId for faster access. 
# That appraoch would likely be feasible for a database where entries are added over time and the computation expense of query + update is more manageable.
#

IMDB_DIR = os.path.join(os.path.dirname(__file__), "IMDB")

with open(os.path.join(IMDB_DIR, "IMDBMovie.txt"), "r", encoding="latin-1") as file:
    movie_data = [re.findall(r"^(\d+),(.*),(\d+),(\d+(?:\.\d+)?)?$", line)[0] for line in file.readlines()[1:]]

movie_data = pd.DataFrame(data = movie_data, columns=["id","name","year","rank"])
movie_data["id"] = movie_data["id"].astype('int32')
movie_data = movie_data.set_index('id')


with open(os.path.join(IMDB_DIR, "IMDBDirectors.txt"), "r", encoding="latin-1") as file:
     director_data = [re.findall(r"^(\d+),(.*),(.*)?$", line)[0] for line in file.readlines()[1:]]
director_data = pd.DataFrame(data = director_data, columns= ["id", "fname", "lname"])
director_data["id"] = director_data["id"].astype('int32')
director_data = director_data.set_index('id')


with open(os.path.join(IMDB_DIR, "IMDBPerson.txt"), "r", encoding="latin-1") as file:
    person_data = [re.findall(r"^(\d+),(.*),(.*),(.*)?$", line)[0] for line in file.readlines()[1:]]
person_data = pd.DataFrame(data = person_data, columns= ["id", "fname", "lname", "gender"])
person_data['id'] = person_data["id"].astype('int32')
person_data = person_data.set_index('id')


#
# Director and movie relationships
#
with open(os.path.join(IMDB_DIR, "IMDBMovie_Directors.txt"), "r", encoding="latin-1") as file:
    movie_directors = [re.findall(r"^(\d+),(\d+)", line)[0] for line in file.readlines()[1:]]
movie_directors = pd.DataFrame(movie_directors, columns = ["did", "mid"])
movie_directors["did"] = movie_directors["did"].astype('int32')
movie_directors["mid"] = movie_directors["mid"].astype('int32')

# Aggregate the movies that each director has directed
director_movies = movie_directors.groupby(['did']).agg({"mid": list})
director_movies = director_movies.rename_axis('id')
director_data = director_movies.join(director_data)

# Aggregate the directors for each movie
movie_directors = movie_directors.groupby(['mid']).agg({"did": list})
movie_directors = movie_directors.rename_axis('id')
movie_data = movie_data.join(movie_directors)

# 
# Movie and Cast member relationships
#
with open(os.path.join(IMDB_DIR, "IMDBCast.txt"), "r", encoding="latin-1") as file:
    cast_data = [re.findall(r"^(\d+),(\d+),(.*)?$", line)[0] for line in file.readlines()[1:]]
cast_data = pd.DataFrame(cast_data, columns=["pid","mid","role"])
cast_data["pid"] = cast_data["pid"].astype('int32')
cast_data["mid"] = cast_data["mid"].astype('int32')
cast_data["role"] = cast_data["role"].str.strip("[]")

# Aggregate the actors and roles for each movie
movie_casts = cast_data.groupby(["mid"]).agg({'pid': list, "role": list})
movie_casts = movie_casts.rename_axis('id')
movie_data = movie_data.join(movie_casts)


# Aggregate the movies that each actor was in
cast_movies = cast_data.groupby(["pid"]).agg({'mid': list, "role": list})
cast_movies = cast_movies.rename_axis('id')
person_data = person_data.join(cast_movies)


print("Director DataFrame")
print(director_data)
print("Person DataFrame")
print(person_data)
print("Movie DataFrame")
print(movie_data)


#
# MongoDb database creation
#

client = pymongo.MongoClient(host="mongodb://localhost:27017/")
db = client["moviesdb"] 
COLLECTION = db["movies"]

print(movie_data.columns.to_list())
print(f"Inserting Movie Documents to collection {COLLECTION.name} in database {db.name}")
for id, row in movie_data.iterrows():
    try:
        movie_document = {
                "doc_type": "movie",
                "name": str(row["name"]),
                "year": int(row["year"]),
                "rank": float(row["rank"]) if row["rank"] else None,
                "movie_id": id,
                "director_ids": row["did"], 
                "actor_ids": row["pid"],
                "actor_roles": row["role"]
            }
        COLLECTION.insert_one(
            movie_document
        )
    except:
        print(f"Error inserting movie: {movie_document}")

print(f"Inserting Director Documents to collection {COLLECTION.name} in database {db.name}")
for id, row in director_data.iterrows():
    try:
        director_document = {
                "doc_type": "director",
                "first_name": row["fname"],
                "last_name": row["lname"],
                "director_id": id,
                "movie_ids": row["mid"]
            }
        COLLECTION.insert_one(
            director_document
        )
    except:
        print(f"Error inserting director: {director_document}")

print(f"Inserting Person Documents to collection {COLLECTION.name} in database {db.name}")
for id, row in person_data.iterrows():
    try:
        person_document = {
                "doc_type": "person",
                "first_name": row["fname"],
                "last_name": row["lname"],
                "gender": row["gender"],
                "person_id": id,
                "movie_ids": row["mid"],
                "movie_roles": row["role"]
            }
        COLLECTION.insert_one(
            person_document
        )
    except:
        print(f"Error inserting person: {person_document}")

# Shrek 2001 query
shrek_movie = COLLECTION.find_one({"name": "Shrek (2001)", "doc_type": "movie"})
print("'Shrek (2001) Document:")
print(shrek_movie)

print("Director(s) of 'Shrek (2001)")
query = {"director_id": {"$in": shrek_movie['director_ids']}}
directors = list(COLLECTION.find(query))
for i, director in enumerate(directors): 
    print(f'\t{director["first_name"]} {director["last_name"]}')

print("Actors in 'Shrek (2001)'")
query = {"person_id": {"$in": shrek_movie['actor_ids']}}
actors = list(COLLECTION.find(query))
for i, actor in enumerate(actors): 
    print(f'\t{actor["first_name"]} {actor["last_name"]}')

client.close()
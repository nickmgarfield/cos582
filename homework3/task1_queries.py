import pymongo
import json

client = pymongo.MongoClient(host="mongodb://localhost:27017/")
db = client["moviesdb"] 
COLLECTION = db["movies"]

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

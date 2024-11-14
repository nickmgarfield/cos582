import psycopg2
import os.path

connection = psycopg2.connect(
    database="moviesdb", user="postgres", password="password"
)
print(connection.info)

cursor = connection.cursor()

try:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Movie(
            id INT PRIMARY KEY, 
            name VARCHAR(500), 
            year INT, 
            rank REAL
        )
        """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS Person (
        id INT PRIMARY KEY, 
        fname VARCHAR(500), 
        lname VARCHAR(500), 
        gender VARCHAR(50)
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS ActsIn (
        pid INT,
        mid INT, 
        role VARCHAR(500),
        PRIMARY KEY (pid, mid), 
        FOREIGN KEY (pid) REFERENCES Person(id),
        FOREIGN KEY (mid) REFERENCES Movie(id)
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS Director (
        id INT PRIMARY KEY, 
        fname VARCHAR(500), 
        lname VARCHAR(500)
    )
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Directs (
            did INT, 
            mid INT, 
            PRIMARY KEY (did, mid), 
            FOREIGN KEY (did) REFERENCES Director(id),
            FOREIGN KEY (mid) REFERENCES Movie(id)
        )"""
    )

    connection.commit()

except:
    connection.rollback()
    
    
#
# Fill in the tables
#
data_dir = os.path.join(os.path.dirname(__file__), "IMDB")
import re

print("Inserting Values to Movie Table")
# The text file data is incomplete in that not every row has a value for rank
# Simple csv parsing will not work for titles containing commas
# We can use a regular expression to easily read the row using capturing groups:
# Group 1: id. Assumes an integer is present (\d+)
# Group 2: title. Title can be any set of characters (.*)
# Group 3: year. Assumes the year is an integer (\d+)
# Group 4: rank. Optional group of a real number or integer positioned as the end of the line(\d+(?:\.\d+)?)?$. This end of line assertion ensures that we don't accidentally split up titles with commas. 
with open(os.path.join(data_dir, "IMDBMovie.txt"), "r", encoding="latin-1") as file:
    movie_data = [re.findall(r"^(\d+),(.*),(\d+),(\d+(?:\.\d+)?)?$", line)[0] for line in file.readlines()[1:]]
    movie_data = [[row[0], row[1].replace("'", '"'), row[2], row[3] if row[3] else 'NULL'] for row in movie_data]
try:
    cursor.execute(
f'''
        INSERT INTO Movie(id,name,year,rank)
        VALUES
        ''' + ",".join(
            f"({row[0]}, '{row[1]}', {row[2]}, {row[3]})\n" for row in movie_data
        )
    )
    connection.commit()
except Exception as exc:
    print(exc)
    connection.rollback()
    
print("Inserting Values to Person Table")
# We will use a similar approach for people, except the 2nd through 4th capturing groups will capture any text
# This appraoch was used to ensure that four values are always present for each row
with open(os.path.join(data_dir, "IMDBPerson.txt"), "r", encoding="latin-1") as file:
    person_data = [re.findall(r"^(\d+),(.*),(.*),(.*)?$", line)[0] for line in file.readlines()[1:]]

person_data = [[row[0]] + [col.replace("'", '"') if col else None for col in row[1:]] for row in person_data]

try:
    cursor.execute(
f'''
        INSERT INTO Person(id,fname,lname,gender)
        VALUES
        ''' + ",".join(
            f"({row[0]}, '{row[1]}', '{row[2]}', '{row[3]}')\n" for row in person_data
        )
    )
    connection.commit()
except Exception as exc:
    print(exc)
    connection.rollback()

print("Inserting Values to Director Table")
# Although not necessary to utilize regular expressions to parse this file, regular expressions were used for consistency
with open(os.path.join(data_dir, "IMDBDirectors.txt"), "r", encoding="latin-1") as file:
    directors_data = [re.findall(r"^(\d+),(.*),(.*)?$", line)[0] for line in file.readlines()[1:]]

directors_data = [[row[0]] + [col.replace("'", '"') if col else None for col in row[1:]] for row in directors_data]

try:
    cursor.execute(
f'''
        INSERT INTO Director(id,fname,lname)
        VALUES
        ''' + ",".join(
            f"({row[0]}, '{row[1]}', '{row[2]}')\n" for row in directors_data
        )
    )
    connection.commit()
except Exception as exc:
    print(exc)
    connection.rollback()


print("Inserting Values to Directs Table")
#Although not necessary to utilize regular expressions to parse this file, regular expressions were used for consistency
with open(os.path.join(data_dir, "IMDBMovie_Directors.txt"), "r", encoding="latin-1") as file:
    movie_directors = [re.findall(r"^(\d+),(\d+)", line)[0] for line in file.readlines()[1:]]

# # By using sets, we can prefilter duplicates that will violate the unique key constraint
movie_directors = set(movie_directors)
for row in movie_directors:
    
    try:
        cursor.execute(
        f'''
            INSERT INTO Directs(did,mid)
            VALUES
            ({row[0]}, {row[1]})
        ''')
        connection.commit()
    
    # Exception case for when the director or movie id is not present in Director or Movie tables
    except psycopg2.errors.ForeignKeyViolation as exc:
        print(f'ForeignKeyViolation: Ignoring (did, mid) = ({row[0]}, {row[1]}) and not inserting to Directs dataset')
        connection.rollback()
        continue
    
    # Ignore data that isn't unique
    except psycopg2.errors.UniqueViolation:
        connection.rollback()
        continue
    
    except Exception as exc:
        connection.rollback()
        print(type(exc))
        print(exc)
        raise exc


print("Inserting Values to ActsIn Table")
with open(os.path.join(data_dir, "IMDBCast.txt"), "r", encoding="latin-1") as file:
    cast_data = [re.findall(r"^(\d+),(\d+),(.*)?$", line)[0] for line in file.readlines()[1:]]

cast_data = [[row[0], row[1]] + ["'" + row[2].replace("'", '"') + "'" if row[2] else "NULL"] for row in cast_data]
for i in range(0,len(cast_data), 100):
    
    try:
        cursor.execute(
        f'''
            INSERT INTO ActsIn(pid,mid,role)
            VALUES
        ''' + ",".join(
             f"({row[0]}, {row[1]}, {row[2]})\n" for row in cast_data[i:i+100]
         )
        )
        connection.commit()
    
    except Exception as exc:
        connection.rollback()
        print(type(exc))
        print(exc)
        raise exc

cursor.close()
connection.close()

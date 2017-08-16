CREATE TABLE movies 
(movie_id INT, movie_name STRING, genre ARRAY<STRING>) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
COLLECTION ITEMS TERMINATED BY '|';

CREATE TABLE ratings 
(user_id INT, movie_id INT, rating FLOAT, time TIMESTAMP) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#';

LOAD DATA local INPATH '/usr/local/JarFiles/movies.dat'
OVERWRITE INTO TABLE movies;

LOAD DATA local INPATH '/usr/local/JarFiles/ratings.dat'
OVERWRITE INTO TABLE ratings;

CREATE VIEW movies_view AS
SELECT movie_id, movie_name, genre 
FROM movies LATERAL VIEW explode(genre) genreTable AS movie_genre;

INSERT OVERWRITE LOCAL DIRECTORY '/usr/local/JarFiles/Hive1_Out/'
SELECT a.user_id, b.genre, AVG(a.rating) AS avg_rating FROM ratings a 
JOIN movies_view b
ON(a.movie_id = b.movie_id)
GROUP BY a.user_id, b.genre
ORDER BY a.user_id, avg_rating DESC;

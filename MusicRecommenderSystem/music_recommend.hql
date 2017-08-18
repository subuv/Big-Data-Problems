CREATE TABLE User_artist_data (userid INT, artistid INT, playcount INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ';

CREATE TABLE Artist_data (artistid INT, artistName STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE Artist_alias (artistid INT, actualID INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA local INPATH '/usr/local/JarFiles/profiledata_06-May-2005/user_artist_data.txt' OVERWRITE INTO TABLE User_artist_data;

LOAD DATA local INPATH '/usr/local/JarFiles/profiledata_06-May-2005/artist_data.txt' OVERWRITE INTO TABLE Artist_data;

LOAD DATA local INPATH '/usr/local/JarFiles/profiledata_06-May-2005/artist_alias.txt' OVERWRITE INTO TABLE Artist_alias;

CREATE VIEW tempVIew AS
SELECT A.userid, B.actualID, A.playcount FROM User_artist_data a 
JOIN  Artist_Alias b
ON (a.artistid = b.artistid) 
ORDER BY A.userid;

INSERT OVERWRITE LOCAL DIRECTORY '/usr/local/JarFiles/' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
select * from tempView;

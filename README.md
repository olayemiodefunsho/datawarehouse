Sparkify is a music streaming company. This project about creating a data wharehouse for Sparkify so that analysis can be done on the data they have generated about their songs and how people play them.

Everything done in the project is pretty straight forward except for the design strategy used for the wharehouse tables and the insert statement used to populate them so I'll take my time to explain that here.


USERS TABLE

INSERT INTO users (u_user_id,u_first_name,u_last_name,u_gender,u_level)
WITH t1 AS
    (SELECT userid,firstname,lastname,gender,level,
    ROW_NUMBER() OVER(PARTITION BY userid ORDER BY dateadd(second, ts, '1970-01-01 00:00:00') desc) AS row_number
    FROM log_data where userid is not null)
SELECT userid,firstname,lastname,gender,level
FROM t1
WHERE t1.row_number = 1

CREATE TABLE users 
(
  u_user_id     INTEGER NOT NULL SORTKEY,
  u_first_name  VARCHAR(200) NOT NULL,
  u_last_name   VARCHAR(200) NOT NULL,
  u_gender      CHAR(1) NOT NULL,
  u_level       VARCHAR(20) NOT NULL
)
DISTSTYLE ALL;


Some userids in the log_data table were null and that had to be eliminated. The log_data table is from a log file so a particular userid exists on several rows as expected. I needed to get distinct userids and that is pretty straight forward except that I wanted the lastest occurence of a particular userid, so that I can get the user's level as at the last transaction in the log_data table. That is the essense of the "ROW_NUMBER() OVER(PARTITION BY userid ORDER BY dateadd" statement. This helps to other each userid group  by the date so that I can later pick rownumber 1 for each userid group. Finally the dateadd function had to be used to covert from timestamp which I stored with BIGINT into actual datetime.

The u_user_id had to be the sort key of course, though that is debatable since I'm using a DISTSTYLE ALL to ensure a full copy of the table would be created in all partitions of the cluster. This is possible because the user table is expected to be a small table, at the moment all distinct users in the dataset is just 97. I gathered that if the dataset does not run into millions there is nothing to be worried about while using the DISTSTYLE ALL



SONGS TABLE

INSERT INTO songs (s_song_id, s_title, s_artist_id, s_year, s_duration)
SELECT song_id, title, artist_id, year, duration
FROM song_data

CREATE TABLE songs 
(
  s_song_id     VARCHAR(200) NOT NULL SORTKEY,
  s_title       VARCHAR(200) NOT NULL,
  s_artist_id   VARCHAR(200) NOT NULL,
  s_year        INTEGER NOT NULL,
  s_duration    DOUBLE PRECISION NOT NULL
)
DISTSTYLE ALL;

The songs table is pretty easy, the song_ids in song_data are already distinct.
DISTSTYLE ALL was also used to have a copy in all partitions of the cluster. here are over 14,000 records here, but that is still not an issue.



ARTISTS TABLE

INSERT INTO artists (a_artist_id,a_name,a_location,a_lattitude,a_longitude)
WITH t1 AS (
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude, 
    ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY len(artist_name)) AS row_number
    FROM song_data)
SELECT t1.artist_id, t1.artist_name, t1.artist_location, t1.artist_latitude, t1.artist_longitude
FROM t1
WHERE t1.row_number = 1

CREATE TABLE artists 
(
  a_artist_id  VARCHAR(200) NOT NULL SORTKEY,
  a_name       VARCHAR(200) NOT NULL,
  a_location   VARCHAR(200),
  a_lattitude  DOUBLE PRECISION,
  a_longitude  DOUBLE PRECISION
)
DISTSTYLE ALL;

The major challenge I faced with getting arists data out of the song_data table was that I realised a single artist_id could belong to the artist himself and all other songs where he collborated with other artists. So for example artist_id "ASDRDEF456DFF" could belong to "beyonce knowles" and the same artist_id "ASDRDEF456DFF" could also be used for another song by her but this time in a collaboration like "beyonce knowles / JayZ". This became a challenge. I had to choose to select only the artist_ids with the shorter artist_name and thats why I have "ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY len(artist_name)) AS row_number"



TIME TABLE
 
INSERT INTO time (t_start_time,t_hour,t_day,t_week,t_month ,t_year ,t_weekday)
SELECT 
DATEADD(second, ts, '1970-01-01 00:00:00'), 
DATEPART(HOUR, DATEADD(SECOND, ts, '1970-01-01 00:00:00')),  
DATEPART(DAY, DATEADD(SECOND, ts, '1970-01-01 00:00:00')),  
DATEPART(WEEK, DATEADD(SECOND, ts, '1970-01-01 00:00:00')),  
DATEPART(MONTH, DATEADD(SECOND, ts, '1970-01-01 00:00:00')),  
DATEPART(YEAR, DATEADD(SECOND, ts, '1970-01-01 00:00:00')),
DATEPART(WEEKDAY, DATEADD(SECOND, ts, '1970-01-01 00:00:00'))  
FROM log_data
GROUP BY ts
ORDER BY ts

CREATE TABLE time 
(
    t_start_time DATETIME NOT NULL SORTKEY, 
    t_hour INTEGER NOT NULL, 
    t_day INTEGER NOT NULL, 
    t_week INTEGER NOT NULL, 
    t_month INTEGER NOT NULL, 
    t_year INTEGER NOT NULL , 
    t_weekday INTEGER NOT NULL
)
DISTSTYLE ALL;

All I had to do here is to get the distinct dates and I used Groupby for that, but since the date is in Timesamp i had to use dateadd to first convert to datetime after which I used datepart to get the parts of the date that i needed.



SONGPLAYS TABLE

INSERT INTO songplays (
    sp_songplay_id,sp_start_time,sp_user_id,sp_level,
    sp_song_id,sp_artist_id,sp_session_id,sp_location,sp_user_agent
)
SELECT 
    CAST(CAST(sessionid AS VARCHAR) + CAST(iteminsession AS VARCHAR) AS INTEGER),
    DATEADD(SECOND, ts, '1970-01-01 00:00:00'), 
    userid,
    (SELECT TOP 1 u_level FROM users WHERE u_user_id = userid),
    (SELECT TOP 1 s_song_id FROM songs WHERE s_title = song),
    (SELECT TOP 1 s_artist_id FROM songs WHERE s_title = song),
    sessionid,location,useragent
FROM log_data 
WHERE page = 'NextSong' AND song IN (SELECT s_title FROM songs)

CREATE TABLE songplays 
(
    sp_songplay_id INTEGER NOT NULL SORTKEY, 
    sp_start_time DATETIME NOT NULL, 
    sp_user_id INTEGER NOT NULL, 
    sp_level VARCHAR(20) NOT NULL, 
    sp_song_id VARCHAR(20) NOT NULL, 
    sp_artist_id VARCHAR(20) NOT NULL, 
    sp_session_id INTEGER NOT NULL, 
    sp_location VARCHAR(200) NOT NULL, 
    sp_user_agent VARCHAR(200)
)
DISTSTYLE EVEN;

For the songs plays the first thing I did was to combine this sessionid and iteminsessionid from the log_data table to form the primary key or distinct attribute of the table. The combination of these two will always be distinct. I had to use in line queries to get some of the columns. I had to use the userid to get the latest level from the users table, since I had already done that before. I had to use just the songs table to fetch both song_id and artist_id just to be double sure that I am fetching from the same song as it is in the songs table. Finally I realised very few songs in the log_data table actually match song titles from the song table. I thought there was no point having any record in the songplays table where I will not be able to use a join to get which song it is from the songs table, so I decided only recoords with title match in the song table will make it into the songplay table and that why I have " AND song IN (SELECT s_title FROM songs)". Of course only records with page = 'NextSong' are needed according to the project intruction.

Since all the dimensions tables are copied to all partitions in the cluster, I could easily use DISTSTYLE EVEN to spread records of the fact table to all the clusters. Queries will still be optimal because any data needed from the dimension tables will be available right there is the cluster.
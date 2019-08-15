import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
ARN = config.get('IAM_ROLE','ARN')


staging_songs_copy = """
    copy song_data from {}
    credentials 'aws_iam_role={}'
    format as json 'auto';
""".format(SONG_DATA,ARN)



# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS log_data;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_data;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE log_data (
    artist VARCHAR(200),
    auth VARCHAR(200) NOT NULL,
    firstname VARCHAR(200),
    gender CHAR(1),
    iteminsession INTEGER,
    lastname VARCHAR(200),
    length double precision,
    level VARCHAR(20) NOT NULL,
    location VARCHAR(200),
    method VARCHAR(20) NOT NULL,
    page VARCHAR(20) NOT NULL,
    registration double precision,
    sessionid INTEGER,
    song VARCHAR(200),
    status INTEGER NOT NULL,
    ts BIGINT NOT NULL,
    useragent VARCHAR(200),
    userid INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE song_data (
    artist_id VARCHAR(20) NOT NULL,
    artist_latitude DOUBLE PRECISION,
    artist_location VARCHAR(200),
    artist_longitude DOUBLE PRECISION,
    artist_name VARCHAR(200) NOT NULL,
    duration DOUBLE PRECISION NOT NULL,
    num_songs INTEGER NOT NULL,
    song_id VARCHAR(20) NOT NULL,
    title VARCHAR(200) NOT NULL,
    year INTEGER NOT NULL
);
""")

songplay_table_create = ("""
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
""")

user_table_create = ("""
CREATE TABLE users 
(
  u_user_id     INTEGER NOT NULL SORTKEY,
  u_first_name  VARCHAR(200) NOT NULL,
  u_last_name   VARCHAR(200) NOT NULL,
  u_gender      CHAR(1) NOT NULL,
  u_level       VARCHAR(20) NOT NULL
)
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE songs 
(
  s_song_id     VARCHAR(200) NOT NULL SORTKEY,
  s_title       VARCHAR(200) NOT NULL,
  s_artist_id   VARCHAR(200) NOT NULL,
  s_year        INTEGER NOT NULL,
  s_duration    DOUBLE PRECISION NOT NULL
)
DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE artists 
(
  a_artist_id  VARCHAR(200) NOT NULL SORTKEY,
  a_name       VARCHAR(200) NOT NULL,
  a_location   VARCHAR(200),
  a_lattitude  DOUBLE PRECISION,
  a_longitude  DOUBLE PRECISION
)
DISTSTYLE ALL;
""")

time_table_create = ("""
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
""")

# STAGING TABLES

staging_events_copy = ("""
    copy log_data from {}
    credentials 'aws_iam_role={}'
    json {};
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = """
    copy song_data from {}
    credentials 'aws_iam_role={}'
    format as json 'auto';
""".format(SONG_DATA,ARN)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    sp_songplay_id,
    sp_start_time,
    sp_user_id,
    sp_level,
    sp_song_id,
    sp_artist_id,
    sp_session_id,
    sp_location,
    sp_user_agent
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
""")

user_table_insert = ("""
INSERT INTO users (u_user_id,u_first_name,u_last_name,u_gender,u_level)
WITH t1 AS
    (SELECT userid,firstname,lastname,gender,level,
    ROW_NUMBER() OVER(PARTITION BY userid ORDER BY dateadd(second, ts, '1970-01-01 00:00:00') desc) AS row_number
    FROM log_data where userid is not null)
SELECT userid,firstname,lastname,gender,level
FROM t1
WHERE t1.row_number = 1
""")

song_table_insert = ("""
INSERT INTO songs (s_song_id, s_title, s_artist_id, s_year, s_duration)
SELECT song_id, title, artist_id, year, duration
FROM song_data
""")

artist_table_insert = ("""
INSERT INTO artists (a_artist_id,a_name,a_location,a_lattitude,a_longitude)
WITH t1 AS (
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude, 
    ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY len(artist_name)) AS row_number
    FROM song_data)
SELECT t1.artist_id, t1.artist_name, t1.artist_location, t1.artist_latitude, t1.artist_longitude
FROM t1
WHERE t1.row_number = 1
""")

time_table_insert = ("""
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
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]

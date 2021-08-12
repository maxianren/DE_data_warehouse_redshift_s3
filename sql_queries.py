import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table
    (artist text,
    auth text,
    firstName text,
    gender text,
    itemInSession int,
    lastName text,
    length numeric,
    level text,
    location text,
    method text,
    page text,
    registration bigint,
    sessionId int,
    song text,
    status int,
    ts bigint,
    userAgent text,
    userId int
    )

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table
    (num_songs int, 
    artist_id text, 
    artist_latitude numeric, 
    artist_longitude numeric, 
    artist_location text, 
    artist_name text, 
    song_id text, 
    title text, 
    duration numeric, 
    year int
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table 
    (songplay_id int IDENTITY(0,1), 
    start_time timestamp NOT NULL SORTKEY, 
    user_id text NOT NULL, 
    level text NOT NULL DISTKEY, 
    song_id text NOT NULL, 
    artist_id text NOT NULL, 
    session_id int NOT NULL, 
    location text, 
    user_agent text)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table 
    (user_id int PRIMARY KEY, 
    first_name text, 
    last_name text, 
    gender text, 
    level text)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table 
    (song_id text PRIMARY KEY, 
    title text, 
    artist_id text, 
    year int, 
    duration numeric)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table 
    (artist_id text PRIMARY KEY, 
    name text, 
    location text, 
    latitude numeric, 
    longitude numeric)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table 
    (start_time timestamp PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events_table
from {}
iam_role {}
COMPUPDATE OFF 
region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs_table
from {}
iam_role {}
COMPUPDATE OFF 
region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent)
SELECT 
    timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events_table se 
    JOIN staging_songs_table ss
    ON se.artist=ss.artist_name
        AND se.song=ss.title
        AND se.length=ss.duration
WHERE se.page='NextSong'
""")

user_table_insert = ("""
INSERT INTO user_table (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level)
SELECT 
    se.userId, 
    se.firstName, 
    se.lastName,
    se.gender,
    se.level
FROM staging_events_table se
WHERE se.page='NextSong'
""")

song_table_insert = ("""
INSERT INTO song_table (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration)
SELECT 
    song_id, 
    title, 
    artist_id,
    year,
    duration
FROM staging_songs_table
""")

artist_table_insert = ("""
INSERT INTO artist_table (
    artist_id, 
    name, 
    location, 
    latitude , 
    longitude) 
SELECT 
    artist_id, 
    artist_name, 
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs_table 
""")

time_table_insert = ("""
INSERT INTO time_table (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday)
SELECT
    dt.start_time,
    EXTRACT(HOUR FROM dt.start_time),
    EXTRACT(DAY FROM dt.start_time),
    EXTRACT(WEEK FROM dt.start_time),
    EXTRACT(MONTH FROM dt.start_time),
    EXTRACT(YEAR FROM dt.start_time),
    EXTRACT(WEEKDAY FROM dt.start_time)
FROM (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_events_table) dt
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

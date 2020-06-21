import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
(
  artist           VARCHAR(255),
  auth             VARCHAR(255),
  firstName        VARCHAR(255),
  gender           VARCHAR(1),
  itemInSession    BIGINT,
  lastName         VARCHAR(255),
  length           FLOAT,
  level            VARCHAR(255),
  location         VARCHAR(255),
  method           VARCHAR(255),
  page             VARCHAR(255),
  registration     FLOAT,
  sessionId        BIGINT,
  song             VARCHAR(255),
  status           BIGINT,
  ts               BIGINT,
  userAgent        VARCHAR(4096),
  userId           BIGINT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
  num_songs        BIGINT,
  artist_id        VARCHAR(255),
  artist_latitude  FLOAT,
  artist_longitude FLOAT,
  artist_location  VARCHAR(255),
  artist_name      VARCHAR(255),
  song_id          VARCHAR(255),
  title            VARCHAR(255),
  duration         FLOAT,
  year             BIGINT
""")

songplay_table_create = ("""
CREATE TABLE songplays
(
  songplay_id      BIGINT IDENTITY(0,1) PRIMARY KEY ,
  start_time       TIMESTAMP NOT NULL,
  user_id          BIGINT NOT NULL,
  level            VARCHAR(32),
  song_id          VARCHAR(64) NOT NULL ,
  artist_id        VARCHAR(64) NOT NULL,
  session_id       BIGINT,
  location         VARCHAR(255),
  user_agent       VARCHAR(4096) 
);
""")

user_table_create = ("""
CREATE TABLE users
(
  user_id          BIGINT NOT NULL PRIMARY KEY,
  first_name       VARCHAR(128),
  last_name        VARCHAR(128),
  gender           VARCHAR(1),
  level            VARCHAR(32)
);
""")

song_table_create = ("""
CREATE TABLE songs
(
  song_id          VARCHAR(64) NOT NULL PRIMARY KEY,
  title            VARCHAR(255) NOT NULL,
  artist_id        VARCHAR(64) NOT NULL,
  year             BIGINT,
  duration         FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE artists
(
  artist_id        VARCHAR(64) NOT NULL PRIMARY KEY,
  name             VARCHAR(255) NOT NULL,
  location         VARCHAR(255),
  latitude         FLOAT,
  longitude        FLOAT
);
""")

time_table_create = ("""
CREATE TABLE time
(
  start_time       DATETIME NOT NULL PRIMARY KEY,
  hour             BIGINT NOT NULL,
  day              BIGINT NOT NULL,
  week             BIGINT NOT NULL,
  month            BIGINT NOT NULL,
  year             BIGINT NOT NULL,
  weekday          BIGINT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
json {}
region {}
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'),config.get('REGION','REGION'))

staging_songs_copy = ("""
COPY staging_events
FROM {}
iam_role {}
json {}
region {}
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'),config.get('AUTO','AUTO'),config.get('REGION','REGION'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
e.ts,
e.userid,
e.level,
s.song_id,
s.artist_id,
e.sessionid,
e.location,
e.useragent AS user_agent
FROM staging_events e
LEFT JOIN staging_songs s ON e.artist = s.artist_name AND e.song = s.title
WHERE e.page = 'NextSong'
;
""")

user_table_insert = ("""
INSERT INTO users
(
SELECT DISTINCT
userid AS user_id,
firstname AS first_name,
lastname AS last_name,
gender,
level
FROM staging_events
WHERE userid IS NOT NULL
);
""")

song_table_insert = ("""
INSERT INTO songs
(
SELECT DISTINCT
song_id,
title,
artist_id,
CASE WHEN year = 0 OR year IS NULL THEN NULL ELSE year END AS year,
duration
FROM staging_songs
);
""")

artist_table_insert = ("""
INSERT INTO artists
(
SELECT DISTINCT
artist_id,
artist_name AS name,
artist_location AS location,
artist_latitude AS latitude,
artist_longitude AS longtitude
FROM staging_songs
);
""")

time_table_insert = ("""
INSERT INTO time
(
SELECT DISTINCT
(timestamp 'epoch' + ts/1000 * interval '1 second')::datetime AS start_time,
DATE_PART('hour', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS hour,
DATE_PART('day', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS day,
DATE_PART('week', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS week,
DATE_PART('month', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS week,
DATE_PART('year', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS year,
DATE_PART('weekday', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS weekday
FROM staging_events
WHERE page = 'NextSong'
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

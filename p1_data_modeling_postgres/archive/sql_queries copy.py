# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

## CREATE FACT TABLE
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    PRIMARY KEY (songplay_id),
    songplay_id  SERIAL,
    start_time   BIGINT      NOT NULL,
    user_id      INT         NOT NULL,
    level        VARCHAR     NOT NULL,
    song_id      INT         NOT NULL,
    artist_id    INT         NOT NULL,
    session_id   INT         NOT NULL,
    location     TEXT,
    user_agent   TEXT)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    PRIMARY KEY (user_id),
    user_id      INT         NOT NULL,
    first_name   VARCHAR,
    last_name    VARCHAR,
    gender       CHAR(1),
    level        VARCHAR     NOT NULL)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    PRIMARY KEY (song_id),
    song_id      VARCHAR     NOT NULL,
    title        VARCHAR     NOT NULL,
    artist_id    VARCHAR     NOT NULL,
    year         INT,
    duration     FLOAT)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    PRIMARY KEY (artist_id),
    artist_id    VARCHAR     NOT NULL,
    name         VARCHAR     NOT NULL,
    location     TEXT,
    lattitude    FLOAT,
    longitude    FLOAT)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    PRIMARY KEY (start_time),
    start_time   TIMESTAMP   NOT NULL,
    hour         INT,
    day          INT,
    week         INT,
    month        INT,
    year         INT,
    weekday      VARCHAR)
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, lattitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS


song_select = ("""SELECT s.song_id, a.artist_id FROM songs s, artists a
WHERE s.artist_id = a.artist_id
    AND s.title = %s
    AND a.name = %s
    AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

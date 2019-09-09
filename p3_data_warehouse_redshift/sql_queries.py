import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
         artist VARCHAR,
           auth VARCHAR,
      firstName VARCHAR,
         gender VARCHAR,
  iteminsession INTEGER,
       lastname VARCHAR,
         length DOUBLE PRECISION,
          level VARCHAR,
       location TEXT,
         method VARCHAR,
           page VARCHAR,
   registration BIGINT,
      sessionid BIGINT,
           song VARCHAR,
         status INTEGER,
             ts BIGINT,
      useragent TEXT,
         userid INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
           song_id VARCHAR,
         num_songs INTEGER,
             title VARCHAR,
          duration DOUBLE PRECISION,
              year INTEGER,
         artist_id VARCHAR,
       artist_name VARCHAR,
   artist_latitude DOUBLE PRECISION,
  artist_longitude DOUBLE PRECISION,
   artist_location VARCHAR
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
  songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY SORTKEY,
   start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
      user_id INTEGER NOT NULL REFERENCES users(user_id),
        level VARCHAR(50) NOT NULL,
      song_id VARCHAR(100) NOT NULL REFERENCES songs(song_id) DISTKEY,
    artist_id VARCHAR(50) NOT NULL REFERENCES artists(artist_id) DISTKEY,
   session_id BIGINT NOT NULL,
     location VARCHAR(255),
   user_agent VARCHAR(500)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
     user_id INTEGER NOT NULL PRIMARY KEY SORTKEY,
  first_name VARCHAR(255) NOT NULL,
   last_name VARCHAR(255) NOT NULL,
      gender VARCHAR(1),
       level VARCHAR(50) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(100) PRIMARY KEY DISTKEY,
      title VARCHAR(255) NOT NULL,
  artist_id VARCHAR(50) NOT NULL REFERENCES artists(artist_id),
       year INTEGER,
   duration DOUBLE PRECISION
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
  artist_id VARCHAR(50) PRIMARY KEY DISTKEY,
       name VARCHAR(255) NOT NULL,
   location VARCHAR(255),
  lattitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION
)
DISTSTYLE all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
  start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INTEGER,
         day INTEGER,
        week INTEGER,
       month INTEGER,
        year INTEGER,
     weekday INTEGER
)
DISTSTYLE all;
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

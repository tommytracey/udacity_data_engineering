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
         gender VARCHAR(1),
  iteminsession INTEGER,
       lastname VARCHAR,
         length DOUBLE PRECISION,
          level VARCHAR,
       location TEXT,
         method VARCHAR,
           page VARCHAR,
   registration BIGINT,
      sessionid INTEGER,
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
  songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY SORTKEY,
   start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
      user_id INTEGER NOT NULL REFERENCES users(user_id),
        level VARCHAR NOT NULL,
      song_id VARCHAR NOT NULL REFERENCES songs(song_id) DISTKEY,
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id) DISTKEY,
   session_id INTEGER NOT NULL,
     location VARCHAR,
   user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
     user_id INTEGER NOT NULL PRIMARY KEY SORTKEY,
  first_name VARCHAR NOT NULL,
   last_name VARCHAR NOT NULL,
      gender VARCHAR,
       level VARCHAR NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY DISTKEY,
      title VARCHAR NOT NULL,
  artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
       year INTEGER,
   duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
  artist_id VARCHAR PRIMARY KEY DISTKEY,
       name VARCHAR NOT NULL,
   location VARCHAR,
  lattitude FLOAT,
  longitude FLOAT
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

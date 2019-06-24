CREATE TABLE IF NOT EXISTS songplays (
    PRIMARY KEY (songplay_id),
    songplay_id  SERIAL,
    start_time   TIMESTAMP   NOT NULL,
    user_id      INT         NOT NULL, 
    level        VARCHAR     NOT NULL,
    song_id      INT         NOT NULL, 
    artist_id    INT         NOT NULL, 
    session_id   INT         NOT NULL, 
    location     TEXT, 
    user_agent   TEXT)
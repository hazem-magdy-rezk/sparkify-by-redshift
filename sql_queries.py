import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"

staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

songplay_table_drop = "DROP TABLE IF EXISTS songplay"

user_table_drop = "DROP TABLE IF EXISTS users"

song_table_drop = "DROP TABLE IF EXISTS songs"

artist_table_drop = "DROP TABLE IF EXISTS artists"

time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE  staging_events (
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          VARCHAR,
        itenInSession   INT,
        lastName        VARCHAR,
        length          NUMERIC,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    FLOAT,
        sessionId       INT,
        song            VARCHAR,
        status          INT,
        ts              BIGINT,
        userAgent       VARCHAR,
        userID          INT        
);
""")

staging_songs_table_create = (""" CREATE TABLE staging_songs
(       num_songs         INT          NOT NULL,
        artist_id         VARCHAR, 
        artist_latitude   NUMERIC,
        artist_longitude  NUMERIC,
        artist_location   VARCHAR, 
        artist_name       VARCHAR,
        song_id           VARCHAR          NOT NULL,
        title             VARCHAR,
        duration          NUMERIC,
        year              INT
);
""")

songplay_table_create = (""" CREATE TABLE songplay
(       songplay_id   INT          IDENTITY(0,1), 
        start_time    TIMESTAMP,
        user_id       INT, 
        level         VARCHAR(11), 
        song_id       VARCHAR          NOT NULL, 
        artist_id     VARCHAR, 
        session_id    INT, 
        location      VARCHAR , 
        user_agent    VARCHAR
);
""")

user_table_create = ("""create table users
(       user_id       INT         PRIMARY KEY    NOT NULL,
        first_name    VARCHAR, 
        last_name     VARCHAR,
        gender        VARCHAR, 
        level         VARCHAR
);
""")

song_table_create = (""" CREATE TABLE songs
(       song_id       VARCHAR         PRIMARY KEY     NOT NULL, 
        title         VARCHAR, 
        artist_id     VARCHAR,
        year          INT,
        duration      NUMERIC
);
""")

artist_table_create = (""" CREATE TABLE artists 
(       artist_id      VARCHAR         PRIMARY KEY    NOT NULL,
        name           VARCHAR,
        location       VARCHAR,
        lattitude      FLOAT,
        longitude      FLOAT 
);
""")

time_table_create = (""" CREATE TABLE time 
(       start_time     TIMESTAMP    PRIMARY KEY   NOT NULL,
        hour           INT,
        day            INT,
        week           INT,
        month          INT,
        year           INT,
        weekday        INT 
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role={}'
format as json {}
region 'us-west-2'
timeformat 'epochmillisecs';
""").format(config.get('S3','LOG_DATA'),
config.get('IAM_ROLE', 'ARN'),
config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
COMPUPDATE OFF
JSON AS'auto'
""").format(config.get('S3','SONG_DATA'),
config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay
       (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
     SELECT DISTINCT 
            DATE_ADD('ms', se.ts, '1970-01-01')   AS  start_time,
            se.userID                             AS  user_id,
            se.level                              AS  level,
            ss.song_id                            AS  song_id,
            ss.artist_id                          AS  artist_id,
            se.sessionId                          AS  session_id, 
            se.location                           AS  location,
            se.userAgent                          AS  user_agent
     FROM   staging_events                        AS  se
     JOIN   staging_songs                         AS  ss
     ON     se.song = ss.title
     AND    se.artist = ss.artist_name
     WHERE  se.page = 'NextSong'
     AND    se.userId IS NOT NULL;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
           userId            AS user_id,
           firstName         AS first_name,
           lastName          AS last_name,
           gender            AS gender,
           level             AS level
    FROM   staging_events
    WHERE  page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO songs (song_id,title, artist_id, year, duration)
   SELECT DISTINCT 
          ss.song_id     As  song_id,
          ss.title       AS  title,
          ss.artist_id   AS  artist_id,
          ss.duration    AS  duration,
          ss.year        AS  year
   FROM   staging_songs  AS  ss; 
""")

artist_table_insert = ("""INSERT INTO artists (artist_id,name, location, latitude, longitude)

  SELECT DISTINCT 
         ss.artist_id        AS  artist_id,
         ss.artist_name      AS  name,
         se.location         AS  location,
         ss.latitude         AS  latitude,
         ss.longitude        AS  longitude
  FROM   staging_events      AS  se
  JOIN   staging_songs       AS  ss
   ON    se.artist=ss.artist_name
   AND   se.song=ss.title 
   AND   se.length=ss.duration;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)

  SELECT DISTINCT ts,
         start_time                      AS start_time,
         EXTRACT(HOUR FROM start_time)   AS hour,
         EXTRACT(DAY FROM start_time)    AS day,
         EXTRACT(WEEK FROM start_time)   AS week,
         EXTRACT(MONTH FROM start_time)  AS month,
         EXTRACT(YEAR FROM start_time)   AS year,
         EXTRACT(DOW FROM start_time)    AS weekday)
  FROM   (SELECT DISTINCT '1970-01-01'::date + ts/1000 * interval '1 second' as start_time
  FROM   staging_events);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

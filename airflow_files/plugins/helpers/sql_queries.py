class SqlQueries:
    songplay_table_insert = ("""
        SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'   AS start_time,
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
        FROM staging_events AS se
        JOIN staging_songs AS ss
        ON (se.artist = ss.artist_name
            AND se.artist = ss.artist_name
            AND se.length = ss.duration)
        WHERE se.page = 'NextSong';
    """)

    user_table_insert = ("""
        SELECT  DISTINCT se.userId      AS user_id,
            se.firstName                AS first_name,
            se.lastName                 AS last_name,
            se.gender                   AS gender,
            se.level                    AS level
        FROM staging_events AS se
        WHERE se.page = 'NextSong';
    """)

    song_table_insert = ("""
        SELECT  DISTINCT ss.song_id     AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
        FROM staging_songs AS ss;
    """)

    artist_table_insert = ("""
        SELECT  DISTINCT ss.artist_id   AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
        FROM staging_songs AS ss;
    """)

    time_table_insert = ("""
        SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
        FROM    staging_events               AS se
        WHERE se.page = 'NextSong';
    """)


    songplays_check_nulls = ("""
        SELECT COUNT(*)
        FROM songplays
        WHERE   songplay_id IS NULL OR
                start_time IS NULL OR
                user_id IS NULL;
    """)

    users_check_nulls = ("""
        SELECT COUNT(*)
        FROM users
        WHERE user_id IS NULL;
    """)

    songs_check_nulls = ("""
        SELECT COUNT(*)
        FROM songs
        WHERE song_id IS NULL;
    """)

    artists_check_nulls = ("""
        SELECT COUNT(*)
        FROM artists
        WHERE artist_id IS NULL;
    """)

    time_check_nulls = ("""
        SELECT COUNT(*)
        FROM time
        WHERE start_time IS NULL;
    """)

    songplays_check_count = ("""
        SELECT COUNT(*)
        FROM songplays;
    """)

    users_check_count = ("""
        SELECT COUNT(*)
        FROM users;
    """)

    songs_check_count = ("""
        SELECT COUNT(*)
        FROM songs;
    """)

    artists_check_count = ("""
        SELECT COUNT(*)
        FROM artists;
    """)

    time_check_count = ("""
        SELECT COUNT(*)
        FROM time;
    """)



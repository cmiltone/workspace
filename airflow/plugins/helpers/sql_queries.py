class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) playid,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    create_stage_events = ("""
        CREATE TABLE IF NOT EXISTS staging_events (
            artist varchar(256),
            auth varchar(256),
            firstname varchar(256),
            gender varchar(256),
            iteminsession int4,
            lastname varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionid int4,
            song varchar(256),
            status int4,
            ts int8,
            useragent varchar(256),
            userid int4
        );
    """)

    create_stage_songs = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
                    num_songs int4,
                    artist_id varchar(256),
                    artist_name varchar(512),
                    artist_latitude numeric(18,0),
                    artist_longitude numeric(18,0),
                    artist_location varchar(512),
                    song_id varchar(256),
                    title varchar(512),
                    duration numeric(18,0),
                    "year" int4
                );
    """)

    copy_sql = ("""
        {create_sql}
        TRUNCATE TABLE {table};
        COPY {table}
        FROM '{bucket}'
        ACCESS_KEY_ID '{{access}}'
        SECRET_ACCESS_KEY '{{secret}}'
        REGION '{region}'
        json
        '{ref}'
    """)

    copy_events_sql = copy_sql.format(
        create_sql = create_stage_events,
        table = 'staging_events',
        bucket = 's3://sparkify/log-data',
        region = 'us-east-1',
        ref = 's3://sparkify/log-metadata/log_json_path.json',
    )

    copy_songs_sql = copy_sql.format(
        create_sql = create_stage_songs,
        table = 'staging_songs',
        bucket = 's3://sparkify/song-data',
        region = 'us-east-1',
        ref = 'auto',
    )

    user_create_sql = ("""
        CREATE TABLE IF NOT EXISTS users (
            userid int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT users_pkey PRIMARY KEY (userid)
        );
    """)

    user_insert_sql = ("""
        INSERT INTO users (userid, first_name, last_name, gender, level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    user_truncate_sql = ("""
        TRUNCATE TABLE users;
    """)
    
    song_create_sql = ("""
        CREATE TABLE IF NOT EXISTS songs (
            songid varchar(256) NOT NULL,
            title varchar(512),
            artistid varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        );
    """)

    song_truncate_sql = ("""
        TRUNCATE TABLE songs;
    """)
    
    song_insert_sql = ("""
        INSERT INTO songs (songid, title, artistid, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)
    
    artist_create_sql = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artistid varchar(256) NOT NULL,
            name varchar(512),
            location varchar(512),
            latitude numeric(18,0),
            longitude numeric(18,0)
        );
    """),

    artist_truncate_sql = ("""
        TRUNCATE TABLE artists;
    """)

    artist_insert_sql = ("""
        INSERT INTO artists (artistid, name, location, latitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_create_sql = ("""
        CREATE TABLE IF NOT EXISTS "time" (
            start_time timestamp NOT NULL,
            "hour" int4,
            "day" int4,
            week int4,
            "month" varchar(256),
            "year" int4,
            weekday varchar(256),
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
        );
    """)

    time_truncate_sql = ("""
        TRUNCATE TABLE "time";
    """)

    time_insert_sql = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
            extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays;
    """)

    songplay_create_sql = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            playid varchar(32) NOT NULL,
            start_time timestamp NOT NULL,
            userid int4 NOT NULL,
            "level" varchar(256),
            songid varchar(256),
            artistid varchar(256),
            sessionid int4,
            location varchar(256),
            user_agent varchar(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        );
    """)
     
    songplay_truncate_sql = ("""
        TRUNCATE TABLE songplays;
    """)

    songplay_insert_sql = ("""
        INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        SELECT
                md5(events.sessionid || events.start_time) AS playid,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    checks = [
        {
            'query': 'SELECT COUNT(*) FROM users where userid=NULL;',
            'expected_result': 0
        },
        {
            'query': 'SELECT COUNT(*) FROM songs where songid=NULL;',
            'expected_result': 0
        },
        {
            'query': 'SELECT COUNT(*) FROM artists where artistid=NULL;',
            'expected_result': 0
        },
        {
            'query': 'SELECT COUNT(*) FROM time where start_time=NULL;',
            'expected_result': 0
        }      
    ]
class CreateSql:
    ''' Staging events table create parameter with create statment included'''
    staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
                   artist VARCHAR, 
                   auth VARCHAR,
                   firstname VARCHAR,
                   gender varchar,
                   iteminsession INTEGER,
                   lastname VARCHAR , 
                   length NUMERIC(10,6),
                   level VARCHAR, 
                   location VARCHAR, 
                   method VARCHAR, 
                   page VARCHAR,
                   registration BIGINT, 
                   sessionid INTEGER,
                   song VARCHAR, 
                   status INTEGER, 
                   ts BIGINT, 
                   useragent VARCHAR,
                   userid INTEGER
               );
               """)
    
    ''' Staging songs table create parameter with create statment included'''
    staging_songs_table_create = ( """
               CREATE TABLE IF NOT EXISTS staging_songs (
                   artist_id VARCHAR , 
                   artist_latitude NUMERIC(10,5),
                   artist_longitude NUMERIC(10,5),
                   artist_location VARCHAR,
                   artist_name varchar,
                   duration NUMERIC(10,5),
                   num_songs INTEGER , 
                   song_id VARCHAR,
                   title VARCHAR, 
                   year INTEGER
               );
               """)
    
    ''' copy statement for json files '''
    copy_sql_json = ("""COPY {}
               FROM '{}'
               ACCESS_KEY_ID '{}'
               SECRET_ACCESS_KEY '{}'
               FORMAT AS JSON '{}';
            """)
    
    ''' songplays table create parameter with create statment included'''
    songplays_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id  INTEGER IDENTITY(1,1) PRIMARY KEY,
                                                                     start_time VARCHAR NOT NULL,
                                                                     user_id VARCHAR NOT NULL,
                                                                     level VARCHAR, 
                                                                     song_id VARCHAR , 
                                                                     artist_id VARCHAR , 
                                                                     session_id VARCHAR, 
                                                                     location VARCHAR, 
                                                                     user_agent VARCHAR                                                                 
                                                                     );
                    """)
    
    ''' songplay insert table parameter with insert statment included'''
    songplays_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                              SELECT 
                            TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS START_TIME,
                                    e.userid,
                                    e.level,
                                    s.song_id,
                                    s.artist_id,
                                    e.sessionid,
                                    e.location,
                                    e.useragent
                            FROM dwh.public.staging_events e 
                            join dwh.public.staging_songs  s on e.song = s.title 
                                                                and e.length = s.duration 
                                                                and e.artist = s.artist_name
                            where page = 'NextSong'
                    """)
    
    ''' user table create parameter with create statment included'''
    user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id INTEGER NOT NULL PRIMARY KEY,
                                                            first_name VARCHAR,
                                                            last_name VARCHAR,
                                                            gender VARCHAR, 
                                                            level VARCHAR
                                                            );
                    """)
    
    ''' user table insert parameter with insert statment included'''
    user_table_insert = ("""INSERT INTO users(user_id,first_name,last_name,gender,level)
                        SELECT DISTINCT
                        userid,
                        firstName,
                        lastName,
                        gender,
                        level
                        FROM staging_events
                        WHERE userid IS NOT NULL
                        """)
    
    ''' songs table create parameter with create statment included'''
    songs_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id VARCHAR NOT NULL PRIMARY KEY,
                                                            title VARCHAR,
                                                            artist_id VARCHAR,
                                                            year INTEGER, 
                                                            duration NUMERIC(10,5)
                                                            );
                    """)
    
    ''' songs table insert parameter with insert statment included'''
    songs_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT 
                        song_id, 
                        title,
                        artist_id,
                        year,
                        duration
                        FROM staging_songs
                        where song_id is not null
                        """)
    
    ''' artists table create parameter with create statment included'''
    artists_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar NOT NULL PRIMARY KEY,
                                                              name VARCHAR,
                                                              location VARCHAR,
                                                              latitude NUMERIC(10,5),
                                                              longitude NUMERIC(10,5)
                                                              );
                    """)
    
    
    ''' artists table insert parameter with insert statment included'''
    artists_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                        SELECT DISTINCT 
                        artist_id, 
                        artist_name,
                        artist_location,
                        artist_latitude,
                        artist_longitude
                        FROM staging_songs
                        where artist_id is not null
                        """)
    
    ''' time table create parameter with create statment included'''
    time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time timestamp NOT NULL PRIMARY KEY,
                                                           hour int,
                                                           day int, 
                                                           week int,
                                                           month int, 
                                                           year int, 
                                                           weekday int
                                                           );
                    """)
    
    ''' time table insert parameter with insert statment included'''
    time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                        SELECT DISTINCT 
                         TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS START_TIME,
                         EXTRACT(HRS FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS HOUR,
                         EXTRACT(D FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS DAY,
                         EXTRACT(W FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS WEEK,
                         EXTRACT(MON FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS MONTH,
                         EXTRACT(Y FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS YEAR,
                         EXTRACT(DOW FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') weekdaydate
                        FROM staging_events
                        where ts is not null 
                        """)
    
    ''' Data quality check sql parameters with source query included'''
    source_data_quality_query = ("""SELECT 'songplays' as sourcetable, count(*) total
                                    FROM dwh.public.staging_events e 
                                    join dwh.public.staging_songs  s on (e.song = s.title and e.length = s.duration and e.artist = s.artist_name)
                                    where page = 'NextSong'""")
    
    ''' Data quality check sql parameters with target query included'''
    target_data_quality_query = ("""Select 'songplays' as targettable, count(*)  total from songplays""")
    
    
class SqlCreateTables:
    create_staging_events_table_sql = ("""
        CREATE TABLE IF NOT EXISTS staging_events(
            artist varchar,
            auth varchar(25),
            first_name varchar(25),
            gender varchar(5),
            item_in_session int,
            last_name varchar(25),
            length float,
            level varchar(5),
            location varchar,
            method varchar(5),
            page varchar(25),
            reg float,
            session_id int,
            song varchar,
            status int,
            ts bigint,
            user_agent varchar,
            user_id int);
        """)

    create_staging_songs_table = ("""
        CREATE TABLE IF NOT EXISTS staging_songs(
            num_songs int,
            artist_id varchar(25),
            artist_latitude float,
            artist_longitude float,
            artist_location varchar,
            artist_name varchar,
            song_id varchar(25),
            title varchar,
            duration float,
            year int);
        """)
    
    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplay_table(
            songplay_id varchar PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            user_id int,
            level varchar(5),
            song_id varchar(25),
            artist_id varchar(25),
            session_id int,
            location varchar,
            user_agent varchar);
        """)
    
    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS user_table(
            user_id int PRIMARY KEY,
            first_name varchar(25),
            last_name varchar(25),
            gender varchar(1),
            Level varchar(5));
        """)
    
    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS song_table(
            song_id varchar(25) PRIMARY KEY,
            title varchar,
            artist_id varchar(25),
            year int,
            duration float);
        """)
    
    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artist_table(
            artist_id varchar(25) PRIMARY KEY,
            artist_name varchar,
            location varchar,
            latitude float,
            longitude float);
        """)
    
    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time_table(
            start_time TIMESTAMP PRIMARY KEY,
            hour int,
            day int,
            week int,
            month int,
            year int,
            weekday boolean);
        """)
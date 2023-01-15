This project was made for a music streaming startup called Sparkify, it has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

the project aim to  build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

database schema:

 Fact Table

    songplays - records in event data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

    users - users in the app
        user_id, first_name, last_name, gender, level
    songs - songs in music database
        song_id, title, artist_id, year, duration
    artists - artists in music database
        artist_id, name, location, lattitude, longitude
    time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

How to run python files :

step1: run create_tables.py  in the terminal >> to drop existing columns and create new ones in the database
     
     type ' python create_tables.py ' then press enter
     
step2: run etl.py  in the terminal >> to create the etl 
   
     type ' python etl.py ' then press enter 
import os
import configparser
from datetime import datetime
from os.path import join, dirname, abspath

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import (
    year,
    month,
    dayofmonth,
    hour,
    weekofyear,
    dayofweek,
    monotonically_increasing_id,
)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Fetches or creates a spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/'

    songdata_schema = StructType([
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
    ])
    # read song data file
    df = spark.read.json(song_data, schema=songdata_schema)

    # extract columns to create songs table
    songs_table = df.select(
        'song_id', 'title', 'artist_id', 'year', 'duration') \
        .dropDuplicates()
    songs_table.createOrReplaceTempView('songs')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet(join(output_data, 'songs/songs.parquet'), 'overwrite')

    # # extract columns to create artists table
    artists_table = df.select(
        'artist_id',
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude',
    ).withColumnRenamed('artist_name', 'name') \
        .withColumnRenamed('artist_location', 'location') \
        .withColumnRenamed('artist_latitude', 'latitude') \
        .withColumnRenamed('artist_longitude', 'longitude') \
        .dropDuplicates()
    artists_table.createOrReplaceTempView('artists')
    # # write artists table to parquet files
    artists_table.write.parquet(
        join(output_data, 'artists/artists.parquet'),
        'overwrite'
    )


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log-data/*'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    actions_df = df.filter(df.page == 'NextSong') \
        .select('ts', 'userId', 'level', 'song', 'artist',
                'sessionId', 'location', 'userAgent')

    # extract columns for users table
    users_table = df.select(
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level'
    ).dropDuplicates()

    users_table.createOrReplaceTempView('users')
    # write users table to parquet files
    users_table.write.parquet(
        join(output_data, 'users/users.parquet'),
        'overwrite'
    )

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    actions_df = actions_df.withColumn(
        'timestamp',
        get_timestamp(actions_df.ts)
    )

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    actions_df = actions_df.withColumn('datetime', get_datetime(actions_df.ts))

    # extract columns to create time table
    time_table = actions_df.select('datetime') \
        .withColumn('start_time', actions_df.datetime) \
        .withColumn('hour', hour('datetime')) \
        .withColumn('day', dayofmonth('datetime')) \
        .withColumn('week', weekofyear('datetime')) \
        .withColumn('month', month('datetime')) \
        .withColumn('year', year('datetime')) \
        .withColumn('weekday', dayofweek('datetime')).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.createOrReplaceTempView('time')
    time_table.write.partitionBy(
        'year', 'month') \
        .parquet(join(output_data, 'time/time.parquet'), 'overwrite')
    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/')
    joined_df = actions_df.join(
        song_df,
        (actions_df.artist == song_df.artist_name),
        'inner'
    )
    # extract columns from joined
    # song and log datasets to create songplays table
    songplays_table = joined_df.select(
        actions_df.datetime.alias('start_time'),
        actions_df.userId.alias('user_id'),
        actions_df.level.alias('level'),
        song_df.song_id.alias('song_id'),
        song_df.artist_id.alias('artist_id'),
        actions_df.sessionId.alias('session_id'),
        actions_df.location.alias('location'),
        actions_df.userAgent.alias('user_agent'),
        year(actions_df.datetime).alias('year'),
        month(actions_df.datetime).alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())

    songplays_table.createOrReplaceTempView('songplays')
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.\
        partitionBy('year', 'month').\
        parquet(join(output_data, 'songplays/songplays.parquet'), 'overwrite')


def main():
    ROOT_DIR = dirname(abspath(__file__))
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = ROOT_DIR + 'data/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

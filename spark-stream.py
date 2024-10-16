import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)

    print('Keyspace created successfully')


def create_table(session):
    session.execute(""""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users(

        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        postcode INT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered TEXT,
        phone TEXT,
        picture TEXT
    """)
    print('Table created successfully')

def insert_data(session, **kwargs):
    print('Inserting data...')

    id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered = kwargs.get('registered')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')
    try:
        session.execute(""" INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, postcode, email,
         username,dob,registered, phone, picture)
         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """), (
            id, first_name, last_name, gender, address, postcode, email, username,dob,registered, phone, picture)
        logging.info(f"Data inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error(f"Couldn't insert data for {first_name} {last_name} due to exception: {e}")

    print('Data inserted successfully')
    return


def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1,") \
            .config("spark.cassandra.connection.host", "localhost") \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session created successfully')
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe couldn't be created due to exception: {e}")
    return spark_df


def create_cassandra_connection():


    try:
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Couldn't create the connection to cassandra due to exception: {e}")
        return None


def create_selection_df(spark_df):

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    df = spark_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select(
        "data.*")
    print(df)

    return df

if __name__ == '__main__':

    # create spark connetion
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to Kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df= create_selection_df(spark_df)
        session = create_cassandra_connection()
        if session is not None:
            create_keyspace(session)
            create_table(session)
            #  insert_data(session)
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option("checkpointLocation", "/tmp/checkpoint")
                               .option("keyspace", "spark_streams")
                               .option("table", "created_users")
                               .start())
            streaming_query.awaitTermination()

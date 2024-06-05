import logging

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType
from pyspark.sql.functions import col, from_json

logging.basicConfig(
    filename='streaming.log',  
    filemode='a',        
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s - %(message)s',  
    datefmt='%Y-%m-%d %H:%M:%S'  
)

def cassandra_connection(contact_points=['localhost']):
    """
    Connect to a Cassandra cluster and return a session object.

    Args:
        contact_points (list): A list of contact points (IP addresses or hostnames) for the Cassandra cluster.
        auth_provider (AuthProvider, optional): An authentication provider instance if authentication is enabled.

    Returns:
        cassandra.cluster.Session: A session object connected to the Cassandra cluster.
        None: If the connection fails.
    """
    cluster = None
    session = None
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

    try:
        cluster = Cluster(contact_points=contact_points, auth_provider=auth_provider)
        session = cluster.connect()
        logging.info("Successfully connected to Cassandra cluster.")
    except Exception as e:
        logging.error("Error connecting to Cassandra cluster: {}".format(e))
        if cluster:
            cluster.shutdown()

    if session:
        return session
    else:
        logging.error("Unable to establish a connection to the Cassandra cluster.")
        return None
    
    

def create_keyspace(session, keyspace_name='profiles'):
    """
    Create a keyspace if it does not already exist.

    Args:
        session (cassandra.cluster.Session): The Cassandra session object.
        keyspace_name (str): The name of the keyspace to create.
    """
    try:
        keyspace_query = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """.format(keyspace_name)
        session.execute(keyspace_query)
        logging.info("Keyspace '{}' created successfully.".format(keyspace_name))
    except Exception as e:
        logging.error("Error creating keyspace '{}': {}".format(keyspace_name, e))
        


def create_table(session, keyspace='profiles'):
    """
    Create the 'users' table in the specified keyspace if it does not already exist.

    Args:
        session (cassandra.cluster.Session): The Cassandra session object.
        keyspace (str): The name of the keyspace where the table will be created.
    """
    try:
        session.set_keyspace(keyspace)
        logging.info("Switched to keyspace '{}' successfully.".format(keyspace))
    except Exception as e:
        logging.error("Error setting keyspace '{}': {}".format(keyspace, e))
        return

    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        user_id UUID PRIMARY KEY, 
        full_name TEXT, 
        gender TEXT,
        address TEXT,
        postcode INT,
        email TEXT,
        phone TEXT
    )
    """
    
    try:
        session.execute(create_table_query)
        logging.info("Table 'users' created successfully.")
    except Exception as e:
        logging.error("Error creating table 'users': {}".format(e))

def spark_connection():
    """
    Establishes a connection to Apache Spark.

    Returns:
        pyspark.sql.SparkSession or None: A SparkSession object if the connection is successful,
            None if there's an error.
    """
    try:
        spark_conn = (
            SparkSession
            .builder
            .appName("Streaming from Kafka")
            .master("spark://localhost:7077")
            .config("spark.streaming.stopGracefullyOnShutdown", True)
            .config("spark.sql.shuffle.partitions", 4)
            .config("spark.cassandra.connection.host", "localhost")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
            )
            .config("spark.executor.extraLibraryPath", "C:\Hadoop\hadoop-3.3.6\lib\native")
            .config("spark.driver.extraLibraryPath", "C:\Hadoop\hadoop-3.3.6\lib\native")
            .getOrCreate()
        )
        
        logging.info("Spark session created successfully")    
        return spark_conn
    
    except Exception as e:
        logging.error("Error connecting to Spark: {}".format(e))
        return None
    
    
def read_kafka_topic(spark_conn):
    """
    Reads data from a Kafka topic using Spark structured streaming.

    Args:
        spark_conn (pyspark.sql.SparkSession): The SparkSession object.

    Returns:
        pyspark.sql.DataFrame: A DataFrame representing the streaming data from the Kafka topic,
            or None if there's an error.
    """
    try:
        print("Inside read_kafak_topic function")
        streaming_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "users_queue") \
            .option("startingOffsets", "earliest") \
            .load()
            
        print("Streaming DF: ", streaming_df)
            
        json_schema = StructType([
            StructField('full_name', StringType(), True), \
            StructField('gender', StringType(), True), \
            StructField('address', StringType(), True), \
            StructField('postcode', LongType(), True), \
            StructField('email', StringType(), True), \
            StructField('phone', StringType(), True)
        ])    

        """Parse value from binary to string"""
        parsed_df = streaming_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                    .select(from_json(col("value"), json_schema).alias("data")) \
                    .select("data.*")
        
        return parsed_df
    
    except Exception as e:
        logging.error("Error reading from Kafka topic: {}".format(e))
        return None


def writeToCassandra(writeDF, _):
    """
    Writes the given DataFrame to a Cassandra database.

    Args:
        writeDF (DataFrame): The DataFrame to be written to Cassandra.
        _ (any): A placeholder parameter that is not used in the function.

    The function writes the DataFrame to a Cassandra table named "users" 
    within the "profiles" keyspace using the Apache Spark Cassandra connector. 
    The data is appended to the existing table.
    """

    writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="users", keyspace="profiles")\
    .save()
    
if __name__ == "__main__":
    
    cassandra_conn = cassandra_connection()
    
    spark_conn = spark_connection()
    if cassandra_conn:
        """Create keyspace"""
        create_keyspace(cassandra_conn, keyspace_name='profiles')
        
        """Create table"""
        create_table(cassandra_conn, keyspace='profiles')
        
        """Get data from Kafka"""
        data_df = read_kafka_topic(spark_conn)
        # print("Data DF: ", data_df)
        
        """Write data to Cassandra users table"""
        data_df.writeStream \
            .option("spark.cassandra.connection.host","localhost:9042")\
            .foreachBatch(writeToCassandra) \
            .outputMode("update") \
            .start()\
            .awaitTermination()
        
        """Shutdown cluster connection"""
        cassandra_conn.cluster.shutdown()
    
        
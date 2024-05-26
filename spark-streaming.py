import logging

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(
    filename='streaming.log',  
    filemode='w',        
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
        logging.error(f"Error connecting to Cassandra cluster: {e}")
        if cluster:
            cluster.shutdown()

    if session:
        return session
    else:
        logging.error("Unable to establish a connection to the Cassandra cluster.")
        return None
    
    

def create_keyspace(session, keyspace_name):
    """
    Create a keyspace if it does not already exist.

    Args:
        session (cassandra.cluster.Session): The Cassandra session object.
        keyspace_name (str): The name of the keyspace to create.
    """
    try:
        keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
        session.execute(keyspace_query)
        logging.info(f"Keyspace '{keyspace_name}' created successfully.")
    except Exception as e:
        logging.error(f"Error creating keyspace '{keyspace_name}': {e}")
        


def create_table(session: Session, keyspace='profiles'):
    """
    Create the 'users' table in the specified keyspace if it does not already exist.

    Args:
        session (cassandra.cluster.Session): The Cassandra session object.
        keyspace (str): The name of the keyspace where the table will be created.
    """
    try:
        session.set_keyspace(keyspace)
        logging.info(f"Switched to keyspace '{keyspace}' successfully.")
    except Exception as e:
        logging.error(f"Error setting keyspace '{keyspace}': {e}")
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
        logging.error(f"Error creating table 'users': {e}")


if __name__ == "__main__":
    
    cassandra_conn = cassandra_connection()
    
    if cassandra_conn:
        """Create keyspace"""
        create_keyspace(cassandra_conn, keyspace_name='profiles')
        
        """Create table"""
        create_table(cassandra_conn, keyspace='profiles')
        
        """Shutdown cluster connection"""
        cassandra_conn.cluster.shutdown()
    
        
CLUSTER_HOST = '13.203.31.219'  # ✅ Use IP address

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = f'{CLUSTER_HOST}:9092'  # Check actual port in SMM
KAFKA_SECURITY_PROTOCOL = 'PLAINTEXT'  # May need to update based on cluster config
KAFKA_SASL_MECHANISM = None  # Check if authentication is enabled
KAFKA_USERNAME = 'cloud-user'  # If auth is enabled
KAFKA_PASSWORD = 'super-secret1'  # If auth is enabled

# Data Warehouse (Hue/Impala)
CDW_HOST = CLUSTER_HOST
CDW_PORT = 21050  # Impala port, verify in Cloudera Manager
CDW_DATABASE = 'shopnow_demo'
CDW_USERNAME = 'cloud-user'
CDW_PASSWORD = 'super-secret1'

SQL_STREAM_BUILDER_URL = f'http://{CLUSTER_HOST}:18121/'

# Storage (HDFS for on-premise cluster)
STORAGE_TYPE = 'hdfs'
HDFS_NAMENODE = CLUSTER_HOST
HDFS_PORT = 8020  # Default HDFS port
HDFS_USER = 'cloud-user'

# Base paths in HDFS
HDFS_BASE_PATH = '/user/cloud-user/streammart-demo'
HDFS_RAW_DATA = f'{HDFS_BASE_PATH}/raw'
HDFS_WAREHOUSE = f'{HDFS_BASE_PATH}/warehouse'

# Full paths for data
CLICKSTREAM_PATH = f'{HDFS_RAW_DATA}/clickstream/'
TRANSACTIONS_PATH = f'{HDFS_RAW_DATA}/transactions/'
PRODUCTS_PATH = f'{HDFS_RAW_DATA}/reference/products.json'

IMPALA_HOST = CLUSTER_HOST
IMPALA_PORT = 21050  # Verify in Cloudera Manager -> Impala -> Config
DATABASE_NAME = 'streammart_demo'

NIFI_URL = f'http://{CLUSTER_HOST}:8080/nifi'
FLINK_URL = f'http://{CLUSTER_HOST}:8078'
HUE_URL = f'http://{CLUSTER_HOST}:8889'
KAFKA_BROKER = f'{CLUSTER_HOST}:9092'



# Storage (HDFS - since this is on-premise)
CLOUD_PROVIDER = 'hdfs'
HDFS_PATH = '/user/cloud-user/shopnow-demo'

def get_storage_path(path):
    """Get HDFS path"""
    return f'hdfs://{HDFS_PATH}/{path}'


# For Python connections (if needed)
def get_impala_connection():
    from impala.dbapi import connect
    return connect(
        host=IMPALA_HOST,
        port=IMPALA_PORT,
        auth_mechanism='NOSASL'  # Or 'PLAIN' if auth required
    )

def get_kafka_config():
    """Get Kafka connection configuration"""
    return {
        'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
        'security_protocol': KAFKA_SECURITY_PROTOCOL,
        'sasl_mechanism': KAFKA_SASL_MECHANISM,
        'sasl_plain_username': KAFKA_USERNAME,
        'sasl_plain_password': KAFKA_PASSWORD
    }
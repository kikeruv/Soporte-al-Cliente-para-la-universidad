# Imports para Cassandra
import logging
import os 
from cassandra.cluster import Cluster

# Imports para Mongo
from pymongo import MongoClient

# Imports para DGraph
# import os
import pydgraph

##################################################
# Config de Cassandra 
# Set logger
log = logging.getLogger("cassandra")
log.setLevel(logging.INFO)
handler = logging.FileHandler('proyecto.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', '127.0.0.1')
# Keyspace por defecto para este proyecto
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'proyecto')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

#Conexiones cacheadas 
_cluster = None
_session = None

def get_cassandra_session():
    """
    Devuelve una sesion de Cassandra reutilizable.
    Solo crea el Cluster/conexion y keyspace la primera vez.
    """
    global _cluster, _session

    if _session is not None:
        return _session

    log.info(f"Conectando a Cassandra en {CLUSTER_IPS}...")
    _cluster = Cluster(CLUSTER_IPS.split(','))
    session = _cluster.connect()

    # Crear keyspace si no existe (mismo estilo que en laboratorios)
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy',
                              'replication_factor': {REPLICATION_FACTOR} }}
    """)

    session.set_keyspace(KEYSPACE)
    log.info(f"Conectado a Cassandra. KEYSPACE en uso: {KEYSPACE}")

    _session = session
    return _session
##################################################

##################################################
# Config de Mongo
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mongo")

# Initialize MongoDB client and database
client = MongoClient('mongodb://localhost:27017/')
db = client.Soporte
################################################

################################################
# Config de DGraph
DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def close_client_stub(client_stub):
    client_stub.close()

# Imports para Cassandra
import logging
import os 
from cassandra.cluster import Cluster

# Imports para Mongo
import falcon.asgi
from pymongo import MongoClient

# Imports para DGraph
# import os
import pydgraph

##################################################
# Config de Cassandra 
# Set logger
log = logging.getLogger("cassandra")
log.setLevel(logging.INFO)
handler = logging.FileHandler('logistics.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', '127.0.0.1')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'logistics')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

def get_cassandra_session():
    log.info(f"Conectando a Cassandra en {CLUSTER_IPS}...")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    # Crear keyspace si no existe (mismo estilo que en laboratorios)
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy',
                              'replication_factor': {REPLICATION_FACTOR} }}
    """)

    session.set_keyspace(KEYSPACE)
    log.info(f"Conectado a Cassandra. KEYSPACE en uso: {KEYSPACE}")
    return session
##################################################

##################################################
# Config de Mongo
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mongo")

class LoggingMiddleware:
    async def process_request(self, req, resp):
        logger.info(f"Request: {req.method} {req.uri}")

    async def process_response(self, req, resp, resource, req_succeeded):
        logger.info(f"Response: {resp.status} for {req.method} {req.uri}")

# Initialize MongoDB client and database
client = MongoClient('mongodb://localhost:27017/')
db = client.Soporte
# Create the Falcon application
app = falcon.asgi.App(middleware=[LoggingMiddleware()])
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

#### Esto fue hecho con chat para verificar que las difernetes base de datos en realidad 
#### estuvieran conectadaso bueno que se conecten  
if __name__ == "__main__":
    # Cassandra
    try:
        session = get_cassandra_session()
        print("✅ Cassandra conectada")
    except Exception as e:
        print("❌ Error en Cassandra:", e)

    # Mongo
    try:
        _ = db.list_collection_names()
        print("✅ MongoDB conectado")
    except Exception as e:
        print("❌ Error en Mongo:", e)

    # Dgraph
    try:
        stub = create_client_stub()
        client = create_client(stub)

        # 🔸 Esta línea SÍ hace una llamada real al servidor
        client.check_version()

        print("✅ Dgraph conectado")
    except Exception as e:
        print("❌ Error en Dgraph:", e)
    finally:
        try:
            close_client_stub(stub)
        except:
            pass
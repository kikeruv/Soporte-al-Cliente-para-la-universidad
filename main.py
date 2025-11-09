# Imports para Cassandra
import logging
import os 
from cassandra.cluster import Cluster
import model
# Imports para Mongo
import falcon.asgi
from pymongo import MongoClient
import logging
from resources import BookResource, BooksResource

# Imports para DGraph

##################################################
# Config de Cassandra 
# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('logistics.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', '127.0.0.1')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'logistics')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')
##################################################

##################################################
# Config de Mongo
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoggingMiddleware:
    async def process_request(self, req, resp):
        logger.info(f"Request: {req.method} {req.uri}")

    async def process_response(self, req, resp, resource, req_succeeded):
        logger.info(f"Response: {resp.status} for {req.method} {req.uri}")

# Initialize MongoDB client and database
client = MongoClient('mongodb://localhost:27017/')
db = client.app
# Create the Falcon application
app = falcon.asgi.App(middleware=[LoggingMiddleware()])

# Instantiate the resources
book_resource = BookResource(db)
books_resource = BooksResource(db)

# Add routes to serve the resources
app.add_route('/books', books_resource)
app.add_route('/books/{book_id}', book_resource)
################################################

################################################
# Config de DGraph


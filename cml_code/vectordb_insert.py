from milvus import default_server
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility
import subprocess
from sentence_transformers import SentenceTransformer
import utils.model_embedding_utils as model_embedding

import os
from pathlib import Path

#####Custom Code#####

import boto3
from botocore.exceptions import NoCredentialsError
from io import StringIO

s3_bucket = "ankity-cdp-aw-delete"  # Replace with your S3 bucket name
s3_prefix = "data"  # Replace with the prefix or folder path in your S3 bucket

s3_client = boto3.client("s3")

# List all objects in the specified S3 bucket and prefix
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

#for s3_object in response["Contents"]:
#    file_key = s3_object["Key"]
#    print("Generating embeddings for: %s" % file_key)
#
#    try:
#        # Download the file from S3
#        file_obj = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
#        file_content = file_obj["Body"].read().decode("utf-8")
#
#        # Process the file content
#        insert_embedding(collection, file_key, file_content)
#
#    except NoCredentialsError:
#        print("AWS credentials not found.")

#####Custom Code#####

def create_milvus_collection(collection_name, dim):
      if utility.has_collection(collection_name):
          utility.drop_collection(collection_name)

      fields = [
      FieldSchema(name='relativefilepath', dtype=DataType.VARCHAR, description='file path relative to root directory ', max_length=1000, is_primary=True, auto_id=False),
      FieldSchema(name='embedding', dtype=DataType.FLOAT_VECTOR, description='embedding vectors', dim=dim)
      ]
      schema = CollectionSchema(fields=fields, description='reverse image search')
      collection = Collection(name=collection_name, schema=schema)

      # create IVF_FLAT index for collection.
      index_params = {
          'metric_type':'IP',
          'index_type':"IVF_FLAT",
          'params':{"nlist":2048}
      }
      collection.create_index(field_name="embedding", index_params=index_params)
      return collection
    
## Create an embedding for given text/doc and insert it into Milvus Vector DB
#def insert_embedding(collection, id_path, text):
#    embedding =  model_embedding.get_embeddings(text)
#    data = [[id_path], [embedding]]
#    collection.insert(data)
    
# Create an embedding for given text/doc and insert it into Milvus Vector DB
def insert_embedding(collection, id_path, text):
    model = SentenceTransformer('sentence-transformers/sentence-t5-base')
    embedding = model.encode(text)
    data = [[id_path], [embedding]]
    collection.insert(data)
    
def main():
  # Reset the vector database files
  print(subprocess.run(["rm -rf milvus-data"], shell=True))

  default_server.set_base_dir('milvus-data')
  default_server.start()

  try:
    connections.connect(alias='default', host='localhost', port=default_server.listen_port)   
    print(utility.get_server_version())

    # Create/Recreate the Milvus collection
    collection_name = 'cloudera_ml_docs'
    collection = create_milvus_collection(collection_name, 768)

    print("Milvus database is up and collection is created")

    # Read KB documents in ./data directory and insert embeddings into Vector DB for each doc
    # The default embeddings generation model specified in this AMP only generates embeddings for the first 256 tokens of text.
#    doc_dir = './data'
#    for file in Path(doc_dir).glob(f'**/*.txt'):
#        with open(file, "r") as f: # Open file in read mode
#            print("Generating embeddings for: %s" % file.name)
#            text = f.read()
#            insert_embedding(collection, os.path.abspath(file), text)
    
    for s3_object in response["Contents"]:
      file_key = s3_object["Key"]
      print("Generating embeddings for: %s" % file_key)

      try:
          # Download the file from S3
          file_obj = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
          file_content = file_obj["Body"].read().decode("utf-8")

          # Process the file content
          insert_embedding(collection, file_key, file_content)

      except NoCredentialsError:
          print("AWS credentials not found.")
    
    collection.flush()
    print('Total number of inserted embeddings is {}.'.format(collection.num_entities))
    print('Finished loading Knowledge Base embeddings into Milvus')

  except Exception as e:
    default_server.stop()
    raise (e)
    
  
  default_server.stop()


if __name__ == "__main__":
    main()

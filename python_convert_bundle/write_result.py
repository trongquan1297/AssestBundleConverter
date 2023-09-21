from pymongo import MongoClient
from elasticsearch import Elasticsearch
import os
import socket
from datetime import datetime
import json
import ssl
import urllib3
from ssl import create_default_context

from dotenv import load_dotenv

load_dotenv()

urllib3.disable_warnings()
# Create the client instance
context = create_default_context(capath="./http_ca.crt")
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE

elasticsearch_host = os.getenv("ES_HOST")
elasticsearch_port = int(os.getenv("ES_PORT"))
elasticsearch_username = os.getenv("ES_USER")
elasticsearch_password = os.getenv("ES_PASS")


class CustomException(Exception):
        def __init__(self, message):
            self.message = message

def insert_result_to_es(file_path, bundle_type, status, ios_bundle, and_bundle, build_time):
    
    file_name = file_path.split('/')[-1][:-4]
    host_name = socket.gethostname()
    current_time = datetime.now()
    time_stamp = current_time.isoformat()
    build_time_in_seconds = "{:.2f}".format(int(build_time))

    es = Elasticsearch(
            elasticsearch_host,
            ssl_context=context,
            basic_auth=(elasticsearch_username, elasticsearch_password),
            verify_certs=False,
        )

    data = {
        "time": time_stamp,
        "worker": host_name,
        "bundle": {
            "name": file_name,
            "type": bundle_type,
            "ios_bundle": ios_bundle,
            "android_bundle": and_bundle,
            "build_time": build_time_in_seconds,
            "status": status
        }
    }
    json_data = json.dumps(data)

    response = es.index(index='build-bundle-report', document=json_data)
    if response["result"] == "created":
        print("JSON data indexed successfully. Document ID:", response["_id"])
    else:
        raise CustomException("Failed to index JSON data.")
import os
import json
from datetime import datetime
from cryptography.fernet import Fernet
from google.cloud import storage
from google.cloud import logging

# Variables de entorno
PROJECT_ID = os.environ.get("PROJECT_ID")
BUCKET_DESENCRIPTADOS = os.environ.get("BUCKET_DESENCRIPTADOS")
SECRET_KEY = os.environ.get("SECRET_KEY")

# Constantes
START_DATETIME = datetime.now()
START_DATE = START_DATETIME.strftime("%Y%m%d") # -%H%M%S
PROCESO_ID = ""
name = ""

# Clientes de GCP
storage_client = storage.Client()
logging_client = logging.Client()

# PRINT_STRUCTURE_LOGS
def print_struct_logs(message, severity="INFO"):

    global PROCESO_ID
    global name
    
    labels = {
        "proceso_id": PROCESO_ID,
        "name": "desencriptar",
        "work_file": name,
        "enroute": "yes",
    }
    
    #logging.googleapis.com/labels
    data = dict()
    data["message"] = message
    data["severity"] = severity
    data["logging.googleapis.com/labels"] = labels
    
    print(json.dumps(data))

# Desencripta el archivo
def decrypt_data(encrypted_blob: storage.Blob, decrypted_blob: storage.Blob, key) -> storage.Blob:
    f = Fernet(key)
    with encrypted_blob.open(mode="rb") as finput:
        with decrypted_blob.open(mode="wb") as foutput:
            foutput.write(f.decrypt(finput.read()))
            
    decrypted_blob.reload()
    return decrypted_blob

def main(event, context):
    global PROCESO_ID
    global name
    
    blob_name = event["name"]
    bucket = event["bucket"]
    
    blob = storage_client.get_bucket(bucket).get_blob(blob_name)
    PROCESO_ID = blob.metadata["proceso_id"]
    name = os.path.basename(blob_name).split(".")[0].lower()
    
    print_struct_logs("Se procede a crear un blob object para almacenar el contenido encriptado.", severity="INFO")
    try:
        output_blob_name = "data/" + START_DATE + "/" + os.path.basename(blob_name).split(".")[0] + ".txt"
        output_bucket = storage_client.get_bucket(BUCKET_DESENCRIPTADOS)
        output_blob = output_bucket.blob(output_blob_name)
    except Exception as error:
        error = f"Fallo (creacion del blob object): {str(error)}"
        print_struct_logs(error, severity="ERROR")
        return -1
    
    print_struct_logs("Se procede a desencriptar el archivo", severity="INFO")
    try:
        output_blob = decrypt_data(blob, output_blob, SECRET_KEY)
        output_blob.metadata = blob.metadata
        output_blob.patch()
    except Exception as error:
        error = f"Exception mientras se desencriptaba el archivo: {str(error)}"
        print_struct_logs(error, severity="ERROR")
        return -1

    duration = round((datetime.now() - START_DATETIME).total_seconds())
    print_struct_logs(f"Proceso terminado con exito ({duration}s)", severity="INFO")
    return 1
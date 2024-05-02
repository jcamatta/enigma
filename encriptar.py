import os
import sys
import uuid
import gzip
from cryptography.fernet import Fernet
from google.cloud import storage
from google.cloud import logging
from google.cloud import secretmanager
from datetime import datetime

# Variables de entorno
PROJECT_ID = os.environ.get("PROJECT_ID")
BUCKET_ENCRIPTADOS = os.environ.get("BUCKET_ENCRIPTADOS")
SECRET_KEY = os.environ.get("SECRET_KEY")

# Constantes
START_DATETIME = datetime.now()
START_DATE = START_DATETIME.strftime("%Y%m%d") # -%H%M%S
PROCESO_ID = uuid.uuid4().hex

# Clientes de GCP
logging_client = logging.Client()
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()

# Crea el directorio temp si no existe
def create_temp_folder() -> str:
    temp_folder = os.path.join(os.getcwd(), "temp")
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder, exist_ok=True)
    return temp_folder

# Comprime un archivo usando gzip
def compress_data(input_file: str) -> str:
    
    # Chequea si existe o crea el directorio temp
    temp_folder = create_temp_folder()
    
    # Construimos el path del archivo comprimido
    gzip_name = os.path.basename(input_file).split(".")[0] + ".gz"
    gzip_path = os.path.join(temp_folder, gzip_name)
    
    # Comprimimos el archivo
    with open(input_file, "rb") as finput, gzip.open(gzip_path, "wb") as foutput:
        foutput.writelines(finput)
    return gzip_path

# Encripta un archivo usando fernet
def encrypt_data(input_file: str, blob: storage.Blob, key: str) -> storage.Blob:
    f = Fernet(key)
    with open(input_file, "rb") as finput:
        with blob.open(mode="wb") as foutput:
            block = finput.read()
            encrypted_block = f.encrypt(block)
            foutput.write(encrypted_block)
            
    blob.reload()
    return blob

def main() -> int:
    
    # 1 = Success | -1 = Failure 
    
    # Terminar el programa si las variables de entorno no estan definidas.
    if not (PROJECT_ID and BUCKET_ENCRIPTADOS and SECRET_KEY):
        print("Se deben setear las variables de entorno.")
        return -1
    
    # Se debe pasar como argumento el path del archivo a encriptar.
    if len(sys.argv) != 3:
        print("Se debe pasar como argumento el path del archivo a encriptar.")
        return -1
    
    input_file = sys.argv[1]
    job_name = sys.argv[2]
    
    # El archivo a encriptar debe existir.
    if not os.path.exists(input_file):
        print("El archivo debe existir", input_file)
        return -1
    
    try:
        logger_name = os.path.basename(input_file).split(".")[0].lower()
        # Metadata de los logs.
        labels = {
            "proceso_id": PROCESO_ID,
            "name": "encriptar",
            "work_file": logger_name,
            "enroute": "yes",
        }
        logger = logging_client.logger(name=logger_name, labels=labels)
    except Exception as error:
        print("Error al crear el logger", error)
        return -1
    
    # print("Se inicia el proceso de encriptacion.")
    logger.log_text("Se inicia el proceso de encriptacion.", severity="INFO")
    
    # print("Se procede a comprimir el archivo.")
    # # logger.log_text("Se procede a comprimir el archivo.", severity="INFO")
    # try:
    #     gzip_path = compress_data(input_file=input_file)
    # except Exception as error:
    #     error = f"Fallo (comprension del archivo): {str(error)}"
    #     print(error)
    #     # logger.log_text(error, severity="ERROR")
    #     return -1
    
    # print("Se procede a crear un blob object para almacenar el contenido encriptado.")
    logger.log_text("Se procede a crear un blob object para almacenar el contenido encriptado.", severity="INFO")
    try:
        blob_name = "data/" + START_DATE + "/" + os.path.basename(input_file).split(".")[0] + ".encrypted"
        bucket = storage_client.get_bucket(BUCKET_ENCRIPTADOS)
        blob = bucket.blob(blob_name)
    except Exception as error:
        error = f"Fallo (creacion del blob object): {str(error)}"
        # print(error)
        logger.log_text(error, severity="ERROR")
        return -1
    
    # print("Se procede a obtener la key para encriptar el archivo")
    logger.log_text("Se procede a obtener la key para encriptar el archivo", severity="INFO")
    try:
        name = f"projects/{PROJECT_ID}/secrets/{SECRET_KEY}/versions/latest"
        response_secret = secret_client.access_secret_version(name=name)
        secret_value = response_secret.payload.data.decode("UTF-8")
        key = secret_value # base64.b64decode(secret_value)
    except Exception as error:
        error = f"Fallo (obtencion del secret key): {str(error)}"
        # print(error)
        logger.log_text(error, severity="ERROR")
        return -1
        
    # print("Se procede a encriptar el archivo comprimido")
    logger.log_text("Se procede a encriptar el archivo comprimido", severity="INFO")
    try:
        blob = encrypt_data(input_file=input_file, blob=blob, key=key)
        metageneration_match_precondition = None
        metageneration_match_precondition = blob.metageneration
        blob.metadata = {"proceso_id": PROCESO_ID, "job_name": job_name}
        blob.patch(if_metageneration_match=metageneration_match_precondition)
    except Exception as error:
        error = f"Fallo (encriptacion del archivo): {str(error)}"
        # print(error)
        logger.log_text(error, severity="ERROR")
        return -1
    
    duration = round((datetime.now() - START_DATETIME).total_seconds())
    # print(f"Proceso terminado con exito ({duration}s)")
    logger.log_text(f"Proceso terminado con exito ({duration}s)", severity="INFO")
    return 1

if __name__ == "__main__":
    result = main()
    if (result == -1):
        print("Fallo el proceso")
    else:
        print("Proceso terminado con exito")
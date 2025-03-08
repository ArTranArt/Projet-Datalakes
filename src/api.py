from typing import Union
from fastapi import File, UploadFile, FastAPI, HTTPException
from io import BytesIO
import boto3
import pymysql
import pymongo
from typing import List
import json
from datetime import datetime
import sys
import time
import zipfile
from botocore.exceptions import ClientError
import os
sys.path.append('/code/src/')

from preprocess_to_staging import (
    process_s3_data_to_csv,
    process_csv_to_mysql,
    # fast_process_csv_to_mysql
)

from process_to_curated import (
    process_weather_data,
    # fast_process_weather_data
)

from fast_preprocess_to_staging import fast_process_csv_to_mysql
from fast_process_to_curated import fast_process_weather_data

app = FastAPI(title="Data Lake API")
CSV_OUTPUT_DIR = "/opt/airflow/data/staging/"

@app.get("/")
def read_root():
    return {"Hello": "World"}

class DatabaseConnections:
    def __init__(self):
        # raw - s3 localstack
        self.s3_client = boto3.client('s3',
                                      endpoint_url='http://localstack:4566',
                                      aws_access_key_id="root",
                                      aws_secret_access_key="root")
        self.bucket_name = "raw"
        
        # staging - mysql
        self.mysql_config = {
            'host': 'host.docker.internal',
            'port': 3306,
            'user': 'root',
            'password': 'root',
            'database': 'staging'
        }
        self.mysql_conn = pymysql.connect(**self.mysql_config)

        # curated - mongodb
        self.mongo_client = pymongo.MongoClient("mongodb://host.docker.internal:27017/")
        self.mongo_db = self.mongo_client["curated"]
        self.weather_stats = self.mongo_db["WeatherStats"]

db = DatabaseConnections()

@app.get("/health", tags=["Health"])
async def health_check():
    """Vérifie la santé de l'API et des connexions aux bases de données."""
    status = {
        "api_status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "connections": {},
    }
    try:
        db.s3_client.list_buckets()
        status["connections"]["s3"] = True
    except:
        status["connections"]["s3"] = False

    try:
        with db.mysql_conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        status["connections"]["mysql"] = True
    except:
        status["connections"]["mysql"] = False

    try:
        db.mongo_client.server_info()
        status["connections"]["mongodb"] = True
    except:
        status["connections"]["mongodb"] = False

    return status

@app.get("/raw/files", response_model=List[str], tags=["S3"])
async def list_files():
    """
    Liste les fichiers dans le bucket RAW.
    """
    try:
        response = db.s3_client.list_objects_v2(Bucket=db.bucket_name)
        return [obj["Key"] for obj in response.get("Contents", [])]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur S3 : {e}")

@app.get("/raw/files/{file_name}", tags=["S3"])
async def get_file(file_name: str):
    """
    Récupère le contenu d'un fichier JSON dans le bucket RAW.
    """
    try:
        obj = db.s3_client.get_object(Bucket=db.bucket_name, Key=file_name)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except db.s3_client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"Fichier '{file_name}' introuvable.")
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail=f"Fichier '{file_name}' non valide JSON.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur S3 : {e}")

@app.get("/staging/cities", response_model=List[str], tags=["MySQL"])
async def list_cities():
    """Liste les tables dans la base MySQL."""
    try:
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'staging';
        """
        with db.mysql_conn.cursor() as cursor:
            cursor.execute(query)
            tables = cursor.fetchall()
        return [table[0].replace('_', '-') for table in tables]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur MySQL : {e}")

@app.get("/staging/cities/{city}", tags=["MySQL"])
async def get_city_data(city: str):
    """Récupère les données pour une ville spécifique dans MySQL."""
    try:
        query = f"SELECT * FROM staging.{city.replace('-', '_')};"
        with db.mysql_conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail=f"Aucune donnée pour la ville : {city}")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur MySQL : {e}")

@app.get("/curated/cities", response_model=List[str], tags=["MongoDB"])
async def list_curated_cities():
    """
    Liste toutes les villes disponibles dans MongoDB.
    """
    try:
        # Utilisation de la collection 'WeatherStats' pour récupérer les villes distinctes
        curated_cities = db.weather_stats.distinct("city")
        return curated_cities
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur MongoDB : {e}")

@app.get("/curated/cities/{city}", tags=["MongoDB"])
async def get_curated_city_data(city: str):
    """
    Récupère les données agrégées pour une ville donnée dans MongoDB.
    """
    try:
        # Recherche des données en utilisant une capitalisation sur le nom de la ville
        data = list(db.weather_stats.find({"city": {"$regex": f"^{city}$", "$options": "i"}}, {"_id": 0})) 
        if not data:
            raise HTTPException(status_code=404, detail=f"Aucune donnée pour la ville : {city}")
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur MongoDB : {e}")

@app.post("/ingest", tags=["Ingestion"])
async def ingest_file(file: UploadFile = File(...)):
    """
    Ingestion d'un fichier JSON ou ZIP de fichiers JSON dans le système.
    Le fichier est traité et ses données insérées dans la base de données.
    """
    start_time = time.time()  # Enregistrer l'heure de départ
    try:
        # Vérifier si c'est un fichier ZIP ou JSON
        file_ext = file.filename.split('.')[-1].lower()
        
        try:
            bucket_name = db.bucket_name
            db.s3_client.head_bucket(Bucket=bucket_name)
            print(f"Le bucket {bucket_name} existe déjà.")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"Le bucket {bucket_name} n'existe pas, création en cours...")
                try:
                    db.s3_client.create_bucket(Bucket=bucket_name)  # Remplace par ta région si nécessaire
                    print(f"Bucket {bucket_name} créé avec succès.")
                except ClientError as create_error:
                    print(f"Erreur lors de la création du bucket : {create_error}")
                    return
            else:
                print(f"Erreur lors de la vérification du bucket : {e}")
                return

        if file_ext == 'json':
            # Traitement du fichier JSON unique
            print(f"Processing JSON file {file.filename}...")
            db.s3_client.upload_fileobj(file.file, db.bucket_name, file.filename)
            process_s3_data_to_csv("raw")
            process_csv_to_mysql()
            process_weather_data()
        elif file_ext == 'zip':
            # Traitement du fichier ZIP
            print(f"Processing ZIP file {file.filename}...")
            with zipfile.ZipFile(file.file, 'r') as zip_ref:
                # Extraire les fichiers du ZIP
                for zip_file_name in zip_ref.namelist():
                    print(f"Extracting file {zip_file_name} from ZIP...")
                    with zip_ref.open(zip_file_name) as file_in_zip:
                        db.s3_client.upload_fileobj(file_in_zip, db.bucket_name, zip_file_name)
                        process_s3_data_to_csv("raw")
                process_csv_to_mysql()
                process_weather_data()

        else:
            raise HTTPException(status_code=400, detail="Format de fichier non supporté. Accepte uniquement .json ou .zip")

        # Mesurer le temps écoulé
        elapsed_time = time.time() - start_time

        return {"message": f"Le fichier '{file.filename}' a été ingéré avec succès en {elapsed_time:.2f} secondes."}

    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Fichier JSON non valide. Erreur : {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur d'ingestion : {e}")
    
@app.post("/ingest-fast", tags=["Ingestion"])
async def ingest_fast(file: UploadFile = File(...)):
    start_time = time.time()
    try:
        file_ext = file.filename.split('.')[-1].lower()
        try:
            bucket_name = db.bucket_name
            db.s3_client.head_bucket(Bucket=bucket_name)
            print(f"Le bucket {bucket_name} existe déjà.")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"Le bucket {bucket_name} n'existe pas, création en cours...")
                try:
                    db.s3_client.create_bucket(Bucket=bucket_name)  # Remplace par ta région si nécessaire
                    print(f"Bucket {bucket_name} créé avec succès.")
                except ClientError as create_error:
                    print(f"Erreur lors de la création du bucket : {create_error}")
                    return
            else:
                print(f"Erreur lors de la vérification du bucket : {e}")
                return

        if file_ext == 'json':
            # Traitement du fichier JSON unique
            print(f"Processing JSON file {file.filename}...")
            db.s3_client.upload_fileobj(file.file, db.bucket_name, file.filename)
            process_s3_data_to_csv("raw")
            fast_process_csv_to_mysql()
            fast_process_weather_data()
        elif file_ext == 'zip':
            # Traitement du fichier ZIP
            print(f"Processing ZIP file {file.filename}...")
            with zipfile.ZipFile(file.file, 'r') as zip_ref:
                # Extraire les fichiers du ZIP
                for zip_file_name in zip_ref.namelist():
                    print(f"Extracting file {zip_file_name} from ZIP...")
                    with zip_ref.open(zip_file_name) as file_in_zip:
                        db.s3_client.upload_fileobj(file_in_zip, db.bucket_name, zip_file_name)
                        process_s3_data_to_csv("raw")
                fast_process_csv_to_mysql()
                fast_process_weather_data()

        else:
            raise HTTPException(status_code=400, detail="Format de fichier non supporté. Accepte uniquement .json ou .zip")

        # Mesurer le temps écoulé
        elapsed_time = time.time() - start_time

        return {"message": f"Le fichier '{file.filename}' a été ingéré avec succès en {elapsed_time:.2f} secondes."}

    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Fichier JSON non valide. Erreur : {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur d'ingestion : {e}")

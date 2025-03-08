import boto3
import pandas as pd
import json
import os
from io import BytesIO
import time
from tqdm import tqdm
import pymysql

# Configuration
BUCKET_NAME = "raw"
COLUMNS_USED = ['date', 'nom', 'pmer', 'tend', 'cod_tend', 'dd', 'ff', 'td', 'u', 'ww', 'pres', 'rafper', 'rr1', 'rr3', 'tc']
CSV_OUTPUT_DIR = "/opt/airflow/data/staging/"

# Connexion MySQL
conn = pymysql.connect(
    host="mysql",
    port=3306,
    user="root",
    password="root",
    database="staging"
)
cursor = conn.cursor()

# Connexion S3
s3 = boto3.client(
    "s3",
    endpoint_url="http://localstack:4566",
    aws_access_key_id="root",
    aws_secret_access_key="root"
)

# Fonctions S3
def get_s3_objects(bucket_name, prefix=""):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return response.get("Contents", [])
    except Exception as e:
        print(f"Erreur lors de la récupération des objets S3 : {e}")
        return []

def get_s3_object_data(bucket_name, object_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        data_bytes = response["Body"].read()
        return json.loads(data_bytes)
    except Exception as e:
        print(f"Erreur lors de la récupération de l'objet {object_key} : {e}")
        return None

# Traitement des données
def json_to_dataframe(json_data):
    try:
        return pd.DataFrame(json_data)
    except ValueError as e:
        print(f"Erreur lors de la conversion en DataFrame : {e}")
        return pd.DataFrame()

def clean_file_name(file_name):
    return file_name.replace("METEO", "").replace(" ", "_").replace("-", "_").replace("'", "_")

def process_s3_data_to_csv(bucket_name):
    objects = get_s3_objects(bucket_name)
    data_by_nom = {}

    for obj in tqdm(objects, desc="Traitement des objets S3"):
        key = obj["Key"]
        if not key.endswith(".json"):
            continue

        json_data = get_s3_object_data(bucket_name, key)
        if json_data:
            df = json_to_dataframe(json_data)
            if not df.empty:
                df_filtered = df[COLUMNS_USED]
                for nom in df_filtered['nom'].unique():
                    df_nom = df_filtered[df_filtered['nom'] == nom]
                    if nom not in data_by_nom:
                        data_by_nom[nom] = []
                    data_by_nom[nom].append(df_nom)

    if not os.path.exists(CSV_OUTPUT_DIR):
        os.makedirs(CSV_OUTPUT_DIR)

    for nom, df_list in data_by_nom.items():
        final_df = pd.concat(df_list, ignore_index=True)
        cleaned_file_name = clean_file_name(nom)
        file_name = f"{cleaned_file_name}.csv"
        csv_path = os.path.join(CSV_OUTPUT_DIR, file_name)
        try:
            final_df.to_csv(csv_path, index=False)
        except Exception as e:
            print(f"Erreur lors de la sauvegarde du fichier CSV pour '{nom}' : {e}")

# Gestion MySQL
def create_table_if_not_exists(city):
    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS staging.`{city}` (
                date DATE,
                nom VARCHAR(255),
                pmer DOUBLE,
                tend DOUBLE,
                cod_tend VARCHAR(50),
                dd DOUBLE,
                ff DOUBLE,
                td DOUBLE,
                u DOUBLE,
                ww VARCHAR(10),
                pres DOUBLE,
                rafper DOUBLE,
                rr1 DOUBLE,
                rr3 DOUBLE,
                tc DOUBLE
            );
        """)
    except Exception as e:
        print(f"Erreur lors de la création de la table {city} : {e}")

def insert_data_from_csv(city, csv_file_path):
    try:
        city_table = city.replace("-", "_")
        create_table_if_not_exists(city_table)
        
        df = pd.read_csv(csv_file_path).fillna(0)  # Remplace les NaN par des chaînes vides
        insert_query = f"""
            INSERT INTO staging.`{city_table}` ({', '.join(df.columns)})
            VALUES ({', '.join(['%s'] * len(df.columns))});
        """
        data = [tuple(row) for row in df.values]
        cursor.executemany(insert_query, data)
        conn.commit()
    except Exception as e:
        print(f"Erreur lors de l'insertion dans la table {city} : {e}")

def process_csv_to_mysql():
    for csv_file in os.listdir(CSV_OUTPUT_DIR):
        if csv_file.endswith(".csv"):
            csv_file_path = os.path.join(CSV_OUTPUT_DIR, csv_file)
            city = os.path.splitext(csv_file)[0].replace("-", "_")
            insert_data_from_csv(city, csv_file_path)

# def fast_process_csv_to_mysql():
#     for csv_file in os.listdir(CSV_OUTPUT_DIR):
#         if csv_file.endswith(".csv"):
#             csv_file_path = os.path.join(CSV_OUTPUT_DIR, csv_file)
#             city = os.path.splitext(csv_file)[0].replace("-", "_")
#             insert_data_from_csv(city, csv_file_path)

# Main
if __name__ == "__main__":
    start_time = time.time()
    try:
        print("Traitement des JSON vers CSV...")
        process_s3_data_to_csv(BUCKET_NAME)

        print("Insertion des données dans MySQL...")
        process_csv_to_mysql()
    except Exception as e:
        print(f"Erreur lors de l'exécution principale : {e}")
    finally:
        cursor.close()
        conn.close()

    print(f"Processus terminé en {round(time.time() - start_time, 2)} secondes.")
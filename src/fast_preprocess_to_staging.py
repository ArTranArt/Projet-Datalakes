import boto3
import pandas as pd
import json
import os
import time
import pymysql
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Configuration
BUCKET_NAME = "raw"
COLUMNS_USED = ['date', 'nom', 'pmer', 'tend', 'cod_tend', 'dd', 'ff', 'td', 'u', 'ww', 'pres', 'rafper', 'rr1', 'rr3', 'tc']
CSV_OUTPUT_DIR = "/opt/airflow/data/staging/"
MAX_WORKERS = 4  # Ajuste selon les ressources de la machine

# Connexion MySQL
conn = pymysql.connect(
    host="mysql",
    port=3306,
    user="root",
    password="root",
    database="staging",
    autocommit=True  # Améliore les performances en évitant commit() après chaque insertion
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
        print(f"Erreur S3 : {e}")
        return []

def get_s3_object_data(bucket_name, object_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        return json.loads(response["Body"].read())
    except Exception as e:
        print(f"Erreur S3 ({object_key}) : {e}")
        return None

# Traitement des données
def json_to_dataframe(json_data):
    try:
        return pd.DataFrame(json_data)
    except ValueError as e:
        print(f"Erreur DataFrame : {e}")
        return pd.DataFrame()

def clean_file_name(file_name):
    return file_name.replace("METEO", "").replace(" ", "_").replace("-", "_").replace("'", "_")

def process_s3_data_to_csv(bucket_name):
    objects = get_s3_objects(bucket_name)
    data_by_nom = {}

    # Parallélisation du téléchargement et du traitement JSON
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_s3_object_data, bucket_name, obj["Key"]): obj["Key"] for obj in objects if obj["Key"].endswith(".json")}

        for future in tqdm(futures, desc="Traitement S3"):
            key = futures[future]
            json_data = future.result()
            if json_data:
                df = json_to_dataframe(json_data)
                if not df.empty:
                    df_filtered = df[COLUMNS_USED]
                    for nom in df_filtered['nom'].unique():
                        df_nom = df_filtered[df_filtered['nom'] == nom]
                        if nom not in data_by_nom:
                            data_by_nom[nom] = []
                        data_by_nom[nom].append(df_nom)

    os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)

    # Sauvegarde des CSV
    for nom, df_list in data_by_nom.items():
        final_df = pd.concat(df_list, ignore_index=True)
        final_df.to_csv(os.path.join(CSV_OUTPUT_DIR, f"{clean_file_name(nom)}.csv"), index=False)

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
        print(f"Erreur table {city} : {e}")

def insert_data_from_csv(city, csv_file_path):
    try:
        city_table = city.replace("-", "_")
        create_table_if_not_exists(city_table)

        df = pd.read_csv(csv_file_path, dtype={"cod_tend": str, "ww": str}, low_memory=False).fillna(0)
        insert_query = f"""
            INSERT INTO staging.`{city_table}` ({', '.join(df.columns)})
            VALUES ({', '.join(['%s'] * len(df.columns))});
        """
        cursor.executemany(insert_query, [tuple(row) for row in df.values])
    except Exception as e:
        print(f"Erreur insertion {city} : {e}")

def fast_process_csv_to_mysql():
    csv_files = [f for f in os.listdir(CSV_OUTPUT_DIR) if f.endswith(".csv")]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(insert_data_from_csv, os.path.splitext(f)[0], os.path.join(CSV_OUTPUT_DIR, f)): f for f in csv_files}

        for future in tqdm(futures, desc="Insertion MySQL"):
            future.result()  # Vérifie les erreurs

# Main
if __name__ == "__main__":
    start_time = time.time()
    try:
        print("Traitement JSON -> CSV...")
        process_s3_data_to_csv(BUCKET_NAME)

        print("Insertion MySQL...")
        fast_process_csv_to_mysql()
    except Exception as e:
        print(f"Erreur : {e}")
    finally:
        cursor.close()
        conn.close()

    print(f"Terminé en {round(time.time() - start_time, 2)}s.")
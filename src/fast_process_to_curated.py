from sqlalchemy import create_engine, text
import pymongo
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# ----------------------------------------
# Configuration des connexions
# ----------------------------------------

mysql_engine = create_engine('mysql+pymysql://root:root@host.docker.internal:3306/staging', pool_pre_ping=True)

mongo_client = pymongo.MongoClient("mongodb://host.docker.internal:27017/")
mongo_db = mongo_client["curated"]
weather_stats = mongo_db["WeatherStats"]

# ----------------------------------------
# Récupérer la liste des tables dans MySQL
# ----------------------------------------

def get_mysql_tables():
    """Récupère la liste des tables dans la base de données MySQL."""
    with mysql_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'staging';
        """))
        return [row[0] for row in result]

# Liste des colonnes à arrondir
columns_to_round = [
    "avg_temp", "min_temp", "max_temp",
    "avg_pressure", "min_pressure", "max_pressure",
    "avg_wind_speed", "max_wind_speed",
    "total_rainfall", "days_with_rain"
]

# ----------------------------------------
# Récupérer et traiter les données de MySQL
# ----------------------------------------

def get_weather_data_from_mysql(city):
    """Récupère les données météorologiques agrégées pour une ville depuis MySQL."""
    query = f"""
    SELECT 
        DATE(date) AS period,
        AVG(tc) AS avg_temp, MIN(tc) AS min_temp, MAX(tc) AS max_temp,
        AVG(pmer) AS avg_pressure, MIN(pmer) AS min_pressure, MAX(pmer) AS max_pressure,
        AVG(ff) AS avg_wind_speed, MAX(ff) AS max_wind_speed,
        SUM(rr1) AS total_rainfall, COUNT(CASE WHEN rr1 > 0 THEN 1 END) AS days_with_rain
    FROM `{city}`
    GROUP BY DATE(date);
    """
    return pd.read_sql_query(query, mysql_engine)

def round_columns(df):
    """Arrondir les colonnes spécifiées à 3 chiffres après la virgule."""
    return df.round({col: 3 for col in columns_to_round})

# ----------------------------------------
# Insertion des données dans MongoDB
# ----------------------------------------

def insert_data_to_mongo(city, df):
    """Insère les données traitées dans MongoDB."""
    if df.empty:
        return
    
    df["city"] = city.capitalize()
    df["period"] = df["period"].astype(str)
    
    records = df.to_dict("records")
    for record in records:
        record["metrics"] = {
            "temperature": {key: record.pop(key) for key in ["avg_temp", "min_temp", "max_temp"]},
            "pressure": {key: record.pop(key) for key in ["avg_pressure", "min_pressure", "max_pressure"]},
            "wind_speed": {key: record.pop(key) for key in ["avg_wind_speed", "max_wind_speed"]},
            "rainfall": {key: record.pop(key) for key in ["total_rainfall", "days_with_rain"]}
        }
        record["extreme_events"] = []
    
    weather_stats.insert_many(records)

# ----------------------------------------
# Main Process (Parallélisé)
# ----------------------------------------

def process_city(city):
    """Processus parallèle pour une ville."""
    df = get_weather_data_from_mysql(city)
    df = round_columns(df)
    insert_data_to_mongo(city, df)

def fast_process_weather_data():
    """Exécute le processus complet avec parallélisation."""
    cities = get_mysql_tables()

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(tqdm(executor.map(process_city, cities), total=len(cities), desc="Traitement des villes"))

    print("Données insérées avec succès dans MongoDB.")

# ----------------------------------------
# Exécution
# ----------------------------------------

if __name__ == "__main__":
    fast_process_weather_data()
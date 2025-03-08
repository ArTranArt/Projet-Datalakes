# Introduction
Dans le cadre de mon cours de Datalakes & Data Integration à l’EFREI, j’ai développé un projet consistant à mettre en place un Data Lake pour le traitement de données météorologiques. Ce projet vise à démontrer la mise en place d’une architecture de données moderne en intégrant plusieurs étapes du pipeline de données : extraction, transformation, stockage et exposition via une interface Streamlit. L’objectif principal est de prouver ma maîtrise des outils de Data Lake et des architectures de gestion de données.

# Architecture
![Architecture du projet](assets/architecture.png)

# Explications
### `run_big_fetch`: 
On récupère les données météo de décembre 2023 sous format JSON. 
### `run_unpack_to_raw` :
On télécharge les fichiers JSON vers un bucket S3 simulé par LocalStack.
### `run_preprocess_to_staging`: 
- Récupération des fichiers JSON depuis le bucket S3.
- Conversion des fichiers JSON en DataFrames et concaténation des données.
- Filtrage des colonnes pertinentes dans le DataFrame.
- Sauvegarde des données filtrées en fichiers CSV par ville dans un répertoire de staging.
- Insertion des données CSV dans une base MySQL en créant dynamiquement une table par ville.
### `run_process_to_curated`: 
- Connexion à MySQL via **SQLAlchemy** et à MongoDB via **pymongo**.
- Récupération des noms de tables (villes) depuis la base de données staging de MySQL.
- Pour chaque ville, exécution d’une requête SQL pour agréger les données météo (température, pression, vent, pluie) par jour.
- Transformation des données en documents MongoDB avec une structure imbriquée (ex : température, pression, vent, pluie sous des clés distinctes).
- Insertion des documents dans la collection WeatherStats de MongoDB.
### `api.py` :
- API FastAPI permettant d’interagir avec les données météo.
- Connexion à S3, MySQL et MongoDB via une classe DatabaseConnections.
- Vérification de la santé de l’API et des bases (/health).
- Liste et récupération des fichiers depuis le bucket S3 (/raw/files).
- Récupération des tables (villes) depuis MySQL (/staging/cities).
- Récupération des données agrégées depuis MongoDB (/curated/cities).
- Endpoint d’ingestion permettant d’uploader des fichiers JSON ou ZIP et de les traiter (`/ingest`).
### `streamlit.py` :
- Interface interactive Streamlit permettant de visualiser les données météo.
- Connexion à l’API FastAPI pour récupérer les données.
- Sélection d’une ville et affichage des données brutes depuis MySQL.
- Visualisation des données agrégées depuis MongoDB.
- Graphiques interactifs (température, pression, vent, pluie) avec Matplotlib ou Plotly.

# Installation et Build du Projet
**Prérequis** : Avoir git et Docker installés.
1. Clonez le dépôt :
 ```
git clone https://github.com/ArTranArt/Projet-Datalakes.git
```
2. Démarrez Docker.
3. Dans le dossier du projet, construisez les containers avec la commande :
```
docker compose up --build -d
```
4. Vérifiez que tous les containers (sauf `airflow-init`) sont bien lancés. Si nécessaire, relancez la commande.

5. Accédez au webserver d'Airflow via `http://localhost:8081`. Les identifiants sont: `airflow:airflow`.

6. Lancez le DAG `datalake-pipeline` et attendez la fin de l’exécution. Si des données sont manquantes, il est possible de vérifier la source des données météo sur [OpenDataSoft](https://public.opendatasoft.com/explore/dataset/donnees-synop-essentielles-omm).

7. Accédez à l'API via `http://localhost:8000` pour tester les endpoints. L’endpoint `/ingest` permet d’ingérer des données à partir du fichier data/ingestion/2024.json, tandis que l’endpoint `/ingest-fast` est plus rapide et performant.

8.	Pour redémarrer le container Streamlit, utilisez la commande suivante :
```
docker compose restart streamlit
```

9.	Accédez à l’interface Streamlit via `http://localhost:8501` pour analyser les données météo des villes sur une période donnée (température, pression, vent, pluie).
Accédez à `streamlit` sur `http://localhost:8501`.

10.	Pour arrêter le projet et supprimer les volumes, exécutez :
```
docker compose down -v
```

# Résultats des tests de performance

La principale différence entre `/ingest` et `/ingest-fast` réside dans l’utilisation de `fast_process_csv_to_mysql()` et `fast_process_weather_data()` à la place de `process_csv_to_mysql()` et `process_weather_data()`, respectivement.

L'utilisation de **ThreadPoolExecutor** a significativement amélioré la vitesse d'exécution :
- `fast_process_csv_to_mysql()` télécharge et traite les fichiers JSON en parallèle, au lieu de les traiter un par un. Cela accélère le chargement des données depuis S3 et leur conversion en DataFrame.
- `fast_process_weather_data()` utilise `ThreadPoolExecutor(max_workers=4)` pour traiter plusieurs villes simultanément, alors que `process_weather_data()` les traitait de manière séquentielle, ce qui était plus lent.

En plus du threading, d'autres optimisations ont été apportées aux deux fonctions.

## Optimisations

### fast_process_csv_to_mysql()
- Ajout de `autocommit=True` dans la connexion MySQL, évitant ainsi les `conn.commit()` manuels après chaque insertion et réduisant le temps d’exécution.
- Optimisation de l’écriture des fichiers CSV : la version rapide regroupe et traite les fichiers de manière plus efficace, limitant les opérations séquentielles coûteuses.

### fast_process_weather_data()
- Modularisation du traitement d’une ville via une nouvelle fonction `process_city(city)`, qui est ensuite exécutée en parallèle grâce à `executor.map(process_city, cities)`, accélérant le traitement global.
- Utilisation d'une table de hachage `(records = df.to_dict("records"))` et de `insert_many()` au lieu de `insert_one()`, ce qui réduit le nombre d’appels à MongoDB et améliore les performances.
- Optimisation de `round_columns()` avec `df.round({col: 3 for col in columns_to_round})`, au lieu d’une modification directe des colonnes, ce qui est plus efficace.

## Gains de performance
- **Ingestion standard (`/ingest`)** : environ **20 secondes**
- **Ingestion rapide (`/ingest-fast`)** : environ **8 secondes**
- **Gain de performance** : **~60% de réduction du temps d’exécution**

Ces résultats démontrent que l’endpoint `/ingest-fast` offre une nette amélioration des performances, rendant le traitement des données plus rapide et efficace, un atout essentiel en environnement de production.

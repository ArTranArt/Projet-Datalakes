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
- **Ingestion standard (`/ingest`)** : environ 20 secondes
- **Ingestion rapide (`/ingest-fast`)** : environ 8 secondes

Ces résultats montrent que l’endpoint /ingest-fast offre des performances améliorées, permettant de traiter plus rapidement les données, ce qui est essentiel dans un environnement de production.

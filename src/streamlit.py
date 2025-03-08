import streamlit as st
import pandas as pd
import pymongo

mongo_client = pymongo.MongoClient("mongodb://host.docker.internal:27017/")
mongo_db = mongo_client["curated"]
weather_stats = mongo_db["WeatherStats"]

# Récupérer les données depuis MongoDB
@st.cache_data
def fetch_data():
    data = list(weather_stats.find({}, {"_id": 0}))  # Exclure l'_id pour éviter des problèmes d'affichage
    return data

# Charger les données dans un DataFrame
data = fetch_data()
df = pd.json_normalize(data, sep="_")

# Convertir 'period' en format datetime
df["period"] = pd.to_datetime(df["period"])

# Interface Streamlit
st.title("Visualisation des données météorologiques (Curated)")

# Déplacer les filtres dans la barre latérale
with st.sidebar:
    st.header("Filtres")
    
    # Choisir l'année
    years = df["period"].dt.year.unique()
    selected_year = st.selectbox("Sélectionnez une année :", options=sorted(years))
    
    # Sélectionner une plage de mois
    selected_month_range = st.slider("Sélectionnez une plage de mois", min_value=1, max_value=12, value=(1, 12))
    
    # Filtrage par ville
    cities = sorted(df["city"].unique())
    selected_city = st.selectbox("Sélectionnez une ville :", options=cities)
    
    # Filtrage par métrique
    metrics = ["temperature", "pressure", "wind_speed", "rainfall"]
    selected_metric = st.selectbox("Sélectionnez une métrique :", options=metrics)
    

# Appliquer les filtres
filtered_df = df[(df["city"] == selected_city) & (df["period"].dt.year == selected_year)]
filtered_df = filtered_df[filtered_df["period"].dt.month.between(*selected_month_range)]

# Filtrer les colonnes en fonction de la métrique sélectionnée
metric_columns = [col for col in filtered_df.columns if col.startswith(f"metrics_{selected_metric}")]

# Afficher les données filtrées
st.write(f"### Données pour la ville : {selected_city}")
st.write(f"Année sélectionnée : {selected_year}")
st.write(f"Plage de mois : {selected_month_range[0]} - {selected_month_range[1]}")

if not filtered_df.empty:
    # Affichage du graphique avec la métrique sélectionnée
    metric_values = filtered_df.set_index("period")[metric_columns]
    st.line_chart(metric_values)

    # Afficher des statistiques supplémentaires selon la métrique choisie
    if selected_metric == "temperature":
        # Température maximale
        max_temp = filtered_df["metrics_temperature_max"].max()
        max_temp_date = filtered_df.loc[filtered_df["metrics_temperature_max"] == max_temp, "period"].values[0]
        st.write(f"📈 **Température maximale** : {max_temp}°C atteint le {pd.to_datetime(max_temp_date).strftime('%Y-%m-%d')}")
        
        # Température minimale différente de 0
        non_zero_min_temps = filtered_df.loc[filtered_df["metrics_temperature_min"] != 0, "metrics_temperature_min"]
        if not non_zero_min_temps.empty:
            min_temp = non_zero_min_temps.min()
            min_temp_date = filtered_df.loc[filtered_df["metrics_temperature_min"] == min_temp, "period"].values[0]
            st.write(f"📉 **Température minimale** : {min_temp}°C atteint le {pd.to_datetime(min_temp_date).strftime('%Y-%m-%d')}")
    
else:
    st.write("Aucune donnée disponible pour les filtres sélectionnés.")
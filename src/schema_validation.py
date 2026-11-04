"""
Module de validation des schémas et de conformité des données météorologiques.
Assure la cohérence des données brutes ingérées avant ingestion dans le Datalake (Staging / Curated).
"""

from typing import List, Dict, Any, Tuple, Union
import pandas as pd

# Colonnes obligatoires pour les données météo SYNOP en staging
REQUIRED_WEATHER_COLUMNS = [
    'date', 'nom', 'pmer', 'tend', 'cod_tend',
    'dd', 'ff', 'td', 'u', 'ww', 'pres', 'rafper',
    'rr1', 'rr3', 'tc'
]

# Bornes physiques météorologiques pour le contrôle qualité des données
METRIC_BOUNDS = {
    'tc': (-60.0, 60.0),       # Température en °C
    'u': (0.0, 100.0),          # Humidité relative en %
    'pmer': (80000.0, 110000.0),# Pression au niveau de la mer en Pa (ou 800 à 1100 hPa)
    'ff': (0.0, 150.0),         # Vitesse du vent en m/s
    'rr1': (0.0, 500.0),        # Précipitations sur 1h en mm
    'rr3': (0.0, 1000.0),       # Précipitations sur 3h en mm
}


def validate_weather_schema(data: Union[pd.DataFrame, List[Dict[str, Any]]]) -> Tuple[bool, List[str]]:
    """
    Vérifie que toutes les colonnes obligatoires sont présentes dans les données ingérées.
    
    :param data: DataFrame pandas ou liste de dictionnaires (JSON)
    :return: (is_valid, missing_columns)
    """
    if isinstance(data, list):
        if not data:
            return False, REQUIRED_WEATHER_COLUMNS
        columns = set(data[0].keys())
    elif isinstance(data, pd.DataFrame):
        if data.empty:
            return False, REQUIRED_WEATHER_COLUMNS
        columns = set(data.columns)
    else:
        raise TypeError("Les données doivent être un DataFrame pandas ou une liste de dictionnaires.")

    missing = [col for col in REQUIRED_WEATHER_COLUMNS if col not in columns]
    return (len(missing) == 0, missing)


def validate_weather_metrics(df: pd.DataFrame) -> Tuple[bool, Dict[str, int]]:
    """
    Contrôle que les métriques physiques respectent des bornes réalistes.
    
    :param df: DataFrame pandas contenant les données météo
    :return: (is_conform, out_of_bounds_counts)
    """
    out_of_bounds = {}
    for col, (min_val, max_val) in METRIC_BOUNDS.items():
        if col in df.columns:
            series = pd.to_numeric(df[col], errors='coerce').dropna()
            # Pour la pression, adapter si exprimée en hPa ou Pa
            if col == 'pmer' and series.mean() < 2000:
                # La valeur est en hPa (ex: 1013.25)
                invalid_count = ((series < 800.0) | (series > 1100.0)).sum()
            else:
                invalid_count = ((series < min_val) | (series > max_val)).sum()
            
            if invalid_count > 0:
                out_of_bounds[col] = int(invalid_count)

    return (len(out_of_bounds) == 0, out_of_bounds)

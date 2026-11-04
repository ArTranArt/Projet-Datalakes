import pandas as pd
import pytest
from schema_validation import (
    REQUIRED_WEATHER_COLUMNS,
    validate_weather_schema,
    validate_weather_metrics,
)
from preprocess_to_staging import (
    COLUMNS_USED,
    json_to_dataframe,
    clean_file_name,
)


def test_schema_columns_definition():
    """Test 2.1 : Vérifie que le schéma attendu contient les 15 colonnes météorologiques clés."""
    assert len(REQUIRED_WEATHER_COLUMNS) == 15
    assert set(REQUIRED_WEATHER_COLUMNS) == set(COLUMNS_USED)
    assert "date" in REQUIRED_WEATHER_COLUMNS
    assert "nom" in REQUIRED_WEATHER_COLUMNS
    assert "tc" in REQUIRED_WEATHER_COLUMNS  # Température
    assert "pmer" in REQUIRED_WEATHER_COLUMNS  # Pression
    assert "rr1" in REQUIRED_WEATHER_COLUMNS  # Précipitations


def test_validate_weather_schema_valid_dataset(sample_weather_dataset):
    """Test 2.2 : Valide un jeu de données conforme aux spécifications du Datalake."""
    is_valid, missing = validate_weather_schema(sample_weather_dataset)
    assert is_valid is True
    assert len(missing) == 0


def test_validate_weather_schema_missing_columns(sample_invalid_weather_record):
    """Test 2.3 : Détecte immédiatement les colonnes manquantes dans un enregistrement corrompu."""
    is_valid, missing = validate_weather_schema([sample_invalid_weather_record])
    assert is_valid is False
    assert "date" in missing
    assert "nom" in missing
    assert "tc" in missing


def test_json_to_dataframe_and_clean_file_name(sample_weather_dataset):
    """Test 2.4 : Vérifie la transformation JSON -> DataFrame et le nettoyage des identifiants de stations."""
    # Conversion JSON en DataFrame
    df = json_to_dataframe(sample_weather_dataset)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "nom" in df.columns

    # Filtrage des colonnes utilisées
    filtered_df = df[COLUMNS_USED]
    assert list(filtered_df.columns) == COLUMNS_USED

    # Test du nettoyage des noms de stations pour MySQL (suppression de "METEO", tirets, apostrophes)
    raw_name_1 = "METEO PARIS-MONTSOURIS"
    raw_name_2 = "NICE-COTE D'AZUR"

    clean_name_1 = clean_file_name(raw_name_1)
    clean_name_2 = clean_file_name(raw_name_2)

    assert clean_name_1 == "_PARIS_MONTSOURIS" or "PARIS_MONTSOURIS" in clean_name_1
    assert "-" not in clean_name_1
    assert "'" not in clean_name_2


def test_weather_metrics_physical_bounds():
    """Test 2.5 : Contrôle qualité des bornes physiques des métriques météo."""
    # Données normales
    df_valid = pd.DataFrame([{
        "tc": 18.2,
        "u": 75.0,
        "pmer": 101325.0,
        "ff": 5.4,
        "rr1": 0.0,
        "rr3": 0.0,
    }])
    is_valid, out_of_bounds = validate_weather_metrics(df_valid)
    assert is_valid is True
    assert len(out_of_bounds) == 0

    # Données avec valeurs aberrantes (ex: température 150°C, humidité 180%)
    df_aberrant = pd.DataFrame([{
        "tc": 150.0,  # Impossible sur Terre
        "u": 180.0,   # Humidité > 100%
        "pmer": 101325.0,
    }])
    is_valid, out_of_bounds = validate_weather_metrics(df_aberrant)
    assert is_valid is False
    assert "tc" in out_of_bounds
    assert "u" in out_of_bounds

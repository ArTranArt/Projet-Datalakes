import pytest
import sys
import os

# Ajouter le répertoire src/ au PYTHONPATH pour les imports des tests
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

@pytest.fixture
def sample_valid_weather_record():
    """Échantillon de donnée météorologique brute valide conforme au schéma SYNOP."""
    return {
        "date": "2023-12-01T12:00:00+00:00",
        "nom": "METEO PARIS-MONTSOURIS",
        "pmer": 101500.0,
        "tend": 12.0,
        "cod_tend": "2",
        "dd": 240.0,
        "ff": 4.5,
        "td": 5.2,
        "u": 85.0,
        "ww": "02",
        "pres": 100800.0,
        "rafper": 8.0,
        "rr1": 0.0,
        "rr3": 0.4,
        "tc": 8.5
    }

@pytest.fixture
def sample_weather_dataset(sample_valid_weather_record):
    """Jeu de données de plusieurs stations météo."""
    record_nice = {
        "date": "2023-12-01T12:00:00+00:00",
        "nom": "NICE-COTE D'AZUR",
        "pmer": 101800.0,
        "tend": 5.0,
        "cod_tend": "1",
        "dd": 180.0,
        "ff": 2.1,
        "td": 8.0,
        "u": 65.0,
        "ww": "00",
        "pres": 101600.0,
        "rafper": 5.0,
        "rr1": 0.0,
        "rr3": 0.0,
        "tc": 15.2
    }
    return [sample_valid_weather_record, record_nice]

@pytest.fixture
def sample_invalid_weather_record():
    """Donnée météo incomplète manquant de colonnes critiques (tc, date, nom)."""
    return {
        "pmer": 101500.0,
        "ff": 4.5,
        "u": 85.0
    }

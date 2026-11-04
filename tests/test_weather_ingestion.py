import json
from unittest.mock import patch, MagicMock
import requests
from fetch import (
    get_date_range,
    fetch_data_from_api,
    save_data_to_file,
    load_data_from_file,
    count_data_elements,
)


def test_get_date_range_with_full_date():
    """Test 1.1 : Vérifie le calcul de la plage horaire d'ingestion pour une date précise (année, mois, jour)."""
    start, end = get_date_range(year=2024, month=1, day=15)
    assert start.startswith("2024-01-15T00:00:00")
    assert end.startswith("2024-01-15T23:59:59")


def test_get_date_range_with_month_only():
    """Test 1.2 : Vérifie le calcul de la plage pour un mois complet (ex: décembre)."""
    start, end = get_date_range(year=2023, month=12)
    assert start.startswith("2023-12-01T00:00:00")
    assert end.startswith("2023-12-31T23:59:59")


def test_fetch_data_from_api_success(sample_weather_dataset):
    """Test 1.3 : Ingestion simulée réussie depuis l'API OpenDataSoft via mock HTTP."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_weather_dataset
    mock_response.raise_for_status = MagicMock()

    with patch("requests.get", return_value=mock_response) as mock_get:
        data = fetch_data_from_api(
            url="https://fake-meteo-api.com/exports/json",
            params={"where": "date >= '2023-12-01'"},
            headers={"accept": "*/*"},
        )
        assert data is not None
        assert len(data) == 2
        assert data[0]["nom"] == "METEO PARIS-MONTSOURIS"
        mock_get.assert_called_once()


def test_fetch_data_from_api_error_handling():
    """Test 1.4 : Gestion robuste des erreurs HTTP (ex: 500, timeout) lors de l'ingestion."""
    with patch("requests.get", side_effect=requests.exceptions.HTTPError("500 Server Error")):
        data = fetch_data_from_api(
            url="https://fake-meteo-api.com/exports/json",
            params={},
            headers={},
        )
        # La fonction doit retourner None sans planter
        assert data is None


def test_save_and_load_weather_data(tmp_path, sample_weather_dataset):
    """Test 1.5 : Vérifie la persistance et le rechargement local des données météo brutes (JSON)."""
    file_path = tmp_path / "raw" / "2023" / "12" / "test_data.json"

    # Sauvegarde
    save_data_to_file(sample_weather_dataset, str(file_path))
    assert file_path.exists()

    # Rechargement
    loaded = load_data_from_file(str(file_path))
    assert count_data_elements(loaded) == 2
    assert loaded[0]["tc"] == 8.5

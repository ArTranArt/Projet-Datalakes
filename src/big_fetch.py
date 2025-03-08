import os
import subprocess
import time
import argparse

def run_fetch_script(start_year, end_year):
    total_time = 0  # Initialisation du temps total
    for year in range(start_year, end_year + 1):
        print(f"Starting fetch for year {year}...")
        year_start_time = time.time()  # Temps de début pour l'année
        for month in range(1, 13):
            command = f"python /opt/airflow/scripts/fetch.py --year {year} --month {month}"
            print(f"Executing: {command}")
            try:
                result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
                print(result.stdout)  # Affiche la sortie normale
            except subprocess.CalledProcessError as e:
                print(f"Error executing {command}:")
                print(e.stderr)  # Affiche les messages d'erreur si la commande échoue
        year_end_time = time.time()  # Temps de fin pour l'année
        elapsed_time = year_end_time - year_start_time
        total_time += elapsed_time  # Ajout du temps de l'année au total
        print(f"Time taken for year {year}: {elapsed_time:.2f} seconds\n")

    # Afficher le temps total pris
    print(f"Total time taken for all years ({start_year}-{end_year}): {total_time:.2f} seconds\n")

if __name__ == "__main__":
    # Initialisation des arguments de la ligne de commande
    parser = argparse.ArgumentParser(description="Run the fetch script for a range of years.")
    parser.add_argument(
        "--start-year", type=int, required=True,
        help="The starting year for fetching data (e.g., 2021)."
    )
    parser.add_argument(
        "--end-year", type=int, required=True,
        help="The ending year for fetching data (e.g., 2023)."
    )
    args = parser.parse_args()

    # Validation des années
    if args.start_year > args.end_year:
        print("Error: Start year must be less than or equal to end year.")
        exit(1)

    # Lancement du script
    run_fetch_script(args.start_year, args.end_year)
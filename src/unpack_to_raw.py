import os
import boto3
from botocore.exceptions import ClientError
import subprocess
import argparse

def upload_to_s3(bucket_name):
    input_dir = "/opt/airflow/data/raw"
    s3 = boto3.client("s3",
                      endpoint_url="http://localstack:4566",
                      aws_access_key_id="root",
                      aws_secret_access_key="root")  # Remplace par l'URL de ton endpoint S3 local si nécessaire

    # Vérifie si le bucket existe, sinon le crée
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Le bucket {bucket_name} existe déjà.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Le bucket {bucket_name} n'existe pas, création en cours...")
            try:
                s3.create_bucket(Bucket=bucket_name)  # Remplace par ta région si nécessaire
                print(f"Bucket {bucket_name} créé avec succès.")
            except ClientError as create_error:
                print(f"Erreur lors de la création du bucket : {create_error}")
                return
        else:
            print(f"Erreur lors de la vérification du bucket : {e}")
            return

    # Télécharge les fichiers dans le bucket
    print(input_dir)
    for root, dirs, files in os.walk(input_dir):
        dirs.sort()  # Trie les dossiers par ordre alphabétique
        files.sort()  # Trie les fichiers par ordre alphabétique

        for file_name in files:
            file_path = os.path.join(root, file_name)

            # Vérifie si le fichier est un JSON ou contient 'data-' dans le nom
            if file_name.endswith('.json') or 'data-' in file_name:
                print(f"Téléchargement du fichier : {file_name} (chemin : {file_path})")

                try:
                    s3.upload_file(file_path, bucket_name, file_name)
                    print(f"Fichier {file_name} téléchargé avec succès dans le bucket {bucket_name}.")
                except Exception as e:
                    print(f"Erreur lors du téléchargement du fichier {file_name} : {e}")
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parcours récursivement les dossiers et charge les fichiers JSON dans un bucket S3.")
    parser.add_argument("--bucket_name", type=str, required=True, help="Nom du bucket S3")
    args = parser.parse_args()

    upload_to_s3(args.bucket_name)
import os
import pandas as pd
import psycopg2

# Configuration de la base de données PostgreSQL
DB_NAME = "mydatabase"
DB_USER = "user"
DB_PASSWORD = "password"
DB_HOST = "db"  # Nom du service Docker
DB_PORT = "5432"

# Nom du fichier CSV
CSV_FILE = "data.csv"

# Vérifier si le fichier CSV existe, sinon le créer avec des données fictives
if not os.path.exists(CSV_FILE):
    print("📂 Fichier data.csv introuvable, création en cours...")
    
    # Génération des données fictives
    data = {
        "nom": ["Alice", "Bob", "Charlie", "David"],
        "age": [25, 30, 35, 40],
        "salaire": [45000, 55000, 60000, 70000]
    }
    
    df = pd.DataFrame(data)
    df.to_csv(CSV_FILE, index=False)
    
    print("✅ Fichier data.csv créé avec succès !")

# Charger le fichier CSV
df = pd.read_csv(CSV_FILE)
print("\n📊 Statistiques du dataset :")
print(df.describe())  # Affiche les stats sur les colonnes numériques
print("\n🔍 Valeurs manquantes :")
print(df.isnull().sum())

# Connexion à PostgreSQL
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    print("✅ Connexion réussie à PostgreSQL !")
except Exception as e:
    print(f"❌ Erreur de connexion : {e}")
    exit()

# Création de la table dans PostgreSQL
table_name = "dataset"
with conn.cursor() as cur:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            {", ".join([f"{col} TEXT" for col in df.columns])}
        );
    """)
    conn.commit()
    print(f"📌 Table '{table_name}' prête dans PostgreSQL !")

# Insertion des données
with conn.cursor() as cur:
    for _, row in df.iterrows():
        cur.execute(f"""
            INSERT INTO {table_name} ({", ".join(df.columns)})
            VALUES ({", ".join(['%s' for _ in row])});
        """, tuple(row))
    conn.commit()
    print(f"✅ Données insérées dans '{table_name}' avec succès !")

# Fermer la connexion
conn.close()
print("🔌 Connexion PostgreSQL fermée.")

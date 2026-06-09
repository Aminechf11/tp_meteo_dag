import requests
import psycopg2

LATITUDE = 48.8566
LONGITUDE = 2.3522

DB_CONFIG = {
    "host": "localhost",
    "database": "meteo_db",
    "user": "postgres",
    "password": "postgres"
}


def recuperer_meteo():
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={LATITUDE}"
        f"&longitude={LONGITUDE}"
        f"&current=temperature_2m,wind_speed_10m"
    )

    response = requests.get(url)
    response.raise_for_status()

    return response.json()


def transformer_meteo(data):
    return {
        "ville": "Paris",
        "temperature": data["current"]["temperature_2m"],
        "vent": data["current"]["wind_speed_10m"]
    }


def charger_postgresql(data):
    conn = psycopg2.connect(**DB_CONFIG)

    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO meteo_data(ville, temperature, vent)
        VALUES (%s,%s,%s)
        """,
        (
            data["ville"],
            data["temperature"],
            data["vent"]
        )
    )

    conn.commit()

    cur.close()
    conn.close()


def enregistrer_suivi():
    conn = psycopg2.connect(**DB_CONFIG)

    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO ingestion_log(nb_lignes)
        VALUES (1)
        """
    )

    conn.commit()

    cur.close()
    conn.close()

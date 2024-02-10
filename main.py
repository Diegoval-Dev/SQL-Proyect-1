import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def conectar_db():
    DB_HOST = 'localhost'
    DB_NAME = 'proyecto1'
    DB_USER = 'postgres'
    DB_PASSWORD = 'admin'
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print("Conexión exitosa a PostgreSQL. Versión:", version[0])
        cursor.close()
    except psycopg2.Error as e:
        print("Error al conectar a PostgreSQL:", e)
    finally:
        archivos_csv = ['Data/appearances.csv', 'Data/games.csv', 'Data/leagues.csv','Data/players.csv', 'Data/shots.csv', 'Data/teams.csv', 'Data/teamstats.csv']
        for archivo in archivos_csv:
            df = pd.read_csv(archivo)
            nombre_tabla = archivo.split('/')[-1].split('.')[0]
            df.to_sql(nombre_tabla, engine, index=False, if_exists='replace') 
        conn.close()

conectar_db()




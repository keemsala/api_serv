from fastapi import FastAPI
from sqlalchemy import create_engine, text
import yaml
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL is None:
    raise ValueError("The DATABASE_URL environment variable MUST be set!")
if not DATABASE_URL.startswith("postgresql"):
   DATABASE_URL = f"postgresql://{DATABASE_URL}"

app = FastAPI()
eng = create_engine(DATABASE_URL)

def create_simple_endpoint(endpoint, query):
   """Function to manufacture simple endpoints for those without much
   Python experience.
   """
   @app.get(endpoint)
   def auto_simple_endpoint():
      f"""Automatic endpoint function for {endpoint}"""
      with eng.connect() as con:
         res = con.execute(query)
         return [r._asdict() for r in res]
            
with open("endpoints.yaml") as f:
   endpoints = yaml.safe_load(f)
   for endpoint, query in endpoints.items():
      create_simple_endpoint(endpoint, query)





#------------------------------------------------
# Custom Endpoints
#------------------------------------------------

@app.get("/weather_stuff/{page}")
def weather_stuff_by_page(page):
     with eng.connect() as con:
        query = """
                SELECT *
                FROM weather_backup_data
                ORDER BY date_time
                LIMIT 50
                OFFSET :off
                """
        res = con.execute(text(query), {'off': 50*int(page)})
        return [r._asdict() for r in res]

@app.get("/mtbh/{page}")
def mtbh_by_page(page, hour:int=None):
     with eng.connect() as con:
        query = """
                SELECT MAX(temperature) AS max_temp, DATE_PART AS hour
                FROM(SELECT DISTINCT(DATE_PART('hour', timestamp_pacific)), temperature 
                FROM weather_backup_data)
                WHERE DATE_PART IS NOT NULL
                GROUP BY DATE_PART
                ORDER BY DATE_PART
                LIMIT 50
                OFFSET :off
                """
        if hour is not None:
            query = """
                SELECT MAX(temperature) AS max_temp, DATE_PART AS hour
                FROM(SELECT DISTINCT(DATE_PART('hour', timestamp_pacific)), temperature 
                FROM weather_backup_data)
                WHERE DATE_PART IS NOT NULL
                AND DATE_PART = :hr
                GROUP BY DATE_PART
                ORDER BY DATE_PART
                LIMIT 50
                OFFSET :off
                """
        res = con.execute(text(query), {'off': 50*int(page), 'hr': hour})
        return [r._asdict() for r in res]

@app.get("/mintbh/{page}")
def mintbh_by_page(page, hour:int=None):
     with eng.connect() as con:
        query = """
                SELECT MIN(temperature) AS max_temp, DATE_PART AS hour
                FROM(SELECT DISTINCT(DATE_PART('hour', timestamp_pacific)), temperature 
                FROM weather_backup_data)
                WHERE DATE_PART IS NOT NULL
                GROUP BY DATE_PART
                ORDER BY DATE_PART
                LIMIT 50
                OFFSET :off
                """
        if hour is not None:
            query = """
                SELECT MIN(temperature) AS max_temp, DATE_PART AS hour
                FROM(SELECT DISTINCT(DATE_PART('hour', timestamp_pacific)), temperature 
                FROM weather_backup_data)
                WHERE DATE_PART IS NOT NULL
                AND DATE_PART = :hr
                GROUP BY DATE_PART
                ORDER BY DATE_PART
                LIMIT 50
                OFFSET :off
                """
        res = con.execute(text(query), {'off': 50*int(page), 'hr': hour})
        return [r._asdict() for r in res]

from dotenv import load_dotenv
import os

BASEDIR = os.path.dirname(os.path.dirname(__file__))
TABLES_INIT_PATH = os.path.join(os.path.dirname(__file__), 'sql_init.sql')
DATASET_PATH = os.path.join(BASEDIR, 'data', 'dataset_cleaned.parquet')
ENV_PATH = os.path.join(BASEDIR, '.env')
load_dotenv(ENV_PATH)

db_config = {
    'DB_NAME' : os.getenv('DB_NAME'),
    'PORT' : os.getenv('PORT'),
    'HOST' : os.getenv('HOST'),
    'USER' : 'postgres',
    'PASSWORD' : os.getenv('PASSWORD'),
}
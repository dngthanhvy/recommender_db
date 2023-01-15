import dotenv

dotenv.load_dotenv()
import os

settings = {
    'host': os.getenv('HOST'),
    'database_name': os.getenv('DATABASE_NAME'),
    'postgres_user': os.getenv('POSTGRES_USER'),
    'postgres_password': os.getenv('POSTGRES_PW'),
    'port': 5431
}

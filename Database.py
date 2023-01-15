import numpy as np
import psycopg2

from utils import db_utils


class Database:

    def __init__(self, settings):
        self._host = settings['host']
        self._port = settings['port']
        self._database = settings['database_name']
        self._user = settings['postgres_user']
        self._password = settings['postgres_password']
        self._conn = None
        print("Created a new database instance with the following settings:")
        print(f"\t - Host: {self._host}")
        print(f"\t - Port: {self._port}")
        print(f"\t - Database: {self._database}")
        print(f"\t - User: {self._user}")
        print(f"\t - Password: {self._password}")
        print("To connect to the database, use the .connect() method.\n")

    def connect(self):
        if not self._conn:
            try:
                self._conn = psycopg2.connect(
                    host=self._host,
                    port=self._port,
                    database=self._database,
                    user=self._user,
                    password=self._password
                )
                print(f"Connected to database {self._database} on {self._host}:{self._port}")
                return self._conn
            except psycopg2.Error as e:
                raise Exception(e)
        raise Exception("This instance is already connected to a database.")

    def disconnect(self):
        if self._conn:
            self._conn.close()
            print(f"Disconnected from database {self._database} on {self._host}:{self._port}")
            self._conn = None
            return True
        raise Exception("This instance is not connected to a database.")

    def find_all(self, table_name, limit=None):
        if not self._conn:
            raise Exception("This instance is not connected to a database.")
        try:
            cursor = self._conn.cursor()
            q = f"SELECT * FROM {table_name}"
            if limit:
                q += f" LIMIT {limit}"
            cursor.execute(q)
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            self._conn.rollback()
            raise Exception(e)

    def count(self, table_name):
        if not self._conn:
            raise Exception("This instance is not connected to a database.")
        try:
            cursor = self._conn.cursor()
            q = f"SELECT COUNT(*) FROM {table_name}"
            cursor.execute(q)
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            self._conn.rollback()
            raise Exception(e)

    def insert(self, table_name, df, db_utils_callback, cols=None, quiet=False):
        if not self._conn:
            raise Exception("This instance is not connected to a database.")

        print(f"Now trying to insert {len(df)} rows into table {table_name}...")

        try:
            cursor = self._conn.cursor()
            values = ','.join(db_utils_callback(df))
            if not cols:
                cols = db_utils.get_columns(df)
            q = f"INSERT INTO {table_name} {cols} VALUES {values}"
            result = cursor.execute(q)
            self._conn.commit()
            cursor.close()
            if not quiet:
                print(f"\t- Successfully inserted {len(df)} rows into table {table_name}.")
            return result
        except Exception as e:
            self._conn.rollback()
            raise Exception(e)

    def insert_chunks(self, table_name, df, db_utils_callback, cols=None, chunk_size=500_000):
        if not self._conn:
            raise Exception("This instance is not connected to a database.")

        print(f"Now trying to insert the dataframe in chunks of {chunk_size}...")

        chunked_df = np.array_split(df, round(len(df) / chunk_size))
        for chunk in chunked_df:
            self.insert(table_name, chunk, db_utils_callback, cols, quiet=True)
        print(f"Successfully inserted {len(df)} rows into table {table_name} in chunks of {chunk_size} rows.")

    def truncate(self, table_name, cascade=False):
        if not self._conn:
            raise Exception("This instance is not connected to a database.")
        try:
            cursor = self._conn.cursor()
            q = f"TRUNCATE TABLE {table_name}"
            if cascade:
                q += " CASCADE"
            cursor.execute(q)
            self._conn.commit()
            cursor.close()
            print(f"Successfully truncated table {table_name}.")
            return True
        except Exception as e:
            self._conn.rollback()
            raise Exception(e)

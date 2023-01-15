import pandas as pd

from Database import Database
from settings import settings
from utils import db_utils


class ETL:
    @staticmethod
    def extract(data_path):
        print(f"Extracting data from {data_path}...")
        dataframe = pd.read_parquet(data_path)
        clients = dataframe['CLI_ID'].unique()
        print(f"\t - Found {len(clients)} clients.")
        tickets = \
            dataframe.groupby(['CLI_ID', 'MOIS_VENTE', 'TICKET_ID'])['TICKET_ID'].count().reset_index(
                name='TICKET_COUNT')[
                ['CLI_ID', 'TICKET_ID', 'MOIS_VENTE']]
        print(f"\t - Found {len(tickets)} tickets.")
        famille = dataframe['FAMILLE'].unique()
        print(f"\t - Found {len(famille)} familles.")
        libelle = \
            dataframe.groupby(['FAMILLE', 'LIBELLE'])['LIBELLE'].count().reset_index(name='LIBELLE_COUNT')[
                ['FAMILLE', 'LIBELLE']]
        print(f"\t - Found {len(libelle)} libelles.")
        transactions = dataframe.groupby(['CLI_ID', 'TICKET_ID', 'LIBELLE', 'PRIX_NET'])['LIBELLE'].count().reset_index(
            name='QUANTITY')
        print(f"\t - Found {len(transactions)} transactions.")
        print(f"Data was successfully extracted from {data_path}.")
        return {
            'clients': clients,
            'tickets': tickets,
            'famille': famille,
            'libelle': libelle,
            'transactions': transactions
        }

    @staticmethod
    def transform(extracted_data):
        print("Transforming data...")

        clients_df = pd.DataFrame({'client_number': extracted_data['clients']}) \
            .reset_index().rename(columns={'index': 'id'})
        clients_df.id += 1
        print("\t - Clients transformed.")

        tickets_df = extracted_data['tickets'] \
            .reset_index() \
            .rename(
            columns={'CLI_ID': 'client_number', 'TICKET_ID': 'ticket_number', 'MOIS_VENTE': 'month', 'index': 'id'}) \
            .merge(right=clients_df, on='client_number') \
            .rename(columns={'id_y': 'client_id', 'id_x': 'id'}) \
            .drop(columns=['client_number'])
        tickets_df.id += 1
        print("\t - Tickets transformed.")

        familles_df = pd.DataFrame({'famille': extracted_data['famille']}) \
            .reset_index().rename(columns={'index': 'id'})
        familles_df.id += 1
        print("\t - Familles transformed.")

        libelles_df = extracted_data['libelle'] \
            .reset_index() \
            .rename(columns={'FAMILLE': 'famille', 'LIBELLE': 'libelle', 'index': 'id'}) \
            .merge(right=familles_df, on='famille') \
            .rename(columns={'id_y': 'famille_id', 'id_x': 'id'}) \
            [['id', 'libelle', 'famille_id']]
        libelles_df.libelle = libelles_df.libelle.str.replace("'", "''")
        libelles_df.id += 1
        print("\t - Libelles transformed.")

        transactions_df = extracted_data['transactions'] \
            .rename(
            columns={'TICKET_ID': 'ticket_number', 'LIBELLE': 'libelle', 'QUANTITY': 'quantity', 'PRIX_NET': 'price'}) \
            .merge(right=tickets_df, on='ticket_number') \
            .rename(columns={'id': 'ticket_id'}) \
            .drop(columns=['CLI_ID', 'ticket_number', 'client_id']) \
            .merge(right=libelles_df, on='libelle') \
            .rename(columns={'id': 'libelle_id'}) \
            .drop(columns=['famille_id', 'libelle', 'month']) \
            .reset_index() \
            .rename(columns={'index': 'id'})
        transactions_df.id += 1
        print("\t - Transactions transformed.")

        print("Data was successfully transformed.")

        return {
            'clients': clients_df,
            'tickets': tickets_df,
            'famille': familles_df,
            'libelle': libelles_df,
            'transactions': transactions_df
        }

    @staticmethod
    def load(transformed_data):
        # Connect to the database
        database = Database({
            'host': settings['host'],
            'postgres_user': settings['postgres_user'],
            'postgres_password': settings['postgres_password'],
            'database_name': settings['database_name'],
            'port': settings['port']
        })

        conn = database.connect()

        # Load famille into the database
        database.insert(
            table_name='famille',
            df=transformed_data['famille'],
            db_utils_callback=db_utils.prepare_famille,
            cols='(id, name)'
        )

        # Load libelle into the database
        database.insert(
            table_name='libelle',
            df=transformed_data['libelle'],
            db_utils_callback=db_utils.prepare_libelle,
            cols='(id, name, family_id)'
        )

        # Load clients into the database
        database.insert_chunks(
            table_name='client',
            df=transformed_data['clients'],
            db_utils_callback=db_utils.prepare_clients,
            cols='(id, client_number)'
        )

        # Load tickets into the database
        database.insert_chunks(
            table_name='ticket',
            df=transformed_data['tickets'],
            db_utils_callback=db_utils.prepare_tickets,
            cols='(id, ticket_number, month, client_id)'
        )

        # Load transactions into the database
        database.insert_chunks(
            table_name='transaction',
            df=transformed_data['transactions'],
            db_utils_callback=db_utils.prepare_transactions,
            cols='(id, ticket_id, libelle_id, price, quantity)',
            chunk_size=1_000_000
        )

        # Close the connection
        database.disconnect()

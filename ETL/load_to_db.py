import time

import pandas as pd
import psycopg2

from settings import db_config, DATASET_PATH


def connect(db_config):
    conn = psycopg2.connect(
        host=db_config['HOST'],
        port=db_config['PORT'],
        database=db_config['DB_NAME'],
        user=db_config['USER'],
        password=db_config['PASSWORD']
    )
    cur = conn.cursor()
    return conn, cur


def insert_dim_to_db(conn, cur, array, table_name, table_cols):
    args = ','.join(f"('{str(i + 1)}', '{str(x)}')" for i, x in enumerate(array))
    q = f"INSERT INTO {table_name} VALUES {args}"
    try:
        cur.execute(q)
        conn.commit()
        inserted_rows = cur.rowcount
        print(f"\t✓ Inserted {inserted_rows} into table {table_name}.\n")
        return inserted_rows
    except Exception as error:
        print(error)
        conn.rollback()
        raise Exception(f"Error inserting data into {table_name}")


def get_table_df(conn, cur, table_name, df_join_col, table_cols=None):
    try:
        if table_cols:
            q = f"SELECT {','.join(table_cols)} FROM {table_name}"
        else:
            q = f"SELECT * FROM {table_name}"
        cur.execute(q)
        conn.commit()
        return pd.DataFrame(cur.fetchall(), columns=['id', df_join_col])
    except Exception as error:
        print(error)
        conn.rollback()
        raise Exception()


def insert_ticket(conn, cur, joined_ticket_table):
    try:
        args = ",".join(f"('{str(id)}', '{str(ticket_number)}', '{str(month)}', '{client_id}')" for
                        (id, ticket_number, month, client_id) in
                        list(joined_ticket_table.itertuples(index=False, name=None)))
        cur.execute(f"INSERT INTO ticket VALUES {args}")
        conn.commit()
        inserted_rows = cur.rowcount
        print(f"\t✓ Inserted {inserted_rows} into table tickets.\n")
        return inserted_rows
    except Exception as error:
        print(error)
        conn.rollback()
        raise Exception()


def _parse_sql_string(sql_string):
    return sql_string.replace("'", "''")


def insert_libelle(conn, cur, joined_libelle_table):
    try:
        args = ",".join(f"('{str(id)}', '{str(_parse_sql_string(libelle))}', '{str(maille_id)}', '{str(prix_net)}')" for
                        (id, libelle, maille_id, prix_net) in
                        list(joined_libelle_table.itertuples(index=False, name=None)))
        cur.execute(f"INSERT INTO libelle VALUES {args}")
        conn.commit()
        inserted_rows = cur.rowcount
        print(f"\t✓ Inserted {inserted_rows} rows into table libelle.\n")
        return inserted_rows
    except Exception as error:
        print(error)
        conn.rollback()
        raise Exception()


def insert_transaction(conn, cur, joined_transaction_table):
    try:
        args = ",".join(f"('{str(id)}', '{str(libelle_id)}', '{str(ticket_id)}', '{str(quantity)}')" for
                        (id, ticket_id, libelle_id, quantity) in
                        list(joined_transaction_table.itertuples(index=False, name=None)))
        cur.execute(f"INSERT INTO transaction VALUES {args}")
        conn.commit()
        inserted_rows = cur.rowcount
        print(f"\t✓ Inserted {inserted_rows} rows into table transaction.\n")
        return inserted_rows
    except Exception as error:
        conn.rollback()
        print(error)


if __name__ == '__main__':
    print(db_config)

    print("DATA INGESTION START")

    program_time_start = time.time()

    # Read the data from the csv file
    extract_time_start = time.time()
    print("→ Reading data from csv file...")
    dataset = pd.read_parquet(DATASET_PATH)
    extract_time_end = time.time()
    extract_elapsed_time = extract_time_end - extract_time_start
    print(f"\t✓ Done! Imported {len(dataset)} rows from the dataset. (elapsed time={extract_elapsed_time:.2f}s)\n")

    # Separate the dataset into tables
    print("→ Normalizing the dataset...")
    normalize_time_start = time.time()
    client = dataset.CLI_ID.unique()
    famille = dataset.FAMILLE.unique()
    ticket = dataset.groupby(['TICKET_ID', 'CLI_ID', 'MOIS_VENTE']).TICKET_ID.count().reset_index(name='COUNT')[
        ['TICKET_ID', 'CLI_ID', 'MOIS_VENTE']]
    libelle = dataset.sort_values('PRIX_NET', ascending=False).groupby(
        ['FAMILLE', 'LIBELLE', 'PRIX_NET']).LIBELLE.count().reset_index(name='COUNT')[
        ['FAMILLE', 'LIBELLE', 'PRIX_NET']].drop_duplicates(subset=['LIBELLE'], keep='last')
    transaction = dataset.groupby(['TICKET_ID', 'LIBELLE']).LIBELLE.count().reset_index(name='QUANTITE')
    normalize_time_end = time.time()
    normalize_elapsed_time = normalize_time_end - normalize_time_start
    print(f"\t✓ Successful. (elapsed_time={normalize_elapsed_time:.2f}s)\n")

    # Connect to the database
    print(f"→ Connecting to the database {db_config['DB_NAME']}...")
    conn, cur = connect(db_config)
    print(f"\t✓ Done! Connected to the database {db_config['DB_NAME']}.\n")

    # Create the tables
    print("→ Creating the tables...")
    with conn.cursor() as create_tables_cursor:
        create_tables_cursor.execute(open("../schema_init.sql", "r").read())
    print("\t✓ Done! Tables created.\n")

    insert_time_start = time.time()

    # Insert client data into the database
    print(f"→ Trying to insert {len(client)} rows of client data into the database...")
    clients_inserted = insert_dim_to_db(conn, cur, client, 'client', ['id', 'client_number'])

    # Insert famille
    print(f"→ Trying to insert {len(famille)} rows of famille data into the database...")
    familles_inserted = insert_dim_to_db(conn, cur, famille, 'famille', ['id', 'name'])

    # Insert ticket
    client_table = get_table_df(conn, cur, 'client', 'CLI_ID')
    client_table['CLI_ID'] = client_table['CLI_ID'].astype(int)
    ticket_client_joined = ticket.merge(client_table, on='CLI_ID', how='left').reset_index()[
        ['index', 'TICKET_ID', 'MOIS_VENTE', 'id']]
    print(f"→ Trying to insert {len(ticket_client_joined)} rows of ticket data into the database...")
    inserted_tickets = insert_ticket(conn, cur, ticket_client_joined)

    # Insert libelle
    famille_table = get_table_df(conn, cur, 'famille', 'FAMILLE')
    libelle_famille_joined = libelle.merge(famille_table, on='FAMILLE', how='left').reset_index()[
        ['index', 'LIBELLE', 'id', 'PRIX_NET']]
    print(f"→ Trying to insert {len(libelle_famille_joined)} rows of libelle data into the database...")
    inserted_libelles = insert_libelle(conn, cur, libelle_famille_joined)

    # Insert transaction
    ticket_table = get_table_df(conn, cur, 'ticket', 'TICKET_ID', ['id', 'ticket_number'])
    libelle_table = get_table_df(conn, cur, 'libelle', 'LIBELLE', ['id', 'name'])
    ticket_table['TICKET_ID'] = ticket_table['TICKET_ID'].astype(int)
    transaction_ticket_join = transaction.merge(ticket_table, on='TICKET_ID', how='left').reset_index()[
        ['index', 'id', 'LIBELLE', 'QUANTITE']].rename(columns={'id': 'ticket_id'})
    transaction_ticket_libelle_join = \
        transaction_ticket_join.merge(libelle_table, on='LIBELLE', how='left').reset_index()[
            ['index', 'ticket_id', 'id', 'QUANTITE']].rename(columns={'id': 'libelle_id'})
    print(f"→ Trying to insert {len(transaction_ticket_libelle_join)} rows of transaction data into the database...")
    inserted_transactions = insert_transaction(conn, cur, transaction_ticket_libelle_join)

    insert_time_end = time.time()
    insert_elapsed_time = insert_time_end - insert_time_start

    print(f"→ Insertion completed. (elapsed time={insert_elapsed_time:.2f}s)")

    # Close the connection
    cur.close()
    conn.close()

    program_time_end = time.time()
    program_elapsed_time = program_time_end - program_time_start

    print(f"DATA INGESTION END (elapsed time={program_elapsed_time:.2f}s)")

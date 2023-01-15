def prepare_clients(clients_df):
    return [f"({row.id}, {row.client_number})" for row in clients_df.itertuples()]


def prepare_famille(famille_df):
    return [f"({row.id}, '{row.famille}')" for row in famille_df.itertuples()]


def prepare_libelle(libelle_df):
    return [f"({row.id}, '{row.libelle}', {row.famille_id})" for row in libelle_df.itertuples()]


def prepare_tickets(tickets_df):
    return [f"({row.id}, '{row.ticket_number}', {row.month}, {row.client_id})" for row in tickets_df.itertuples()]


def prepare_transactions(transactions_df):
    return [f"({row.id}, {row.ticket_id}, {row.libelle_id}, {row.price}, {row.quantity})" for row in
            transactions_df.itertuples()]


def get_columns(df):
    return '(' + ','.join(df.columns) + ')'

import mysql.connector
from production.credentials import db_credentials

def get_next_pub_id(params=None, config = db_credentials):
    
    connection = mysql.connector.connect(**config)  
    cursor = connection.cursor()

    try:
        # Call the procedure
        cursor.callproc('ecomm_get_next_pub_id', [0])  # 0 is a placeholder for OUT param

        # Retrieve OUT parameter
        for result in cursor.stored_results():
            # If your procedure SELECTs the result, this is where you’d get it
            pass

        # Access the OUT parameter from the procedure call
        next_pub_id = cursor.statement.split('CALL')[1].strip()
        out_param = cursor._executed_args[0] if hasattr(cursor, '_executed_args') else 0

        # MySQL connector stores OUT params in the callproc result list
        next_id = cursor.callproc('ecomm_get_next_pub_id', [0])[0]
        return next_id

    finally:
        cursor.close()



def execute_query(query, params=None, config = db_credentials):
    
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor(dictionary=True)

        cursor.execute(query, params or ())
        
        if query.strip().lower().startswith("select"):
            result = cursor.fetchall()
        else:
            connection.commit()
            result = cursor.rowcount  # number of rows affected

        cursor.close()
        connection.close()
        return result

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def delete_transaction(delete_queries, config = db_credentials):

    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        connection.start_transaction()

        for query in delete_queries:
            delete_query = query['query']
            params = query['params']
            cursor.execute(delete_query, params or ())

        connection.commit()
        deleted_count = cursor.rowcount

        cursor.close()
        connection.close()
        return deleted_count

    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        if connection.is_connected():
            connection.rollback()
            cursor.close()
            connection.close()
        return 0


def delete(delete_query, params=None, config = db_credentials):

    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        connection.start_transaction()

        cursor.execute(delete_query, params or ())

        connection.commit()
        deleted_count = cursor.rowcount

        cursor.close()
        connection.close()
        return deleted_count

    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        if connection.is_connected():
            connection.rollback()
            cursor.close()
            connection.close()
        return 0


def single_insert(insert_query, params=None, config = db_credentials):

    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        connection.start_transaction()

        cursor.execute(insert_query, params or ())

        connection.commit()
        inserted_count = cursor.rowcount

        cursor.close()
        connection.close()
        return inserted_count

    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        if connection.is_connected():
            connection.rollback()
            cursor.close()
            connection.close()
        return None

def execute_delete_and_insert(
    delete_query,
    delete_params,
    insert_query,
    insert_data, 
    config = db_credentials):

    """
    Executes a DELETE followed by a multi-row INSERT in a single transaction.

    Parameters:
        delete_query (str): SQL DELETE query with placeholders
        delete_params (list or tuple): Parameters for DELETE query
        insert_query (str): SQL INSERT query with placeholders
        insert_data (list of tuples): Each tuple is a row to insert

    Returns:
        int: Number of inserted rows if successful, None if failed
    """

    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        connection.start_transaction()

        # Execute DELETE
        cursor.execute(delete_query, delete_params)

        # Execute INSERT
        cursor.executemany(insert_query, insert_data)

        connection.commit()
        inserted_count = cursor.rowcount

        cursor.close()
        connection.close()
        return inserted_count

    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        if connection.is_connected():
            connection.rollback()
            cursor.close()
            connection.close()
        return None



def execute_multi_insert(query, data, config = db_credentials):
    """
    Executes a multi-row insert using executemany().
    
    Parameters:
        query (str): INSERT INTO ... VALUES (%s, %s, ..., %s)
        data (list of tuples): Each tuple represents a row to insert
    """

    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        cursor.executemany(query, data)
        connection.commit()
        row_count = cursor.rowcount  # Total rows inserted

        cursor.close()
        connection.close()
        return row_count

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
        


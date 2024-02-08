# ==================================================================================================
def execute_commit(connection, query):
    """ Execute a query and commit """
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()
    connection.commit()

# ==================================================================================================
def execute_fetchone(connection, query):
    """ Execute a query and fetch first result """
    cursor = connection.cursor() 
    cursor.execute(query)
    output = cursor.fetchone()
    cursor.close()
    return output

# ==================================================================================================
def execute_fetchmany(connection, query, fetch_size):
    """ Execute a query and fetch many results """
    cursor = connection.cursor()
    try:
        cursor.prefetchrows = 2000
    except:
        cursor.itersize = 2000
        
    cursor.arraysize = 2000
    cursor.execute(query)
    output = cursor.fetchmany(fetch_size)
    cursor.close()
    return output

# ==================================================================================================
def execute_fetchall(connection, query, mode='normal', header=False, batch_size=100000):
    """ Execute a query and fetch all results """
    cursor = connection.cursor()
    try:
        cursor.prefetchrows = 2000
    except:
        cursor.itersize = 2000
        
    cursor.arraysize = 2000
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    
    # Normal fetch all
    if mode == 'normal':
        output = cursor.fetchall()
        cursor.close()
    
    # Batch fetch all
    elif mode == 'batch':
        output = []
        count = 0
        while True:
            rows = cursor.fetchmany(batch_size)
            count += len(rows)
            if len(rows) == 0:
                break
            output.extend(rows)
            print(f'Extracted {count} rows')
            
    # Debug fetch all, display each row as it is extracted
    elif mode == 'debug':
        output = []
        count = 0
        while True:
            try:
                rows = cursor.fetchone()
                if rows is None:
                    break
                count += 1
                output.append(rows)
                print(f"Row {count}: {rows}")
            except Exception as e:
                print(f"Error occurred at row {count}: {e}")
                raise e
        
    else:
        raise Exception('Invalid mode')

    return (output, column_names) if header else output
# ==================================================================================================
    
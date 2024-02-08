from pathlib import Path
import json
import os

import cx_Oracle as cx
import psycopg2 as pg
import paramiko as pm

cred_path = os.environ.get('CRED_KEY')

# ==================================================================================================
def connect_oracle(database, schema, **kwargs):
    # Relative path to get credentials
    with open(Path(cred_path, f"{database}.json")) as f:
        credentials = json.load(f)
        
    # Get credentials
    schema_cred = credentials[schema]
    host, port, service_name = schema_cred['host'], schema_cred['port'], schema_cred['service_name']
    user, password = schema_cred['user'], schema_cred['password']

    # Create connection
    dsn = cx.makedsn(host, port, service_name=service_name)
    connection = cx.connect(user=user, password=password, dsn=dsn, **kwargs)
    print(f'Connected to ORACLE {schema}@{database}')
    return connection

# ==================================================================================================
def connect_postgre(database, schema=None, **kwargs):
    # Relative path to get credentials
    with open(Path(cred_path, f"{database}.json")) as f:
        credentials = json.load(f)
    
    # Get credentials
    database_cred = credentials[database]
    host, user, password = database_cred['host'], database_cred['user'], database_cred['password']
    
    # If schema is assigned
    options = f"-c search_path={schema}" if schema is not None else None

    # Create connection
    connection = pg.connect(host=host, database=database, user=user, password=password, options=options, **kwargs)
    print(f'Connected to POSTGRE {schema}@{database}')
    return connection

# ==================================================================================================
def connect_server(server):
    # Relative path to get credentials
    with open(Path(cred_path, "server.json")) as f:
        credentials = json.load(f)
    
    # Get credentials
    server_cred = credentials[server]
    hostname, port, username = server_cred['hostname'], server_cred['port'], server_cred['username']
    if server == 'spotify':    # Special condition for spotify server
        p_key = pm.RSAKey.from_private_key_file(Path(cred_path, f'{server}.pem'), password=server_cred['passphrase'])
    else:
        key_filename = Path(cred_path, f'{server}.pem')
    
    # Connect to virtual machine
    client = pm.SSHClient()
    client.set_missing_host_key_policy(pm.AutoAddPolicy())
    if server == 'spotify':    # Special condition for spotify server
        client.connect(hostname=hostname, port=port, username=username, pkey=p_key)
    else:
        client.connect(hostname=hostname, port=port, username=username, key_filename=str(key_filename))
    print(f'Connected to SERVER {server}')
    return client

#===================================================================================================
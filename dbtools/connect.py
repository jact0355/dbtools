from pathlib import Path
import json
import os

import cx_Oracle as cx
import psycopg2 as pg
import paramiko as pm

cred_path = Path(os.environ.get('CRED_KEY', '.')) 

# ==================================================================================================
class OracleConnector():
    def __init__(self, schema, display_schema=False, **kwargs):
        schema = schema.lower()
        with open(cred_path / 'oracle.json') as f:
            credentials = json.load(f)
            self.schema = credentials[schema]
        
        # Check schema if needed
        print(f"Schema Info: {self.schema}") if display_schema else None
        
        # Connect to input schema
        self.connect(schema)
    
    def connect(self, schema, encoding='UTF-8', **kwargs):
        # Get credentials
        host, port, service_name, user, password = (self.schema[key] for key in ('host', 'port', 'service_name', 'user', 'password'))
        # Create connection
        try:
            dsn = cx.makedsn(host, port, service_name=service_name)
            self.connection = cx.connect(user=user, password=password, dsn=dsn, encoding=encoding, **kwargs)
            print(f'Connected to ORACLE: {schema.upper()}')
        except cx.DatabaseError as e:
            print(f'Connection to ORACLE: {schema.upper()} failed: {e}')
# ==================================================================================================
            
        
        

# # ==================================================================================================
# def connect_oracle(database, schema, encoding='UTF-8', **kwargs):
#     # Relative path to get credentials
#     with open(cred_path / 'oracle' / f"{database}.json") as f:
#         credentials = json.load(f)
        
#     # Get credentials
#     schema_cred = credentials[schema]
#     host, port, service_name, user, password = (schema_cred[key] for key in ('host', 'port', 'service_name', 'user', 'password'))
    
#     # Create connection
#     dsn = cx.makedsn(host, port, service_name=service_name)
#     connection = cx.connect(user=user, password=password, dsn=dsn, encoding=encoding, **kwargs)
#     print(f'Connected to ORACLE {schema}@{database}')
#     return connection

# # ==================================================================================================
# def connect_postgre(database, schema=None, **kwargs):
#     # Relative path to get credentials
#     with open(cred_path / 'postgre' / f"{database}.json") as f:
#         credentials = json.load(f)
    
#     # Get credentials
#     database_cred = credentials[database]
#     database, host, user, password = (database_cred[key] for key in ('database', 'host', 'user', 'password'))
    
#     # If schema is assigned
#     options = f"-c search_path={schema}" if schema else ''

#     # Create connection
#     connection = pg.connect(host=host, database=database, user=user, password=password, options=options, **kwargs)
#     print(f'Connected to POSTGRE {schema}@{database}')
#     return connection

# # ==================================================================================================
# def connect_server(server):
#     # Relative path to get credentials
#     with open(cred_path / 'server' / "server.json") as f:
#         credentials = json.load(f)
    
#     # Get credentials
#     server_cred = credentials[server]
#     hostname, port, username = (server_cred[key] for key in ('hostname', 'port', 'username'))
#     key_filename = cred_path / 'server' / f"{server}.pem"
    
#     # Connect to virtual machine
#     client = pm.SSHClient()
#     client.set_missing_host_key_policy(pm.AutoAddPolicy())
#     if server == 'spotify':    # Special condition for spotify server
#         p_key = pm.RSAKey.from_private_key_file(str(key_filename), password=server_cred['passphrase'])
#         client.connect(hostname=hostname, port=port, username=username, pkey=p_key)
#     else:
#         client.connect(hostname=hostname, port=port, username=username, key_filename=str(key_filename))
#     print(f'Connected to SERVER {server}')
#     return client

# #===================================================================================================
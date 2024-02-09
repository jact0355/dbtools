# ==================================================================================================
from pathlib import Path
import pandas as pd
import cx_Oracle
from dbtools import connect_oracle

# ==================================================================================================
# CHANGE INPUT PATH HERE
input_path = Path(r"C:\Users\user\Documents\Spotify Revenue Share")
# CHANGE CONNECTION TO DATABASE HERE
dsn = cx_Oracle.makedsn(host=, port=, service_name=)
connection = cx_Oracle.connect(user=, password=, dsn=dsn)

# ==================================================================================================
def clean_revenue_share(df):
    # Remove leading and trailing spaces
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Change date to datetime format
    df['Start date'] = pd.to_datetime(df['Start date'], format='%Y-%m-%d')
    df['End date'] = pd.to_datetime(df['End date'], format='%Y-%m-%d')
    # Change country, currency, payable type, invoice currency to uppercase
    df['Country'] = df['Country'].str.upper()
    df['Currency'] = df['Currency'].str.upper()
    df['Payable type'] = df['Payable type'].str.upper()
    df['Invoice Currency'] = df['Invoice Currency'].str.upper()
    # Change all Nan value in Minima per user to 0
    df['Minima per user'] = df['Minima per user'].fillna(0)
    return df

# ==================================================================================================
def insert_revenue_share(rows, file, connection=connection):
    with connection.cursor() as cursor:
        query = f" delete from SPOT_MON_REVENUE where file_name = '{file.name}'"
        cursor.execute(query)
        connection.commit()
        query = f"""
                INSERT INTO SPOT_MON_REVENUE(
                    licensor, start_date, end_date, product, country,
                    currency, total_tracks, revenue_share, adjusted_user, net_revenue, 
                    minima_per_user, royalty_pool, payable_type, invoice_currency, fx_rate, file_name
                    )
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)
                """
        cursor.executemany(query, rows)
        connection.commit()
    connection.close()

# ==================================================================================================
def main(connection=connection):
    for file in input_path.glob("*.txt"):
        # File name checking 
        if 'spotify-revshare-for' not in file.stem:
            print(f"Skipping {file.stem} as it is not a Spotify revenue share file")
            continue
        
        print(f"Start processing {file.stem}")
        df = pd.read_csv(file, sep="\t")
        
        print("Start data cleaning")
        df = clean_revenue_share(df)

        # Add a new column for file name
        df['File name'] = file.name
        # Convert dataframe to list of tuples
        rows = [tuple(x) for x in df.values.tolist()]
        
        print("Inserting data into database")
        insert_revenue_share(rows, file, connection=connection)
        
        print(f"Done processing {file.stem}")

# ==================================================================================================
if __name__ == "__main__":
    main()
# ==================================================================================================



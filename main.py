import urllib.parse
import pandas as pd
from dotenv import dotenv_values
from sqlalchemy import create_engine

config_env = dotenv_values(".env")

# create engine 73
db73_host = config_env['DB73_HOST']
db73_username = config_env['DB73_USERNAME']
db73_password = config_env['DB73_PASSWORD']
db73_name = config_env['DB73_NAME']
db73_port = int(config_env['DB73_PORT'])

db73_password_encoded = urllib.parse.quote(db73_password)

# create engine 133
db133_host = config_env['DB133_HOST']
db133_username = config_env['DB133_USERNAME']
db133_password = config_env['DB133_PASSWORD']
db133_name = config_env['DB133_NAME']
db133_port = int(config_env['DB133_PORT'])


db133_password_encoded = urllib.parse.quote(db133_password)

db73_uri = f"mysql+pymysql://{db73_username}:{db73_password_encoded}@{db73_host}:{db73_port}/{db73_name}"
engine73 = create_engine(db73_uri)

db133_uri = f"mysql+pymysql://{db133_username}:{db133_password_encoded}@{db133_host}:{db133_port}/{db133_name}"
engine133 = create_engine(db133_uri)


# check connection
# connect to the database
try:
    # Test the connection
    engine73.connect()
    print('Database 73 Connected Successfully')
except Exception as e:
    print('Error Connecting to Database 73')
    print(e)

table_name = "dustboy_value"


def import_to_133(df):
    try:
        with engine133.connect() as con133:
            df.to_sql(table_name, con133, if_exists='replace', index=False)
            print(df)
            count_rows = df.shape[0]
            print(f"Data inserted {count_rows} rows to database 133 successfully!")

    except Exception as e:
        print(e)
    finally:
        engine133.dispose()


def main():
    try:
        query = """
        SELECT * FROM dustboy_value
        WHERE log_datetime >= subdate(now(), INTERVAL 3 DAY) AND log_datetime < now()
        """
        with engine73.connect() as con73:
            df = pd.read_sql_query(query, con73)
            import_to_133(df)
    except Exception as e:
        print(e)
    finally:
        engine73.dispose()


if __name__ == "__main__":
    main()

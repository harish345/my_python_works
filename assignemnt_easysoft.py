##########################################################
## Python program to read csv and store into a Database ##
# libraries used:
#   pandas
#   sqlite3
###########################################################
import json
import sqlite3
import pandas as pd
import uuid

DEFAULT_DB = 'default.db'
conn = sqlite3.connect(DEFAULT_DB)


def db_init():
    try:
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS football 
        (id text primary key,
        div text not null,
        date_time text not null,
        home_team text not null,
        away_team text not null,
        fthg int default 0,
        ftag int default 0)""")
        conn.commit()
        print("Database and table initialized")
    except Exception as e:
        print("Failed to initialize DB " + str(e))


def get_data_from_url(url):
    try:
        data = pd.read_csv(url)
        print(f"Fetched data from {url}")
        return data
    except Exception as e:
        print("Unable to read data from url" + str(e))
        return None


def sort_df(df, ascending):
    df["DateTime"] = pd.to_datetime(df["Date"] + " " + df["Time"])
    df.sort_values(by="DateTime", ascending=ascending, inplace=True)


def print_data_to_console(df):
    df_json = json.loads(df.drop("DateTime", axis=1).to_json(orient='records'))
    print("Writing data as josn to console")
    for obj in df_json:
        print(json.dumps(obj, indent=3))


def write_to_db(df):
    try:
        cols = ['Div', 'DateTime', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']
        df2 = df.loc[:, cols]
        df2['id'] = [str(uuid.uuid4()) for _ in range(len(df.index))]
        df2.DateTime = df2.DateTime.dt.strftime('%Y-%m-%d %H:%M:%S')
        df2.rename(columns={'Div': 'div', 'DateTime': 'date_time', 'HomeTeam': 'home_team', 'AwayTeam': 'away_team'},
                   inplace=True)
        df2.to_sql('football', conn, if_exists='append', index=False)
        print("Data written to football table")
    except Exception as e:
        print("Failed to write data to db " + str(e))


if __name__ == "__main__":
    csv_url = "https://www.football-data.co.uk/mmz4281/1920/E0.csv"
    db_init()
    data_df = get_data_from_url(csv_url)  # Get data from url
    sort_df(data_df, ascending=False)  # Sort df from most recent to earliest
    print_data_to_console(data_df)  # print each row in df as json object
    write_to_db(data_df)  # Store data to DB
    conn.close()

# Import Libraries
import time
import pandas as pd
import pandas.io.sql as pd_sql
import numpy as np
import sqlalchemy as sa
import pymysql
pymysql.install_as_MySQLdb()
import datetime as datetime

# ----------------------------------------------------------------------------------------------------------------------
# List of functions
# ----------------------------------------------------------------------------------------------------------------------

conn = sa.create_engine("mysql://root:Betfair@localhost/betfair?host=localhost?port=3306", convert_unicode=True)

# Read Sql
def read_sql(table):
    df = pd.read_sql(table, conn)
    return df

# Write in MySql Database
def write_to_sql(df, tablename):
    pd_sql.to_sql(df, tablename, conn, if_exists='append', index=False)
    print('Successful push to DB')

# Get number of goals (return list of goals)
def get_goals(df):
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['minutes'] = df['timestamp'] - df['timestamp'].iloc[0]
    goals_list = [np.nan, np.nan, np.nan]

    if df['runner_name'].iloc[0] == 'Under 2.5 Goals':
        df['return_ltp'] = df['ltp'].pct_change()
        df['is_goal'] = df['return_ltp'] > 0.2
        df_goals = df[df['is_goal'] == True]
        df_goals_minutes_list = df_goals['minutes'].tolist()
        goals_list[0:len(df_goals_minutes_list)] = df_goals_minutes_list

        if df['minutes'].iloc[-1] < datetime.timedelta(hours=1, minutes=45, seconds=0, milliseconds=0):
            goals_list[2] = df['minutes'].iloc[-1]

    elif df['runner_name'].iloc[0] == 'Over 2.5 Goals':
        df['return_ltp'] = df['ltp'].pct_change()
        df['is_goal'] = df['return_ltp'] < -0.2
        df_goals = df[df['is_goal'] == True]
        df_goals_minutes_list = df_goals['minutes'].tolist()
        goals_list[0:len(df_goals_minutes_list)] = df_goals_minutes_list

        if df['minutes'].iloc[-1] < datetime.timedelta(hours=1, minutes=45, seconds=0, milliseconds=0):
            goals_list[2] = df['minutes'].iloc[-1]

    return goals_list

#
def strat0(df, stake, tax, minute):
    # Lay under 2.5 for the 1st 15 InPlay minutes and see if there is alpha
    df = df.copy()

    if df.empty == False:
        # set timestamp as index and convert it to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['minutes'] = df['timestamp'] - df['timestamp'].iloc[0]
        df = df.set_index(['minutes'])

        country_code = df['country_code'].iloc[0]
        date = df['date'].iloc[0]
        market_id = df['market_id'].iloc[0]
        event_name = df['event_name'].iloc[0]
        runner_name = df['runner_name'].iloc[0]

        ltp_in = df['ltp'].iloc[0]

        try:
            ltp_out = \
            df['ltp'].loc[datetime.timedelta(hours=0, minutes=minute):datetime.timedelta(hours=0, minutes=minute + 5)][
                0]
            if df['runner_name'].iloc[0] == 'Under 2.5 Goals':
                pnl = (ltp_in / ltp_out - 1) * stake
            elif df['runner_name'].iloc[0] == 'Over 2.5 Goals':
                pnl = (1 - ltp_in / ltp_out) * stake

            if pnl > 0:
                pnl = pnl * (1 - tax)  # Taxe sur les gains
            else:
                pnl = pnl

        except Exception as e:
            print(e)
            if df['runner_name'].iloc[0] == 'Under 2.5 Goals':
                ltp_out = 1000
                pnl = -stake
            elif df['runner_name'].iloc[0] == 'Over 2.5 Goals':
                ltp_out = 1
                pnl = (1 - ltp_in / ltp_out) * stake

        print('ltp in: ' + str(ltp_in))
        print('ltp out: ' + str(ltp_out))
        print('P&L: ' + str(format(pnl, '.2f')))

        goals_list = get_goals(df)
        goal_1 = goals_list[0]
        goal_2 = goals_list[1]
        goal_3 = goals_list[2]
        return [country_code, date, market_id, event_name, runner_name, ltp_in, ltp_out, pnl, goal_1, goal_2, goal_3]

    else:
        print('empty dataframe')
        return [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 0, np.nan, np.nan, np.nan]

# Convert Data from Seconds to Minutes
def sec_to_min_df(df):
    # Lay under 2.5 for the 1st 15 InPlay minutes and see if there is alpha
    df = df.copy()
    # set timestamp as index and convert it to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['minutes'] = df['timestamp'] - df['timestamp'].iloc[0]
    df = df.set_index(['minutes'])
    df_min = df.resample('min').first()
    df_min['timestamp_min'] = df_min['timestamp'].iloc[0] + df_min.index

    return df_min

# Get the list of rtadable team from Jan 18 Data
def get_tradable_teams(threshold_volume, threshold_occurences, overunder):
    import time
    query = "SELECT country_code, market_id, event_name, date, MAX(tv) FROM eu_ou25 WHERE runner_name = '" + overunder + " 2.5 Goals' GROUP BY market_id"
    start_time = time.time()
    df_all_teams = read_sql(query)
    elapsed_time = time.time() - start_time

    print(elapsed_time)

    df_all_teams = df_all_teams[df_all_teams['MAX(tv)'] >= threshold_volume]
    df_all_teams['home_team'], df_all_teams['away_team'] = df_all_teams['event_name'].str.split(' v ', 1).str
    #     df_all_teams['home_team'] = df_all_teams['event_name'].str.split(' v ')[0]
    #     df_all_teams['away_team'] = df_all_teams['event_name'].str.split(' v ')[1]
    list_all_teams = df_all_teams['home_team'].tolist()
    list_all_teams.extend(df_all_teams['away_team'].tolist())
    list_tradable_teams = []

    for i in list_all_teams:
        if list_all_teams.count(i) >= threshold_occurences:
            list_tradable_teams.append(i)
    list_tradable_teams = list(set(list_tradable_teams))

    return list_tradable_teams


def get_tradable_BASIC_matches(list_tradable_teams, overunder):
    import time
    query = "SELECT country_code, market_id, event_name, date FROM basic_oct17_mar18_ou25 WHERE inplay=1 AND runner_name = '" + overunder + " 2.5 Goals' GROUP BY market_id"
    start_time = time.time()
    df_all_teams = read_sql(query)
    elapsed_time = time.time() - start_time
    print(elapsed_time)
    df_all_teams['home_team'], df_all_teams['away_team'] = df_all_teams['event_name'].str.split(' v ', 1).str
    #     df_all_teams['home_team'] = df_all_teams['event_name'].str.split(' v ')[0]
    #     df_all_teams['away_team'] = df_all_teams['event_name'].str.split(' v ')[1]
    df_tradable_matches = df_all_teams.copy()
    df_tradable_matches = df_tradable_matches[df_tradable_matches['home_team'].isin(list_tradable_teams)]
    df_tradable_matches = df_tradable_matches[df_tradable_matches['away_team'].isin(list_tradable_teams)]
    return df_tradable_matches

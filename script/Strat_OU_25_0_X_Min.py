# ----------------------------------------------------------------------------------------------------------------------
# Strategy 0-25
# ----------------------------------------------------------------------------------------------------------------------

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
                # pnl = (1 - ltp_in / ltp_out) * stake # Ancien code gérant mal le Over / Under
                pnl = (ltp_in / ltp_out - 1) * stake

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
    query = "SELECT country_code, market_id, event_name, date FROM top520_liquid_under_25 WHERE inplay=1 AND runner_name = '" + overunder + " 2.5 Goals' GROUP BY market_id"
    # query = "SELECT country_code, market_id, event_name, date FROM basic_oct17_mar18_ou25 WHERE inplay=1 AND runner_name = '" + overunder + " 2.5 Goals' GROUP BY market_id"
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

# ----------------------------------------------------------------------------------------------------------------------
# Get list of liquid teams
# ----------------------------------------------------------------------------------------------------------------------

threshold_volume = 50000
threshold_occurences = 1
overunder = 'Under'

list_tradable_teams = get_tradable_teams(threshold_volume, threshold_occurences, overunder)
list_tradable_teams

# ----------------------------------------------------------------------------------------------------------------------
# Get list of tradable matchs
# ----------------------------------------------------------------------------------------------------------------------

df_tradable_matches = get_tradable_BASIC_matches(list_tradable_teams,overunder)
df_tradable_matches

# ----------------------------------------------------------------------------------------------------------------------
# Get list of tradable matchs
# ----------------------------------------------------------------------------------------------------------------------

events = df_tradable_matches['market_id'].tolist()
events_name = df_tradable_matches['event_name'].tolist()
print(events_name)



stake = 100
tax = 0.08
minutes = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 105, 110]
writer = pd.ExcelWriter(
    'E:\\strat0\\strat0_' + str(time.strftime('%Y-%m-%d_%H-%M-%S', time.gmtime(time.time()))) + '.xlsx')

start_time = time.time()

for minute in minutes:

    print('\nMinute: ' + str(minute))

    country_code = []
    date = []
    market_id = []
    event_name = []
    runner_name = []
    ltp_in = []
    ltp_out = []
    pnl = []
    goal_1 = []
    goal_2 = []
    goal_3 = []
    i = 0

    for ind, val in enumerate(events):
        i += 1
        if len(str(val)) == 10:
            val_ = str(val) + '0'
        elif len(str(val)) == 9:
            val_ = str(val) + '00'
        elif len(str(val)) == 8:
            val_ = str(val) + '000'
        elif len(str(val)) == 7:
            val_ = str(val) + '0000'
        else:
            val_ = str(val)

        query = "select * from top520_liquid_under_25 where runner_name = '" + overunder + " 2.5 Goals' and inplay= '1' and market_id = '" + val_ + "'"
        # query = "select * from basic_oct17_mar18_ou25 where runner_name = '" + overunder + " 2.5 Goals' and inplay= '1' and market_id = '" + val_ + "'"
        print('Match: ' + str(events_name[ind]))
        print(str(i) + '/' + str(len(events)))
        df_raw_ou25 = read_sql(query)
        values = strat0(df_raw_ou25, stake, tax, minute)

        country_code.append(values[0])
        date.append(values[1])
        market_id.append(values[2])
        event_name.append(values[3])
        runner_name.append(values[4])
        ltp_in.append(values[5])
        ltp_out.append(values[6])
        pnl.append(values[7])
        goal_1.append(values[8])
        goal_2.append(values[9])
        goal_3.append(values[10])

        print('Cumulative P&L: ' + str(format(sum(pnl), '.2f')))
        print('\n')

    final_df = pd.DataFrame({'country_code': country_code, 'date': date, \
                             'market_id': market_id, 'event_name': event_name, \
                             'runner_name': runner_name, 'ltp_in': ltp_in, 'ltp_out': ltp_out, \
                             'pnl': pnl, 'goal_1': goal_1, 'goal_2': goal_2, 'goal_3': goal_3})
    final_df.to_excel(writer, '0-' + str(minute) + 'min')
    elapsed_time = time.time() - start_time
    print(elapsed_time)

writer.save()

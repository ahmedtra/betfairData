from data.hist_trades_loader import Loader

import pandas as pd
import datetime
import numpy as np


def get_goals(market_type,event_name=None):
    data_loader = Loader(market_type)
    # event_name = "Real Madrid v Barcelona"

    runner_05 = "Over 0.5 Goals"
    runner_15 = "Over 1.5 Goals"
    runner_25 = "Over 2.5 Goals"
    runner_35 = "Over 3.5 Goals"
    runner_45 = "Over 4.5 Goals"
    runner_55 = "Over 5.5 Goals"
    runner_65 = "Over 6.5 Goals"
    runner_75 = "Over 7.5 Goals"
    runner_85 = "Over 8.5 Goals"

    data_05 = data_loader.create_event_name_table(runner_05,event_name)
    data_05['timestamp'] = pd.to_datetime(data_05['timestamp']).apply(lambda x: x.date())
    # data_05.to_excel("event_names_05.xlsx")
    data_15 = data_loader.create_event_name_table(runner_15,event_name)
    data_15['timestamp'] = pd.to_datetime(data_15['timestamp']).apply(lambda x: x.date())
    # data_15.to_excel("event_names_15.xlsx")
    data_25 = data_loader.create_event_name_table(runner_25,event_name)
    data_25['timestamp'] = pd.to_datetime(data_25['timestamp']).apply(lambda x: x.date())
    # data_25.to_excel("event_names_25.xlsx")
    data_35 = data_loader.create_event_name_table(runner_35,event_name)
    data_35['timestamp'] = pd.to_datetime(data_35['timestamp']).apply(lambda x: x.date())
    # data_35.to_excel("event_names_35.xlsx")
    data_45 = data_loader.create_event_name_table(runner_45,event_name)
    data_45['timestamp'] = pd.to_datetime(data_45['timestamp']).apply(lambda x: x.date())
    # data_45.to_excel("event_names_45.xlsx")
    data_55 = data_loader.create_event_name_table(runner_55,event_name)
    data_55['timestamp'] = pd.to_datetime(data_55['timestamp']).apply(lambda x: x.date())
    # data_45.to_excel("event_names_55.xlsx")
    data_65 = data_loader.create_event_name_table(runner_65,event_name)
    data_65['timestamp'] = pd.to_datetime(data_65['timestamp']).apply(lambda x: x.date())
    # data_45.to_excel("event_names_65.xlsx")
    data_75 = data_loader.create_event_name_table(runner_75,event_name)
    data_75['timestamp'] = pd.to_datetime(data_75['timestamp']).apply(lambda x: x.date())
    # data_45.to_excel("event_names_75.xlsx")
    data_85 = data_loader.create_event_name_table(runner_85,event_name)
    data_85['timestamp'] = pd.to_datetime(data_85['timestamp']).apply(lambda x: x.date())
    # data_45.to_excel("event_names_85.xlsx")

    data_full = data_05.merge(data_15, left_on=['event_name', 'timestamp'], right_on=['event_name', 'timestamp'],how='outer')
    data_full = data_full.merge(data_25, left_on=['event_name', 'timestamp'], right_on=['event_name', 'timestamp'],how='outer')
    data_full = data_full.merge(data_35, left_on=['event_name', 'timestamp'], right_on=['event_name', 'timestamp'],how='outer')
    data_full = data_full.merge(data_45, left_on=['event_name', 'timestamp'], right_on=['event_name', 'timestamp'],how='outer')

    data_full = data_full.merge(data_55, left_on=['event_name', 'timestamp'], right_on=['event_name', 'timestamp'],how='outer')
    data_full = data_full.merge(data_65, left_on=['event_name', 'timestamp'], right_on=['event_name', 'timestamp'],how='outer')
    data_full = data_full.merge(data_75, left_on=['event_name', 'timestamp'], right_on=['event_name', 'timestamp'],how='outer')
    data_full = data_full.merge(data_85, left_on=['event_name', 'timestamp'], right_on=['event_name', 'timestamp'],how='outer')

    data_full['goals'] = (data_full == 'WINNER').T.sum()

    data_full.to_excel("event_names_full.xlsx")

    return data_full

def get_goals(df):
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['minutes'] = df['timestamp'] - df['timestamp'].iloc[0]
    goals_list = [np.nan,np.nan,np.nan]
    if df['runner_name'].iloc[0] == 'Under 2.5 Goals':
        df['return_ltp'] = df['ltp'].pct_change()
        df['is_goal'] = df['return_ltp'] > 0.20
        df_goals = df[df['is_goal']==True]
        df_goals_minutes_list = df_goals['minutes'].tolist()
        goals_list[0:len(df_goals_minutes_list)] = df_goals_minutes_list
        if df['minutes'].iloc[-1] < datetime.timedelta(hours=1, minutes=45,seconds=0,milliseconds=0):
            goals_list[2] = df['minutes'].iloc[-1]
    elif df['runner_name'].iloc[0] == 'Over 2.5 Goals':
        df['return_ltp'] = df['ltp'].pct_change()
        df['is_goal'] = df['return_ltp'] < -0.20
        df_goals = df[df['is_goal']==True]
        df_goals_minutes_list = df_goals['minutes'].tolist()
        goals_list[0:len(df_goals_minutes_list)] = df_goals_minutes_list
        if df['minutes'].iloc[-1] < datetime.timedelta(hours=1, minutes=45,seconds=0,milliseconds=0):
            goals_list[2] = df['minutes'].iloc[-1]
    return goals_list

def strat0(stake,tax,minute,bidask, event_name):
    import time
    # Load the full Over 2.5 dataset
    data_loader = Loader("over_under")
    # data_25 = data_loader.create_full_dataframe("Over 2.5 Goals")
    data_25 = data_loader.create_full_dataframe("Over 2.5 Goals",event_name)
    print(data_25['market_start_time'].unique())

    # Join with tradable teams' teams
    ##### TO BE IMPLEMENTED #####

    # Compute PnL of each game 1 by 1
    unique_event_ids = data_25['event_id'].unique()

    list_goal_1 = []
    list_goal_2 = []
    list_goal_3 = []
    list_country_code = []
    list_date = []
    list_market_id = []
    list_event_name = []
    list_runner_name = []
    list_ltp_in = []
    list_ltp_out = []
    list_min_in = []
    list_min_out = []
    list_pnl = []

    start_time = time.time()
    for ind,val in enumerate(unique_event_ids):
        print(str(ind) + '/' + str(len(unique_event_ids)))
        df = data_25.copy()
        df = df[df['event_id']==val]

        df_full = df.copy()
        df = df[df['inplay'] == 1]
        if df.empty == False:
            # set timestamp as index and convert it to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['minutes'] = df['timestamp'] - df['timestamp'].iloc[0]
            df = df.set_index(['minutes'], drop=False)

            country_code = df['country_code'].iloc[0]
            date = df['date'].iloc[0]
            market_id = df['market_id'].iloc[0]
            event_name = df['event_name'].iloc[0]
            runner_name = df['runner_name'].iloc[0]

            # ltp_in at t=0 : if early goal or not available --> take last quote offplay instead

            ltp_in = df_full[df_full['inplay']==0]['ltp'].iloc[-1] * (1 - bidask)

            # ltp_in = df['ltp'].iloc[0] * (1 - bidask)
            min_in = df['minutes'].iloc[0]
            try:
                ltp_out = df['ltp'].loc[datetime.timedelta(hours=0, minutes=minute):][0] * (1 + bidask)
                min_out = df['minutes'].loc[datetime.timedelta(hours=0, minutes=minute):][0]
                pnl = (ltp_in / ltp_out - 1) * stake

                if pnl > 0:
                    pnl = pnl * (1 - tax)  # Tax on gains
                else:
                    pnl = pnl

            except Exception as e:
                print(e)
                if df['runner_name'].iloc[0] == 'Under 2.5 Goals':
                    ltp_out = 1000
                    min_out = np.nan
                    pnl = -stake
                elif df['runner_name'].iloc[0] == 'Over 2.5 Goals':
                    ltp_out = 1
                    min_out = np.nan
                    pnl = (ltp_in / ltp_out - 1) * stake

            print('ltp in: ' + str(ltp_in))
            print('ltp out: ' + str(ltp_out))
            print('P&L: ' + str(format(pnl, '.2f')))

            goals_list = get_goals(df)
            list_goal_1.append(goals_list[0])
            list_goal_2.append(goals_list[1])
            list_goal_3.append(goals_list[2])
            list_country_code.append(country_code)
            list_date.append(date)
            list_market_id.append(market_id)
            list_event_name.append(event_name)
            list_runner_name.append(runner_name)
            list_ltp_in.append(ltp_in)
            list_ltp_out.append(ltp_out)
            list_min_in.append(min_in)
            list_min_out.append(min_out)
            list_pnl.append(pnl)
        else:
            print('empty dataframe')
            return None

    final_df = pd.DataFrame({'country_code':list_country_code,'date':list_date,\
                             'market_id':list_market_id,'event_name':list_event_name,\
                             'runner_name':list_runner_name,'ltp_in':list_ltp_in,'ltp_out':list_ltp_out,\
                             'min_in':list_min_in,'min_out':list_min_out,\
                             'pnl':list_pnl,'goal_1':list_goal_1,'goal_2':list_goal_2,'goal_3':list_goal_3})
    print('Cumulative P&L: ' + str(format(sum(list_pnl), '.2f')))
    elapsed_time = time.time() - start_time
    print('total time (in s): ' + str(elapsed_time))

    return final_df


if __name__ == "__main__":

    # Compute the number of goals for each event_name
    # df_goals = get_goals("over_under")

    # Strat0
    stake = 2
    tax = 0.08
    minute = 15
    bidask = 0.025
    import time

    results = []
    events = pd.read_csv("E:\\strat0\\list_event_names.csv", header = 0, index_col=0, sep = ";")
    events = events["event_name"].unique().tolist()
    failed = []
    i = 1
    for event in events:
        try:
            df = strat0(stake,tax,minute,bidask, event)
            if df is None:
                failed.append(event)
            else:
                results.append(strat0(stake, tax, minute, bidask, event))
        except:
            failed.append(event)
            print("failed : " + event)

        print("progress : " + str(i / len(events)))
        i = i+1

    pd.DataFrame(failed).to_excel("E:\\strat0\\strat0_failed" + str(time.strftime('%Y-%m-%d_%H-%M-%S', time.gmtime(time.time()))) + ".xlsx")
    df_strat0 = pd.concat(results, axis = 0)


    writer = pd.ExcelWriter(
        'E:\\strat0\\strat0_' + str(time.strftime('%Y-%m-%d_%H-%M-%S', time.gmtime(time.time()))) + '.xlsx')

    df_strat0.to_excel(writer, '0-' + str(minute) + 'min')
    writer.save()




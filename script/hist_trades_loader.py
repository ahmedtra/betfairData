from sqlalchemy import create_engine

from common import initialize_logging
from data.hist_trades_loader import Loader


import pandas as pd
from pandas.io import sql
import MySQLdb

def process_day(df_all):
    df_goals = df_all[df_all['event'].str.contains('Over/Under ')]
    df_score = df_all[df_all['event']== 'Correct Score']
    games_list = df_goals['event_name'].unique().tolist()
    print(games_list)
    print('#games = '+str(len(games_list)))
    df_OverUnder = pd.DataFrame()

    for ind,game in enumerate(games_list):

            df_g = get_OverUnder(df_goals[df_goals['event_name']==game], df_score[df_score['event_name']==game])
            df_OverUnder = pd.concat([df_OverUnder,df_g])

    return df_OverUnder


def get_Goals(df):
    event_list = ['Over/Under 1.5 Goals','Over/Under 2.5 Goals','Over/Under 3.5 Goals','Over/Under 4.5 Goals',\
                  'Over/Under 5.5 Goals','Over/Under 6.5 Goals','Over/Under 7.5 Goals','Over/Under 8.5 Goals']
    selection_list = ['Over 1.5 Goals','Over 2.5 Goals','Over 3.5 Goals','Over 4.5 Goals',\
                      'Over 5.5 Goals','Over 6.5 Goals','Over 7.5 Goals','Over 8.5 Goals']

    df_0 = df.copy()
    df_0 = df_0[df_0['event']=='Over/Under 0.5 Goals']
    df_0 = df_0[df_0['selection']=='Over 0.5 Goals']
    df_0 = df_0[df_0['in_play']=='IP']
    df_0 = pd.pivot_table(df_0,values=['win_flag'],\
                index=['competition_type','competition','event_name','scheduled_off'],\
                          columns=['selection'],aggfunc=max)
    N_Goals = 0
    if df_0.values == 1.0:
        N_Goals += 1
        for i, elem in enumerate(event_list):
            df_aux = df.copy()
            df_aux = df_aux[df_aux['event'] == elem]
            df_aux = df_aux[df_aux['selection'] == selection_list[i]]
            df_aux = df_aux[df_aux['in_play'] == 'IP']
            df_aux = pd.pivot_table(df_aux, values=['win_flag'], \
                            index=['competition_type', 'competition', 'event_name', 'scheduled_off'], \
                            columns=['selection'], aggfunc=max)
            if df_aux.values == 1.0:
                N_Goals += 1
            else:
                break

    else:
        N_Goals = 0

    # print(N_Goals)
    return N_Goals

def get_Score(df):
    df_0 = df.copy()
    df_0 = df_0[df_0['event'] == 'Correct Score']
    if len(df_0)>0:
        df_0 = df_0[df_0['win_flag'] == 1]
        if len(df_0)>0:
            list_score = split_score(df_0['selection'].iloc[0])
        else:
            list_score = [None,None]
    else:
        list_score = [None,None]

    return list_score

def split_score(s):
    if isinstance(s, str):
        list_score = s.split(" - ")
        if len(list_score)==2:
            return list_score
        else:
            return [None, None]
    else:
        return [None, None]


def get_OverUnder(df, df_score): # To be called on 1 game only (pass a dataframe containing 1 match only)

    event_list = [ 'Over/Under 1.5 Goals','Over/Under 2.5 Goals','Over/Under 3.5 Goals','Over/Under 4.5 Goals',\
                  'Over/Under 5.5 Goals','Over/Under 6.5 Goals','Over/Under 7.5 Goals','Over/Under 8.5 Goals']
    selection_list = ['Under 1.5 Goals','Under 2.5 Goals','Under 3.5 Goals','Under 4.5 Goals',\
                      'Under 5.5 Goals','Under 6.5 Goals','Under 7.5 Goals','Under 8.5 Goals']
    #
    # df_tot = pd.DataFrame(index=['competition_type', 'competition', 'event_name', 'scheduled_off', 'latest_taken'], \
    #           columns=['selection'])
    df_0 = df.copy()
    df_0 = df_0[df_0['event']=='Over/Under 0.5 Goals']
    df_0 = df_0[df_0['selection']=='Under 0.5 Goals']
    df_0 = df_0[df_0['in_play']=='IP']

    if len(df_0) == 0:
        df_0 = df.iloc[0:2]
        df_0["odds"] = 1001
        df_0["latest_taken"] = 1001

    df_0 = pd.pivot_table(df_0,values=['odds'], \
              index=['competition_type', 'competition', 'event_name', 'scheduled_off', 'latest_taken'], \
              columns=['selection'])
    # print(df_0)

    df_0 = df_0.iloc[[0,-1]]
    df_temp = df_0.copy()
    df_0 = df_0.reset_index(level=4, drop=True).reset_index()
    df_tot = df_0.copy()
    for i,elem in enumerate(event_list):
        try:
            df_aux = df.copy()
            df_aux = df_aux[df_aux['event'] == elem]
            df_aux = df_aux[df_aux['selection'] == selection_list[i]]
            df_aux = df_aux[df_aux['in_play'] == 'IP']
            df_aux = pd.pivot_table(df_aux, values=['odds'],\
                index=['competition_type', 'competition', 'event_name','scheduled_off','latest_taken'],\
                                  columns=['selection'])
            df_aux = df_aux[df_aux.index.levels[4] > df_temp.iloc[-1].name[4]]
            df_aux = df_aux.iloc[[df_aux.reset_index()['odds'].idxmax()[0], -1]]
            df_temp = df_aux.copy()

            df_aux = df_aux.reset_index(level=4, drop=True).reset_index()
            df_tot = df_tot.merge(df_aux)
        except:
            pass

    df_tot = df_tot.iloc[[0,-1]].reset_index(drop=True)
    df_tot['NGoals'] = get_Goals(df)

    score = get_Score(df_score)
    df_tot['Goals_1'] = score[0]
    df_tot['Goals_2'] = score[1]
    #
    # print(df_tot)

    return df_tot


if __name__ == "__main__":

    initialize_logging("favorites")
    engine = create_engine("mysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="Betfair",
                                   db="betfair"))
    data_loader = Loader()
    result = data_loader.load_data_by_date()
    i = 0
    for data in result:
        i+=1
        df =process_day(data)
        df.to_sql(con = engine, name = "goals", if_exists = "append", flavor = "mysql")
        df.to_csv('data'+str(i)+'.csv')
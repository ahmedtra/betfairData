from data.hist_trades_loader import Loader


import pandas as pd
from sqlalchemy import create_engine

def process_day(df_all):
    df_all = df_all[df_all['event'].str.contains('Match Odds')]
    games_list = df_all['event_name'].unique().tolist()
    print(games_list)
    print('#games = '+str(len(games_list)))

    all_rows = []
    for ind,game in enumerate(games_list):
        Match_fields = df_all[df_all['event_name']==game]
        row = get_MatchOdds(Match_fields, game)
        all_rows.append(row)

    df_sql = pd.DataFrame(all_rows, columns = ["scheduled_off", "full_description", "competition", "competition_type",
                                               "event_name" ,"team1", "team2", "1", "x", "2" , "vol_1", "vol_x", "vol_2", "bet_1",
                                               "bet_x", "bet_2", "favorite_odd", "favorite_flag", "winner",
                                               "IP_1", "IP_x", "IP_2", "min_from_start_1", "min_from_start_x",
                                               "min_from_start_2", "vm_ep_1", "vm_ep_x", "vm_ep_2"])

    return df_sql

def split_team(s, i):
    if isinstance(s, str):
        teams = s.split(" v ")
        if len(teams) > i:
            return teams[i]
        else:
            return None
    else:
        return None


def get_MatchOdds(df, game): # To be called on 1 game only (pass a dataframe containing 1 match only)

    team1 = split_team(game, 0)
    team2 = split_team(game, 1)
    teamx = "The Draw"

    scheduled_off = df.iloc[0]["scheduled_off"]
    dt_actual_off = df.iloc[0]["dt_actual_off"]
    full_description = df.iloc[0]["full_description"]
    competition = df.iloc[0]["competition"]
    competition_type = df.iloc[0]["competition_type"]
    event_name = df.iloc[0]["event_name"]


    if team1 is None or team2 is None:
        teams = df['selection'].unique().tolist()
        teams.remove("The Draw")
        if len(teams) == 2:
            if full_description.find(teams[0]) < full_description.find(teams[1]):
                team1 = teams[0]
                team2 = teams[1]
            else:
                team1 = teams[1]
                team2 = teams[0]

    teams = {"1":team1, "x": teamx, "2":team2}

    df_ip = df.sort_values("first_taken")
    df_ip = df_ip[df_ip["in_play"] == "IP"].groupby("selection").first()

    df_ep = df[df["latest_taken"] < scheduled_off]
    df_ep = df_ep.sort_values("latest_taken")
    df_ep = df_ep[df_ep["in_play"] == "PE"]
    grouped_df = df_ep.groupby("selection")
    volume_matched_ep = grouped_df.sum()
    df_ep = grouped_df.last()

    volume_total = []
    winner = None
    odds = []
    inp = []
    favorite_odd = 1001
    favorite_flag = None

    volume = []
    bets = []
    time_to_scheduled_off = []

    for name, team in teams.items():
        if team in df_ip.index:
            od = round(df_ip.loc[team, "odds"], 2)
            vol = round(df_ip.loc[team, "volume_matched"], 2)
            bet = df_ip.loc[team, "number_bets"]
            time_to_game = round((df_ip.loc[team, "first_taken"] - scheduled_off).total_seconds() / 60, 2)
            odds.append(od)
            volume.append(vol)
            bets.append(bet)
            time_to_scheduled_off.append(time_to_game)
            if df_ip.loc[team, "win_flag"] == 1:
                winner = name
            if od < favorite_odd:
                favorite_flag = name
                favorite_odd = od
            inp.append(df_ip.loc[team, "in_play"])
        elif team in df_ep.index:
            od = round(df_ep.loc[team, "odds"], 2)
            vol = round(df_ep.loc[team, "volume_matched"], 2)
            bet = df_ep.loc[team, "number_bets"]
            time_to_game = round((df_ep.loc[team, "latest_taken"] - scheduled_off).total_seconds() / 60, 2)
            odds.append(od)
            volume.append(vol)
            bets.append(bet)
            time_to_scheduled_off.append(time_to_game)
            if df_ep.loc[team, "win_flag"] == 1:
                winner = name
            if od < favorite_odd:
                favorite_flag = name
                favorite_odd = od
            inp.append(df_ep.loc[team, "in_play"])
        else:
            odds.append(None)
            volume.append(None)
            bets.append(None)
            time_to_scheduled_off.append(None)
            inp.append(None)

        if team in volume_matched_ep:
            volume_total.append(volume_matched_ep.loc[team, "volume_matched"])
        else:
            volume_total.append(0)

    odds = [scheduled_off, full_description, competition, competition_type, event_name, team1, team2] \
           + odds + volume + bets + [favorite_odd, favorite_flag, winner] + inp + time_to_scheduled_off \
            + volume_matched_ep

    return odds



if __name__ == "__main__":



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
        df.to_sql(con = engine, name = "favorite", if_exists = "append")
        df.to_csv('data'+str(i)+'.csv')
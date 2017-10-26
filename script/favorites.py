from data.hist_trades_loader import Loader


import pandas as pd


def process_day(df_all):
    df_all = df_all[df_all['event'].str.contains('Match Odds')]
    games_list = df_all['event_name'].unique().tolist()
    print(games_list)
    print('#games = '+str(len(games_list)))

    N_Goals = {}
    all_rows = []
    for ind,game in enumerate(games_list):
        Match_fields = df_all[df_all['event_name']==game]
        row = get_MatchOdds(Match_fields, game)
        all_rows.append(row)

    df_sql = pd.DataFrame(all_rows, columns = ["scheduled_off", "full_description", "competition", "competition_type",
                                               "event_name" , "1", "x", "2" , "vol_1", "vol_x", "vol_2", "bet_1",
                                               "bet_x", "bet_2", "favorite_odd", "favorite_flag", "winner",
                                               "IP_1", "IP_x", "IP_2"])
    
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
    full_description = df.iloc[0]["full_description"]
    competition = df.iloc[0]["competition"]
    competition_type = df.iloc[0]["competition_type"]
    event_name = df.iloc[0]["event_name"]



    teams = {"1":team1, "x": teamx, "2":team2}

    df_ip = df.sort_values("first_taken")
    df_ip = df_ip[df_ip["in_play"] == "IP"].groupby("selection").first()

    df_ep = df.sort_values("latest_taken")
    df_ep = df_ep[df_ep["in_play"] == "PE"].groupby("selection").last()

    winner = None
    odds = []
    inp = []
    favorite_odd = 1001
    favorite_flag = None

    volume = []
    bets = []

    for name, team in teams.items():
        if team in df_ip.index():
            od = df_ip[team, "odds"]
            vol = df_ip[team, "volume_matched"]
            bet = df_ip[team, "number_bets"]
            odds.append(od)
            volume.append(vol)
            bets.append(bet)
            if df_ip[team, "win_flag"] == 1:
                winner = name
            if od < favorite_odd:
                favorite_flag = name
                favorite_odd = od
            inp.append(df_ip[team, "in_play"])
        elif team in df_ep.index():
            od = df_ep[team, "odds"]
            vol = df_ep[team, "volume_matched"]
            bet = df_ep[team, "number_bets"]
            odds.append(od)
            volume.append(vol)
            bets.append(bet)
            if df_ep[team, "win_flag"] == 1:
                winner = name
            if od < favorite_odd:
                favorite_flag = name
                favorite_odd = od
            inp.append(df_ep[team, "in_play"])
        else:
            odds.append(None)

    odds = [scheduled_off, full_description, competition, competition_type, event_name] + odds + volume + bets + [favorite_odd, favorite_flag, winner] + inp

    return odds



if __name__ == "__main__":
    data_loader = Loader()
    result = data_loader.load_data_by_date()
    for data in result:
        df = process_day(data)
        df.to_csv('test_data.csv')
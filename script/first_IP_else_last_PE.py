
def split_team(s, i):
    if isinstance(s, str):
        teams = s.split(" v ")
        if len(teams) > i:
            return teams[i]
        else:
            return None
    else:
        return None


def get_MatchOdds_first_IP_else_last_PE(df, game): # To be called on 1 game only (pass a dataframe containing 1 match only)

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
        if teamx in teams:
            teams.remove(teamx)
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

    df_pe = df[df["latest_taken"] < dt_actual_off]
    df_pe = df_pe.sort_values("latest_taken")
    df_pe = df_pe[df_pe["in_play"] == "PE"]
    grouped_df = df_pe.groupby("selection")
    volume_matched_pe = grouped_df.sum()
    df_pe = grouped_df.last()

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
        elif team in df_pe.index:
            od = round(df_pe.loc[team, "odds"], 2)
            vol = round(df_pe.loc[team, "volume_matched"], 2)
            bet = df_pe.loc[team, "number_bets"]
            time_to_game = round((df_pe.loc[team, "latest_taken"] - scheduled_off).total_seconds() / 60, 2)
            odds.append(od)
            volume.append(vol)
            bets.append(bet)
            time_to_scheduled_off.append(time_to_game)
            if df_pe.loc[team, "win_flag"] == 1:
                winner = name
            if od < favorite_odd:
                favorite_flag = name
                favorite_odd = od
            inp.append(df_pe.loc[team, "in_play"])
        else:
            odds.append(None)
            volume.append(None)
            bets.append(None)
            time_to_scheduled_off.append(None)
            inp.append(None)

        if team in volume_matched_pe.index:
            volume_total.append(round(volume_matched_pe.loc[team, "volume_matched"], 2))
        else:
            volume_total.append(0)

    odds = [dt_actual_off, scheduled_off, full_description, competition, competition_type, event_name, team1, team2] \
           + odds + volume + bets + [favorite_odd, favorite_flag, winner] + inp + time_to_scheduled_off \
            + volume_total

    return odds



def get_MatchOdds_last_PE_else_first_IP(df, game): # To be called on 1 game only (pass a dataframe containing 1 match only)

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
        if teamx in teams:
            teams.remove(teamx)
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

    df_pe = df[df["latest_taken"] < dt_actual_off]
    df_pe = df_pe.sort_values("latest_taken")
    df_pe = df_pe[df_pe["in_play"] == "PE"]
    grouped_df = df_pe.groupby("selection")
    volume_matched_pe = grouped_df.sum()
    df_pe = grouped_df.last()

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
        if team in df_pe.index:
            od = round(df_pe.loc[team, "odds"], 2)
            vol = round(df_pe.loc[team, "volume_matched"], 2)
            bet = df_pe.loc[team, "number_bets"]
            time_to_game = round((df_pe.loc[team, "latest_taken"] - dt_actual_off).total_seconds() / 60, 2)
            odds.append(od)
            volume.append(vol)
            bets.append(bet)
            time_to_scheduled_off.append(time_to_game)
            if df_pe.loc[team, "win_flag"] == 1:
                winner = name
            if od < favorite_odd:
                favorite_flag = name
                favorite_odd = od
            inp.append(df_pe.loc[team, "in_play"])
        elif team in df_ip.index:
            od = round(df_ip.loc[team, "odds"], 2)
            vol = round(df_ip.loc[team, "volume_matched"], 2)
            bet = df_ip.loc[team, "number_bets"]
            time_to_game = round((df_ip.loc[team, "first_taken"] - dt_actual_off).total_seconds() / 60, 2)
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
        else:
            odds.append(None)
            volume.append(None)
            bets.append(None)
            time_to_scheduled_off.append(None)
            inp.append(None)

        if team in volume_matched_pe.index:
            volume_total.append(round(volume_matched_pe.loc[team, "volume_matched"], 2))
        else:
            volume_total.append(0)

    odds = [dt_actual_off,scheduled_off, full_description, competition, competition_type, event_name, team1, team2] \
           + odds + volume + bets + [favorite_odd, favorite_flag, winner] + inp + time_to_scheduled_off \
            + volume_total

    return odds

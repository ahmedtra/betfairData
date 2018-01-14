from data.hist_trades_loader import Loader


import pandas as pd
import datetime
from match import list_match

if __name__ == "__main__":

    data_loader = Loader("json")
    result = data_loader.load_data_by_date()
    df = pd.DataFrame()
    dates = [datetime.date(2015,5,d) for d in range(10, 15)]

    for date in dates:
        data = data_loader.load_df_data_date(date)
        # data = data[data["selection"] ==  "Under 0.5 Goals"]
        # data = data[data["event"] ==  "Match Odds"]
        # data = data[data["event_name"] ==  "Cabofriense v Luverdense"]
        # data = data[data["event_name"].isin(list_match)]
        # sorted_data = data.sort_values("first_taken")
        # sorted_data = sorted_data[sorted_data["in_play"] == "IP"]
        # first_odd = sorted_data.groupby("event_name").first()
        # df = df.append(first_odd)
        df = df.append(data)

    df.to_csv('week.csv')
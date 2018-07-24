from data.hist_trades_loader import Loader

import pandas as pd
import datetime
from match import list_match

if __name__ == "__main__":

    data_loader = Loader("over_under")

    runner_05 = "Over 0.5 Goals"
    runner_15 = "Over 1.5 Goals"
    runner_25 = "Over 2.5 Goals"
    runner_35 = "Over 3.5 Goals"
    runner_45 = "Over 4.5 Goals"

    data_05 = data_loader.create_event_name_table(runner_05)
    data_05.to_excel("event_names_05.xlsx")
    data_15 = data_loader.create_event_name_table(runner_15)
    data_15.to_excel("event_names_15.xlsx")
    data_25 = data_loader.create_event_name_table(runner_25)
    data_25.to_excel("event_names_25.xlsx")
    data_35 = data_loader.create_event_name_table(runner_35)
    data_35.to_excel("event_names_35.xlsx")
    data_45 = data_loader.create_event_name_table(runner_45)
    data_45.to_excel("event_names_45.xlsx")


    # data_05 = data_loader.load_data(runner_05, "Real Madrid v Barcelona")
    # data_05.to_csv("data_05.csv")
    # data_15 = data_loader.load_data(runner_15)
    # data_25 = data_loader.load_data(runner_25)
    # data_35 = data_loader.load_data(runner_35)
    # data_45 = data_loader.load_data(runner_45)

    def get_goals_table(df_05, df_15, df_25, df_35, df_45):

        events = df_05['market_id'].unique().tolist()
        i=0
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

            df_05_aux = df_05[df_05['market_id'] == val_].copy()
            df_15_aux = df_15[df_15['market_id'] == val_].copy()
            df_25_aux = df_25[df_25['market_id'] == val_].copy()
            df_35_aux = df_35[df_35['market_id'] == val_].copy()
            df_45_aux = df_45[df_45['market_id'] == val_].copy()



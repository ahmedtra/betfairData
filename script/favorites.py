import traceback

from structlog import get_logger

import first_IP_else_last_PE
from common import initialize_logging
from data.hist_trades_loader import Loader


import pandas as pd
from sqlalchemy import create_engine

def process_day(df_all, get_MatchOdds):
    df_all = df_all[df_all['event'].str.contains('Match Odds')]
    date = df_all["scheduled_off"].iloc[0]
    games_list = df_all['event_name'].unique().tolist()
    print(games_list)
    print('#games = '+str(len(games_list)))

    all_rows = []
    for ind,game in enumerate(games_list):
        try:
            get_logger().info("match", match = game, date = date)
            Match_fields = df_all[df_all['event_name']==game]
            row = get_MatchOdds(Match_fields, game)
            all_rows.append(row)
        except Exception:
            get_logger().error(traceback.format_exc())
    df_sql = pd.DataFrame(all_rows, columns = ["dt_actual_off", "scheduled_off", "full_description", "competition", "competition_type",
                                               "event_name" ,"team1", "team2", "1", "x", "2" , "vol_1", "vol_x", "vol_2", "bet_1",
                                               "bet_x", "bet_2", "favorite_odd", "favorite_flag", "winner",
                                               "IP_1", "IP_x", "IP_2", "min_from_start_1", "min_from_start_x",
                                               "min_from_start_2", "vm_pe_1", "vm_pe_x", "vm_pe_2"])
    return df_sql



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
        df =process_day(data, first_IP_else_last_PE.get_MatchOdds_first_IP_else_last_PE)
        df.to_sql(con = engine, name = "favorite_first_IP_last_PE", if_exists = "append")
        df =process_day(data, first_IP_else_last_PE.get_MatchOdds_last_PE_else_first_IP)
        df.to_sql(con = engine, name = "favorite_last_PE_first_IP", if_exists = "append")
        # df.to_csv('data'+str(i)+'.csv')
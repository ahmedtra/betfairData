from datetime import datetime
from os.path import join
import pandas as pd
import pandas.io.sql as pd_sql
import json
import sqlalchemy as sa
import pymysql
pymysql.install_as_MySQLdb()
# from common import get_json_files_dirs, safe_delete

from data.cassandra_wrapper.access import  CassTradesOVERUNDERRepository


# import multiprocessing

TYPE = "basic"

def convert_to_datatime(df):
    df["date"] = df["date"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.000"))
    df["market_start_time"] = df["market_start_time"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.000"))
    df["timestamp"] = df["timestamp"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.000"))
    df["open_date"] = df["open_date"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.000"))
    return df

def sec_to_min_df(df_all):
    # Lay under 2.5 for the 1st 15 InPlay minutes and see if there is alpha

    list_events = df_all["event_name"].unique().tolist()

    df_res = []

    for event in list_events:
        df = df_all[df_all["event_name"] == event].copy()
        df = df[df["inplay"] == 1]
        if df.empty==False:
            # set timestamp as index and convert it to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['minutes'] = df['timestamp'] - df['timestamp'].iloc[0]
            df = df.set_index(['minutes'])
            df_min = df.resample('min').first()
            df_min['timestamp_min'] = df_min['timestamp'].iloc[0] + df_min.index
            df_res.append(df_min)

    return pd.concat(df_res, axis = 1)


class Recorder():
    def __init__(self):
        self.save_meta_data = False
        # dir_origin, dir_completed = get_json_files_dirs()
        # self.path = dir_origin
        # self.path_completed = dir_completed
        self.cass_repository = CassTradesOVERUNDERRepository()

    def write_to_sql(self, df, tablename):
        conn = sa.create_engine("mysql://root:Betfair@localhost/betfair?charset=utf8?host=localhost?port=3306", convert_unicode=True)
        pd_sql.to_sql(df, tablename, conn, if_exists='append', index=False)


    def process_json_file(self, filename, file_path = False):
            if not file_path:
                filepath = join(self.path, filename)
            else:
                filepath = filename

            market_def = {}

            runners = {}
            file_data = []
            for line in open(filepath):
                json_file = json.loads(line)

                if "marketDefinition" in list(json_file["mc"][0].keys()):
                    market_def = json_file["mc"][0]["marketDefinition"]
                    market_def["market_id"] = json_file["mc"][0]["id"]
                    if "start_time" in market_def.keys() and "date" not in market_def.keys():
                        market_def["date"] = market_def["start_time"].date()

                    market_def["open_date"] = datetime.strptime(market_def["openDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    if "runners" in market_def.keys():
                        for runner in market_def["runners"]:
                            runners[runner["id"]] = runner
                    if "regulators" in market_def.keys():
                        if market_def["regulators"][0] != "MR_INT":
                            break
                    if "countryCode" not in market_def.keys():
                        market_def["countryCode"] = None
                    if "marketTime" not in market_def.keys():
                        market_def["marketTime"] = None
                    else:
                        market_def["start_time"] = datetime.strptime(market_def["marketTime"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    if "timezone" not in market_def.keys():
                        market_def["timezone"] = None
                    if "bettingType" not in market_def.keys():
                        market_def["bettingType"] = None
                    if "marketType" not in market_def.keys():
                        market_def["marketType"] = None
                    #self.record_market_defintion(market_def)

                if "rc" not in list(json_file["mc"][0].keys()):

                    continue

                for ltp in json_file["mc"][0]["rc"]:
                    data = {}
                    data["ltp"] = ltp["ltp"]
                    if "tv" in ltp.keys():
                        data["tv"] = ltp["tv"]
                    if "batb" in ltp.keys():
                        for elem in ltp["batb"]:
                            data["batb_price_"+str(elem[0])] = elem[1]
                            data["batb_volume_" + str(elem[0])] = elem[2]
                    if "batl" in ltp.keys():
                        for elem in ltp["batl"]:
                            data["batl_price_"+str(elem[0])] = elem[1]
                            data["batl_volume_" + str(elem[0])] = elem[2]
                    if "trd" in ltp.keys():
                        vwap = 0
                        trd_vol = 0
                        for elem in ltp["trd"]:
                            vwap = vwap + elem[0]*elem[1]
                            trd_vol = trd_vol + elem[1]
                        vwap = vwap / trd_vol
                        data["vwap"] = vwap
                        data["trd_vol"] = trd_vol

                    data["market_id"] = json_file["mc"][0]["id"]
                    data["selection_id"] = ltp["id"]


                    if ltp["id"] in list(runners.keys()):
                        data["runner_name"] = runners[ltp["id"]]["name"]
                        data["sort_priority"] = runners[ltp["id"]]["sortPriority"]
                        data["status"] = runners[ltp["id"]]["status"]
                        if "inPlay" not in market_def.keys():
                            data["inplay"] = 0
                        else:
                            data["inplay"] = market_def["inPlay"] * 1
                    if "date" not in market_def.keys():
                        date_start_day = datetime.fromtimestamp(json_file["pt"] / 1000).date()
                        market_def["date"] = datetime(date_start_day.year, date_start_day.month, date_start_day.day)
                    data["date"] = market_def["date"]
                    data["timestamp"] = datetime.fromtimestamp(json_file["pt"] / 1000)
                    data["market_start_time"] = market_def["start_time"]
                    data["event_name"] = market_def["eventName"]
                    data["country_code"] = market_def["countryCode"]
                    data["type"] = market_def["name"]
                    data["timezone"] = market_def["timezone"]
                    data["open_date"] = market_def["open_date"]
                    data["betting_type"] = market_def["bettingType"]
                    data["market_type"] = market_def["marketType"]
                    data["event_id"] = market_def["eventId"]
                    data["status_market"] = market_def["status"]
                    file_data.append(data)


            # add the last status (Winner / Loser) of the data
            if len(file_data) > 0:
                for runner in market_def["runners"]:
                    data = {}
                    if "date" not in market_def.keys():
                        date_start_day = datetime.fromtimestamp(json_file["pt"] / 1000).date()
                        market_def["date"] = datetime(date_start_day.year, date_start_day.month, date_start_day.day)
                    data["date"] = market_def["date"]
                    data["timestamp"] = datetime.fromtimestamp(json_file["pt"] / 1000)
                    data["market_start_time"] = market_def["start_time"]
                    data["event_name"] = market_def["eventName"]
                    data["country_code"] = market_def["countryCode"]
                    data["type"] = market_def["name"]
                    data["timezone"] = market_def["timezone"]
                    data["open_date"] = market_def["open_date"]
                    data["betting_type"] = market_def["bettingType"]
                    data["market_type"] = market_def["marketType"]
                    data["event_id"] = market_def["eventId"]
                    data["status_market"] = market_def["status"]
                    data["status"] = runner["status"]
                    data["sort_priority"] = runner["sortPriority"]
                    data["runner_name"] = runner["name"]
                    data["selection_id"] = runner["id"]
                    data["market_id"] = json_file["mc"][0]["id"]
                    if "inPlay" not in market_def.keys():
                        data["inplay"] = 0
                    else:
                        data["inplay"] = market_def["inPlay"] * 1
                    file_data.append(data)

            df = pd.DataFrame(file_data)
            print(df.tail(3))
            if len(df) == 0:
                print("file empty")
                return
            # Push Dataframe into the Database
            try:
                if df["market_type"].unique()[0] == "MATCH_ODDS":
                    # self.write_to_sql(df,'eu_mo_apr_2018')
                    print("Successful push to eu_mo_apr_2018")
                elif "OVER_UNDER" in df["market_type"].unique()[0]:
                    # self.write_to_sql(df,'eu_ou_25_apr_2018')
                    if TYPE == "basic":
                        df = convert_to_datatime(df)
                        self.record_trade(df, "basic_min")
                        # self.record_trade(file_data, "basic_min") # TEST CHANGE, A REMETTRE COMME AVANT
                        # self.write_to_sql(df, 'basic_may18_ou25')
                    else:
                        df["date"] = df["date"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.000"))
                        df["market_start_time"] = df["market_start_time"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.000"))
                        df["timestamp"] = df["timestamp"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.000"))
                        df["open_date"] = df["open_date"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.000"))

                        data_min = sec_to_min_df(df)
                        df["timestamp_min"] = 0
                        self.record_trade(df, "advanced_sec")
                        self.record_trade(data_min, "advanced_min")

                    # print("Successful push to eu_ou_25_apr_2018")
            except Exception as e:
                print("Error: " + str(e))
                pass
            print("\n")



            # self.query_secdb.commit_changes()

            # file_completed = join(self.path_completed, filename.name)

            # safe_delete(filepath)

            # safe_move(filepath, file_completed)

    def record_trade(self, data, base):
        self.cass_repository.save_async(data, base)

    def record_market_defintion(self, market_def):
        market_start_time = datetime.strptime(market_def["marketTime"], "%Y-%m-%dT%H:%M:%S.%fZ")
        market_def["start_time"] = market_start_time
        if "countryCode" not in market_def.keys():
            market_def["countryCode"] = None
        if "timezone" not in market_def.keys():
            market_def["timezone"] = None
        if "bettingType" not in market_def.keys():
            market_def["bettingType"] = None

        if self.save_meta_data:
            self.query_secdb.add_event(market_def["eventId"], market_def["eventName"],
                                       market_def["countryCode"], market_def["timezone"],
                                       None, market_start_time, None, 1)
            self.query_secdb.add_market(market_def["market_id"], market_def["name"],
                                        market_start_time, market_def["bettingType"],
                                        market_def["eventId"])

        if "runners" in market_def.keys():
            if self.save_meta_data:
                for runner in market_def["runners"]:
                    self.query_secdb.add_runner(runner["id"], runner["name"], None, runner["sortPriority"], None)
                    self.query_secdb.add_runner_map(market_def["market_id"], runner["id"])






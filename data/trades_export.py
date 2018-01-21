from betfair.constants import PriceData, MarketProjection
from betfair.models import PriceProjection, MarketFilter
from datetime import datetime, date
from time import sleep
from os import listdir, scandir
from os.path import isfile, join
import pandas as pd
import json
from common import safe_move, get_json_files_dirs, safe_delete

from data.cassandra_wrapper.access import CassTradesRepository

# import multiprocessing

class Recorder():
    def __init__(self):
        self.save_meta_data = False
        dir_origin, dir_completed = get_json_files_dirs()
        self.path = dir_origin
        self.path_completed = dir_completed
        self.cass_repository = CassTradesRepository()
        #self.query_secdb = DBQuery()
    #
    # def file_generator(self, chunk = 8):
    #     def loop_files(path):
    #         for file in scandir(path):
    #             if isfile(join(path, file)):
    #                 yield file
    #
    #     i = 0
    #     l = []
    #     for filename in loop_files(self.path):
    #         filepath = join(self.path, filename)
    #         l.append(filepath)
    #         i += 1
    #         if i == chunk:
    #             yield l
    #             i = 0
    #             l = []
    #
    #
    # def read_json_files(self):
    #     for filenames in self.file_generator():
    #         jobs = []
    #         for files in filenames:
    #             p = multiprocessing.Process(target=self.process_json_file, args=(files, ))
    #             jobs.append(p)
    #             p.start()
    #         for p in jobs:
    #             p.join()

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
                    data["market_id"] = json_file["mc"][0]["id"]
                    data["selection_id"] = ltp["id"]

                    if ltp["id"] in list(runners.keys()):
                        data["runner_name"] = runners[ltp["id"]]["name"]
                        data["sort_priority"] = runners[ltp["id"]]["sortPriority"]
                        data["status"] = runners[ltp["id"]]["status"]
                        if "inPlay" not in market_def.keys():
                            data["inplay"] = False
                        else:
                            data["inplay"] = market_def["inPlay"]
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
            self.record_trade(file_data)

            # self.query_secdb.commit_changes()

            # file_completed = join(self.path_completed, filename.name)

            safe_delete(filepath)

            # safe_move(filepath, file_completed)

    def record_trade(self, data):
        self.cass_repository.save_async(data)

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






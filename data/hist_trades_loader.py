from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from data.cassandra_wrapper.access import CassTradesHistRepository, CassTradesRepository, CassTradesOVERUNDERRepository
from data.sql_wrapper.query import RunnerMapQuery


class Loader():
    def __init__(self, type, base = "basic_min"):
        if type == "old":
            self.cass_repository = CassTradesHistRepository()
        elif type == "json":
            self.cass_repository = CassTradesRepository()
        elif type == "over_under":
            self.cass_repository = CassTradesOVERUNDERRepository()

        self.base = base
        self.query_secdb = RunnerMapQuery()
        self.executor = ThreadPoolExecutor(max_workers=2)

    def load_df_data_date(self, date):
        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)
        result = self.load_data(date, row_factory=pandas_factory)
        return result

    def load_data(self, prim, event_name = None, row_factory = None):

        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)

        result = self.cass_repository.load_data_async(prim, event_name, row_factory = pandas_factory)
        df = result.result()._current_rows
        last_rows = pd.DataFrame()
        while result.has_more_pages:
            result = self.cass_repository.get_next_page(result)
            df = df.append(result.result()._current_rows)
        return df

    def create_event_name_table(self, prim, event_name = None, row_factory = None):

        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)

        result = self.cass_repository.load_data_async(prim, event_name, row_factory = pandas_factory)

        new_tab = result.result()._current_rows
        new_tab = new_tab[new_tab["ltp"].isnull()]
        new_tab = new_tab[["event_name", "market_id", "timestamp", "country_code", "status"]]
        df = new_tab
        last_rows = pd.DataFrame()
        while result.has_more_pages:
            result = self.cass_repository.get_next_page(result)
            new_tab = result.result()._current_rows
            new_tab = new_tab[new_tab["ltp"].isnull()]
            new_tab = new_tab[["event_name", "market_id", "timestamp", "country_code", "status"]]

            df = df.append(new_tab)
        return df

    def load_data_by_date(self):

        dates = self.cass_repository.get_all_dates()
        previous_data = None
        for date, in dates:
            print(date)
            if previous_data is None:
                previous_data = self.load_df_data_date(date)
                continue
            previous_data_future = self.executor.submit(self.load_data, date)
            yield previous_data
            previous_data = previous_data_future.result()


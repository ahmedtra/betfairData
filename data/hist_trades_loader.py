from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from data.cassandra_wrapper.access import CassTradesHistRepository
from data.sql_wrapper.query import RunnerMapQuery


class Loader():
    def __init__(self):
        self.cass_repository = CassTradesHistRepository()
        self.query_secdb = RunnerMapQuery()
        self.executor = ThreadPoolExecutor(max_workers=2)

    def load_df_data_date(self, date):
        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)
        result = self.load_data(date, row_factory=pandas_factory)
        return result

    def load_data(self, date, row_factory = None):
        result = self.cass_repository.load_data_async(date, row_factory = row_factory)
        df = result.result()._current_rows
        while result.has_more_pages:
            result = self.cass_repository.get_next_page(result)
            df = df.append(result.result()._current_rows)
        return df

    def load_data_by_date(self):

        dates = self.cass_repository.get_all_dates()
        previous_data = None
        for date, in dates:
            if previous_data is None:
                previous_data = self.load_df_data_date(date)
                continue
            previous_data_future = self.executor.submit(self.load_data_by_date, date)
            yield previous_data
            previous_data = previous_data_future.result()

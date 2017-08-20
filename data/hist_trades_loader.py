import pandas as pd

from data.cassandra_wrapper.access import CassTradesHistRepository
from data.sql_wrapper.query import RunnerMapQuery

class Loader():
    def __init__(self):
        self.cass_repository = CassTradesHistRepository()
        self.query_secdb = RunnerMapQuery()

    def load_df_data_date(self, date):
        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)
        result = self.load_data(date, row_factory=pandas_factory)
        return result

    def load_data(self, date, row_factory = None):
        result = self.cass_repository.load_data_async(date, row_factory = row_factory)
        return result.result()._current_rows

    def load_data_by_date(self):

        dates = self.cass_repository.get_all_dates()
        for date, in dates:
            yield self.load_df_data_date(date)

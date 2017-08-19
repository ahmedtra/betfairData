import pandas as pd

from data.cassandra_wrapper.access import CassTradesHistRepository
from data.sql_wrapper.query import RunnerMapQuery

class Loader():
    def __init__(self):
        self.cass_repository = CassTradesHistRepository()
        self.query_secdb = RunnerMapQuery()

    def load_df_data(self, market_id, selection_id):
        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)
        result = self.load_data(market_id, selection_id, row_factory=pandas_factory)
        return result

    def load_data(self, market_id, selection_id, row_factory = None):
        result = self.cass_repository.load_data_async(market_id, selection_id, row_factory = row_factory)
        return result.result()._current_rows

    def load_data_by_event(self):
        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)
        events = self.cass_repository.get_all_events()
        for event in events:
            yield self.cass_repository.load_data_events_async(events, row_factory=pandas_factory)
    
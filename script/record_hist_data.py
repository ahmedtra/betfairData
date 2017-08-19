from common import initialize_logging
from data.hist_trades_export import Recorder
from data.sql_wrapper.connection import initialize_secdb

if __name__ == "__main__":
    initialize_secdb()
    initialize_logging("hist_betfair_data")

    json_trades_recorder = Recorder()
    json_trades_recorder.read_files()




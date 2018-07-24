from data.hist_trades_loader import Loader

import pandas as pd
import datetime
from match import list_match

if __name__ == "__main__":

    data_loader = Loader("over_under")
    match = "Real Madrid v Barcelona"
    data = pd.DataFrame()

    for i in range(5):
        runner = "Over {}.5 Goals".format(i)
        data = data.append(data_loader.load_data(runner, match))

    for i in range(5):
        runner = "Under {}.5 Goals".format(i)
        data = data.append(data_loader.load_data(runner, match))

    data.to_excel("data_over_under.xlsx")



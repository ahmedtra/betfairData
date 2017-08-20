from data.hist_trades_loader import Loader


import pandas as pd

if __name__ == "__main__":

    data_loader = Loader()
    result = data_loader.load_data_by_date()
    for data in result:

        data = data[data["event"] == "Over/Under 0.5 Goals"]
        
        if len(data) == 0:
            continue

        print(len(data))

from common import safe_delete
from data.trades_export_ADVANCED import Recorder
import os
from bz2 import BZ2Decompressor
# from common import safe_delete
import multiprocessing

json_trades_recorder = Recorder()

# outputpath = "E:\\Json_ADVANCED_Mar18_EU\\"
# source_path = "E:\\Betfair Data JSON\\ADVANCED - 2018 - EU\\"

# source_path = "E:\\Betfair Data JSON\\BASIC - MAY 2018- OU - 25"
source_path = "E:\Betfair Data JSON\BASIC - MAY 2015 - APR 2018 - OU"
outputpath = "E:\\Json_BASIC_May18_EU\\"

def decompress_file(filepath, newfilepath):
    with open(newfilepath, 'wb') as new_file, open(filepath, 'rb') as file:
        decompressor = BZ2Decompressor()
        for data in iter(lambda: file.read(100 * 1024), b''):
            new_file.write(decompressor.decompress(data))

    json_trades_recorder.process_json_file(new_file.name, True)
    safe_delete(new_file.name)


def file_generator(chunk):
    list = []
    i = 0
    for root, dirs, files in os.walk(source_path):
        for name in files:
            if name[1] == '.':
                filepath = os.path.join(root, name)
                newfilepath = os.path.join(outputpath, name + '.decompressed')
                list.append((filepath, newfilepath))
                i += 1

                if i == chunk:
                    yield list
                    list = []
                    i = 0

if __name__ == "__main__":
    # initialize_secdb()
    # initialize_logging("decompress_betfair_data_2")

    for list_files in file_generator(4):
        jobs = []
        for files in list_files:
            p = multiprocessing.Process(target = decompress_file, args = files)
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()


import os
from bz2 import BZ2Decompressor
from os import listdir
from os.path import isfile, join
import multiprocessing
from common import safe_delete

dirpath = "C:\\Users\\ahmed\\Downloads\\Sites\\xdw\\api\\fd39b9c4-dee9-44ca-a7f5-81d74fc3f711"
outputpath = "E:\Json"


# files = [f for f in listdir(dirpath) if isfile(join(dirpath, f))]

def decompress_file(filepath, newfilepath):
    with open(newfilepath, 'wb') as new_file, open(filepath, 'rb') as file:
        decompressor = BZ2Decompressor()
        for data in iter(lambda: file.read(100 * 1024), b''):
            new_file.write(decompressor.decompress(data))
    safe_delete(filepath)

def file_generator(chunk):
    list = []
    i = 0
    for root, dirs, files in os.walk("E:\Betfair Data JSON"):
        for name in files:
            filepath = os.path.join(root, name)
            newfilepath = os.path.join(outputpath,name + '.decompressed')
            list.append((filepath, newfilepath))
            i += 1

            if i == chunk:
                yield list
                list = []
                i = 0

if __name__ == "__main__":
    for list_files in file_generator(8):
        jobs = []
        for files in list_files:
            p = multiprocessing.Process(target = decompress_file, args = files)
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()








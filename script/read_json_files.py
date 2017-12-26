import os
from bz2 import BZ2Decompressor
from os import listdir
from os.path import isfile, join

dirpath = "C:\\Users\\ahmed\\Downloads\\Sites\\xdw\\api\\fd39b9c4-dee9-44ca-a7f5-81d74fc3f711"
outputpath = "C:\\Betfair\\decompressed_data"


files = [f for f in listdir(dirpath) if isfile(join(dirpath, f))]


for filename in files:
    filepath = os.path.join(dirpath, filename)
    newfilepath = os.path.join(outputpath,filename + '.decompressed')
    with open(newfilepath, 'wb') as new_file, open(filepath, 'rb') as file:
        decompressor = BZ2Decompressor()
        for data in iter(lambda : file.read(100 * 1024), b''):
            new_file.write(decompressor.decompress(data))


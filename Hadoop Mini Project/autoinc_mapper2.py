#!C:\Users\yogit\anaconda3\python.exe

import sys
# input comes from STDIN (standard input)
for line in sys.stdin:
# [derive mapper output key values]
    line = line.strip()

    # parse the input we got from mapper.py
    vin, make, year = line.split(' ')

    print(make,year,1)

 
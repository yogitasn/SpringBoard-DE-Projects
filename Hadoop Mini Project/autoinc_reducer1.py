#!C:\Users\yogit\anaconda3\python.exe

import sys
# [Define group level master information]

current_vin =  None
vin = None
make = None
year = None

def reset():
# [Logic to reset master info for every new group]
# Run for end of every group
    current_vin =  None
    vin = None
    make = None
    year = None

def flush():
# [Write the output]
# input comes from STDIN
   print('%s\t%s\t%s' %(vin,make,year))

for line in sys.stdin:
# [parse the input we got from mapper and update the master info]

    line = line.strip()

    # [parse the input we got from mapper and update the master info]
    line= line.split("\t")
    vin = line[0]
    incident_type= line[1]

    if current_vin == vin:
        if incident_type == 'I':
            make= line[2]
            year= line[3]
        elif incident_type == 'A':
             flush()
        reset()

# [detect key changes]
    if current_vin != vin:
        if current_vin != None:
        # write result to STDOUT
          if incident_type == 'I':
           # print("Inside diffrent vin")
            make = line[2]
            year = line[3]
          elif incident_type == 'A':
             flush()
        reset()
        

# [update more master info after the key change handling]
    
    current_vin = vin

# do not forget to output the last group if needed!
#flush()
from datetime import datetime, timedelta
from pyspark.sql.functions import udf
import re

commaRep = udf(lambda x: re.sub('[\'\s,]','', x))

braceRepl = udf(lambda x: re.sub('\(|\)','', x))

def format_minstoHHMMSS(x):
    try:
        duration=datetime.timedelta(minutes=int(x))
        seconds = duration.total_seconds()
        minutes = seconds // 60
        hours = minutes // 60
        return "%02d:%02d:%02d" % (hours, minutes % 60, seconds % 60)
    except:
        None

udf_format_minstoHHMMSS=udf(lambda x: format_minstoHHMMSS(x))
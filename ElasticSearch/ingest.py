from elasticsearch_dsl import DocType, String, Date, Float, GeoPoint
from elasticsearch_dsl.connections import connections
import csv, time
from datetime import datetime, timedelta, tzinfo

class FixedOffset(tzinfo):
    """
    Class to fix time offset during ingestion in minutes:
    `time = utc_time + utc_offset`.
    """
    def __init__(self, offset):
        self.__offset = timedelta(minutes=offset)
        hours, minutes = divmod(offset, 60)
        #NOTE: the last part is to remind about deprecated POSIX GMT+h timezones
        #  that have the opposite sign in the name;
        #  the corresponding numeric value is not used e.g., no minutes
        self.__name = '<%+03d%02d>%+d' % (hours, minutes, -hours)

    def utcoffset(self, dt=None):
        return self.__offset

    def tzname(self, dt=None):
        return self.__name

    def dst(self, dt=None):
        return timedelta(0)

    def __repr__(self):
        return 'FixedOffset(%d)' % (self.utcoffset().total_seconds() / 60)


# Define a default Elasticsearch client
connections.create_connection(hosts=['http://ec2-52-37-208-115.us-west-2.compute.amazonaws.com'])

class Reading(DocType):
    '''
    Class that defines what fields to ingest
    '''
    datetime = Date()
    location = GeoPoint(lat_lon=True)
    usage = Float()
    #ingests both raw and analyzed (suffixes, etc.) versions of below fields
    #this allows for more complex searchability, but blows up db size
    term = String(fields={'raw': String(index='not_analyzed')})
    building = String(fields={'raw': String(index='not_analyzed')})
    description = String(fields={'raw': String(index='not_analyzed')})
    meter = String(fields={'raw': String(index='not_analyzed')})

    class Meta:
        index = 'meter'

    def save(self, ** kwargs):
        return super(Reading, self).save(** kwargs)

# create the mappings in elasticsearch
Reading.init()
count=0
skip=0

with open('campus_buildings_geo_meters_data.csv','rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        count += 1
        if count>skip:
            usage=""
            try:
                usage=float(row[7])
            except:
                pass
            if usage=="" or usage==0:
                print(str(count)+": Skipped (usage is 0 or empty).")
                continue

            #fixing date
            naive_date_str, _, offset_str = row[1].replace("+"," +").rpartition(' ')
            offset_str=offset_str.replace(":","")
            naive_dt = datetime.strptime(naive_date_str, '%Y-%m-%d %H:%M:%S')
            offset = int(offset_str[-4:-2])*60 + int(offset_str[-2:])
            if offset_str[0] == "-":
                offset = -offset
            dt = naive_dt.replace(tzinfo=FixedOffset(offset))
            #setting all fields of the Reading class
            reading = Reading()
            reading.meter=row[0]
            reading.datetime=dt
            reading.term=row[2]
            reading.building=row[3]
            if row[5]!="" and row[4]!="":
                reading.location={"lat": row[5], "lon": row[4]}
            reading.description=row[6]
            reading.usage=usage
            #save fields to db
            reading.save()

            if count % 1000 == 0:
                print(str(count)+":"+str(reading))
        else:
            print(str(count)+": Skipped.")

# Display cluster health
print(connections.get_connection().cluster.health())

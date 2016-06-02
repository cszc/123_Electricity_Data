from elasticsearch_dsl import DocType, String, Date, Float, GeoPoint
from elasticsearch_dsl.connections import connections
import csv, time
from datetime import datetime, timedelta, tzinfo

class FixedOffset(tzinfo):
    """Fixed offset in minutes: `time = utc_time + utc_offset`."""
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
connections.create_connection(hosts=['localhost'])

#we may still want to add building code into a separate column
class Reading(DocType):
    datetime = Date()
    term = String(fields={'raw': String(index='not_analyzed')})
    building = String(fields={'raw': String(index='not_analyzed')})
    location = GeoPoint(lat_lon=True)
    description = String(fields={'raw': String(index='not_analyzed')})
    meter = String(fields={'raw': String(index='not_analyzed')})
    usage = Float()

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
        count=count+1
        if count>skip:
            usage=""
            try:
                usage=float(row[7])
            except:
                pass
            if usage=="" or usage==0:
                print str(count)+": Skipped (usage is 0 or empty)."
                continue

            naive_date_str, _, offset_str = row[1].replace("+"," +").rpartition(' ')
            offset_str=offset_str.replace(":","")
            naive_dt = datetime.strptime(naive_date_str, '%Y-%m-%d %H:%M:%S')
            offset = int(offset_str[-4:-2])*60 + int(offset_str[-2:])
            if offset_str[0] == "-":
                offset = -offset
            dt = naive_dt.replace(tzinfo=FixedOffset(offset))

            reading = Reading()
            #         0,                  1,            2,  3,             4,            5,                           6,    7
            # 079039684,2014-03-12 00:30:00,"Winter 2014",A71,-87.6034113871,41.7893116662,"Bernard Mitchell Hospitial",484.2
            #
            reading.meter=row[0]
            reading.datetime=dt
            reading.term=row[2]
            reading.building=row[3]
            if row[5]!="" and row[4]!="":
                reading.location={"lat": row[5], "lon": row[4]}
            reading.description=row[6]
            reading.usage=usage

            #saved=False
            #while saved is False:
            #    try:
            #        reading.save()
            #        print str(count)+":"+str(reading)
            #        saved=True
            #    except:
            #        time.sleep(.5)
            reading.save()
            print str(count)+":"+str(reading)

        else:
            print str(count)+": Skipped."
# Display cluster health
print(connections.get_connection().cluster.health())

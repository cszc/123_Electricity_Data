import csv
import json
from pytz import timezone,utc
from datetime import datetime,date
data=None

csvDataFilename="electric_interval_data_UNIVERSITY_OF_CHICAGO_7000_03-12-2014_to_03-10-2016.csv"
csvMetersFilename="meter_building.csv"

with open('campus_buildings.json') as data_file:
    data = json.load(data_file)

buildings={}

for feature in data['features']:
    bdId=feature['properties']['BD_ID']
    buildings[bdId]=feature['properties']
    count=0
    lat=0
    lon=0

    for coordinate in feature['geometry']['coordinates'][0]:
        if count==0:
            lat=coordinate[1]
            lon=coordinate[0]
        else:
            lat=lat+coordinate[1]
            lon=lon+coordinate[0]
        count=count+1
    lat=lat/count
    lon=lon/count
    buildings[bdId]['CLON']=lon
    buildings[bdId]['CLAT']=lat


#for building in buildings:
#    print str(building)
#exit(-1)

meters={}
with open(csvMetersFilename,'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        meterId=row[0]
        if len(meterId)==8: meterId="0"+meterId
        meters[meterId]=str(row[1]).strip()
count=0
skip=5

print "METER_ID,DATETIME,TERM,BD_ID,CLON,CLAT,DISCRIPT1,USAGE"
with open(csvDataFilename,'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        count=count+1
        if count>skip :
        #                0,        1,         2,    3,    4
        # "Electric Usage",079039684,2014-03-12,01:00,484.2,kWh,33.0,F
            dummy=row[0]
            meterId=str(row[1]).strip()
            if "A13 Bookstore (Black 1)" in meterId:
                meterId="086145076"
            elif "A06 Crerar Library (Black 1)" in meterId:
                continue
            elif "A06 Crerar Library (Blue 1)" in meterId:
                continue
            elif "A06 Crerar Library (Blue 2)" in meterId:
                continue
            usage=""
            try:
                usage=round(float(row[4]),4)
            except:
                pass

            if usage is None or usage==0:
                continue
            bdId=meters[meterId]
            if "D69" in bdId:
                bdId="O36"
            desc=""
            cLat=""
            cLon=""
            try:
                desc=str(buildings[bdId]['DISCRIPT1'])
                cLat=buildings[bdId]['CLAT']
                cLon=buildings[bdId]['CLON']
            except:
                pass

            bdId=meters[meterId]
            date1=datetime.strptime(row[2]+" "+row[3], '%Y-%m-%d %H:%M')
            local_dt= timezone('America/Chicago').localize(date1)
            date1 = local_dt.astimezone (utc)

            parts=row[2].split("-")
            date2=date(int(parts[0]),int(parts[1]),int(parts[2]))
            term=""
            if date(2013, 9, 22) <= date2 <= date(2013, 12, 14):
              term="Autumn 2013"
            elif date(2014, 1, 6) <= date2 <= date(2014, 3, 22):
              term="Winter 2014"
            elif date(2014, 3, 31) <= date2 <= date(2014, 6, 14):
              term="Spring 2014"
            elif date(2014, 6, 23) <= date2 <= date(2014, 8, 30):
              term="Summer 2014"
            elif date(2014, 9, 29) <= date2 <= date(2014, 12, 13):
              term="Autumn 2014" 
            elif date(2015, 1, 5) <= date2 <= date(2015, 3, 21):
              term="Winter 2015"
            elif date(2015, 3, 30) <= date2 <= date(2015, 6, 13):
              term="Spring 2015"
            elif date(2015, 6, 22) <= date2 <= date(2015, 8, 29):
              term="Summer 2015"
            elif date(2015, 9, 28) <= date2 <= date(2015, 12, 12):
              term="Autumn 2015"
            elif date(2016, 1, 4) <= date2 <= date(2016, 3, 19):
              term="Winter 2016"
            elif date(2016, 3, 28) <= date2 <= date(2016, 6, 11):
              term="Spring 2016"
            else:
              term=""

            
            print meterId+","+str(date1)+',"'+term+'",'+bdId+","+str(cLon)+","+str(cLat)+',"'+str(desc)+'",'+str(usage)


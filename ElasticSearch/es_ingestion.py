from elasticsearch_dsl import DocType, String, Date, Float
from elasticsearch_dsl.connections import connections
import csv

# Define a default Elasticsearch client
connections.create_connection(hosts=['localhost'])

#we may still want to add building code into a separate column
class Reading(DocType):
    datetime = Date()
    temperature = Float()
    usage = Float()
    meter = String(fields={'raw': String(index='not_analyzed')})
    term = String(fields={'raw': String(index='not_analyzed')})

    class Meta:
        index = 'meter'

    def save(self, ** kwargs):
        return super(Reading, self).save(** kwargs)

# create the mappings in elasticsearch
Reading.init()

with open('fullelectric.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    reader.readline()
    for row in reader:
        reading = Reading()
        reading.datetime, reading.usage, reading.temperature, reading.meter, \
        reading.term = row
        reading.save()

# Display cluster health
print(connections.get_connection().cluster.health())

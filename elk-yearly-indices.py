#!bin/python

# forked from https://gist.github.com/EikeDehling/c1c81d89ec92597de3d3db49e99f5a1f

from elasticsearch import Elasticsearch
from datetime import datetime
import time

es = Elasticsearch()

indices_state = es.cluster.state()['metadata']['indices']

for source_index in sorted(indices_state.keys(), reverse=True):

    # Skip closed indices
    if indices_state[source_index]['state'] != 'open':
        print "Opening closed index {0}".format(source_index)
        es.indices.open(source_index)
        time.sleep(5)

    # Indices are called like this : "logstash-2016.10.07"
    try:
        date = datetime.strptime(source_index, 'logstash-%Y.%m.%d')
    except Exception, e:
        # Index name does not match pattern, skip
        continue
    destination_index = "logstash-{0}-{1}".format(date.year, date.month)

    print "Reindexing data in index {0} into {1}".format(source_index, destination_index)

    result = es.reindex({
        "source": {"index": source_index},
        "dest": {"index": destination_index}
    }, wait_for_completion=True, request_timeout=300)

    print result

    if result['total'] and result['took'] and not result['timed_out']:
        print "Seems reindex was successfull, going to delete the old index!"
es.indices.delete(source_index, timeout='300s')

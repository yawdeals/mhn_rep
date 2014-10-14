#!/opt/hpfeeds/env/bin/python

import sys
import logging
logging.basicConfig(level=logging.WARNING)
import hpfeeds
import pymongo
import json
from datetime import datetime

def main():
    if len(sys.argv) < 7:
        print >> sys.stderr, "Usage: python %s <host> <port> <ident> <secret> <channel,channel2,channel3,...> <mongoBD> <mongoCollection>"%(sys.argv[0])
        sys.exit(1)

    HOST, PORT, IDENT, SECRET, CHANNELS, MONGO_DB, MONGO_COLL = [arg.encode("utf-8") for arg in sys.argv[1:8]]
    CHANNELS = CHANNELS.split(",")
    PORT = int(PORT)
    
    hpc = hpfeeds.new(HOST, PORT, IDENT, SECRET)
    print >>sys.stderr, 'connected to', hpc.brokername
    
    mongo = pymongo.MongoClient()
    db = mongo[MONGO_DB]
    coll = db[MONGO_COLL]

    def on_message(identifier, channel, payload):
        try:
            payload = str(payload).strip()
            record = {'identifier': identifier, 'channel': channel, 'payload': json.loads(payload), 'insert_timestamp': datetime.utcnow()}
            coll.insert(record)

        except Exception, e:
            print >> sys.stderr, "Error", e

    def on_error(payload):
        print >>sys.stderr, ' -> errormessage from server: {0}'.format(payload)
        hpc.stop()
        mongo.close()
        sys.exit(1)

    hpc.subscribe(CHANNELS)
    hpc.run(on_message, on_error)
    mongo.close()
    hpc.close()
    return 0

if __name__ == '__main__':
    try: sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)

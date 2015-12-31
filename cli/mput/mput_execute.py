"""
    DB Wrapping class for the multiple put


    Indigo Command Line Interface -- multiple put.

    Copyright 2015 Archive Analytics Solutions

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""
import os.path

from .config import NUM_THREADS
from .db import DB
from .mput_threads import *
from .utils import _dirmgmt, counter_timer
from Queue import  Empty


def clear_db_queue(q,db) :
    while True :
            try:
                row_id,state,T0,T1 =  q.get(block=False)
                db.update(row_id,state)
            except Empty:
                return None

def mput_execute(app, arguments):
    db = DB(app, arguments)
    tgt_prefix = arguments['<tgt-dir-in-repo>']
    dir_cache = _dirmgmt( )
    db_queue = Queue(16*1024)
    client = app.get_client(arguments)
    q, threads = thread_setup(NUM_THREADS, None if True else db.cnx  ,  client, file_putter, cache = dir_cache , db_queue = db_queue )
    for t in threads : t.start()

    debug = arguments.get('-D',0)
    debug = int(debug) if isinstance(debug,basestring) and debug.isdigit() else 0


    while True:
        clear_db_queue(db_queue,db)

        thisdir = db.get_and_lock()   ## get another directories worth of files
        if not thisdir   :
            break
        # Start by ensuring there is a container to go into...
        path = thisdir[0][0]

        tgtdir = os.path.normpath(os.path.join(unicode(tgt_prefix), path.lstrip('/')))
        ### This is now done in the threads...
        if False:
            if not dir_cache.getdir(tgtdir,client) :
                 print "FAILED to create <{}> or one of its ancestors"
                 continue

        T0,N = time.time(),0            # instrumentation
        for path, name, start_time, end_time, row_id in thisdir:
            N += 1
            # Queue up the put request to a thread...
            q.put((os.path.join(path, name),  os.path.join(tgtdir ,name) , row_id))

    # Now wait for all the workers to finish

    N,T0 = q.qsize(),time.time()
    while True :
        clear_db_queue(db_queue,db)
        if q.qsize() < 1 : break
        else :
            time.sleep(3)
            if debug > 1 :
                N1 = q.qsize()
                T1 = time.time()
                print "{} entries left, rate = {:,.2f}/sec".format(N1,(N-N1)/(T1-T0))
                N,T0 = N1,T1


    print 'Queue is empty',q.qsize()
    ### Clear any remaining DB updates
    clear_db_queue(db_queue,db)

    q.join()  # suspends until the queue is empty and all the workers have acknowledged completion

    clear_db_queue(db_queue,db)

    print 'Done'



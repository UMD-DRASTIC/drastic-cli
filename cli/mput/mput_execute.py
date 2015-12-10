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
import os

from .config import NUM_THREADS
from .db import DB
from .mput_threads import *
from .utils import _dirmgmt



def mput_execute(app, arguments):
    db = DB(app, arguments)
    tgt_prefix = arguments['<tgt-dir-in-repo>']
    dir_cache = _dirmgmt(size_limit=1024)

    client = app.get_client(arguments)
    q, threads = thread_setup(NUM_THREADS, db.cnx, db.label, client, file_putter)

    ctr = 0
    T0 = time.time()
    T1 = T0
    while True:
        thisdir = db.get_and_lock()
        if not thisdir: break
        ctr1 = 0
        # Start by ensuring there is a container to go into...
        path = thisdir[0][0]
        tgtdir = os.path.normpath(os.path.join(unicode(tgt_prefix), path.lstrip('/')))
        try:
            dir_cache.getdir(tgtdir, client)  # Load the path (and its parents) into the cache....
        except Exception as e:
            print e, path
            continue

        ### Now we know that the target dir exists... process all the files..
        for path, name, start_time, end_time, row_id in thisdir:
            ctr1 += 1
            # Then actually putting the data ...
            tgtname = os.path.join(tgtdir, name)
            q.put((os.path.join(path, name), tgtname, row_id))
            if NUM_THREADS == 0:
                file_putter(q, client, db.cnx, db.label, one_shot=True)
        ctr += ctr1
        T2 = time.time()
        print '{tgtdir} : {ctr1} files in {T21:.2f}s = {ctr1_T21:.2f}/sec , {ctr} total files in {T20:.2f} = {ctr_T20:.2}/sec'.format(
            tgtdir=tgtdir, ctr=ctr, ctr1=ctr1, T20=T2 - T0, T21=T2 - T1, ctr1_T21=ctr1 / (T2 - T1),
            ctr_T20=ctr / (T2 - T0)
        )
        T1 = T2
    # Now wait for all the workers to finish

    q.join()  # suspends until the queue is empty and all the workers have acknowledged completion

    T2 = time.time()
    print '{ctr} total files  in {T20} = {ctr_T20}/sec'.format(T20=T2 - T0, ctr=ctr, ctr_T20=ctr / (T2 - T0))
    return ctr

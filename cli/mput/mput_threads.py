"""

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

import sqlite3
import time
from Queue import Queue
from threading import Thread

import os.path
from requests import ConnectionError

# Start
# We have two functions, the outer one is just to manage the status of the operation in the database
# the child function ( the worker ) actually puts the file
#
def file_putter(q, client, cnx, cache = None , db_queue = None  ) :
    """
    Pull local (source) file and remote ( target ) object paths and send them, and then
    update the database that tracks the files....

    :param q: Queue
    :param client:  IndigoClient
    :param cnx: sqlite3.Connection  a database connection to update the file status on completion
    :param cache: .utils._dirmgmt
    :param logger_queue: Queue
    :return: N/A
    """
    ### Set everything up ... primarily database connection
    _stmt1 = '''UPDATE transfer SET state = ? ,start_time=? , end_time = ? Where row_id = ?'''
    cs = None
    if cnx :
        if isinstance(cnx,basestring) :
            cnx = sqlite3.connect(cnx)
        if not isinstance(cnx,sqlite3.Connection) :
            raise ValueError("don't know what to do with {} for database connection".format(cnx))
        cs = cnx.cursor()
    ### Now loop on the queue entry ... which will continue until the parent thread 'joins'
    while True:
        src, target, row_id = q.get()
        T0 = time.time()
        ret = file_putter_worker(src,target  , client,   cache =  cache )
        T1 = time.time()

        q.task_done()
        if ret and ret['ok'] : status = 'DONE'
        else :
            status = 'FAIL'
            if ret : print ret['msg']
        if db_queue :
            db_queue.put((row_id,status,T0,T1))
        elif cs :
            try:
                cs.execute(_stmt1, (status, T0, T1, row_id))
                cs.connection.commit()
            except sqlite3.OperationalError as e :
                pass


def file_putter_worker(src, target , client, cache = None ):
    """
    :param src: basestring
    :param target: basestring
    :param client:  IndigoClient
    :param cache: .util._dirmgmt
    :return: N/A
    """

    ### Handle directory creation here...
    ### Note that the cache object recursively creates containers... walking up the tree until it finds a container
    ### and then walking down creating as it goes ...
    ###
    if cache is not None :                  # Cache may be empty, or it may be not present, so be precise.
        tgtdir,nm = os.path.split(target)
        if not cache.getdir(tgtdir, client):
            return {'ok': False, 'msg': 'Failed to Create {} or one of its parents'.format(tgtdir)}

    with open(src, 'rb') as fh:
        try:
            res = client.put(target, fh)
            if res.ok() :
                print 'put ',str(target)
                return {'ok' : True }
        except ConnectionError as e:
            return {'ok': False, 'msg': 'Connection Error'}
        except Exception as e:
            return {'ok': False, 'msg': u'failed to put {} to {} [{} / {}]'.format(src, target,type(e), e)}


def thread_setup(N, cnx, client, target=file_putter , cache = None , db_queue = None  ):
    """

    :param N: int                           -- Number of worker threads...
    :param cnx: sqlite3.Connection          -- database connection object
    :param client: IndigoClient             -- the CDMI client object ... it appears to be thread safe,so no point in replicating it
    :param target:                          -- function
    :param cache:  _dirmgmt                 -- Cache of found filenames...
    :return: [ queue , [threads]  ]
    """
    q = Queue(4096)
    threads = []
    for k in range(N):
        t = Thread(target=target, args=(q, client, cnx ,  cache , db_queue ))
        t.setDaemon(True)
        #t.start()
        threads.append(t)
    return [q, threads]

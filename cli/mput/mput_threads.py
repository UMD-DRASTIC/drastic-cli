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

import sys
import time
from Queue import Queue
from threading import Thread

from requests import ConnectionError


def file_putter(q, client, cnx,  one_shot=False):
    """
    :param q: the queue from which src and target files will be called
    :param client:  the client object that implements the various CDMI calls to the host
    :param cnx: a database connection to update the file status on completion
    :param label: the name of the table containing the work queue
    :return: N/A
    """
    if cnx:
        cs = cnx.cursor()
        stmt1 = '''UPDATE transfer SET state = ? ,start_time=? , end_time = ? Where row_id = ?'''

    total_len = 0
    while True:
        src, target, row_id = q.get()
        T0 = time.time()
        fail = False
        with open(src, 'rb') as fh:
            try:
                from io import SEEK_CUR
                res = client.put(target, fh)

            except ConnectionError as e:
                fail = True
            except Exception as e:
                print u'failed to put {} to {}'.format(src, target)
                print e
                # Since it wasn't a connection error abandon this one .. mark as failed.
                fail = True

            if fail:
                T1 = time.time()
                cs.execute(stmt1, ('FAIL', T0, T1, row_id))
                cs.connection.commit()
                q.task_done()
                if one_shot:
                    return
                else:
                    continue

                    # The Put apparently succeeded...
                    # total_len += fh.seek(0, SEEK_CUR)  #

        if cnx and row_id:
            T1 = time.time()
            if res.ok():
                cs.execute(stmt1, ('DONE', T0, T1, row_id))
                cs.connection.commit()
            else:
                print >> sys.stderr, res.msg(), '\n', target
                cs.execute(stmt1, ('FAIL', T0, T1, row_id))
                cs.connection.commit()
                q.put((src, target, row_id))  # Stick it back on and try later...
        else:
            if not res.ok():
                print res.msg()
                q.put((src, target, row_id))  # Stick it back on and try later...
        q.task_done()  # Acknowledge that task has completed...
        if one_shot:
            return


def thread_setup(N, cnx, client, target=file_putter):
    """

    :param N: Number of worker threads...
    :param cnx: databse connection object
    :param label: name of work queue table, for use  in constructing SQL queries
    :param client: the CDMI client object ... it appears to be thread safe,so no point in replicating it
    :param target: the target path name on the server
    :return: [ queue , [threads]  ]
    """
    q = Queue(4096)
    threads = []
    for k in xrange(N):
        t = Thread(target=target, args=(q, client, cnx,  False))
        t.setDaemon(True)
        t.start()
        threads.append(t)
    return [q, threads]

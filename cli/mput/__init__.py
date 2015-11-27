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


import os
import sys
import time
from Queue import  Queue
from collections import OrderedDict
from threading import Thread

NUM_THREADS = 16



__all__ = ( 'mput' ,   'mput-prepare' , 'mput_status' , 'mput_execute' )
from db import DB
from mput import mput




"""
  indigo mput-prepare [-l label] (-walk <file-list> | -read (<source-dir>|-))
  indigo mput-execute [-l label] <tgt-dir-in-repo>
  indigo mput (-walk|-read)  (<file-list>|<source-dir>|-) <tgt-dir-in-repo>
  indigo mput-status [-l label]"""


#### Pull paths from the database and put 'em ...
class LimitedSizeDict(OrderedDict):
  def __init__(self, *args, **kwds):
    self.size_limit = kwds.pop("size_limit", None)
    OrderedDict.__init__(self, *args, **kwds)
    self._check_size_limit()

  def set(self, key, value):
    OrderedDict.__setitem__(self, key, value)
    self._check_size_limit()

  def _check_size_limit(self):
    if self.size_limit is not None:
      while len(self) > self.size_limit:
        self.popitem(last=False)

class _dirmgmt(LimitedSizeDict) :
    def __init__(self,*args,**kwds):
        LimitedSizeDict.__init__(self,*args,**kwds)

    def getdir(self,path,client) :
        if path in self  : return True
        rq = client.ls(path)
        if not rq.ok() :
            p1,n1 = os.path.split(path)
            if p1 not in self :
                self.getdir(p1,client)
            client.mkdir(path)
            self.set( path, True )
        return True


def mput_execute(app, arguments):
    db = DB(app, arguments)
    tgt_prefix = arguments['<tgt-dir-in-repo>']
    dir_cache = _dirmgmt(size_limit=1024)

    client = app.get_client(arguments)
    q, threads = thread_setup(NUM_THREADS,db.cnx,db.label,client, file_putter )

    ctr = 0
    T0 = time.time()
    T1 = T0
    while True:
        thisdir = db.get_and_lock()
        if not thisdir : break
        ctr1 = 0
        for  path ,  name ,  start_time ,  end_time ,  row_id   in thisdir :
            ctr1 += 1
            # Start by ensuring there is a container to go into...
            tgtdir = os.path.normpath( os.path.join( tgt_prefix,path.lstrip('/')))
            try :
                ok = dir_cache.getdir(tgtdir,client)
            except Exception as e :
                print e, path
                continue
            # Then actually putting the data ...
            tgtname = os.path.join(tgtdir,name)
            q.put(( os.path.join(path,name),tgtname , row_id ) )
            if NUM_THREADS == 0 :
                file_putter(q, client, db.cnx , db.label , one_shot=True )
        ctr += ctr1
        T2 = time.time()
        print '{tgtdir} : {ctr1} files in {T21}s = {ctr1_T21}/sec , {ctr} total files  in {T20} = {ctr_T20}/sec'.format(
            tgtdir=tgtdir, ctr =ctr , ctr1 = ctr1 , T20 = T2-T0, T21=T2-T1,ctr1_T21=ctr1/(T2-T1),ctr_T20 = ctr/(T2-T0)
            )
        T1 = T2
    # Now wait for all the workers to finish

    q.join()    # suspends until the queue is empty and all the workers have acknowledged completion

    T2 = time.time()
    print '{ctr} total files  in {T20} = {ctr_T20}/sec'.format( T20=T2-T0 , ctr = ctr, ctr_T20 = ctr/(T2-T0)  )
    return ctr


def mput_status(app, arguments):
    reset_flag = bool(  arguments['--reset']  )
    db = DB(app, arguments)
    print >>sys.stdout,db.status()
    cs1 = db.cnx.cursor()
    cs1.execute('''UPDATE {0}
        SET state = 'RDY', start_time=strftime('%s','now'), end_time = strftime('%s','now')
            WHERE STATE = 'FAIL' or 'STATE' = 'WRK' '''.format(db.label))
    cs1.connection.commit()
    return None





def file_putter(q, client, cnx , label = 'transfer' , one_shot = False) :
    """
    :param q: the queue from which src and target files will be called
    :param client:  the client object that implements the various CDMI calls to the host
    :param cnx: a database connection to update the file status on completion
    :param label: the name of the table containing the work queue
    :return: N/A
    """
    cs = cnx.cursor()
    stmt1 =  '''UPDATE {0} SET state = ? ,start_time=? , end_time = ? Where row_id = ?'''.format(label)

    while True :
        src,target, row_id = q.get()
        T0 = time.time()
        with open(src , 'rb') as fh:
                res = client.put(target, fh )
                T1 = time.time()
                if res.ok():
                    cs.execute(stmt1,('DONE',T0,T1,row_id))
                else:
                    print >>sys.stderr, res.msg(),'\n',target
                    cs.execute(stmt1,('FAIL',T0,T1,row_id))
                cs.connection.commit()
        q.task_done()           # Acknowledge that task has completed...
        if one_shot : return

def thread_setup(N, cnx,label, client, target = file_putter ) :
    """

    :param N: Number of worker threads...
    :param cnx: databse connection object
    :param label: name of work queue table, for use  in constructing SQL queries
    :param client: the CDMI client object ... it appears to be thread safe,so no point in replicating it
    :param target: the target path name on the server
    :return: [ queue , [threads]  ]
    """
    q = Queue(4096)
    threads = [ ]
    for k in xrange(N) :
        t = Thread(target=target, args=(q, client ,cnx,label ,  False))
        t.setDaemon(True)
        t.start()
        threads.append(t)
    return [q, threads ]




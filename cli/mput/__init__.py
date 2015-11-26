import os
import sqlite3
import sys
import time
from collections import OrderedDict


class DB:
    def __init__(self, app, args):
        p = app.session_path
        if os.path.isfile(p): p,_ = os.path.split(p)
        self.dbname = os.path.join( p , 'work_queue.db')
        try :
            self.cnx = sqlite3.connect(self.dbname)
        except Exception as e :
            print e
            print "Cannot open ",p
        #####
        self.cs = self.cnx.cursor()

        label = None
        if '--label' in args:
            label = args['--label']
        if not label and '-l' in args:
            label = args['-l']
        if not label: label = 'transfer'
        self.label = label

        self.cs.execute('''CREATE TABLE IF NOT EXISTS {0}
                (row_id INTEGER PRIMARY KEY AUTOINCREMENT ,
                 path TEXT,  name TEXT,
                 state TEXT CHECK (state in ('RDY','WRK','DONE','FAIL')) NOT NULL DEFAULT 'RDY'  ,
                 start_time INTEGER default CURRENT_TIMESTAMP,
                 end_time INTEGER ,
                 UNIQUE ( path,name )
                  ) '''.format(label))
        self.cs.execute('''CREATE INDEX IF NOT EXISTS {0}_state_idx ON {0}(state) where state =  'DONE' '''.format(label))
        self.cs.execute('''CREATE INDEX IF NOT EXISTS {0}_state1_idx ON {0}(state) where state <> 'DONE' '''.format(label))
        self.cs.execute('''CREATE INDEX IF NOT EXISTS {0}_path_idx ON {0}(path) WHERE state = 'RDY' '''.format(label))
        self.cs.execute('''CREATE INDEX IF NOT EXISTS {0}_path1_idx ON {0}(path)   '''.format(label))

    def update(self, rowid, state):
        if state == 'WRK':
            cmd = '''UPDATE {0} SET state = ? , start_time = strftime('%s','now') Where row_id = ?'''.format(self.label)
        else:
            cmd = '''UPDATE {0} SET state = ? , end_time = strftime('%s','now') Where row_id = ?'''.format(self.label)
        try:
            self.cs.execute(cmd, [state, rowid])
            self.cs.connection.commit()
            return rowid
        except Exception as e:
            print e
            self.cs.connection.rollback()
            return None

    def get_and_lock(self):
        """
            This function will get retrieve an entry where the
        :return:
        """
        self.cs.execute('''BEGIN''')
        ## Select A Path's worth of files.... where some are
        cmd = '''WITH DIR as ( SELECT path from {0} WHERE STATE = 'RDY' LIMIT 1)
                    SELECT path ,name,start_time,end_time,row_id from {0} JOIN DIR USING (path)
            '''.format(self.label)
        self.cs.execute(cmd)
        results = self.cs.fetchall()
        data = [data[-1:] for data in results]
        cmd = '''UPDATE {0} SET STATE = 'WRK' , start_time = strftime('%s','now') WHERE row_id = ?'''.format(self.label)
        self.cs.executemany(cmd,data)
        self.cs.connection.commit()
        return results

    def insert(self, path):
        """
            Put a new path in , or ignore if it is already there.
            :path: Path to put in work queue _if_ not present
        """
        if not os.path.exists(path):
            print >> sys.stderr, '{0} does not exist ...skipping '.format(path)
            return None
        p1, n1 = os.path.split(os.path.abspath(path))  # Avoid naive duplication
        cmd = '''insert or ignore INTO {0} (path,name,state) VALUES ( ? , ? , ? )'''.format(self.label)
        self.cs.execute(cmd, (p1, n1, 'RDY'))
        ret =  self.cs.lastrowid
        self.cs.connection.commit()
        return ret

    def status(self , reset = False) :
        friendly = dict(DONE = 'Done',FAIL = 'Failed' , RDY = 'Ready' , WRK = 'Processing')
        self.cs.execute('SELECT state,count(*),avg(end_time-start_time) from {0} group by state order by state'.format(self.label))
        retval = u''
        retval  = '{0:10s} |{1:23s} |{2:20s}\n'.format('State','Count','Average time in State')
        retval += '{0:10s} |{1:23s} |{2:20s}\n'.format('-'*10,'-'*23,'-'*20)

        for k in self.cs :
            k = list(k)
            k[0] = friendly[k[0]]
            retval += '{0:10s} |{1:23,} |{2:20.2f}\n'.format(*k)
        return retval


"""
  indigo mput-prepare [-l label] (-walk <file-list> | -read (<source-dir>|-))
  indigo mput-execute [-l label] <tgt-dir-in-repo>
  indigo mput (-walk|-read)  (<file-list>|<source-dir>|-) <tgt-dir-in-repo>
  indigo mput-status [-l label]"""

def mput_prepare(app, arguments):
    db = DB(app, arguments)

    ### Instrumentation
    t0 = time.time()
    t1 = t0
    ctr = 0
    ####################
    if arguments['--walk']:
        tree = arguments['<file-list>']
        if '~' in tree : tree = os.path.expanduser(tree)
        if not tree or not os.path.isdir(tree):
            raise ValueError("can't find the tree to walk ")
        tree = os.path.abspath(tree)

        for dirname,_,files in os.walk(tree,topdown=True,followlinks=True) :
            for fn in files :
                ctr += 1
                db.insert(os.path.abspath(os.path.join(dirname,fn)).decode('utf-8'))
            t2 = time.time()
            if ( t2 - t1 ) > 30 :
                print '{0:,} registered in {1:.2f} secs -- {2}/sec'.format(ctr, (t2-t1), ctr / (t2 - t0))
                t1 = t2
    ####################
    elif arguments['--read'] :
        if arguments['<file-list>'] == '-' : fp = sys.stdin
        else : fp = open(arguments['<file-list>'],'rU')
        for path in fp :
            if not os.path.exists(path) :
                print >>sys.stderr,"skipping -- file does not exist : ",path
                continue
            ctr += 1
            db.insert(os.path.abspath( path).decode('utf-8'))
            if ctr% 5000 :
                t2 = time.time()
                if ( t2 - t1 ) > 30 :
                    print '{0:,} registered in {1:.2f} secs -- {2:.2f}/sec'.format(ctr, (t2-t1), ctr / (t2 - t0))
                    t1 = t2
    #####################
    # Summary
    t2 = time.time()
    print '{0:,} registered in {1:.2f} secs -- {2:.2f}/sec'.format(ctr, (t2-t1), ctr / (t2 - t0))


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

    ctr = 0
    T0 = time.time()
    T1 = T0
    while True:
        thisdir = db.get_and_lock()
        if not (thisdir) : break
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
            with open(os.path.join(path,name), 'rb') as fh:
                res = client.put(tgtname, fh )
                if res.ok():
                    cdmi_info = res.json()
                    # TODO : remove this
                    print cdmi_info[u'parentURI'] + cdmi_info[u'objectName']
                    db.update(row_id,'DONE')
                else:
                    print >>sys.stderr, res.msg()
                    db.update(row_id,'FAIL')
        ctr += ctr1
        T2 = time.time()
        print '{tgtdir} : {ctr1} files in {T21}s = {ctr1_T21}/sec , {ctr} total files  in {T20} = {ctr_T20}/sec'.format(
            tgtdir=tgtdir, ctr =ctr , ctr1 = ctr1 , T20 = T2-T0, T21=T2-T1,ctr1_T21=ctr1/(T2-T1),ctr_T20 = ctr/(T2-T0)
            )
        T1 = T2

    T2 = time.time()
    print '{ctr} total files  in {T20} = {ctr_T20}/sec'.format(   ctr = ctr, ctr_T20 = ctr/(T2-T0)  )
    return ctr



def mput_status(app, arguments):
    db = DB(app, arguments)
    print >>sys.stdout,db.status()
    return None


def mput(app, arguments):
    raise NotImplementedError

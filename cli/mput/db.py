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
import sqlite3
import sys


class DB:
    def __init__(self, app, args):
        p = app.session_path

        # construct the path
        if os.path.isfile(p): p,_ = os.path.split(p)
        self.dbname = os.path.join( p , 'work_queue.db')

        # if the directory doesn't exist try to make it.
        if not os.path.isdir(p):
            try:
                os.mkdir(p)
            except Exception as e:
                print 'cannot make directory {}'.format(p)
                raise

        # open or create the database
        try :
            self.cnx = sqlite3.connect(self.dbname , check_same_thread = False )
        except Exception as e :
            print e
            raise RuntimeError("Cannot open {}".format(self.dbname))
        #####
        self.cs = self.cnx.cursor()

        # set the label to the first candidate...
        label = filter(bool, [args.get('--label', None), args.get('-l', None), 'transfer'])[0]
        self.label = label

        self.cs.execute('''CREATE TABLE IF NOT EXISTS "{0}"
                (row_id INTEGER PRIMARY KEY AUTOINCREMENT ,
                 path TEXT,  name TEXT,
                 state TEXT CHECK (state in ('RDY','WRK','DONE','FAIL')) NOT NULL DEFAULT 'RDY'  ,
                 start_time INTEGER default CURRENT_TIMESTAMP,
                 end_time INTEGER ,
                 UNIQUE ( path,name )
                  ) '''.format(label))
        self.cs.execute(
            '''CREATE INDEX IF NOT EXISTS "{0}_state_idx" ON "{0}"(state) where state =  'DONE' '''.format(label))
        self.cs.execute(
            '''CREATE INDEX IF NOT EXISTS "{0}_state1_idx" ON "{0}"(state) where state <> 'DONE' '''.format(label))
        self.cs.execute(
            '''CREATE INDEX IF NOT EXISTS "{0}_path_idx" ON "{0}"(path) WHERE state = 'RDY' '''.format(label))
        self.cs.execute('''CREATE INDEX IF NOT EXISTS "{0}_path1_idx" ON "{0}"(path)   '''.format(label))


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
        if data :
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

        retval  = u'{0:10s} |{1:23s} |{2:20s}\n'.format('State','Count','Average time in State')
        retval += '{0:10s} |{1:23s} |{2:20s}\n'.format('-'*10,'-'*23,'-'*20)

        for k in self.cs :
            k = list(k)
            k[0] = friendly[k[0]]
            retval += '{0:10s} |{1:23,} |{2:20.2f}\n'.format(*k)

        ### See if we need to reset the work queue
        if reset :
            cmd = '''UPDATE {0}
                        SET state = 'RDY', start_time=strftime('%s','now'), end_time = strftime('%s','now')
                        WHERE STATE = 'FAIL' or 'STATE' = 'WRK' '''.format(self.label)
            self.cs.execute(cmd)
            self.cs.connection.commit()
            retval += u'\n\n    Failed and Processing values reset after.'
        # And now return the status
        return retval


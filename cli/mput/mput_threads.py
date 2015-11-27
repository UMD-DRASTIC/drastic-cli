from .mput_threads import *


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

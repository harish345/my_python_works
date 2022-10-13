from threading import Thread
#from multiprocessing import Process
import time
import Util
from LoggingUtil import logger

def runDataIngestion(json, func,source):
    starttime = time.time()
    procs = []
    conns=[]
    l = len(json)
    logger.info("total rows:"+ str(l))
    chunk = round(l / 8)+1

    # instantiating process with arguments
    if chunk > 32:
        for i in range(0, 8):
            logger.info('Started with chunk '+ str( i * chunk)+ ' '+ str( chunk * (i + 1)))
            con = Util.DBConnection().get_connection()
            conns.append(con)
            proc = Thread(target=func, args=(json[i * chunk:chunk * (i + 1)],source,con))
            procs.append(proc)
            proc.start()

        # complete the processes
        for proc in procs:
            proc.join()
        for con in conns:
            if con!=None:
                con.close()
    else:
        con = Util.DBConnection().get_connection()
        func(json,source,con)

    logger.info('Job completed in {} seconds for source {}'.format(time.time() - starttime,source))

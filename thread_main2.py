import time
import logging
import subprocess
import os
from multiprocessing import Process
logging.basicConfig(level=logging.DEBUG,
                format='[%(asctime)s] [%(thread)d] %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                #filename='myapp.log',
                filemode='w')

if __name__ == '__main__':
    plist=[]
    logging.info( "crawl_thread start")
    import thread_crawl
    proc=Process(target=thread_crawl.start)
    proc.start()
    plist.append(proc);

    time.sleep(10)
    logging.info( "down_thread start")
    import thread_down
    proc=Process(target=thread_down.start)
    proc.start()
    plist.append(proc);

    time.sleep(10)
    logging.info( "img_thread start")
    import img_down
    proc=Process(target=img_down.start_downimg)
    proc.start()
    plist.append(proc);
    for proc in plist: proc.join()
    logging.info( "thread main finished")


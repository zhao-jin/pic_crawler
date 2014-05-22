import time
import logging
import subprocess
import os
logging.basicConfig(level=logging.DEBUG,
                format='[%(asctime)s] [%(thread)d] %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                #filename='myapp.log',
                filemode='w')

if __name__ == '__main__':
	if os.fork() == 0:
		logging.info( "crawl_thread start")
		import thread_crawl
		thread_crawl.start()
		logging.info( "crawl_thread end")
		os._exit(0)
	time.sleep(10)
	if os.fork() == 0:
		logging.info( "down_thread start")
		import thread_down
		thread_down.start()
		logging.info( "down_thread end")
		os._exit(0)
	time.sleep(10)
	if os.fork() == 0:
		logging.info( "img_thread start")
		import img_down
		img_down.start_downimg()
		logging.info( "img_thread end")
		os._exit(0)

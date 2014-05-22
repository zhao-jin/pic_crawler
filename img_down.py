#!/usr/bin/env python
# -*- coding:utf-8 -*-
import urllib2
import re
import sys
from threading import Thread
import time
import Queue
import os
import MySQLdb
import logging
logging.basicConfig(level=logging.DEBUG,
                format='[%(asctime)s] [%(thread)d] %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                #filename='img_down.log',
                filemode='w')

	

class MysqlThread(Thread):
	sql_queue=Queue.Queue(0)
	conn =None
	cur=None
	
	def __init__(self):
		self.conn=MySQLdb.connect(host='localhost',user='root',db='x365x_data',charset='utf8')
		self.cur=self.conn.cursor()
		Thread.__init__(self)
		self.start()
		logging.info("MysqlThread started!")
	
	def __del__(self ):
		self.cur.close()
		self.conn.close()
		Thread.__del__(self)
		logging.info("MysqlThread exit!")
		
	def AddSql(self, sql):
		logging.info("add task:[%s] " % sql)
		self.sql_queue.put(sql)
	
	def run(self):		
		while True:
			try:
				sql=self.sql_queue.get()
				if sql == "EXIT!":
					logging.info("MySQLThread BREAK!")
					break
				logging.info("sql exe:[%s]" % sql)
				self.cur.execute(sql)
				self.conn.commit()
			except MySQLdb.Error,e:
				logging.info("Mysql Error %d: %s" % (e.args[0], e.args[1]))
				break
				
db_proxy=MysqlThread()

class ImageDownThread(Thread):
	def __init__(self, work_queue,idx):
		Thread.__init__(self)
		self.work_queue = work_queue
		self.idx=idx
		self.start()
	
	def run(self):
		while True:
			url,localfile=self.work_queue.get()
			if url == "EXIT!":
				logging.info("ImageDownThread[%d] BREAK!" % self.idx)
				break
			ImgDownload(url,localfile)



def ImgDownload(url,fname):
	try:
		logging.info("downloading image:{%s} to {%s}" % (url,fname))
		flist=os.path.basename(fname).replace('_','.').split('.')
		db_proxy.AddSql("update thread_imgs set state=-2 where tid=%s and idx=%s;" % (flist[1], flist[0]))
		req = urllib2.Request(url)
		res = urllib2.urlopen(req)
		pic = res.read()
		dirname = os.path.dirname(fname)
		if not os.path.exists(dirname): os.makedirs(dirname)
		f = open(fname, "wb");
		f.write(pic);
		f.close()
		db_proxy.AddSql("update thread_imgs set state=1 where tid=%s and idx=%s;" % (flist[1], flist[0]))
	except Exception,e:
		flist=os.path.basename(fname).replace('_','.').split('.')
		logging.info("ImgDownload failed url=[%s],id=%s:%s, err=[%s]" % (url,flist[1],flist[0], str(e)))
		db_proxy.AddSql("update thread_imgs set state=-1 where tid=%s and idx=%s;" % (flist[1], flist[0]))

def UpdateDbState():
	conn=MySQLdb.connect(host='localhost',user='root',db='x365x_data',charset='utf8')
	conn=MySQLdb.connect(host='localhost',user='root',db='x365x_data',charset='utf8')
	cur=conn.cursor()
	count=cur.execute("insert into crawled_thread(tid,downcnt)  select tid, count(*) as dcnt from thread_imgs as t where state !=0 group by tid on duplicate key update downcnt =values(downcnt);")
	logging.info("affect %d rows when updating downcnt" % count)
	count=cur.execute("update crawled_thread set state=2 where state = 1 and (picnum=downcnt or picnum=0);")
	logging.info("affect %d rows when updating state" % count)
	logging.info("affect %d rows when updating state2" % count)
	conn.commit()
	cur.close()
	conn.close()

def start_downimg():
	baseDir="H:\\game\\data\\x365x\\"	
	logging.info("procedure started!")
	workNum=20
	workers=[]
	work_queue=Queue.Queue(0)


	sql="select tid,dir from crawled_thread;"
	conn=MySQLdb.connect(host='localhost',user='root',db='x365x_data',charset='utf8')
	cur=conn.cursor()
	count=cur.execute(sql)
	logging.info("total get %d threads from DB" % count)
	results = cur.fetchall() 
	cur.close()
	conn.close()	
	dictTidir={}	
	for res in results:
		threadDir=baseDir+filter(lambda ch: ch not in ' \'"\\/?|<>:*', res[1])+"\\"
		dictTidir[res[0]]=threadDir
	
	logging.info("init dict Thread dir complete!")

	while count > 0:
		for i in range(0, workNum):
			workers.append(ImageDownThread(work_queue,i))
		
		sql="select tid,idx,url from thread_imgs where state=0;"
		conn=MySQLdb.connect(host='localhost',user='root',db='x365x_data',charset='utf8')
		cur=conn.cursor()
		count=cur.execute(sql)
		logging.info("total get %d image to download" % count)
		results = cur.fetchall() 
		cur.close()
		conn.close()
		for res in results:
			threadDir=dictTidir[res[0]]			
			localfile="%s%05d_%d.jpg" % (threadDir, res[1], res[0])
			url=res[2]
			work_queue.put((url, localfile))
	
	
		for wk in workers:
			work_queue.put(("EXIT!","EXIT!"))
			work_queue.put(("EXIT!","EXIT!"))
	
		for wk in workers:
			if wk.isAlive():wk.join(30)  
		logging.info("%d imgs downloaded at cur round" % count)		
		time.sleep(10)
		workers = []
			
	UpdateDbState()		
			
	db_proxy.AddSql("EXIT!")
	if db_proxy.isAlive(): db_proxy.join()
	logging.info("procedure ended!")
	
if __name__ == '__main__':
	start_downimg()

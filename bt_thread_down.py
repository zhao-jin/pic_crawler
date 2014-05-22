#!/usr/bin/env python
# -*- coding:utf-8 -*-

import urllib2
import re
import sys
from threading import Thread
import time
import random
import hashlib
import Queue
from bs4 import BeautifulSoup
import os
import MySQLdb
import logging
logging.basicConfig(level=logging.DEBUG,
                format='[%(asctime)s] [%(thread)d] %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                #filename='thread_down.log',
                filemode='w')

class MysqlThread(Thread):
	sql_queue=Queue.Queue(0)
	conn =None
	cur=None
	
	def __init__(self):
		self.conn=MySQLdb.connect(host='localhost',user='root',db='x365x_bt',charset='utf8')
		self.cur=self.conn.cursor()
		Thread.__init__(self)
		self.start()
	
	def __del__(self ):
		self.cur.close()
		self.conn.close()
		Thread.__del__(self)
		logging.info( "MysqlThread exit!")
		
	def AddSql(self, sql):
		logging.info( "add task:[%s] " % sql)
		self.sql_queue.put(sql)
	
	def run(self):		
		while True:
			try:
				sql=self.sql_queue.get()
				if sql == "EXIT!":
					logging.info( "MySQLThread BREAK!")
					break
				logging.info( "sql exe:[%s]" % sql)
				self.cur.execute(sql)
				self.conn.commit()
			except MySQLdb.Error,e:
				logging.info( "Mysql Error %d: %s" % (e.args[0], e.args[1]))
				break
				
db_proxy=MysqlThread()


def DownloadPage(curUrl):
	htmlPage=""
	try:
		headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
		req = urllib2.Request(url=curUrl, headers = headers)
		res = urllib2.urlopen(req)
		headers = res.info()
		htmlPage = res.read()
		logging.info( headers['Content-Encoding'])
		if ('Content-Encoding' in headers and headers['Content-Encoding']) or \
			('content-encoding' in headers and headers['content-encoding']):
			logging.info( "Gzip-ing")
			import gzip
			import StringIO
			data = StringIO.StringIO(htmlPage)
			gz = gzip.GzipFile(fileobj=data)
			htmlPage = gz.read()
			gz.close()
	except Exception,e:
		logging.info( "download page [%s] err: %s."	% (curUrl, str(e)))
		
	return htmlPage

	
def CrawlThreadImage(tid):
	url_queue=Queue.Queue(0)
	threadUrl="http://meizi.us/thread-%d-1-1.html" % tid
	url_queue.put(threadUrl)
	seqID=int(0)
	
	while url_queue.qsize() > 0:
		url=url_queue.get()
		logging.info( "parse photo page:[%s]", url)
		page=DownloadPage(url).decode('utf8','ignore')
		rc='<img src="*([^" ]*.jpg)"*[^>]*'
		images = re.findall(rc, page, re.MULTILINE | re.DOTALL)
		for image in images:
			seqID+=1
			image=filter(lambda ch: ch not in ' \'"', image)
			logging.info( "\tcrawled img on[%d]: [%s]\n" % (tid,image))
			#img_queue.put((image,localfile))
			db_proxy.AddSql("insert ignore into thread_imgs(tid,idx,url,state)values(%d,%d,'%s',0);" % (tid,seqID,image))
		soup=BeautifulSoup(page)	
		nextTag=soup.findAll('a',text=re.compile(ur'^[\u4e00-\u9fa5]+\>$'))
		if len(nextTag) > 0:
			url_queue.put(nextTag[0]['href'])

	updateSql="update crawled_thread set state= %d , picnum=%d where tid=%d;" % (1, seqID, tid) 
	db_proxy.AddSql(updateSql)

	
def startThreadDown():		
	#db_proxy=MysqlThread()	
	sql="select tid,dir from crawled_thread where state = 0 order by pubtime desc;"
	conn=MySQLdb.connect(host='localhost',user='root',db='x365x_bt',charset='utf8')
	cur=conn.cursor()
	count=cur.execute(sql)
	logging.info( "total get %d threads from DB\n" % count)
	results = cur.fetchall() 
	cur.close()
	conn.close()
	for res in results:		
		CrawlThreadImage(res[0])
	
			
	db_proxy.AddSql("EXIT!")
	if db_proxy.isAlive(): db_proxy.join()
		
	logging.info("ThreadDown end")
	
	return count

def start():
	while startThreadDown():		
		time.sleep(1)
			
if __name__ == '__main__':
	start()
	

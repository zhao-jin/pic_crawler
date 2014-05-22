#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import time
import Queue
from threading import Thread
import MySQLdb
import urllib2
from bs4 import BeautifulSoup
import logging
logging.basicConfig(level=logging.DEBUG,
                format='[%(asctime)s] [%(thread)d] %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                #filename='crawl_thread.log',
                filemode='w')

class MysqlThread(Thread):
	sql_queue=Queue.Queue(0)
	conn =None
	cur=None
	
	def __init__(self):
		self.conn=MySQLdb.connect(host='localhost',user='root',db='x365x_bt',charset='utf8')
		self.cur=self.conn.cursor()
		Thread.__init__(self)
	
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
				logging.infoging.info( "Mysql Error %d: %s" % (e.args[0], e.args[1]))
				break

dbThread = MysqlThread()

class UrlDownloader:
	curUrl=None
	
	def __init__(self, url):
		self.curUrl=url
	
	def Download(self):
		headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
		req = urllib2.Request(url=self.curUrl, headers = headers)
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
		return htmlPage
		
		
	


def crawler(url):
	t=UrlDownloader(url)
	page=t.Download().decode('utf-8', 'ignore')
	soup=BeautifulSoup(page, from_encoding="utf-8")
	rows=soup.findAll("table", { "class":"row" })
	for row in rows:		
		title=row.find("td", {"class":"f_title"}).find("a")
		#titleName=title.text.replace("'", "").replace('"','').replace('?','')[0:32]
		titleName=filter(lambda ch: ch not in ' \'"\\/?|<>:', title.text)[0:32]
		link=title['href']
		logging.info( "parse thread:%s\n" % link)
		views=row.find("td", {"class":"f_views"}).text
		start=link.find("thread-")+len("thread-")
		end=link.find("-", start)
		tid=link[start:end]
		pubtime=row.find("td", {"class":"f_author"}).br.text
		dirName="%05d-%s-%s" % (int(views), titleName, tid)
		sql=u"insert into crawled_thread(tid, title,views,state,dir,pubtime)values(%s,'%s',%s,0,'%s','%s') on duplicate key update views=%s;" % (tid,titleName,views, dirName,pubtime,views)
		dbThread.AddSql(sql)


def start():
	dbThread.start()

	for i in range(1,40):
		url="http://meizi.us/forum-17-%d.html" % i
		crawler(url)

	
	dbThread.AddSql("EXIT!")
	dbThread.join(180)	    
			
		

if __name__ == '__main__':
	start()
	

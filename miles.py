#!/usr/bin/env python
# -*- coding:utf-8 -*-


import time
import Queue
import os
import MySQLdb
from threading import Thread
import urllib2
import logging

logging.basicConfig(level=logging.DEBUG,
                format='[%(asctime)s] [%(thread)d] %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                #filename='myapp.log',
                filemode='w')


def Log(msg):
	logging.info(msg)
	

class MysqlThread(Thread):
	sql_queue=Queue.Queue(0)
	conn =None
	cur=None
	
	def __init__(self,dbname):
		self.conn=MySQLdb.connect(host='localhost',user='root',db=dbname,charset='utf8')
		self.cur=self.conn.cursor()
		Thread.__init__(self)
		self.start()
		Log("MysqlThread started!")
	
	def __del__(self ):
		self.cur.close()
		self.conn.close()
		Log("MysqlThread exit!")
		
	def AddSql(self, sql):
		Log("add task:[%s] " % sql)
		self.sql_queue.put(sql)
		
	def Exit(self,timeout=60):
		self.AddSql('EXIT!')
		if self.isAlive():self.join(timeout)
	
	def run(self):		
		while True:
			try:
				sql=self.sql_queue.get()
				if sql == "EXIT!":
					Log("MySQLThread BREAK!")
					break				
				cout=self.cur.execute(sql)								
				Log("sql exe:[%s], affect %d rows" % (sql,cout))
				self.conn.commit()
			except MySQLdb.Error,e:
				Log("Mysql Error %d: %s" % (e.args[0], e.args[1]))
				break

class PageInfo():
	
	def __init__(self,content,hdr):
		self.pgContent=content
		self.pgHeaders={}
		for i in hdr:
			self.pgHeaders[i.upper()]=hdr[i]
		
	def	GetContent(self):
		return self.pgContent;

	def GetHeaders(self):
		return self.pgHeaders
	
	def GetHdr(self, key):
		return self.pgHeaders.get(key.upper(), '')
		
	def inflat(self):
		enc=self.GetHdr('Content-Encoding')
		if enc.lower() == 'gzip':
			Log("unGzip-ing.....")
			import gzip                                                        
			import StringIO                                                    
			data = StringIO.StringIO(self.pgContent)                                 
			gz = gzip.GzipFile(fileobj=data)                                   
			self.pgContent = gz.read()                                               
			gz.close()
			
	## retriev charset from 'text/html; charset=utf-8'
	def GetCharset(self):
		tType='utf-8'
		contType=self.GetHdr('CONTENT-TYPE').lower()
		Tlist=contType.split(';')
		for i in Tlist:
			ss=i.strip()
			iFind=ss.find('charset=')
			if iFind >= 0:
				start=iFind+len('charset=')
				tType=ss[start:]
		return tType
		
def DownLoadUrl(url):
	headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
	req = urllib2.Request(url=url, headers = headers)                   
	res = urllib2.urlopen(req)                                             
	pgInfo = PageInfo(res.read(), res.info())
	res.close() 
	pgInfo.inflat()
	return pgInfo
		
	
def DownloadPage(curUrl):
	headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
	req = urllib2.Request(url=curUrl, headers = headers)
	res = urllib2.urlopen(req)
	headers = res.info()
	htmlPage = res.read()
	if ('Content-Encoding' in headers and headers['Content-Encoding']) or \
		('content-encoding' in headers and headers['content-encoding']):
		print "Gzip-ing"
		import gzip
		import StringIO
		data = StringIO.StringIO(htmlPage)
		gz = gzip.GzipFile(fileobj=data)
		htmlPage = gz.read()
		gz.close()
	return htmlPage

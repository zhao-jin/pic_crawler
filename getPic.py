#!/usr/bin/env python
# -*- coding:utf-8 -*-

import urllib2
import re
import sys
from threading import Thread
import time
import random
import hashlib

class tieba(object):
    
    url = None
    dirPath = None
    __md5 = None
    postfix = None

    def __init__(self):
        self.url = "http://meizi.us/thread-329894-"
        self.postfix = "-1.html"
        self.dirPath = sys.path[0] + "/tieba/"
        self.__md5 = hashlib.md5()

    def getImages(self, page):
        url = self.url + str(page) + self.postfix;
        headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
        req = urllib2.Request(url=url, headers = headers)
        res = urllib2.urlopen(url)
        headers = res.info()
        html = res.read()
        print headers['Content-Encoding']
        if ('Content-Encoding' in headers and headers['Content-Encoding']) or \
           ('content-encoding' in headers and headers['content-encoding']):
            print "Gzip-ing"
            import gzip
            import StringIO
            data = StringIO.StringIO(html)
            gz = gzip.GzipFile(fileobj=data)
            html = gz.read()
            gz.close()
        #print "len=%d, html=[%s]" % (len(html),html)
        #rc = '<img src="[^"]*"[^>]*\/>'
        rc='<img src="*([^" ]*.jpg)"*\s[^>]*>'
        html = re.findall(rc, html, re.MULTILINE | re.DOTALL)
        print "page (%d) get url from [%s], imgCnt=%d\n" % (page,url,len(html))    
        return html

    def saveImg(self, images):
        for i in images:
            print "downloading image:{%s}\n" % i
            rand = str(random.randint(1, 10000)) + i
            self.__md5.update(rand)
            fname = self.__md5.hexdigest()
            fname = self.dirPath + fname + ".jpg"

            req = urllib2.Request(i)
            res = urllib2.urlopen(i)
            pic = res.read()
            f = open(fname, "wb");
            f.write(pic);
            f.close()

class catch(Thread):
    startPage = None
    endPage = None

    def __init__(self, start, end):
        Thread.__init__(self)
        self.startPage = start
        self.endPage = end
        
    def run(self):
        loop = range(self.startPage, self.endPage + 1)
        for i in loop:
            t = tieba()
            imgs = t.getImages(i)
            t.saveImg(imgs)
            print "get page %d success" % i
            sys.stdout.flush()

if __name__ == '__main__':
    maxPage = 150
    threadSum = 1
    if threadSum > maxPage:
        threadSum = maxPage
    urlCount = maxPage / threadSum
    
    for i in range(0, threadSum):
        c = catch(i * urlCount, (i + 1)* urlCount - 1)
        c.start()

#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import miles
import logging

def Log(msg):
	logging.info(msg)

def rm_empty_dir(path):	
	if not os.path.isdir(path):
		Log("Error! [%s] is not dir!" % path)
		return;
	db_writer=miles.MysqlThread("x365x_bt")
	for each in os.listdir(path):
		filecnt=0
		subdir=path+'\\'+each
		if os.path.isdir(subdir):
			for i in os.listdir(subdir):
				filecnt += 1
			#print subdir, filecnt
			sql="update crawled_thread set downcnt=%d where dir='%s';" % (filecnt, each.decode('gbk','ignore').encode('utf-8','ignore'))
			db_writer.AddSql(sql)
			if filecnt == 0:
				Log("rm empty dir:%s" % subdir)
				os.rmdir(subdir)
	db_writer.Exit()
			
    
if __name__ == '__main__':
	path=raw_input("please input the path to rm:")
	rm_empty_dir(path)
	raw_input("press any key to exit!")

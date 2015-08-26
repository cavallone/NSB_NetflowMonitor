#!/usr/bin/python
#-*- coding: utf-8 -*

import MySQLdb
import datetime
from datetime import datetime, timedelta

namedict = {'10.1.1.1':'ScoringBoard', '10.1.2.2':'UserRoute', '10.1.1.2':'UserRoute', '10.1.100.3':'FireWall', '10.1.2.3':'FireWall','10.1.10.3':'FireWall', '10.1.10.1':'WebSite', '10.1.100.150':'BigBoss', '10.1.100.1':'WebSiteInside', '10.1.100.50':'FileServer', '10.1.100.25':'Staff'}

def cleancolumn():
	db = MySQLdb.connect(host = '192.168.6.145', user = 'root', passwd = 'bemo0GF!', db = 'iss')
	cursor = db.cursor()
	clear = 'UPDATE realtime_topo SET event = \'\''
	cursor.execute(clear)
	db.commit()
	db.close()

def checkevent():
	db = MySQLdb.connect(host = 'localhost', user = 'root', passwd = 'csyang', db = 'netflow_system')
	cursor = db.cursor()
	sql = 'SELECT * FROM anomaly_log ORDER BY id DESC LIMIT 0,1'
	cursor.execute(sql)
	result = cursor.fetchall()
	for row in result:
		if datetime.now()-row[1] <= timedelta(minutes=5):
			srcip = row[4]
			dstip = row[6]
			event = row[3]
			if srcip in namedict:
				src = namedict[srcip]
			elif '10.8.0.' in srcip:
				src = 'User'
			else:
				src = ''

			if dstip in namedict:
				dst = namedict[dstip]
			elif '10.8.0.' in dstip:
				dst = 'User'
			else:
				dst = ''
			
			db.close()
			return (src, dst, event)

		else:
			pass

def insertevent(q):
	db = MySQLdb.connect(host = '192.168.6.145', user = 'root', passwd = 'bemo0GF!', db = 'iss')
	cursor = db.cursor()
	sql = 'UPDATE realtime_topo SET event = \'{0}\' WHERE src = \'{1}\' AND dst = \'{2}\''.format(q[2], q[0], q[1])
	cursor.execute(sql)
	db.commit()
	db.close()

if __name__ == '__main__':
	cleancolumn()
	retrieve = checkevent()
	if retrieve:
		insertevent(retrieve)
	else:
		pass


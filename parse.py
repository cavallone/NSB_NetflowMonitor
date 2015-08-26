#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import subprocess
import datetime
import MySQLdb
from datetime import datetime, timedelta

namedict = {'10.1.1.1':'(ScoringBoard)', '10.1.2.2':'(UserRoute)', '10.1.1.2':'(UserRoute)', '10.1.100.3':'(FireWall)', '10.1.2.3':'(FireWall)','10.1.10.3':'(FireWall)', '10.1.10.1':'(WebSite)', '10.1.100.150':'(BigBoss)', '10.1.100.1':'(WebSiteInside)', '10.1.100.50':'(FileServer)', '10.1.100.25':'(Staff)'}

now = datetime.now() - timedelta(seconds=90)
nowdate = datetime.strftime(now, '%Y-%m-%d')
nowtime = datetime.strftime(now, '%Y%m%d%H%M')

flow = subprocess.Popen(args = 'nfdump -r /netflow/flow/'+nowdate+'/nfcapd.'+nowtime, 
                        shell = True, 
                        stdout = subprocess.PIPE)
s = flow.stdout.read()
s = s.strip()
flows = s.splitlines()
flows = flows[1:-4]

db = MySQLdb.connect(host = 'localhost', user = 'root', passwd = 'csyang', db = 'nsb_netflow')
cursor = db.cursor()

for f in flows:
	item = f.split()
	starttime = item[1]
	sip = item[4].split(':')[0]
	sport = item[4].split(':')[1]
	dip = item[6].split(':')[0]
	dport = item[6].split(':')[1]

	if sip in namedict:
		sip = sip + namedict[sip]
	elif '10.8.0.' in sip:
		sip = sip + '(User)'
	else:
		pass

	if dip in namedict:
		dip = dip + namedict[dip]
	elif '10.8.0.' in dip:
		dip = dip + '(User)'
	else:
		pass
	
	if sip.split('.')[0] == '10' and dip.split('.')[0] == '10' and dip.split('.')[3] != '255' :
		if item[9] is 'M':			
			sql = 'INSERT INTO record(date, time, protocol, src_ip, src_port, dst_ip, dst_port, packet_count, byte_count)VALUES(\'{0}\', \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\', \'{7}\', \'{8}\')'.format(item[0], starttime[:8], item[3], sip, sport, dip, dport, item[7], item[8]+' '+item[9])
		else:
			sql = 'INSERT INTO record(date, time, protocol, src_ip, src_port, dst_ip, dst_port, packet_count, byte_count)VALUES(\'{0}\', \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\', \'{7}\', \'{8}\')'.format(item[0], starttime[:8], item[3], sip, sport, dip, dport, item[7], item[8])
	else:
		sql = None
#		sql = 'INSERT INTO record(date, time, protocol, src_ip, src_port, dst_ip, dst_port)VALUES(\'{0}\', \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\')'.format(item[0], starttime[:8], item[3], sip, sport, dip, dport)

	try:
		if sql:
			cursor.execute(sql)
			db.commit()
	except:
		print 'cannot write in db'

db.close()

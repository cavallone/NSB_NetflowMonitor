#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import subprocess
import datetime
import MySQLdb
from datetime import datetime, timedelta

namedict = {'10.1.1.2':'(Staff)', '10.1.3.2':'(WebServer)', '10.1.4.2':'(DBServer)', '10.1.5.3':'(BossComputer)', '10.1.2.2':'(MailServer)',
           '10.1.5.2':'(NAT)', '10.1.2.3':'(NAT)', '10.1.3.3':'(NAT)', '10.1.4.3':'(NAT)', '10.1.1.3':'(NAT)'}

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
	else:
		pass

	if dip in namedict:
		dip = dip + namedict[dip]
	else:
		pass
	
	if sip.split('.')[0] == '10' and dip.split('.')[0] == '10' and dip.split('.')[3] != '255' :
		sql = 'INSERT INTO record2(date, time, protocol, src_ip, src_port, dst_ip, dst_port)VALUES(\'{0}\', \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\')'.format(item[0], starttime[:8], item[3], sip, sport, dip, dport)
	else:
		sql = 'INSERT INTO record(date, time, protocol, src_ip, src_port, dst_ip, dst_port)VALUES(\'{0}\', \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\')'.format(item[0], starttime[:8], item[3], sip, sport, dip, dport)
	
	try:
		cursor.execute(sql)
		db.commit()
	except:
		print 'cannot write in db'

db.close()

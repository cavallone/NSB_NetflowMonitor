#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import subprocess
import datetime
import MySQLdb
from datetime import datetime, timedelta

namedict = {'10.1.1.1':'ScoringBoard', '10.1.2.2':'UserRoute', '10.1.1.2':'UserRoute', '10.1.100.3':'FireWall', '10.1.2.3':'FireWall','10.1.10.3':'FireWall', '10.1.10.1':'WebSite', '10.1.100.150':'BigBoss', '10.1.100.1':'WebSiteInside', '10.1.100.50':'FileServer', '10.1.100.25':'Staff'}

servicedict = {'21':'FTP', '23':'Telnet', '80':'HTTP', '443':'HTTPS', '445':'SMB', '8080':'HTTP'}

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

db = MySQLdb.connect(host = '192.168.6.145', user = 'root', passwd = 'bemo0GF!', db = 'iss')
cursor = db.cursor()

log = open('/tmp/log', 'a')

for f in flows:
	item = f.split()
	sip = item[4].split(':')[0]
	sport = item[4].split(':')[1]
	dip = item[6].split(':')[0]
	dport = item[6].split(':')[1]

	if sip in namedict:
		src = namedict[sip]
	elif '10.8.0.' in sip:
		src = 'User'
	else:
		src = ''

	if dip in namedict:
		dst = namedict[dip]
	elif '10.8.0.' in dip:
		dst = 'User'
	else:
		dst = ''

	if sport in servicedict:
		event = servicedict[sport]
	elif dport in servicedict:
		event = servicedict[dport]
	else:
		event = 'undefined'
	
	if src and dst:
		sql = 'UPDATE realtime_topo SET event = \'{0}\' WHERE src = \'{1}\' AND dst = \'{2}\''.format(event, src, dst)
		log.write(nowdate + ' ' + nowtime + ' ' + sql + '\n')
	else:
		sql = None
		print f
		print src + '//' + sport + '//' + dst + '//' + dport + '//' + event

	try:
		if sql:
			cursor.execute(sql)
			db.commit()
	except:
		print 'cannot write in db'

log.close()
db.close()

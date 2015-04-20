#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import subprocess
import time
import MySQLdb

t = time.time()
nowdate = time.strftime('%Y-%m-%d', time.localtime(t))
nowtime = time.strftime('%Y%m%d%H%M', time.localtime(t))

flow = subprocess.Popen(args = 'nfdump -r /nfs/netflow/'+nowdate+'/nfcapd.'+nowtime, 
                        shell = True, 
                        stdout = subprocess.PIPE)
s = flow.stdout.read()
s = s.strip()
flows = s.splitlines()
flows = flows[1:-4]

db = MySQLdb.connect(host = 'localhost', user = 'root', passwd = 'csyang')
cursor = db.cursor()

for f in flows:
	item = f.split()

#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
import json
import urllib2
import threading

url_prefix = None

set_err = 0
get_err = 0
val_err = 0

def run_test(times, key):
    global set_err, get_err, val_err
    key = str(key)
    for i in xrange(times):
        val = str(i)
        req = urllib2.urlopen('%s%s/set/%s' % (url_prefix, key, val))
        rsp = json.loads(req.read())
        if rsp['ret'] != 0:
            set_err += 1
            continue
        req = urllib2.urlopen('%s%s/get' % (url_prefix, key))
        rsp = json.loads(req.read())
        if rsp['ret'] != 0:
            get_err += 1
            continue
        if rsp['val'] != val:
            val_err += 1
            continue

if __name__ == '__main__':
    # python kv_http_test.py 127.0.0.1:9909 10 100
    if len(sys.argv) < 4 or '-h' in sys.argv or '--help' in sys.argv:
        print 'Usage: python kv_http_test.py host:port thread_num times_of_one_thread'
        sys.exit()
    
    url_prefix = 'http://%s/' % sys.argv[1]
    thread_num = int(sys.argv[2])
    times_of_one_thread = int(sys.argv[3])
    
    ts = []
    for i in xrange(thread_num):
        t = threading.Thread(target=run_test, args=(times_of_one_thread, i))
        t.setDaemon(True)
        t.start()
        ts.append(t)
    
    for t in ts:
        t.join()
    
    print set_err, get_err, val_err

#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
import json
import socket

import raft

debug = False

udp_socket = None

kv = {}
g_seq = 0
session = {}

def command_exec(command):
    global udp_socket, kv, session

    msg = json.loads(command)

    if msg['cmd'] == 'set':
        kv[msg['key']] = msg['val']
    elif msg['cmd'] == 'del':
        if msg['val'] in kv:
            del kv[msg['key']]

    if msg['seq'] in session:
        udp_socket.sendto(json.dumps({
            'ret': 0,
        }), session[msg['seq']])
        del session[msg['seq']]

if __name__ == '__main__':
    # python kv.py 127.0.0.1:9901 127.0.0.1:9902 127.0.0.1:9903
    # python kv.py 127.0.0.1:9902 127.0.0.1:9901 127.0.0.1:9903
    # python kv.py 127.0.0.1:9903 127.0.0.1:9902 127.0.0.1:9901
    if len(sys.argv) < 4 or '-h' in sys.argv or '--help' in sys.argv:
        print 'Usage: python kv.py selfHost:port partner1Host:port partner2Host:port ...'
        sys.exit()

    def get_addr_by_str(s):
        hps = s.split(':')
        return (hps[0], int(hps[1]))

    self = get_addr_by_str(sys.argv[1])
    partners = []
    for i in xrange(2, len(sys.argv)):
        partners.append(get_addr_by_str(sys.argv[i]))

    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind(self)
    udp_socket.settimeout(0.1)

    def send_to(msg, addr):
        msg['_raft'] = 1
        buff = json.dumps(msg)
        udp_socket.sendto(buff, addr)

    node = raft.Node(self, partners)
    node.RegisterSendFunc(send_to)
    node.RegisterExecFunc(command_exec)
    if debug:
        node.Debug()

    while True:
        try:
            buff, addr = udp_socket.recvfrom(65536)
            try:
                msg = json.loads(buff)
            except ValueError:
                msg = {}
            if '_raft' in msg:
                del msg['_raft']
                node._onMsgRecv(addr, msg)
            else:
                while True:
                    if 'cmd' not in msg or \
                        msg['cmd'] not in ('get', 'set', 'del'):
                        break
                    if 'key' not in msg or \
                        not isinstance(msg['key'], basestring):
                        break
                    if msg['cmd'] == 'set' and ('val' not in msg or
                        not isinstance(msg['val'], basestring)):
                        break

                    if debug:
                        if msg['cmd'] != 'get':
                            print 'New Request %s' % msg

                    if not node.IsLeader():
                        udp_socket.sendto(json.dumps({
                            'ret': -999,
                            'err': 'Not Leader',
                            'redirect': node.GetLeader(),
                        }), addr)
                        break

                    if msg['cmd'] == 'get':
                        if msg['key'] in kv:
                            udp_socket.sendto(json.dumps({
                                'ret': 0,
                                'val': kv[msg['key']],
                            }), addr)
                        else:
                            udp_socket.sendto(json.dumps({
                                'ret': -1,
                                'err': 'Not Found',
                            }), addr)
                    else:
                        g_seq += 1
                        seq = g_seq
                        session[seq] = addr
                        msg['seq'] = seq
                        node.AppendCommand(json.dumps(msg))

                    break

        except socket.timeout:
            pass
        except socket.error, e:
            # 傻逼Windows, UDP对面没开端口也会抛连接被RESET异常
            if str(e) != '[Errno 10054] ':
                raise
        node._onTick()

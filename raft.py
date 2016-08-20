#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
import time
import json
import random
import socket

class _STATE:
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class _COMMAND_TYPE:
    NOP = 1

class Log(object):
    def add(self, command, index, term):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def deleteEntriesFrom(self, entry_from):
        raise NotImplementedError

    def deleteEntriesTo(self, entry_to):
        raise NotImplementedError

    def __getitem__(self, item):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def _destroy(self):
        raise NotImplementedError

    def __repr__(self):
        s = ''
        for i in xrange(len(self)):
            s += '  %s\n' % (self[i], )
        return s

    def getCurrIndex(self):
        return self[-1][1]

    def getCurrTerm(self):
        return self[-1][2]

    def getPrevIndexTerm(self, next_index):
        prev_index = next_index - 1
        entries = self.getEntries(prev_index, 1)
        if entries:
            return prev_index, entries[0][2]
        return None, None

    def getEntries(self, from_index, count=None):
        first_index = self[0][1]
        if from_index is None or from_index < first_index:
            return []
        diff = from_index - first_index
        if count is None:
            result = self[diff:]
        else:
            result = self[diff: diff + count]
        return result

class BaseLog(Log):
    def __init__(self):
        self._log = []

    def clear(self):
        self._log = []

    def add(self, command, index, term):
        self._log.append((command, index, term))

    def deleteEntriesFrom(self, entry_from):
        first_index = self._log[0][1]
        diff = entry_from - first_index
        if diff < 0:
            return
        del self._log[diff:]

    def deleteEntriesTo(self, entry_to):
        first_index = self._log[0][1]
        diff = entry_to - first_index
        if diff < 0:
            return
        self._log = self._log[diff:]

    def __getitem__(self, item):
        return self._log[item]

    def __len__(self):
        return len(self._log)

    def _destroy(self):
        pass

class Node(object):

    def __init__(self, self_addr, others_addrs):
        self._self_addr = self_addr
        self._others_addrs = others_addrs

        self._send = None
        self._exec = None
        self._exec_finish = None

        now = time.time()

        # 初始都是Follower
        self._state = _STATE.FOLLOWER
        self._election_deadline = now + self._genTimeout()
        self._votes_count = 0
        self._leader = None
        self._new_append_entries_time = 0

        # latest term server has seen (initialized to 0
        # on first boot, increases monotonically)
        self._curr_term = 0
        # candidateId that received vote in current
        # term (or null if none)
        self._voted_for = None

        # log entries; each entry contains command
        # for state machine, and term when entry
        # was received by leader (first index is 1)
        self._log = BaseLog()

        # index of highest log entry known to be
        # committed (initialized to 0, increases
        # monotonically)
        self._commit_index = 1
        # index of highest log entry applied to state
        # machine (initialized to 0, increases
        # monotonically)
        self._last_applied = 1

        # for each server, index of the next log entry
        # to send to that server (initialized to leader
        # last log index + 1)
        self._next_index = {}
        # for each server, index of highest log entry
        # known to be replicated on server
        # (initialized to 0, increases monotonically)
        self._match_index = {}

        self._leader_commit_index = 0

        if len(self._log) == 0:
            self._log.add(_COMMAND_TYPE.NOP, 1, 0)

        self._debug = False

    def RegisterSendFunc(self, func):
        self._send = func

    def RegisterExecFunc(self, func):
        # func(command)
        self._exec = func

    def RegisterExecFinishFunc(self, func):
        # func(ret, err, command)
        self._exec_finish = func

    def IsLeader(self):
        return self._state == _STATE.LEADER

    def GetLeader(self):
        return self._leader

    def AppendCommand(self, command):
        assert isinstance(command, str)
        assert self._state == _STATE.LEADER

        self._log.add(command, self._log.getCurrIndex() + 1, self._curr_term)
        self._sendAppendEntriesReq()

        return 0

    def Debug(self, flag=True):
        self._debug = flag

    def _onTick(self):
        now = time.time()

        if self._state in (_STATE.FOLLOWER, _STATE.CANDIDATE):
            if self._election_deadline < now:
                # 超时触发新的一轮选举
                self._election_deadline = now + self._genTimeout()
                self._state = _STATE.CANDIDATE
                self._leader = None
                self._curr_term += 1
                self._voted_for = self._self_addr
                self._votes_count = 1
                for addr in self._others_addrs:
                    msg = {
                        'type': 'RequestVoteReq',
                        'term': self._curr_term,
                        'last_log_index': self._log.getCurrIndex(),
                        'last_log_term': self._log.getCurrTerm(),
                    }
                    self._send(msg, addr)
                    if self._debug:
                        print '[%s] RequestVoteReq to %s:%s, log(%s, %s)' % \
                            (msg['term'], addr[0], addr[1],
                                msg['last_log_index'], msg['last_log_term'])

        elif self._state == _STATE.LEADER:
            while self._commit_index < self._log.getCurrIndex():
                # 超过半数复制即提交
                next_commit_index = self._commit_index + 1
                count = 1
                for addr in self._others_addrs:
                    if self._match_index[addr] >= next_commit_index:
                        count += 1
                if count > (len(self._others_addrs) + 1) / 2:
                    self._commit_index = next_commit_index
                else:
                    break
            self._leader_commit_index = self._commit_index

            if self._new_append_entries_time < now:
                self._sendAppendEntriesReq()

        if self._commit_index > self._last_applied:
            count = self._commit_index - self._last_applied
            entries = self._log.getEntries(self._last_applied + 1, count)
            for entry in entries:
                command = entry[0]
                if command != _COMMAND_TYPE.NOP:
                    if self._debug:
                        print 'Exec command=%s ...' % command
                    ret = self._exec(command)
                    if self._debug:
                        print 'Exec finish ret=%s' % ret
                    if self._exec_finish:
                        self._exec_finish(ret, None, command)
                self._last_applied += 1

    def _onMsgRecv(self, addr, msg):
        now = time.time()

        if msg['type'] == 'RequestVoteReq':
            if self._debug:
                print '[%s] RequestVoteReq from %s:%s, log(%s, %s)' % \
                    (msg['term'], addr[0], addr[1],
                        msg['last_log_index'], msg['last_log_term'])

            # 接收新一轮选举
            if msg['term'] > self._curr_term:
                self._curr_term = msg['term']
                self._voted_for = None
                self._state = _STATE.FOLLOWER
                self._leader = None

            if self._state in (_STATE.FOLLOWER, _STATE.CANDIDATE):
                if msg['term'] >= self._curr_term:
                    last_index = msg['last_log_index']
                    last_term = msg['last_log_term']
                    if last_term < self._log.getCurrTerm() or \
                        (last_term == self._log.getCurrTerm() and
                            last_index < self._log.getCurrIndex()):
                        return
                    if self._voted_for is not None:
                        return

                    # 投票给选举发起者
                    self._voted_for = addr
                    self._election_deadline = now + self._genTimeout()
                    self._send({
                        'type': 'RequestVoteRsp',
                        'term': msg['term'],
                    }, addr)
                    if self._debug:
                        print '[%s] RequestVoteRsp to %s:%s' % \
                            (msg['term'], addr[0], addr[1])

        elif msg['type'] == 'RequestVoteRsp':
            if self._debug:
                print '[%s] RequestVoteRsp from %s:%s' % \
                    (msg['term'], addr[0], addr[1])
            # 新得选票
            if self._state == _STATE.CANDIDATE and \
                msg['term'] == self._curr_term:
                self._votes_count += 1
                # 票数过半当Leader
                if self._votes_count > (len(self._others_addrs) + 1) / 2:
                    self._onBecomeLeader()

        elif msg['type'] == 'AppendEntriesReq':
            # if 'entries' in msg and msg['entries']:
            #     print '[%s] AppendEntriesReq from %s:%s\n  msg=%s' % \
            #         (msg['term'], addr[0], addr[1], msg)

            if msg['term'] >= self._curr_term:
                self._election_deadline = now + self._genTimeout()
                if self._leader != addr:
                    self._leader = addr
                    if self._debug:
                        print 'Follow New Leader %s:%s' % self._leader
                if msg['term'] > self._curr_term:
                    self._curr_term = msg['term']
                    self._voted_for = None
                self._state = _STATE.FOLLOWER
                new_entries = msg.get('entries', [])
                self._leader_commit_index = msg['commit_index']

                prev_index = msg['prev_log_index']
                prev_term = msg['prev_log_term']
                prev_entries = self._log.getEntries(prev_index)

                def debug_show(s=None):
                    if not self._debug:
                        return
                    if s:
                        print s
                    print 'Leader:'
                    print '  prev_index: %s' % prev_index
                    print '  prev_term: %s' % prev_term
                    print '  entries: %s' % new_entries
                    print 'Local Logs[%s]' % len(self._log)

                if not prev_entries:
                    # 缺很多
                    debug_show('[Missing more]')
                    self._sendAppendEntriesRsp(addr, reset=True, success=False)
                    return

                if prev_entries[0][2] != prev_term:
                    # 最新一条不一致
                    debug_show('[Last conflict]')
                    self._sendAppendEntriesRsp(addr, next_index=prev_index,
                        reset=True, success=False)
                    return

                if len(prev_entries) > 1:
                    # 有多余的
                    debug_show('[Redundant]')
                    self._log.deleteEntriesFrom(prev_index + 1)

                next_index = prev_index + 1
                if new_entries:
                    debug_show('[Append]')
                    for entry in new_entries:
                        assert len(self._log) + 1 == entry[1]
                        self._log.add(*entry)

                    next_index = new_entries[-1][1]
                    if self._debug:
                        print 'Local Logs[%s] (New)' % len(self._log)

                self._sendAppendEntriesRsp(addr, next_index=next_index, success=True)

                self._commit_index = min(self._leader_commit_index,
                    self._log.getCurrIndex())

        elif msg['type'] == 'AppendEntriesRsp':
            if self._state == _STATE.LEADER:
                next_index = msg['next_index']
                reset = msg['reset']
                success = msg['success']
                curr_index = next_index - 1
                if reset:
                    self._next_index[addr] = next_index
                if success:
                    self._match_index[addr] = curr_index

    def _onBecomeLeader(self):
        self._leader = self._self_addr
        self._state = _STATE.LEADER
        if self._debug:
            print 'Self New Leader %s:%s' % self._leader

        for addr in self._others_addrs:
            self._next_index[addr] = self._log.getCurrIndex() + 1
            self._match_index[addr] = 0

        # 领导人完全特性保证了领导人一定拥有所有已经被提交的日志条目，
        # 但是在他任期开始的时候，他可能不知道那些是已经被提交的。
        # 为了知道这些信息，他需要在他的任期里提交一条日志条目。
        # Raft 中通过领导人在任期开始的时候提交一个空白的没有任何操作的日志条目到日志中去来实现。
        self._log.add(_COMMAND_TYPE.NOP, self._log.getCurrIndex() + 1, self._curr_term)
        self._sendAppendEntriesReq()

    def _sendAppendEntriesReq(self):
        now = time.time()
        self._new_append_entries_time = now + 0.1 #心跳

        for addr in self._others_addrs:
            send_at_least_one = True
            next_index = self._next_index[addr]
            while next_index <= self._log.getCurrIndex() or send_at_least_one:
                prev_index, prev_term = self._log.getPrevIndexTerm(next_index)
                entries = []
                if next_index <= self._log.getCurrIndex():
                    entries = self._log.getEntries(next_index)
                    self._next_index[addr] = entries[-1][1] + 1
                msg = {
                    'type': 'AppendEntriesReq',
                    'term': self._curr_term,
                    'commit_index': self._commit_index,
                    'entries': entries,
                    'prev_log_index': prev_index,
                    'prev_log_term': prev_term,
                }
                self._send(msg, addr)
                next_index = self._next_index[addr]
                send_at_least_one = False
                break # 先弄成一次同步

    def _sendAppendEntriesRsp(self, addr,
        next_index=None, reset=False, success=False):
        if next_index is None:
            next_index = self._log.getCurrIndex() + 1
        self._send({
            'type': 'AppendEntriesRsp',
            'next_index': next_index,
            'reset': reset,
            'success': success,
        }, addr)

    def _genTimeout(self):
        tmin, tmax = 0.4, 1.4
        return tmin + (tmax - tmin) * random.random()

if __name__ == '__main__':
    # python raft.py 127.0.0.1:9901 127.0.0.1:9902 127.0.0.1:9903
    # python raft.py 127.0.0.1:9902 127.0.0.1:9901 127.0.0.1:9903
    # python raft.py 127.0.0.1:9903 127.0.0.1:9902 127.0.0.1:9901
    if len(sys.argv) < 4 or '-h' in sys.argv or '--help' in sys.argv:
        print 'Usage: python raft.py selfHost:port partner1Host:port partner2Host:port ...'
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
        buff = json.dumps(msg)
        udp_socket.sendto(buff, addr)

    node = Node(self, partners)
    node.RegisterSendFunc(send_to)
    node.Debug()

    while True:
        try:
            buff, addr = udp_socket.recvfrom(65536)
            msg = json.loads(buff)
            node._onMsgRecv(addr, msg)
        except socket.timeout:
            pass
        except socket.error, e:
            # 傻逼Windows, UDP对面没开端口也会抛连接被RESET异常
            if str(e) != '[Errno 10054] ':
                raise
        node._onTick()

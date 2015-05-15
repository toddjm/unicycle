#!/usr/bin/env python
import re
import socket
import subprocess
import sys
import time

def sendline_wait_on_ack(socket, str, timeout):
    print "Sending: %s" % (str)
    socket.send(str + '\n')
    ack = readline(socket, timeout)
    if not ack == "OK":
        StandardError("bad: %s" % (str))
    print "Recived: %s" % (ack)

def readline(socket, timeout):
    socket.settimeout(float(timeout))
    out = ""
    while 1:
        data = socket.recv(1)
        if not data or data == '\n':
            break
        out += data
    return out

HOST = "127.0.0.1"
PORT = 7102
s = None

p_child = subprocess.Popen("java Collect.ReaderTest -local_port %d" % (PORT), shell=True)
time.sleep(1)
 
for res in socket.getaddrinfo(HOST, PORT, socket.AF_UNSPEC, socket.SOCK_STREAM):
    print res
    af, socktype, proto, canonname, sa = res
    try:
        print "Get socket..."
        s = socket.socket(af, socktype, proto)
    except socket.error, msg:
        s = None
        continue
    try:
        print "Connect..."
        s.connect(sa)
    except socket.error, msg:
        print "Connect except..."+str(msg)
        s.close()
        s = None
        continue
    break
 
if s is None:
    print 'could not open socket'
    sys.exit(1)

sendline_wait_on_ack(s, "Hello, world", 5)
s.send('EXIT\000')
s.close()

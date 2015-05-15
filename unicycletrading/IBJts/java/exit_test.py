#!/usr/bin/python
import os
import subprocess
import time

p_child = subprocess.Popen("java Collect.ExitTest", shell=True, stdin=subprocess.PIPE)
time.sleep(2)

if (p_child.poll() == None):
    print "Child still running."
    p_child.communicate("EXIT\000")
#     p_child.terminate()
#     p_child.wait()
#     print "Child terminated."

else:
    print "[ERROR] Child is not running."
print "Done"

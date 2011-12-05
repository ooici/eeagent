import logging
import socket
import sys
import dashi.bootstrap as bootstrap
from threading import Thread
import os
import time
import signal
from eeagent.execute import get_exe_factory
from eeagent.message import EEAgentMessageHandler

class HeartbeatThread(Thread):

    def __init__(self, log=logging, mess=None, interval=30):
        Thread.__init__(self)
        self._done = False
        self._log = log
        self._mess = mess
        self._interval = interval

    def run(self):
        while not self._done:
            try:
                time.sleep(self._interval)
                self._mess.beat_it()
            except Exception, ex:
                self._log.log(logging.ERROR, "An exception occurred during the heartbeat")
                self.end()
                self._res = ex

    def end(self):
        self._done = True

    def get_results(self):
        return self._res


class ExecutorThread(Thread):

    def __init__(self, log=logging, exe_list=None, poll_interval=2):
        Thread.__init__(self)
        self._done = False
        self._log = log
        self._exe_list = exe_list
        self._poll_interval = poll_interval

    def run(self):
        while not self._done:
            try:
                for e in self._exe_list:
                    e.poll()
                time.sleep(self._poll_interval)
            except Exception, ex:
                self._log.log(logging.ERROR, "An exception occurred polling executables")
                self.end()
                self._res = ex

    def end(self):
        self._done = True

    def get_results(self):
        return self._res

class MessegerThread(Thread):

    def __init__(self, log=logging, mess=None, to=2):
        Thread.__init__(self)
        self._done = False
        self._log = log
        self._mess = mess
        self._to = to

    def run(self):
        while not self.done:
            try:
                self._mess.consume(timeout=self._to)
            except socket.timeout, ex:
                self._log.log(logging.DEBUG, "Dashi timeout wakeup %s" % str(ex))
            except Exception, res_ex:
                self._res = res_ex
                self.end()

    def end(self):
        self.done = True

    def get_results(self):
        return self._res


def determine_path():
    """find path of current file,
    Borrowed from wxglade.py"""
    try:
        root = __file__
        if os.path.islink(root):
            root = os.path.realpath(root)
        return os.path.dirname(os.path.abspath(root))
    except:
        print "I'm sorry, but something is wrong."
        print "There is no __file__ variable. Please contact the author."
        sys.exit()

thread_list = []

def death_handler(signum, frame):
    global thread_list
    for t in thread_list:
        t.end()

def main(args=sys.argv[1:]):
    global thread_list

    # get config
    config_files = []
    c = os.path.join(determine_path(), "config", "eeagent.yml")
    if c:
        config_files.append(c)
    CFG = bootstrap.configure(config_files=config_files, argv=args)
    log = bootstrap.get_logger("eeagent", CFG)

    # create pidantic objects
    factory_map = {}
    for lt in CFG.launch_types:
        factory = get_exe_factory(lt, CFG.launch_types[lt])
        factory_map[lt] = factory

    thread_list = []
    exe_thread = ExecuterThread(log=log, exe_list=factory_map, poll_interval=CFG.poll_interval)
    thread_list.append(exe_thread)
    # create message handler
    messenger = EEAgentMessageHandler(CFG, factory_map, log)
    mess_thread = MessegerThread(log=log, mess=messenger)
    thread_list.append(mess_thread)

    heart_thread = HeartbeatThread(log=log, mess=messenger, interval=CFG.heartbeat)
    thread_list.append(heart_thread)
    
    signal.signal(signal.SIGTERM, death_handler)
    signal.signal(signal.SIGINT, death_handler)
    signal.signal(signal.SIGQUIT, death_handler)

    for t in thread_list:
        t.start()
    for t in thread_list:
        t.join()

    rc = 0
    for t in thread_list:
        res = t.get_results()
        if res:
            log.logging(logging.ERROR, "An error occured in processing %s" % (str(res)))
            rc = 1

    return rc



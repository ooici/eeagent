import logging
import socket
import sys
import dashi.bootstrap as bootstrap
from threading import Thread
import signal
import time
import datetime
from eeagent.beatit import beat_it
from eeagent.execute import get_exe_factory
from eeagent.message import EEAgentMessageHandler
from eeagent.util import build_cfg, get_logging

class HeartBeater(object):
    def __init__(self, CFG, factory, log=logging):

        self._log = log
        self._log.log(logging.DEBUG, "Starting the heartbeat thread")
        self._dashi = bootstrap.dashi_connect(CFG.eeagent.name, CFG)
        self._CFG = CFG
        self._res = None
        self._interval = int(CFG.eeagent.heartbeat)
        self._res = None
        self._done = False
        self._factory = factory
        self._next_beat(datetime.datetime.now())

    def _next_beat(self, now):
        self._beat_time = now + datetime.timedelta(seconds=self._interval)

    def poll(self):
        now = datetime.datetime.now()
        if now > self._beat_time:
            self._next_beat(now)
            beat_it(self._dashi, self._CFG, self._factory)


class EEAgentMain(object):

    def __init__(self, args):
        self._args = args
        self.CFG = build_cfg(self._args)
        self.log = get_logging(self.CFG)

    def start(self):

        self._done = False

        # There can be only 1 process manager per eeagent (per supd, per ion)
        try:
            self._factory = get_exe_factory(self.CFG.eeagent.launch_type.name, self.CFG)
            self._interval = 1
            self.messenger = EEAgentMessageHandler(self.CFG, self._factory, self.log)
            self.heartbeater = HeartBeater(self.CFG, self._factory, log=self.log)

            self._res = None
        except Exception, ex:
            self.log.log(logging.ERROR, "Failed to start EEAgentMain: %s" % (str(ex)))
            raise

    def get_cfg(self):
        return self.CFG

    def death_handler(self, signum, frame):
        self.end()

    def wait(self):
        try:
            while not self._done:
                try:
                    try:
                        self.messenger.poll(timeout=self._interval)
                    except socket.timeout, ex:
                        self.log.log(logging.DEBUG, "Dashi timeout wakeup %s" % str(ex))
                    self.heartbeater.poll()
                except Exception, res_ex:
                    self._res = res_ex
                    self.log.log(logging.ERROR, "EEAgentMessagingThread error %s" % str(res_ex))

            self._factory.terminate()
        except Exception, wait_ex:
            self.log.log(logging.ERROR, "error waiting in EEAgentMain: %s" % (str(wait_ex)))
            raise

        return 0

    def end(self):
        self._done = True

class MainRunnerThread(Thread):

    def __init__(self, main):
        Thread.__init__(self)
        self._main = main

    def run(self):
        self._main.start()
        self._main.wait()

    def end(self):
        self._main.end()

eeagent = None
def death_handler(signum, frame):
    if not eeagent:
        return
    eeagent.end()

def main(args=sys.argv):
    global eeagent
    try:
        signal.signal(signal.SIGTERM, death_handler)
        signal.signal(signal.SIGINT, death_handler)
        signal.signal(signal.SIGQUIT, death_handler)
    except Exception, ex:
        pass
    eeagent = EEAgentMain(args)
    eeagent.start()
    return eeagent.wait()

if __name__ == '__main__':
    rc = main(sys.argv)
    sys.exit(rc)
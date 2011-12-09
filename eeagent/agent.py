import logging
import socket
import sys
import dashi.bootstrap as bootstrap
from threading import Thread
import signal
import time
from eeagent.beatit import beat_it
from eeagent.message import EEAgentMessageHandler
from eeagent.util import get_process_managers, build_cfg, get_logging

class HeartbeatThread(Thread):

    def __init__(self, CFG, process_managers_map, log=logging):
        Thread.__init__(self)

        self._log = log
        self._log.log(logging.DEBUG, "Starting the heartbeat thread")
        self._dashi = bootstrap.dashi_connect(CFG.eeagent.name, CFG)
        self._CFG = CFG
        self._res = None
        self._interval = int(CFG.eeagent.heartbeat)
        self._res = None
        self._done = False
        self._process_managers_map = process_managers_map

    def run(self):
        while not self._done:
            try:
                time.sleep(self._interval)
                beat_it(self._dashi, self._CFG, self._process_managers_map.values())
            except Exception, ex:
                self._log.log(logging.ERROR, "An exception occurred during the heartbeat")
                self.end()
                self._res = ex

    def end(self):
        self._done = True

    def get_results(self):
        return self._res


class EEAgentMessagingThread(Thread):

    def __init__(self, CFG, process_managers_map, log=logging):
        Thread.__init__(self)
        self._done = False
        # get config
        self.CFG = CFG
        self.log = log
        self.log.log(logging.DEBUG, "Starting the messenging thread")
        self._res = None
        self._interval = 2 
        self.messenger = EEAgentMessageHandler(self.CFG, process_managers_map, self.log)
        self.heartbeater = HeartbeatThread(self.CFG, process_managers_map, log=self.log)
        self.heartbeater.start()

    def run(self):
        while not self._done:
            try:
                self.messenger.poll(timeout=self._interval)
            except socket.timeout, ex:
                self.log.log(logging.DEBUG, "Dashi timeout wakeup %s" % str(ex))
            except Exception, res_ex:
                self._res = res_ex
                self.end()
                self.log.log(logging.ERROR, "EEAgentMessagingThread error %s" % str(res_ex))
        self.heartbeater.join()

    def end(self):
        self.heartbeater.end()
        self._done = True

    def get_result(self):
        res = self.heartbeater.get_results()
        if res:
            return res

        return self._res

class EEAgentMain(object):

    def __init__(self, args):

        self.CFG = build_cfg(args)
        self.log = get_logging(self.CFG)

        # There can be only 1 process manager per eeagent (per supd, per ion)
        self._process_managers_map = get_process_managers(self.CFG)

        self.messaging = EEAgentMessagingThread(self.CFG, self._process_managers_map, log=self.log)

        signal.signal(signal.SIGTERM, self.death_handler)
        signal.signal(signal.SIGINT, self.death_handler)
        signal.signal(signal.SIGQUIT, self.death_handler)

        self.messaging.start()

    def get_cfg(self):
        return self.CFG

    def death_handler(self, signum, frame):
        self.messaging.end()
        for m in self._process_managers_map.values():
            m.terminate()

    def wait(self):
        self.messaging.join()

        res = self.messaging.get_result()
        if res:
            raise res

        return 0


def main(args=sys.argv):
    eeagent = EEAgentMain(args)
    return eeagent.wait()

if __name__ == '__main__':
    rc = main(sys.argv)
    sys.exit(rc)
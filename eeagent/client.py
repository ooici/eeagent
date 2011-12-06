import dashi.bootstrap as bootstrap
import socket
import sys
from threading import Thread
from dashi import DashiConnection
import threading
import signal
from dashi.bootstrap import dashi_connect
import os
import uuid
from eeagent.types import EEAgentLaunchType

class TalkConsole(object):

    def __init__(self):
        self._prompt = ">> "

    def write(self, msg):
        sys.stdout.write("\r%s" % (msg))
        sys.stdout.write("\n" + self._prompt)
        sys.stdout.flush()

    def input(self):
        line = raw_input(self._prompt)
        return line.strip()

class EEAgentClient(Thread):

    def __init__(self, console, CFG, log):
        Thread.__init__(self)

        self.pd_name = CFG.pd.name
        self.exchange = CFG.ampq.exchange
        self._log = log
        self._lock = threading.RLock()
        self.dashi = dashi_connect(self.pd_topic, CFG)
        self.done = False
        self.console = console
        self.dashi.handle(self.heartbeat, "heartbeat")

    def heartbeat(self, message):
        self.console.write(str(message))

    def launch(self, argv):
        self.dashi.fire()
        upid = uuid.uuid4[0]
        params = {}
        params['exec'] = argv[0]
        params['argv'] = argv[1:]
        self.dashi.fire(self.CFG.ee_name, "launch_process", u_pid=upid, round=0, run_type=EEAgentLaunchType.fork, parameters=params)

    def run(self):
        while not self.done:
            try:
                self.dashi.consume(timeout=2)
            except socket.timeout, ex:
                pass

    def end(self):
        self.done = True

def launch(talker, line_a):
    talker.launch(line_a)


def main(args=sys.argv[1:]):
    global thread_list

    # get config
    config_files = []
    c = os.path.join(determine_path(), "config", "eeagent.yml")
    if c:
        config_files.append(c)
    CFG = bootstrap.configure(config_files=config_files, argv=args)
    log = bootstrap.get_logger("eeagent", CFG)

    signal.signal(signal.SIGTERM, death_handler)
    signal.signal(signal.SIGINT, death_handler)
    signal.signal(signal.SIGQUIT, death_handler)

    console = TalkConsole()
    talker = EEAgentClient(console, CFG, log)
    talker.start()
    done = False
    while not done:
        line = console.input()

        if line == "quit":
            done = True
            talker.end()
        else:
            line_a = line.split()
            cmd = line_a[0].strip()
            func = command_table[cmd]
            func(talker, line_a[1:])
        
if __name__ == '__main__':
    rc = main(sys.argv[1:])
    sys.exit(rc)


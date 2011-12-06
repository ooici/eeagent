import dashi.bootstrap as bootstrap
import logging
import socket
import sys
from threading import Thread
from dashi import DashiConnection
import threading
import signal
from dashi.bootstrap import dashi_connect
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

        self.CFG = CFG
        self.ee_name = CFG.eeagent.name
        self.pd_name = CFG.pd.name
        self.exchange = CFG.server.amqp.exchange
        self._log = log
        self._lock = threading.RLock()
        self.dashi = dashi_connect(self.pd_name, CFG)
        self.done = False
        self.console = console
        self.dashi.handle(self.heartbeat, "heartbeat")

    def heartbeat(self, message):
        self.console.write(str(message))

    def launch(self, argv):
        upid = str(uuid.uuid4()).split("-")[0]
        params = {}
        params['exec'] = argv[0]
        params['argv'] = argv[1:]
        self.dashi.fire(self.ee_name, "launch_process", u_pid=upid, round=0, run_type=EEAgentLaunchType.supd, parameters=params)

    def proc_term(self, argv):
        upid = argv[0]
        round = int(argv[1])
        self.dashi.fire(self.ee_name, "terminate_process", u_pid=upid, round=round)

    def proc_clean(self, argv):
        upid = argv[0]
        round = int(argv[1])
        self.dashi.fire(self.ee_name, "cleanup", u_pid=upid, round=round)

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

def proc_term(talker, line_a):
    talker.proc_term(line_a)

def proc_clean(talker, line_a):
    talker.proc_clean(line_a)

g_command_table = {}
g_command_table['launch'] = launch
g_command_table['terminate'] = proc_term
g_command_table['cleanup'] = proc_clean


def main(args=sys.argv[1:]):
    global thread_list

    # get config
    config_files = []
    config_files.append(args[0])
    CFG = bootstrap.configure(config_files=config_files, argv=args)
    #log = bootstrap.get_logger("eeagent", CFG)
    log = logging

 #   signal.signal(signal.SIGTERM, death_handler)
#    signal.signal(signal.SIGINT, death_handler)
  #  signal.signal(signal.SIGQUIT, death_handler)

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
            try:
                func = g_command_table[cmd]
                func(talker, line_a[1:])
            except Exception, ex:
                console.write(str(ex))
        
if __name__ == '__main__':
    rc = main(sys.argv[1:])
    sys.exit(rc)


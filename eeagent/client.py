import os
import dashi.bootstrap as bootstrap
import logging
import socket
import sys
from threading import Thread
import simplejson as json
import threading
from dashi.bootstrap import dashi_connect
import uuid
from eeagent.types import EEAgentLaunchType
from eeagent.util import determine_path

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

    def __init__(self, incoming, CFG, log=logging):
        Thread.__init__(self)

        self.CFG = CFG
        self.ee_name = CFG.eeagent.name
        self.pd_name = CFG.pd.name
        self.exchange = CFG.dashi.exchange
        self._log = log
        self.dashi = dashi_connect(self.pd_name, CFG)
        self.done = False
        self.incoming = incoming
        self.dashi.handle(self.heartbeat, "heartbeat")

    def heartbeat(self, message):
        self.incoming(json.dumps(message))

    def launch(self, argv, round=0):
        upid = str(uuid.uuid4()).split("-")[0]
        params = {}
        params['exec'] = argv[0]
        params['argv'] = argv[1:]
        self.dashi.fire(self.ee_name, "launch_process", u_pid=upid, round=round, run_type=EEAgentLaunchType.supd, parameters=params)
        return (upid, round)

    def proc_term(self, upid, round):
        self.dashi.fire(self.ee_name, "terminate_process", u_pid=upid, round=round)

    def dump(self):
        print "sending dump"
        self.dashi.fire(self.ee_name, "dump_state")

    def proc_clean(self, upid, round):
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
    talker.proc_term(line_a[0], int(line_a[1]))

def proc_clean(talker, line_a):
    talker.proc_clean(line_a[0], int(line_a[1]))

def proc_dump(talker, line_a):
    talker.dump()


g_command_table = {}
g_command_table['launch'] = launch
g_command_table['terminate'] = proc_term
g_command_table['cleanup'] = proc_clean
g_command_table['dump'] = proc_dump


def main(args=sys.argv):
    global thread_list

    # get config
    config_files = []
    c = os.path.join(determine_path(), "config", "default.yml")
    if os.path.exists(c):
        config_files.append(c)
    else:
        raise Exception("default configuration file not found")
    CFG = bootstrap.configure(config_files=config_files)
    print CFG
    #log = bootstrap.get_logger("eeagent", CFG)
    log = logging

    console = TalkConsole()
    talker = EEAgentClient(console.write, CFG, log)
    talker.start()
    done = False
    while not done:
        line = console.input()
        line = line.strip()
        if not line:
            continue
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
    rc = main()
    sys.exit(rc)


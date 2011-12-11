import logging
import socket
from threading import Thread
import simplejson as json
from dashi.bootstrap import dashi_connect
import uuid
from eeagent.types import EEAgentLaunchType


class EEAgentClient(object):

    def __init__(self, incoming, CFG, log=logging):
        self.CFG = CFG
        self.ee_name = CFG.eeagent.name
        self.pd_name = CFG.pd.name
        self.exchange = CFG.dashi.exchange
        self._log = log
        self.dashi = dashi_connect(self.pd_name, CFG)
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

    def terminate(self, upid, round):
        self.dashi.fire(self.ee_name, "terminate_process", u_pid=upid, round=round)

    def dump(self):
        self.dashi.fire(self.ee_name, "dump_state")

    def cleanup(self, upid, round):
        self.dashi.fire(self.ee_name, "cleanup", u_pid=upid, round=round)

    def poll(self, timeout=None, count=None):
        if timeout:
            count = 1
        try:
            self.dashi.consume(timeout=timeout, count=count)
        except socket.timeout, ex:
            pass 

def launch(talker, line_a):
    talker.launch(line_a)

def proc_term(talker, line_a):
    talker.terminate(line_a[0], int(line_a[1]))

def proc_clean(talker, line_a):
    talker.cleanup(line_a[0], int(line_a[1]))

def proc_dump(talker, line_a):
    talker.dump()


g_command_table = {}
g_command_table['launch'] = launch
g_command_table['terminate'] = proc_term
g_command_table['cleanup'] = proc_clean
g_command_table['dump'] = proc_dump

class EEAgentCLIMessageReaderThread(Thread):

    def __init__(self, client):
        Thread.__init__(self)
        self.done = False
        self.client = client

    def end(self):
        self.done = True

    def run(self):
        while not self.done:
            self.client.poll(timeout=2)

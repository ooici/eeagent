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

    def launch(self, params, round=0, run_type=EEAgentLaunchType.supd):
        upid = str(uuid.uuid4()).split("-")[0]
        self.dashi.fire(self.ee_name, "launch_process", u_pid=upid, round=round, run_type=run_type, parameters=params)
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
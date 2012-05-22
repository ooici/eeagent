import logging
import socket
from threading import Thread
import simplejson as json
from dashi.bootstrap import dashi_connect
import uuid
from eeagent.types import EEAgentLaunchType


class EEAgentClient(object):

    def __init__(self, incoming=None, CFG=None, dashi=None, ee_name=None,
            pd_name=None, handle_heartbeat=True, log=logging):
        self.CFG = CFG
        self.ee_name = ee_name or CFG.eeagent.name
        if dashi:
            self.dashi = dashi
        else:
            self.pd_name = pd_name or CFG.pd.name
            self.exchange = CFG.server.amqp.exchange
            self.dashi = dashi_connect(self.pd_name, CFG)
        self._log = log
        self.incoming = incoming
        if handle_heartbeat:
            self.dashi.handle(self.heartbeat, "heartbeat")

    def heartbeat(self, message):
        self.incoming(json.dumps(message))

    def launch(self, params, round=0, run_type=EEAgentLaunchType.supd):
        upid = str(uuid.uuid4()).split("-")[0]
        self.dashi.fire(self.ee_name, "launch_process", u_pid=upid, round=round, run_type=run_type, parameters=params)
        return (upid, round)

    def terminate(self, upid, round):
        self.dashi.fire(self.ee_name, "terminate_process", u_pid=upid, round=round)

    def restart(self, upid, round):
        self.dashi.fire(self.ee_name, "restart_process", u_pid=upid, round=round)

    def dump(self, rpc=True):
        if rpc:
            return self.dashi.call(self.ee_name, "dump_state", rpc=True
        else:
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

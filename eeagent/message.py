import logging
from dashi.bootstrap import dashi_connect
from eeagent.beatit import beat_it, make_beat_msg
from eeagent.core import EEAgentCore

class EEAgentMessageHandler(object):

    def __init__(self, CFG, process_managers_map, log, core_class=None):

        if core_class:
            self.core = core_class(CFG, process_managers_map, log)
        else:
            self.core = EEAgentCore(CFG, process_managers_map, log)

        self.CFG = CFG
        self._process_managers_map = process_managers_map
        self.ee_name = CFG.eeagent.name
        self.exchange = CFG.server.amqp.exchange
        self._log = log
        self.dashi = dashi_connect(self.ee_name, CFG)

        self.dashi.handle(self.launch_process, "launch_process")
        self.dashi.handle(self.terminate_process, "terminate_process")
        self.dashi.handle(self.restart_process, "restart_process")
        self.dashi.handle(self.dump_state, "dump_state")
        self.dashi.handle(self.cleanup, "cleanup")

    def dump_state(self, rpc=False):
        if rpc:
            return make_beat_msg(self._process_managers_map, self.CFG)
        else:
            beat_it(self.dashi, self.CFG, self._process_managers_map, log=self._log)

    def launch_process(self, u_pid, round, run_type, parameters):
        self.core.launch_process(u_pid, round, run_type, parameters)

    def terminate_process(self, u_pid, round):
        self.core.terminate_process(u_pid, round)

    def restart_process(self, u_pid, round):
        self.core.restart_process(u_pid, round)

    def cleanup(self, u_pid, round):
        self.core.cleanup(u_pid, round)

    def poll(self, count=None, timeout=None):
        if timeout:
            count = 1
        return self.dashi.consume(count=count, timeout=timeout)

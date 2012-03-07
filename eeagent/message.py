import logging
import threading
from dashi.bootstrap import dashi_connect
from pidantic.pidantic_exceptions import PIDanticStateException
from eeagent.beatit import beat_it
from eeagent.eeagent_exceptions import EEAgentParameterException
from eeagent.execute import PidWrapper
from eeagent.util import make_id

def eeagent_lock(func):
    def call(self, *args,**kwargs):
        with self._lock:
            return func(self, *args,**kwargs)
    return call


class EEAgentMessageHandler(object):

    def __init__(self, CFG, process_managers_map, log):
        self.CFG = CFG
        self._process_managers_map = process_managers_map
        self.pd_name = CFG.pd.name
        self.ee_name = CFG.eeagent.name
        self.exchange = CFG.server.amqp.exchange
        self._log = log
        self._lock = threading.RLock()
        self.dashi = dashi_connect(self.ee_name, CFG)

        self.dashi.handle(self.launch_process, "launch_process")
        self.dashi.handle(self.terminate_process, "terminate_process")
        self.dashi.handle(self.dump_state, "dump_state")
        self.dashi.handle(self.cleanup, "cleanup")

    @eeagent_lock
    def dump_state(self):
        beat_it(self.dashi, self.CFG, self._process_managers_map, log=self._log)

    @eeagent_lock
    def launch_process(self, u_pid, round, run_type, parameters):
        if run_type != self.CFG.eeagent.launch_type.name:
            raise EEAgentParameterException("Unknown run type %s" % (run_type))

        try:
            factory = self._process_managers_map
            factory.run(make_id(u_pid, round), parameters)
        except Exception, ex:
            self._log.exception("Error on launch %s" % (str(ex)))

    def _find_proc(self, u_pid, round):
        id = make_id(u_pid, round)
        process = self._process_managers_map.lookup_id(id)
        return process

    @eeagent_lock
    def terminate_process(self, u_pid, round):
        process = self._find_proc(u_pid, round)
        if not process:
            return
        try:
            process.terminate()
        except PIDanticStateException, pse:
            self._log.log(logging.WARN, "Attempt to terminate a process in the state %s" % (str(process.get_state())))

    @eeagent_lock
    def cleanup(self, u_pid, round):
        allowed_states = [PidWrapper.PENDING, PidWrapper.TERMINATED, PidWrapper.EXITED, PidWrapper.REJECTED, PidWrapper.FAILED]
        process = self._find_proc(u_pid, round)
        if not process:
            return
        state = process.get_state()
        if state not in allowed_states:
            self._log.log(logging.WARN, "Attempt to cleanup a process in the state %s" % (str(process.get_state())))

        try:
            process.clean_up()
        except Exception, ex:
            self._log.log(logging.WARN, "Failed to cleanup: %s" % (str(ex)))

    def poll(self, count=None, timeout=None):
        if timeout:
            count = 1
        return self.dashi.consume(count=count, timeout=timeout)

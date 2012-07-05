import threading
import logging
from eeagent.util import make_id
from eeagent.execute import PidWrapper
from eeagent.eeagent_exceptions import EEAgentParameterException
from pidantic.pidantic_exceptions import PIDanticStateException

def eeagent_lock(func):
    def call(self, *args,**kwargs):
        with self._lock:
            return func(self, *args,**kwargs)
    return call


class EEAgentCore(object):
    
    ee_name = None

    def __init__(self, CFG, process_managers_map, log):
        self.CFG = CFG
        self._process_managers_map = process_managers_map
        self.ee_name = CFG.eeagent.name

        self._log = log
        self._lock = threading.RLock()

    def _find_proc(self, u_pid, round):
        id = make_id(u_pid, round)
        process = self._process_managers_map.lookup_id(id)
        return process

    @eeagent_lock
    def launch_process(self, u_pid, round, run_type, parameters):
        self._log.debug("core launch process")
        if run_type != self.CFG.eeagent.launch_type.name:
            raise EEAgentParameterException("Unknown run type %s" % (run_type))

        try:
            factory = self._process_managers_map
            factory.run(make_id(u_pid, round), parameters)
        except Exception, ex:
            self._log.exception("Error on launch %s" % (str(ex)))

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
    def restart_process(self, u_pid, round):
        process = self._find_proc(u_pid, round)
        if not process:
            return
        try:
            print process.__class__.__name__
            process.restart()
        except PIDanticStateException, pse:
            self._log.log(logging.WARN, "Attempt to restart a process in the state %s" % (str(process.get_state())))

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
    

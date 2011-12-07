import logging
from dashi import DashiConnection
from datetime import datetime
import threading
from dashi.bootstrap import dashi_connect
from pidantic.pidantic_exceptions import PIDanticStateException
from eeagent.eeagent_exceptions import EEAgentParameterException
from eeagent.execute import PidWrapper

def eeagent_lock(func):
    def call(self, *args,**kwargs):
        with self._lock:
            return func(self, *args,**kwargs)
    return call


class EEAgentMessageHandler(object):

    def __init__(self, CFG, factory_map, log):
        self.pd_name = CFG.pd.name
        self.ee_name = CFG.eeagent.name
        self.exchange = CFG.server.amqp.exchange
        self._log = log
        self._lock = threading.RLock()
        self.dashi = dashi_connect(self.ee_name, CFG)
        self._factory_map = factory_map

        self.dashi.handle(self.launch_process, "launch_process")
        self.dashi.handle(self.terminate_process, "terminate_process")
        self.dashi.handle(self.dump_state, "dump_state")
        self.dashi.handle(self.get_error_info, "get_error_info")
        self.dashi.handle(self.cleanup, "cleanup")

    def _make_id(self, u_pid, round):
        return "%s-%s" % (u_pid, round)

    def _unmake_id(self, id):
        return id.rsplit("-", 1)

    @eeagent_lock
    def dump_state(self):
        self._beat_it()

    @eeagent_lock
    def launch_process(self, u_pid, round, run_type, parameters):

        if run_type not in self._factory_map:
            raise EEAgentParameterException("Unknown run type %s" % (run_type))

        factory = self._factory_map[run_type]
        factory.run(self._make_id(u_pid, round), parameters)

    def _find_proc(self, u_pid, round):
        id = self._make_id(u_pid, round)
        process = None
        for (k, v) in self._factory_map.iteritems():
            process = v.lookup_id(id)
            if process:
                break
        return process

    def _get_all_procs(self):
        ps = []
        for (k, v) in self._factory_map.iteritems():
            processes = v.get_all()
            if processes:
                ps = ps + processes.values()
        return ps

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
    def beat_it(self):
        self._beat_it()

    def _beat_it(self):
        try:
            d = self._get_beat_header()
            processes = []
            for p in self._get_all_procs():
                t = self._get_process_info(p)
                processes.append(t)
            d['processes'] = processes

            self.dashi.fire(self.pd_name, "heartbeat", message=d)
        except Exception, ex:
            self._log(logging.ERROR, "Heartbeat error %s" %  (str(ex)))

    @eeagent_lock
    def get_error_info(self, u_pid, round):
        process = self._find_proc(u_pid, round)
        if not process:
            return

        d = self._get_beat_header()
        d['processes'] = [self._get_process_info(process),]
        return d

    @eeagent_lock
    def cleanup(self, u_pid, round):
        allowed_states = [PidWrapper.REQUESTING, PidWrapper.TERMINATED, PidWrapper.EXITED, PidWrapper.REJECTED]
        process = self._find_proc(u_pid, round)
        if not process:
            return
        state = process.get_state()
        if state not in allowed_states:
            self._log.log(logging.WARN, "Attempt to cleanup a process in the state %s" % (str(process.get_state())))

        try:
            process.clean_up()
        except Exception, ex:
            self._log.log(logging.WARN, "AFailed to cleanup: %s" % (str(ex)))

    def _get_beat_header(self):
        d = {}
        d['ee_id'] = ""
        d['node_id'] = ""
        d['timestamp'] = str(datetime.now())
        return d

    def _get_process_info(self, process):
        (name, round) = self._unmake_id(process.get_name())
        t = (name, round, process.get_state(), process.get_error_message())
        return t
        
    def poll(self, timeout=None):
        return self.dashi.consume(self, timeout=timeout)

from pidantic.fork import ForkPidanticFactory
from pidantic.supd.pidsupd import SupDPidanticFactory
from eeagent.eeagent_exceptions import EEAgentParameterException


class PidWrapper(object):
    """
    This class wraps a pidantic pid.  The point of this class is to get an in-memory reference to the
    users launch request in the event that the pidantic object failed to run.  This minimzes lost messages
    in the event of sqldb errors, supervisord errors, or pyon errors.
    """
    REQUESTING = (100, "REQUESTING")
    RUNNING = (500, "RUNNING")
    TERMINATING = (600, "TERMINATING")
    INVALID = (900, "INVALID")
    REJECTED = (850, "REJECTED")
    FAILED = (800, "FAILED")
    EXITED = (1000, "EXITED")
    TERMINATED = (700, "TERMINATED")

    state_map = {}
    state_map["STATE_PENDING"] = REQUESTING
    state_map["STATE_STARTING"] = REQUESTING
    state_map["STATE_RUNNING"] = RUNNING
    state_map["STATE_STOPPING"] = TERMINATING
    state_map["STATE_STOPPING_RESTART"] = INVALID
    state_map["STATE_REQUEST_CANCELED"] = REJECTED
    state_map["STATE_TERMINATED"] = TERMINATED

    def __init__(self, exe, name, p=None):
        self._name = name
        self._pidantic = p
        self._exe = exe
        self._error_message = "Launch request lost on submission"


    def get_state(self):
        if not self._pidantic:
            return PidWrapper.FAILED

        state = self._pidantic.get_state()
        if state == "STATE_EXITED":
            if self._pidantic.get_result_code() != 0:
                new_state = PidWrapper.FAILED
            else:
                new_state = PidWrapper.EXITED
            # have to inspect for error
        else:
            new_state = PidWrapper.state_map[state]
        return new_state

    def get_name(self):
        return self._name

    def get_error_message(self):
        if self._pidantic:
            return self._pidantic.get_error_message()
        return self._error_message

    def set_pidantic(self, p):
        self._pidantic = p

    def set_error_message(self, msg):
        self._error_message = msg

    def terminate(self):
        if not self._pidantic:
            return
        self._pidantic.terminate()

    def clean_up(self):
        if not self._pidantic:
            return
        self._pidantic.cleanup()
        self._exe.remove_proc(self)
        
class ForkExe(object):

    def __init__(self, **kwargs):
        pass
    
class PyonExe(object):

    def __init__(self):
        pass

class SupDExe(object):

    def __init__(self, **kwargs):
        self._working_dir = kwargs['directory']
        self._eename = kwargs['name']
        supdexe = kwargs['supdexe']
        self._slots = kwargs['slots']
        self._factory = SupDPidanticFactory(directory=self._working_dir, name=self._eename, supdexe=supdexe)

        sis = self._factory.stored_instances()
        self._known_pids = {}
        for s in sis:
            name = s.get_name()
            pw = PidWrapper(self, name, s)
            self._known_pids[name] = pw

    def run(self, name, parameters):
        pw = PidWrapper(self, name)
        pid = None
        try:
            command = parameters['exec'] + " " + ' '.join(parameters['argv'])
            self._known_pids[name] = pw
            pid = self._factory.get_pidantic(command=command, process_name=name, directory=self._working_dir)
            pw.set_pidantic(pid)
            if len(self._get_running()) <= self._slots:
                pid.start()
            else:
                pid.cancel_request()
            return pw
        except Exception, ex:
            pw.set_error_message(str(ex))
            if pid:
                pid.cancel_request()

        return pw

    def remove_proc(self, proc):
        self._known_pids.pop(proc.get_name())

    def lookup_id(self, name):
        if name not in self._known_pids:
            return None
        return self._known_pids[name]

    def get_all(self):
        return self._known_pids

    def _get_running(self):
        running_states = [PidWrapper.RUNNING, PidWrapper.TERMINATING, PidWrapper.REQUESTING]
        running = [i for i in self._known_pids.values() if i.get_state() in running_states]
        return running

    def poll(self):
        return self._factory.poll()

def get_exe_factory(name, CFG):

    if name == "supd":
        factory = SupDExe(name=CFG.eeagent.name, **CFG.eeagent.launch_types.supd)
    elif name == "pyon":
        factory = PyonExe()
    elif name == "fork":
        factory = ForkExe(directory=CFG.eeagent.launch_types.fork.directory)
    else:
        raise EEAgentParameterException("%s is an unknown launch type" % (name))

    return factory

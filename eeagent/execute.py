from pidantic.supd.pidsupd import SupDPidanticFactory
from eeagent.eeagent_exceptions import EEAgentParameterException

class PyonExe(object):

    def __init__(self):
        pass

class SupDExe(object):

    def __init__(self, **kwargs):
        self._working_dir = kwargs['directory']
        self._eename = kwargs['name']
        self._factory = SupDPidanticFactory(directory=self._working_dir, name=self._eename)

        sis = self._factory.stored_instances()
        self._known_pids = {}
        for s in sis:
            self._known_pids[s.get_name()] = s

    def run(self, name, parameters):
        command = parameters['exec'] + ' '.join(parameters['argv'])
        pid = self._factory.get_pidantic(command=command, process_name=name, directory=self._working_dir)
        self._known_pids[name] = pid
        return pid

    def lookup_id(self, name):
        if name not in self._known_pids:
            return None
        return self._known_pids[name]

    def get_all(self):
        return self._known_pids

    def poll(self):
        return self._factory.poll()

def get_exe_factory(name, CFG):

    if name == "supd":
        factory = SupDExe(directory=CFG.eeagent.launch_types.supd.directory, name=CFG.eeagent.name)
    elif name == "pyon":
        factory = PyonExe()
    else:
        raise EEAgentParameterException("%s is an unknown launch type" % (lt))

    return factory

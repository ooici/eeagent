# Copyright 2013 University of Chicago

import os
import tempfile
import yaml
import logging
import simplejson as json
from subprocess import check_call, CalledProcessError
from pidantic.supd.pidsupd import SupDPidanticFactory
from eeagent.eeagent_exceptions import EEAgentParameterException
from eeagent.util import _set_param_or_default, unmake_id


class PidWrapper(object):
    """
    This class wraps a pidantic pid.  The point of this class is to get an in-memory reference to the
    users launch request in the event that the pidantic object failed to run.  This minimzes lost messages
    in the event of sqldb errors, supervisord errors, or pyon errors.
    """

    PENDING = (400, "PENDING")
    RUNNING = (500, "RUNNING")
    TERMINATING = (600, "TERMINATING")
    TERMINATED = (700, "TERMINATED")
    EXITED = (800, "EXITED")
    FAILED = (850, "FAILED")
    REJECTED = (900, "REJECTED")
    INVALID = (999, "INVALID")

    RESTARTABLE_STATES = (TERMINATED, EXITED, REJECTED,)

    state_map = {}
    state_map["STATE_INITIAL"] = PENDING
    state_map["STATE_PENDING"] = PENDING
    state_map["STATE_STARTING"] = PENDING
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

    def __repr__(self):
        return "%s: %s in state %s" % (
                self.__class__.__name__, self._name, self.get_state())

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

    @property
    def restartable(self):
        if self.get_state() in self.RESTARTABLE_STATES:
            return True
        else:
            return False

    def get_all_state(self):
        return self._pidantic.get_all_state()

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

    def restart(self):
        if not self._pidantic:
            return
        self._pidantic.restart()

    def clean_up(self):
        if not self._pidantic:
            return
        self._pidantic.cleanup()
        self._exe._remove_proc(self._name)

    def set_state_change_callback(self, cb, user_arg):
        self._pidantic.set_state_change_callback(cb, user_arg)


class PyonSinglePidWrapper(object):
    """This class is used to wrap a PidWrapper so we can get the state from
    the control_cc program, rather than just asking supervisord for that
    state.

    It should only be used in eeagent/eeagent/beatit.py
    """

    def __init__(self, pidwrapper, pyon_dir, control_cc_cache, log=logging):
        self.log = log
        self.pidwrapper = pidwrapper
        self.upid = self.pidwrapper.get_name()
        self.control_cc_cache = control_cc_cache
        self._pyon_dir = pyon_dir
        self._control_cc = os.path.join(self._pyon_dir, "bin", "control_cc")

    def get_state(self):
        state = self.pidwrapper.get_state()
        cached_control_cc_state = self.control_cc_cache.get_state(self.upid)
        if state == PidWrapper.RUNNING and cached_control_cc_state != PidWrapper.RUNNING:
            # Attempt to read the Pyon pidfile, and get state from control_cc
            all_state = self.get_all_state()
            state_for_this_proc = all_state.pop()
            pidfile = "cc-pid-%s" % state_for_this_proc["pid"]
            pidfile = os.path.join(self._pyon_dir, pidfile)
            if os.access(pidfile, os.R_OK):
                try:
                    check_call([self._control_cc, pidfile, "status"], cwd=self._pyon_dir)
                    self.log.debug("Got return code %s from control_cc for upid %s. Setting state to %s." % ('0', self.upid, str(state)))
                except CalledProcessError, error:
                    # control_cc returns 2 when a process is still starting
                    if error.returncode == 2:
                        state = PidWrapper.FAILED
                    else:
                        state = PidWrapper.PENDING
                    self.log.warning("Got return code %s from control_cc for upid %s. Setting state to %s." % (error.returncode, self.upid, str(state)))
            else:
                self.log.warning("Pidfile %s not available for pyon process %s, keeping state at %s." % (pidfile, self.upid, str(state)))
            self.control_cc_cache.set_state(self.upid, state)

        return state

    # The rest of these are passthroughs

    def get_all_state(self):
        return self.pidwrapper.get_all_state()

    def get_error_message(self):
        return self.pidwrapper.get_error_message()

    def get_name(self):
        return self.pidwrapper._name

    def set_pidantic(self, p):
        self.pidwrapper._pidantic(p)

    def set_error_message(self, msg):
        self.pidwrapper.set_error_message(msg)

    def terminate(self):
        self.pidwrapper.terminate()

    def clean_up(self):
        self.pidwrapper.cleanup()

    def set_state_change_callback(self, cb, user_arg):
        self.pidwrapper.set_state_change_callback(cb, user_arg)


class ControlCCCache(object):
    """
    This is a cache indexed by upid to mark whether a upid has gotten
    a state from control_cc
    """
    def __init__(self):
        self._has_control_cc_state = {}

    def get_state(self, upid):
        return self._has_control_cc_state.get(upid)

    def set_state(self, upid, state):
        self._has_control_cc_state[upid] = state


class PyonExe(object):

    def __init__(self, eeagent_cfg, pyon_container, log=logging):
        self.log = log
        self.log.debug("Starting PyonExe")
        self._eename = eeagent_cfg.name
        self._slots = int(eeagent_cfg.slots)
        self._working_dir = eeagent_cfg.launch_type.persistence_directory
        self._known_pws = {}

        from pidantic.pyon.pidpyon import PyonPidanticFactory
        self._factory = PyonPidanticFactory(pyon_container=pyon_container,
            name=self._eename, directory=self._working_dir, log=self.log)

        pidantic_instances = self._factory.reload_instances()
        if len(pidantic_instances.keys()) > 0:
            self.log.error("Restarting eeagent, and found dead processes: %s" % 
                    ','.join(pidantic_instances.keys()))

        for name, pidantic in pidantic_instances.iteritems():
            upid = pidantic.get_name()
            pw = PidWrapper(self, upid)
            pw.set_pidantic(pidantic)
            self._known_pws[upid] = pw
        self._state_change_cb = None
        self._state_change_cb_arg = None

    def set_state_change_callback(self, cb, user_arg):
        self._state_change_cb = cb
        self._state_change_cb_arg = user_arg

        for name in self._known_pws:
            pw = self._known_pws[name]
            pw.set_state_change_callback(self._state_change_cb, self._state_change_cb_arg)

    def run(self, name, parameters):

        pyon_params = {}

        config = yaml.dump(parameters.get('config', {}))

        pid = self._factory.get_pidantic(directory=self._working_dir,
                process_name=name,
                pyon_name=parameters.get('name'),
                module_uri=parameters.get('module_uri'),
                module=parameters.get('module'),
                cls=parameters.get('cls'),
                config=config)

        pw = PidWrapper(self, name, p=pid)
        self._known_pws[name] = pw

        if self._state_change_cb:
            pw.set_state_change_callback(self._state_change_cb, self._state_change_cb_arg)

        running_jobs = self._get_running()
        if len(running_jobs) <= self._slots:
            pid.start()
        else:
            pid.cancel_request()
        return None

    def poll(self):
        return self._factory.poll()

    def terminate(self):
        self._factory.terminate()

    def get_all(self):
        return self._known_pws

    def _remove_proc(self, proc_name):
        del self._known_pws[proc_name]

    def _get_running(self):
        running_states = [PidWrapper.RUNNING, PidWrapper.TERMINATING, PidWrapper.PENDING]
        a = self.get_all().values()
        running = [i.get_state() for i in a]

        running = [i for i in a if i.get_state() in running_states]
        return running

    def lookup_id(self, process_name, ignore_round=False):

        if ignore_round:
            process_upid, process_round = unmake_id(process_name)
            for name, proc in self._known_pws.iteritems():
                upid, round = unmake_id(name)

                if process_upid == upid:
                    return proc
            else:
                return None

        else:
            if process_name not in self._known_pws:
                return None
            return self._known_pws[process_name]


class PyonRelExe(object):

    def __init__(self, eeagent_cfg, log=logging):
        self.log = log
        self.log.debug("Starting PyonRelExe")
        self.name = eeagent_cfg.name
        mandatory_args = ['pyon_directory', 'supd_directory']

        for a in mandatory_args:
            if a not in eeagent_cfg.launch_type:
                raise EEAgentParameterException("the %s of the pyon container must be set" % (a))

        self._pyon_dir = eeagent_cfg.launch_type.pyon_directory
        self._supdexe = SupDExe(eeagent_cfg)
        self._pyon_exe = os.path.join(self._pyon_dir, "bin/pycc")

        if "container_args" in eeagent_cfg.launch_type:
            pyon_args = eeagent_cfg.launch_type.container_args
        else:
            pyon_args = ""
        self.pyon_args = pyon_args.split()

        self.control_cc_cache = ControlCCCache()
        self.tempfiles = []

    def set_state_change_callback(self, cb, user_arg):
        self._supdexe.set_state_change_callback(cb, user_arg)

    def run(self, name, parameters):
        # check parameters and massage into a supd call

        rel_file_str = "rel"
        rel_params = ["name", "module", "cls"]

        if rel_file_str not in parameters and not all(x in parameters for x in rel_params):
            raise EEAgentParameterException("a rel or name, module, class, must be in the parameters for a pyon run: %s" % parameters)
        rel_file_contents = parameters.get(rel_file_str)
        if not rel_file_contents:
            rel_file_contents = self._build_rel(parameters['name'], parameters['module'], parameters['cls'])

        prefix = "%s." % rel_file_contents.get("name", "tmp")
        rel_suffix = ".rel.yml"

        (osf, tmp_file) = tempfile.mkstemp(prefix=prefix, suffix=rel_suffix, text=True)
        os.write(osf, json.dumps(rel_file_contents))
        os.close(osf)
        self.tempfiles.append(tmp_file)

        extra_args = parameters.get("container_args", [])

        try:
            logging_cfg = parameters["logging"]
            log_suffix = ".logging.yml"
            (log_osf, log_tmp_file) = tempfile.mkstemp(prefix=prefix,
                    suffix=log_suffix, text=True)
            os.write(log_osf, json.dumps(logging_cfg))
            os.close(log_osf)
            extra_args.extend(["--logcfg", log_tmp_file])
            self.tempfiles.append(log_tmp_file)
        except KeyError:
            # No logging config to add
            pass

        try:
            pyon_cfg = parameters["config"]
            cfg_suffix = ".config.yml"
            (pyon_cfg_osf, pyon_cfg_tmp_file) = tempfile.mkstemp(prefix=prefix,
                    suffix=cfg_suffix, text=True)
            os.write(pyon_cfg_osf, json.dumps(pyon_cfg))
            os.close(pyon_cfg_osf)
            extra_args.extend(["--config", pyon_cfg_tmp_file])
            self.tempfiles.append(pyon_cfg_tmp_file)
        except KeyError:
            # No pyon config to add
            pass

        args = ["--rel", tmp_file] + self.pyon_args + extra_args

        supd_params = {
            'exec': self._pyon_exe,
            'argv': args,
            'working_directory': self._pyon_dir,
        }
        rc = self._supdexe.run(name, supd_params)
        return rc

    def _build_rel(self, name, module, cls):
        rel = {
            'type': 'release',
            'version': '0.1',
            'description': 'deploy started by eeagent',
            'ion': '0.0.1',
            'name': 'eeagent_deploy',
            'apps': [
                {
                'name': name,
                'description': 'process started by eeagent',
                'version': '0.1',
                'processapp': [name, module, cls]
                }
            ]
        }

        return rel

    def get_known_pws(self):
        return self._supdexe.get_known_pws()

    def lookup_id(self, name, ignore_round=False):
        return self._supdexe.lookup_id(name, ignore_round=ignore_round)

    def get_all(self):
        _all = self._supdexe.get_all()
        wrapped = {}
        for upid, pidwrapper in _all.iteritems():
            wrapped[upid] = PyonSinglePidWrapper(pidwrapper, self._pyon_dir,
                    self.control_cc_cache, log=self.log)
        return wrapped

    def poll(self):
        return self._supdexe.poll()

    def terminate(self):
        self._supdexe.terminate()
        for removeme in self.tempfiles:
            os.remove(removeme)


class SupDExe(object):

    def __init__(self, eeagent_cfg, log=logging):
        self.log = log
        self.log.debug("Starting SupDExe")
        self._working_dir = eeagent_cfg.launch_type.supd_directory
        self._eename = eeagent_cfg.name
        supdexe = _set_param_or_default(eeagent_cfg.launch_type, 'supdexe', None)
        self._slots = int(eeagent_cfg.slots)
        self._factory = SupDPidanticFactory(directory=self._working_dir, name=self._eename, supdexe=supdexe)
        pidantic_instances = self._factory.reload_instances()
        self._known_pws = {}
        for name in pidantic_instances:
            pidantic = pidantic_instances[name]
            pw = PidWrapper(self, name)
            pw.set_pidantic(pidantic)
            self._known_pws[name] = pw
        self._state_change_cb = None
        self._state_change_cb_arg = None

    def set_state_change_callback(self, cb, user_arg):
        self._state_change_cb = cb
        self._state_change_cb_arg = user_arg

        for name in self._known_pws:
            pw = self._known_pws['name']
            pw.set_state_change_callback(self._state_change_cb, self._state_change_cb_arg)

    def run(self, name, parameters):
        pw = PidWrapper(self, name)
        self._known_pws[name] = pw
        command = parameters['exec'] + " " + " ".join(parameters['argv'])

        dir = self._working_dir
        if "working_directory" in parameters:
            dir = parameters["working_directory"]
        pid = self._factory.get_pidantic(command=command, process_name=name, directory=dir)
        pw.set_pidantic(pid)
        if self._state_change_cb:
            pw.set_state_change_callback(self._state_change_cb, self._state_change_cb_arg)

        running_jobs = self._get_running()
        x = len(running_jobs)
        if x <= self._slots:
            pid.start()
        else:
            pid.cancel_request()
        return pw

    def get_known_pws(self):
        return self._known_pws

    def _remove_proc(self, proc_name):
        del self._known_pws[proc_name]

    def lookup_id(self, process_name, ignore_round=False):

        if ignore_round:
            process_upid, process_round = unmake_id(process_name)
            for name, proc in self._known_pws.iteritems():
                upid, round = unmake_id(name)

                if process_upid == upid:
                    return proc
            else:
                return None

        else:
            if process_name not in self._known_pws:
                return None
            return self._known_pws[process_name]

    def get_all(self):
        return self._known_pws

    def _get_running(self):
        running_states = [PidWrapper.RUNNING, PidWrapper.TERMINATING, PidWrapper.PENDING]
        a = self.get_all().values()
        running = [i.get_state() for i in a]

        running = [i for i in a if i.get_state() in running_states]
        return running

    def poll(self):
        return self._factory.poll()

    def terminate(self):
        self._factory.terminate()


def get_exe_factory(name, CFG, pyon_container=None, log=logging):

    if name == "supd":
        factory = SupDExe(CFG.eeagent, log=log)
    elif name == "pyon":
        if not pyon_container:
            msg = "You must supply a pyon container instance to 'pyon' launch_type"
            raise EEAgentParameterException(msg)
        factory = PyonExe(CFG.eeagent, pyon_container, log=log)
    elif name == "pyon_single":
        factory = PyonRelExe(CFG.eeagent, log=log)
    else:
        raise EEAgentParameterException("%s is an unknown launch type" % (name))

    return factory

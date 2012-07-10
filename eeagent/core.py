import threading
import logging

from fnmatch import fnmatch
from urlparse import urlparse

from eeagent.util import make_id
from eeagent.execute import PidWrapper
from eeagent.eeagent_exceptions import EEAgentParameterException, EEAgentUnauthorizedException
from pidantic.pidantic_exceptions import PIDanticStateException


def eeagent_lock(func):
    def call(self, *args, **kwargs):
        with self._lock:
            return func(self, *args, **kwargs)
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

        # check to see if download_code enabled
        if (parameters.get('module_uri') and
           (not self.CFG.eeagent.get('code_download') or
           not self.CFG.eeagent.code_download.get('enabled', False))):
            msg = "Code download not enabled in this eeagent"
            raise EEAgentUnauthorizedException(msg)

        # check if url in whitelist
        if parameters.get('module_uri'):
            uri = parameters.get('module_uri')
            if not self._check_whitelist(uri):
                msg = "%s not in code_download whitelist: '%s'" % (uri,
                        ", ".join(self._get_whitelist()))
                raise EEAgentUnauthorizedException(msg)

        factory = self._process_managers_map
        factory.run(make_id(u_pid, round), parameters)

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

    def _get_whitelist(self):
        try:
            whitelist = self.CFG.eeagent.code_download.whitelist
        except AttributeError:
            whitelist = []
        return whitelist

    def _check_whitelist(self, uri):
        whitelist = self._get_whitelist()

        parsed = urlparse(uri)
        if parsed.scheme == 'file':
            return True
        elif parsed.scheme.startswith('http'):
            host = parsed.hostname
            for allowed in whitelist:
                if fnmatch(host, allowed):
                    return True

            return False
        else:
            return False

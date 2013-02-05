import threading
import logging

from fnmatch import fnmatch
from urlparse import urlparse

from eeagent.util import make_id, unmake_id
from eeagent.execute import PidWrapper
from eeagent.eeagent_exceptions import EEAgentParameterException, EEAgentUnauthorizedException
from pidantic.pidantic_exceptions import PIDanticStateException

RESTARTABLE_STATES = []


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

    def _find_proc(self, u_pid, round, ignore_round=False):
        id = make_id(u_pid, round)
        process = self._process_managers_map.lookup_id(id, ignore_round=ignore_round)
        return process

    @eeagent_lock
    def launch_process(self, u_pid, round, run_type, parameters):
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

        # Check to see whether this process already exists
        process = self._find_proc(u_pid, round, ignore_round=True)
        if process is not None and not process.restartable:
            existing_upid, existing_round = unmake_id(process._name)
            state = str(process.get_state())

            if int(round) > int(existing_round):
                msg = ("VERY BAD THING: A launch request for '%s' has been "
                   "recieved, but it is in state %s. The existing process has "
                   "round %s, but the PD is asking to start process with round "
                   "%s. Restarting the process anyway, but something is wrong."
                   % (u_pid, state, existing_round, round))
                self._log.error(msg)
            else:
                msg = ("BAD THING: A launch request for '%s' has been "
                   "recieved, but it is in state %s. The existing process has "
                   "round %s, and the PD is asking to start process with round "
                   "%s. Maybe a message never arrived? Restarting now."
                   % (u_pid, state, existing_round, round))
                self._log.warning(msg)

            try:
                process.terminate()
            except PIDanticStateException, pse:
                self._log.warning("Attempt to terminate a process in the state %s" % (str(process.get_state())))
            try:
                process.clean_up()
            except Exception, ex:
                self._log.warning("Failed to cleanup: %s" % (str(ex)))

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
        """This is disabled, because there is a tricky issue. Currently, the 
        eeagent does not know how to restart a process with an incremented
        round number. There are three possible fixes:

        1. Make the PD not increment the round number. If we did this, we would
        need to make sure that the eeagent doesn't heartbeat any of these state
        changes back to the PD. This is probably a lot of trouble.

        2. Instead of doing the restart at the PIDantic level, do it right here
        with a terminate -> cleanup -> launch operation. This is tricky, because
        we need to re-extract the parameters from the running process to create
        a new process request.

        3. Modify the PIDantic restart operation to update the round number.
        This might be tricky because pidantic uses upid-round identifiers, and
        some parts of the code might react strangely to a changed ID.
        """
        self._log.warning("Restart operation disabled for now")
        return

        process = self._find_proc(u_pid, round)
        try:
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

import logging
import socket
from dashi import bootstrap
import datetime
from eeagent.execute import get_exe_factory
import os
from eeagent.eeagent_exceptions import EEAgentParameterException
import os

def _set_param_or_default(kwvals, key, default=None):
    try:
        rc = kwvals[key]
        if rc == None:
            return default
    except:
        rc = default
    return rc

def validate_supd(CFG):
    x = CFG.eeagent.launch_types.supd.directory
    if not os.path.exists(CFG.eeagent.launch_types.supd.directory):
        os.mkdir(CFG.eeagent.launch_types.supd.directory)
    x = CFG.eeagent.launch_types.supd.slots

def make_id(u_pid, round):
    return "%s-%s" % (u_pid, round)

def unmake_id(id):
    return id.rsplit("-", 1)

def timeout_poll(poll_obj, timeout):
    endtime = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    done = False

    while not done:
        try:
            poll_obj.poll(timeout=timeout, count=1)
        except socket.timeout, ex:
            pass
        now = datetime.datetime.now()
        if now > endtime:
            done = True


def validate_pyon(CFG):
    pass

def validate_fork(CFG):
    pass

g_launch_types = {"supd" : validate_supd, "pyon": validate_pyon, "fork" : validate_fork}

def validate_config(CFG):

    try:
        # make sure the amqp parameters are there
        x = CFG.server.amqp.username
        x = CFG.server.amqp.password
        x = CFG.server.amqp.host
        x = CFG.server.amqp.vhost

        # make sure a logger is there
        x = CFG.loggers.eeagent.handlers
        x = CFG.loggers.eeagent.level

        # make sure ee agent variables are there
        x = CFG.eeagent.name
        x = CFG.eeagent.heartbeat
        x = CFG.eeagent.poll_interval

        # verify the pd args
        x = CFG.pd.name

        # verify the launch type
        x = CFG.eeagent.launch_types
        if len(CFG.eeagent.launch_types) < 1:
            raise EEAgentParameterException("There should be at least 1 launch type configured")
        for t in CFG.eeagent.launch_types:
            if t not in g_launch_types:
                raise EEAgentParameterException("The launch type %s in not known" % (t))
            func = g_launch_types[t]
            func(CFG)
    except AttributeError, ex:
        raise EEAgentParameterException("parameter %s has not been set in your configuration" % ex.args[0])

def determine_path():
    """find path of current file,
    Borrowed from wxglade.py"""
    try:
        root = __file__
        if os.path.islink(root):
            root = os.path.realpath(root)
        return os.path.dirname(os.path.abspath(root))
    except:
        print "I'm sorry, but something is wrong."
        print "There is no __file__ variable. Please contact the author."
        raise

def build_cfg(args):
    config_files = []
    c = os.path.join(determine_path(), "config", "default.yml")
    if os.path.exists(c):
        config_files.append(c)
    else:
        raise Exception("default configuration file not found")

    CFG = bootstrap.configure(config_files=config_files, argv=args)
    validate_config(CFG)
    return CFG

def get_logging(CFG):
    # for now just return the default
    return logging

def get_process_managers(CFG):
    # create pidantic objects
    process_managers_map = {}
    for lt in CFG.eeagent.launch_types:
        factory = get_exe_factory(lt, CFG)
        process_managers_map[lt] = factory
    return process_managers_map

from eeagent.eeagent_exceptions import EEAgentParameterException

def validate_supd(CFG):
    x = CFG.eeagent.launch_types.supd.directory

def validate_pyon(CFG):
    pass

g_launch_types = {"supd" : validate_supd, "pyon": validate_pyon}

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
        x = CFG.pd.topic

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

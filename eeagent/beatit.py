import logging
from datetime import tzinfo, timedelta, datetime
import simplejson as json
from eeagent.util import unmake_id


def beat_it(dashi, CFG, pm, log=logging):

    try:
        beat_msg = make_beat_msg(pm, CFG)
        log.log(logging.DEBUG, "Sending the heartbeat : %s" % (json.dumps(beat_msg)))
        dashi.fire(CFG.pd.name, "heartbeat", message=beat_msg)
    except Exception, ex:
        log.exception("Error Sending the heartbeat : %s" % (str(ex)))


def make_beat_msg(pm, CFG, log=logging):
    beat_msg = {}
    beat_msg['eeagent_id'] = CFG.eeagent.name

    # include node ID if it is present in config
    node_id = CFG.eeagent.get('node_id')
    if node_id is not None:
        beat_msg['node_id'] = CFG.eeagent.node_id

    # include timestamp in UTC, in iso8601/rfc3339
    beat_msg['timestamp'] = datetime.now(utc).isoformat()

    beat_processes = []
    # we can have many process managers per eeagent, walk them all to get all the processes
    pm.poll()
    processes = pm.get_all()
    for pname in processes:
        p = processes[pname]
        (name, round) = unmake_id(p.get_name())
        try:
            state = p.get_state()
        except:
            log.exception("Had a problem getting process state")
            raise
        beat_p = {'upid': name, 'round': round, 'state': state, 'msg': p.get_error_message()}
        beat_processes.append(beat_p)
    beat_msg['processes'] = beat_processes

    return beat_msg


_ZERO = timedelta(0)

# WWWWHHHHHYYY is this not in standard library


class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return _ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return _ZERO


utc = UTC()

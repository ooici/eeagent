import logging
import datetime
import simplejson as json
from eeagent.util import unmake_id

def beat_it(dashi, CFG, pm, log=logging):

    try:
        beat_msg = make_beat_msg(pm)
        log.log(logging.DEBUG, "Sending the heartbeat : %s" % (json.dumps(beat_msg)))
        dashi.fire(CFG.pd.name, "heartbeat", message=beat_msg)
    except Exception, ex:
        log.exception("Error Sending the heartbeat : %s" % (str(ex)))

def make_beat_msg(pm):
    beat_msg = {}
    beat_msg['eeagent_id'] = ""
    beat_msg['timestamp'] = str(datetime.datetime.now())

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

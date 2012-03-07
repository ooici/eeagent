import shutil
import tempfile
import threading
import unittest
import datetime
from dashi import bootstrap
from eeagent import agent
import os
import time
import uuid
from eeagent.agent import EEAgentMain, MainRunnerThread
import simplejson as json
from eeagent.client import EEAgentClient
from eeagent.types import EEAgentLaunchType
from eeagent.util import timeout_poll, _set_param_or_default, validate_config

g_slot_count=3
g_timeout=5

def _get_cmd_args():
    global g_slot_count
    global g_timeout
    memory_name = str(uuid.uuid4()).split("-")[0]
    pdname = str(uuid.uuid4()).split("-")[0]
    eename = str(uuid.uuid4()).split("-")[0]
    tmp_dir = tempfile.mkdtemp(prefix="/tmp/supd")

    cmd_line_args = [
        "fakeexe",
        "--server.memory.name=%s" % (memory_name),
        "--eeagent.launch_type.name=pyon_single",
        "--eeagent.launch_type.container_args=--noshell",
        "--eeagent.launch_type.supd_directory=%s" % (tmp_dir),
         "--eeagent.name=%s" % (pdname),
         "--eeagent.slots=%d" % (g_slot_count),
         "--pd.name=%s" % (eename),
         "--dashi.exchange=%s" % (eename),
         "--eeagent.heartbeat=%d" % (g_timeout)
    ]
    return cmd_line_args

class PyonRelEEAgentTests(unittest.TestCase):

    pdname = str(uuid.uuid4()).split("-")[0]
    eename = str(uuid.uuid4()).split("-")[0]
    pdname = str(uuid.uuid4()).split("-")[0]
    tmp_dir = tempfile.mkdtemp()
    memory_name = str(uuid.uuid4()).split("-")[0]
    exchange_name = str(uuid.uuid4()).split("-")[0]
    timeout=5
    slot_count=3
    pyon_location_string = 'PYON_LOCATION'
    skip = pyon_location_string not in os.environ

    cmd_line_args = _get_cmd_args()
    if not skip:
        cmd_line_args.append('--eeagent.launch_type.pyon_directory=%s' % (os.environ[pyon_location_string]))
    eeagent = EEAgentMain(cmd_line_args)
    mainThread = MainRunnerThread(eeagent)

    @classmethod
    def setupClass(cls):
        print "setUpModule"
        if cls.skip:
            return

        try:
            cls.mainThread.start()
        except Exception, ex:
            pass
        time.sleep(2)

    @classmethod
    def teardownClass(cls):
        if cls.skip:
            return

        try:
            print "tearDownModule"
            cls.mainThread.end()
            cls.mainThread.join()
            shutil.rmtree(cls.tmp_dir)
        except Exception, ex:
            pass

    def setUp(self):
        if PyonRelEEAgentTests.skip:
            return
        self.beats = []
        try:
            self.client = EEAgentClient(self.heartbeat, PyonRelEEAgentTests.eeagent.get_cfg())
        except Exception, ex:
            pass

    def tearDown(self):
        if PyonRelEEAgentTests.skip:
            return
        try:
            self._clean_all_jobs()
        except  Exception, ex:
            pass
        del self.client

    def _test_for_pyon(self):
        if PyonRelEEAgentTests.skip:
            raise unittest.SkipTest()

    def heartbeat(self, msg):
        b = json.loads(msg)
        self.beats.append(b)

    def _clean_all_jobs(self):
        self.beats = []
        self.client.dump()
        self.client.poll(count=1)

        b = self.beats[-1]
        while len(b['processes']) > 0:
            self.beats = []
            for pd in b['processes']:
                if pd['state'][1] == "RUNNING":
                    self.client.terminate(pd['upid'], pd['round'])
                else:
                    self.client.cleanup(pd['upid'], pd['round'])
            self.client.dump()
            self.client.poll(count=1)
            b = self.beats[-1]
        # get the next heartbeat before leaving
        self.client.poll(count=1)

    def test_run_simple(self):
        self._test_for_pyon()

        yml_params = {"rel":{
            "apps" :
             {"type": 'application',
              "name": "hello2",
              "description": "Hello service (app version)",
              "version": "0.1",
              "mod": "examples.service.hello_service",
              'dependencies': '[]',
              "config": {"some": '"Hi"'},
             }
           }
          }
        (upid, round) = self.client.launch(yml_params, run_type=EEAgentLaunchType.pyon_single)
        self.client.dump()
        self.client.poll(count=1)

        proc = self._find_process_in_beat(upid)
        assert proc

    def _find_process_in_beat(self, upid, state=None):
        for b in self.beats:
            for pd in b['processes']:
                if pd['upid'] == upid:
                    if state is None:
                        return pd
                    elif state == pd['state'][1]:
                        return pd
        return None


if __name__ == '__main__':
    unittest.main()

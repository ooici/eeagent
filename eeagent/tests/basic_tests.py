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
        "--eeagent.launch_type.name=supd",
         "--eeagent.launch_type.supd_directory=%s" % (tmp_dir),
         "--eeagent.name=%s" % (pdname),
         "--eeagent.slots=%d" % (g_slot_count),
         "--pd.name=%s" % (eename),
         "--dashi.exchange=%s" % (eename),
         "--eeagent.heartbeat=%d" % (g_timeout),
         "--server.amqp.exchange=%s" % (eename),
    ]
    return cmd_line_args

class BasicEEAgentTests(unittest.TestCase):
    eeagent = EEAgentMain(_get_cmd_args())
    mainThread = MainRunnerThread(eeagent)

    @classmethod
    def setupClass(cls):
        print "setUpModule"
        try:
            cls.mainThread.start()
        except Exception, ex:
            pass
        time.sleep(2)

    @classmethod
    def teardownClass(cls):
        try:
            print "tearDownModule"
            cls.mainThread.end()
            cls.mainThread.join()
            shutil.rmtree(cls.tmp_dir)
        except Exception, ex:
            pass

    def setUp(self):
        self.beats = []
        try:
            self.client = EEAgentClient(self.heartbeat, BasicEEAgentTests.eeagent.get_cfg())
        except Exception, ex:
            pass

    def tearDown(self):
        try:
            self._clean_all_jobs()
        except  Exception, ex:
            pass
        del self.client

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

    def test_fill_the_slots(self):
        global g_slot_count
        jobs = []
        params = {'exec' : "/bin/sleep", 'argv': '600'}
        for i in range(0, g_slot_count):
            uid_round = self.client.launch(params)
            jobs.append(uid_round)
        timeout_poll(self.client, 3)
        self.client.dump()
        self.client.poll(count=1)
        for j in jobs:
            pd = self._find_process_in_beat(j[0], "RUNNING")
            self.assertTrue(pd is not None)

    def test_over_fill_the_slots(self):
        global g_slot_count
        jobs = []
        params = {'exec' : "/bin/sleep", 'argv': '600'}
        for i in range(0, g_slot_count):
            uid_round = self.client.launch(params)
            jobs.append(uid_round)
        uid_round = self.client.launch(params)
        timeout_poll(self.client, 5)
        self.client.dump()
        self.client.poll(count=1)
        print self.beats[-1]
        bl = len(self.beats[-1]['processes'])
        self.assertTrue(bl == g_slot_count + 1)
        for j in jobs:
            pd = self._find_process_in_beat(j[0], "RUNNING")
            self.assertTrue(pd is not None)
        # make sure the over flow one reported in
        pd = self._find_process_in_beat(uid_round[0])
        self.assertTrue(pd is not None)
        # make sure it reported in the right state
        pd = self._find_process_in_beat(uid_round[0], "REJECTED")
        self.assertTrue(pd is not None)

    def test_not_a_command(self):
        params = {'exec' : "/not/A/Command", 'argv' : "1"}
        (upid, round) = self.client.launch(params)
        self.client.poll(timeout=1)
        self.client.dump()
        self.client.poll(count=1)
        pd = self._find_process_in_beat(upid, "FAILED")
        self.assertTrue(pd is not None)

    def test_complete_command(self):
        params = {'exec' : "/bin/sleep", 'argv' : "1"}
        (upid, round) = self.client.launch(params)
        # poll longer than the exe to get the exit code
        timeout_poll(self.client, 3)
        self.client.dump()
        # poll for one message so we get the result of dump of the next heart beat
        self.client.poll(count=1)
        pd = self._find_process_in_beat(upid, "EXITED")
        self.assertTrue(pd is not None)

    def test_launch_beat_terminate_cleanup(self):
        params = {'exec' : "/bin/sleep", 'argv' : "600"}
        (upid, round) = self.client.launch(params)
        # give it time to start
        timeout_poll(self.client, 3)
        self.client.dump()
        self.client.poll(count=1)
        pd = self._find_process_in_beat(upid, "RUNNING")
        self.assertTrue(pd is not None, "launch did not hit the running stage")
        self.client.dump()
        self.client.poll(count=1)
        pd = self._find_process_in_beat(upid)
        self.assertTrue(pd is not None, "upid should have been found in the heart beat")
        self.client.terminate(upid, 0)
        self.client.dump()
        self.client.poll(count=1)
        pd = None
        count = 0
        while not pd:
            self.client.dump()
            self.client.poll(count=1)
            pd = self._find_process_in_beat(upid, "TERMINATED")
            self.assertTrue(count < 5, "We should have gotten the terminated message in less that 5 beats")
            count = count + 1
        self.assertTrue(pd is not None, "The terminated state was not found")

        self.client.cleanup(upid, 0)
        self.client.dump()
        self.client.poll(count=1)
        # clear one out and give it a beat
        self.beats = []
        self.client.dump()
        self.client.poll(count=1)
        pd = self._find_process_in_beat(upid)
        self.assertTrue(pd is None)

    def test_dump_time(self):
        self.client.dump()
        self.client.poll(timeout=0.5)
        self.assertTrue(len(self.beats) > 0)
        self.assertTrue('timestamp' in self.beats[0])
        self.assertTrue('processes' in self.beats[0])

    def test_dump(self):
        self.client.dump()
        self.client.poll(count=1)
        self.assertTrue(len(self.beats) > 0)
        self.assertTrue('timestamp' in self.beats[0])
        self.assertTrue('processes' in self.beats[0])

    def test_many_dumps(self):
        count = 4
        for i in range(0, count):
            self.client.dump()
        self.client.poll(count=count)
        self.assertTrue(len(self.beats) == count, "beats should be %d long is %d" % (count, len(self.beats)))
        
    def test_heartbeat_ever(self):
        self.client.poll(count=1)
        self.assertTrue(len(self.beats) > 0, "There should be heart beat messages %s" % (str(self.beats)))

    def test_heartbeat_time(self):
        global g_timeout
        count = 1
        start = datetime.datetime.now()
        self.client.poll(count=1)
        end = datetime.datetime.now()
        if datetime.timedelta(seconds=g_timeout*2) < end - start:
            self.fail("The heartbeat took too long")
        self.assertTrue(len(self.beats) >= count, "beats should be %d long is %d" % (count, len(self.beats)))

    def test_heartbeat_time_poll(self):
        global g_timeout
        count = 1
        timeout_poll(self.client, g_timeout*2)
        self.assertTrue(len(self.beats) >= count, "beats should be %d long is %d" % (count, len(self.beats)))

    def test_complete_command(self):
        params = {'exec' : "/bin/sleep", 'argv' : "1"}
        (upid, round) = self.client.launch(params)
        self.client.poll(timeout=3)
        self.client.dump()
        self.client.poll(count=1)
        pd = self._find_process_in_beat(upid, "EXITED")
        self.assertTrue(pd is not None)

    def test_basic_util(self):
        kwvals = {}
        key = 'x'
        dv = 'y'
        kwvals[key] = None
        rc = _set_param_or_default(kwvals, key, default=dv)
        self.assertEqual(dv, rc)
        rc = _set_param_or_default(kwvals, 'somethingelse', default=dv)
        self.assertEqual(dv, rc)

    def test_call_agent_main(self):

        class TestMainRunnerThread(threading.Thread):

            def __init__(self):
                threading.Thread.__init__(self)

            def run(self):
                agent.main(_get_cmd_args())

        t = TestMainRunnerThread()
        t.start()
        time.sleep(1)
        agent.death_handler(0, None)
        t.join()


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

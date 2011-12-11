import sys

class TalkConsole(object):

    def __init__(self):
        self._prompt = ">> "

    def write(self, msg):
        sys.stdout.write("\r%s" % (msg))
        sys.stdout.write("\n" + self._prompt)
        sys.stdout.flush()

    def input(self):
        line = raw_input(self._prompt)
        return line.strip()


def launch(talker, line_a):
    talker.launch(line_a)

def proc_term(talker, line_a):
    talker.terminate(line_a[0], int(line_a[1]))

def proc_clean(talker, line_a):
    talker.cleanup(line_a[0], int(line_a[1]))

def proc_dump(talker, line_a):
    talker.dump()


g_command_table = {}
g_command_table['launch'] = launch
g_command_table['terminate'] = proc_term
g_command_table['cleanup'] = proc_clean
g_command_table['dump'] = proc_dump

class EEAgentClientMain(object):

    def __init__(self, args):
        self._args = args

    def start(self):
        self.CFG = build_cfg(self._args)
        self.log = get_logging(self.CFG)
        self._done = False
        self.console = TalkConsole()
        self.talker = EEAgentClient(self.console.write, self.CFG, self.log)
        self.client_thread = EEAgentCLIMessageReaderThread(self.talker)

    def wait(self):
        self.client_thread.start()
        self._done = False
        while not self._done:
            line = self.console.input()
            line = line.strip()
            if not line:
                continue
            if line == "quit":
                self._done = True
            else:
                line_a = line.split()
                cmd = line_a[0].strip()
                try:
                    func = g_command_table[cmd]
                    func(self.talker, line_a[1:])
                except Exception, ex:
                    self.console.write(str(ex))
        self.client_thread.end()
        return 0

    def death_handler(self, signum, frame):
        self.end()

    def end(self):
        self._done = True


class EEAgentCLIMessageReaderThread(Thread):

    def __init__(self, client):
        Thread.__init__(self)
        self.done = False
        self.client = client

    def end(self):
        self.done = True

    def run(self):
        while not self.done:
            self.client.poll(timeout=2)


def main(args=sys.argv):
    client = EEAgentClientMain(args)
    client.start()
    return client.wait()

if __name__ == '__main__':
    rc = main()
    sys.exit(rc)


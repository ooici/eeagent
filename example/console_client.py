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


def main(args=sys.argv):
    client = EEAgentClientMain(args)
    client.start()
    return client.wait()

if __name__ == '__main__':
    rc = main()
    sys.exit(rc)


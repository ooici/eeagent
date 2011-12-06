import os

class EEAgentLaunchType:
    pyon = "pyon"
    fork = "fork"

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

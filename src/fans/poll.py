import time
import threading


def until(pred, interval = 0.01, timeout = 0):
    beg = time.time()
    while not pred():
        time.sleep(interval)
        if timeout and time.time() - beg >= timeout:
            return False
    return True


def threaded(func):
    thread = threading.Thread(target = func)
    thread.start()
    return thread

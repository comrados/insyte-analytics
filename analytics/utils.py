import datetime
import threading


def string_to_datetime(string):
    return datetime.datetime.strptime(string, "%Y-%m-%d_%H:%M:%S%z")


def string_to_date(string):
    return datetime.datetime.strptime(string, "%Y-%m-%d").date()


def string_to_time(string):
    return datetime.datetime.strptime(string, "%H:%M:%S").time()


def thread_label():
    thread = threading.current_thread()
    return thread.name + ' (' + str(thread.ident) + ') '


def log_msg(msg):
    return thread_label() + msg


def utctime():
    return str(datetime.datetime.utcnow())

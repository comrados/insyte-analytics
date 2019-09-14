import datetime


def string_to_datetime(string):
    return datetime.datetime.strptime(string, "%Y-%m-%d_%H:%M:%S%z")


def string_to_date(string):
    return datetime.datetime.strptime(string, "%Y-%m-%d").date()


def string_to_time(string):
    return datetime.datetime.strptime(string, "%H:%M:%S%z").time()

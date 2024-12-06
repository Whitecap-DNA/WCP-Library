from datetime import datetime

import pytz


def get_current_time(aware: bool=False, tz: str='Canada/Mountain') -> datetime:
    """
    Get the current time (Mountain Time)

    you can find a list of timezones by printing pytz.all_timezones

    :param aware:
    :param tz:
    :return:
    """

    tz = pytz.timezone(tz)
    current_time = datetime.now(tz)

    if not aware:
        return current_time.replace(tzinfo=None)
    return current_time


def convert_tz(time: datetime, original_tz: str, aware: bool=False, tz: str='Canada/Mountain') -> datetime:
    """
    Convert time to a different timezone

    you can find a list of timezones by printing pytz.all_timezones

    :param time:
    :param original_tz:
    :param aware:
    :param tz:
    :return:
    """

    time = time.replace(tzinfo=pytz.timezone(original_tz))
    converted_time = time.astimezone(pytz.timezone(tz))
    return converted_time if aware else converted_time.replace(tzinfo=None)

def get_utc_timestamp(time: datetime, original_tz: str='Canada/Mountain') -> int:
    """
    Get the UTC timestamp of a datetime object

    you can find a list of timezones by printing pytz.all_timezones

    :param time:
    :param original_tz:
    :return:
    """

    converted_time = convert_tz(time, original_tz, aware=False, tz='UTC')
    return int(converted_time.timestamp())

def get_local_timestamp(time: datetime, original_tz: str='Canada/Mountain') -> int:
    """
    Get the local timestamp of a datetime object

    you can find a list of timezones by printing pytz.all_timezones

    :param time:
    :param original_tz:
    :return:
    """

    converted_time = convert_tz(time, original_tz, aware=False)
    return int(converted_time.timestamp())

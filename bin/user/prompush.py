#!/usr/bin/env python

"""
reference weewx record format:

{
  'daily_rain': 0.0,
  'wind_average': 3.5007240370967794,
  'outHumidity': 83.62903225806451,
  'heatindex': 61.59999999999994,
  'day_of_year': 36.0,
  'inTemp': 61.59999999999994,
  'windGustDir': 200.470488,
  'barometer': 30.238869061178168,
  'windchill': 61.59999999999994,
  'dewpoint': 56.59074077611711,
  'rain': 0.0,
  'pressure': 30.076167509542763,
  'long_term_rain': 1.900000000000002,
  'minute_of_day': 1348.0,
  'altimeter': 30.230564691725238,
  'usUnits': 1,
  'interval': 5,
  'dateTime': 1417218600.0,
  'windDir': 187.7616636785259,
  'outTemp': 61.59999999999994,
  'windSpeed': 3.804394058064512,
  'inHumidity': 83.62903225806451,
  'windGust': 6.21371
}

"""

__version__ = '0.1.0'

import weewx
import weewx.restx
import weeutil.weeutil

import Queue
import sys
import syslog


class PromPush(weewx.restx.StdRESTful):
    """
    sends weew records to the prometheus push gw using the prometheus_client
    library
    """

    def __init__(self, engine, config_dict):
        super(PromPush, self).__init__(engine, config_dict)
        try:
            _prom_dict = weeutil.weeutil.accumulateLeaves(
                config_dict['StdRESTful']['PromPush'], max_level=1)
        except KeyError as e:
            logerr("config error: missing parameter %s" % e)
            return

        _manager_dict = weewx.manager.get_manager_dict(
            config_dict['DataBindings'], config_dict['Databases'], 'wx_binding')

        self.archive_queue = Queue.Queue()
        self.archive_thread = PromPushThread(self.archive_queue, _manager_dict,
                                             **_prom_dict)

        self.archive_thread.start()
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

        loginfo("data will be sent to pushgateway at %s:%s" %
                (_prom_dict['host'], _prom_dict['port']))

    def new_archive_record(self, event):
        self.archive_queue.put(event.record)


class PromPushThread(weewx.restx.RESTThread):
    """
    thread for sending data to the configured prometheus pushgateway
    """

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = '9091'
    DEFAULT_JOB = 'weewx'
    DEFAULT_INSTANCE = ''
    DEFAULT_PUSH_INTERVAL = 300
    DEFAULT_TIMEOUT = 10
    DEFAULT_MAX_TRIES = 3
    DEFAULT_RETRY_WAIT = 5

    def __init__(self, queue, manager_dict,
                 host=DEFAULT_HOST,
                 port=DEFAULT_PORT,
                 job=DEFAULT_JOB,
                 instance=DEFAULT_JOB,
                 skip_post=False,
                 max_backlog=sys.maxint,
                 stale=60,
                 log_success=True,
                 log_failure=True,
                 timeout=DEFAULT_TIMEOUT,
                 max_tries=DEFAULT_MAX_TRIES,
                 retry_wait=DEFAULT_RETRY_WAIT):


        super(PromPushThread, self).__init__(
            queue,
            protocol_name='PromPush',
            manager_dict=manager_dict,
            max_backlog=max_backlog,
            stale=stale,
            log_success=log_success,
            log_failure=log_failure,
            timeout=timeout,
            max_tries=max_tries,
            retry_wait=retry_wait
        )

        self.host = host
        self.port = port
        self.job = job
        self.instance = instance
        self.skip_post = weeutil.weeutil.to_bool(skip_post)

    def post_metric(self, name, value, timestamp):
        # TODO: handle instance packing
        loginfo( "%s{%s=%s}" % (self.job, name, value))


    def process_record(self, record, dbmanager):
        _ = dbmanager

        if self.skip_post:
            loginfo("-- prompush: skipping post")

        else:
            for key, val in record.iteritems():
                self.post_metric(key, val, record['dateTime'])


#---------------------------------------------------------------------
# misc. logging functions
def logmsg(level, msg):
    syslog.syslog(level, 'prom-push: %s' % msg)

def logdbg(msg):
    logmsg(syslog.LOG_DEBUG, msg)

def loginfo(msg):
    logmsg(syslog.LOG_INFO, msg)

def logerr(msg):
    logmsg(syslog.LOG_ERR, msg)

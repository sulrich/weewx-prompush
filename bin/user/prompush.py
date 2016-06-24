#!/usr/bin/env python

"""
sample output from simulator

key          value               type
----------------------------------------
outHumidity  79.9980573766       gauge
maxSolarRad  960.080999341
altimeter    32.0845040681       guage
heatindex    32.4567414016       gauge
radiation    748.170598504       gauge
inDewpoint   31.0785251193       gauge
inTemp       63.0012950398       gauge
barometer    31.0999352459       gauge
windchill    32.4567414016
dewpoint     26.9867627099       gauge
windrun      1.20018113179e-05
rain         0.0                 gauge
humidex      32.4567414016       gauge
pressure     31.0999352459       gauge
ET           0.480818085118
rainRate     0.0                 gauge
usUnits      1
appTemp      28.2115054547       gauge
UV           10.4743883791       gauge
dateTime     1466708460.0
windDir      359.988202072       gauge
outTemp      32.4567414016       gauge
windSpeed    0.00032377056758    gauge
inHumidity   29.9974099203       gauge
windGust     0.0004618843668     gauge
windGustDir  359.986143469       gauge
cloudbase    2122.17697538

"""

weather_metrics = {
    'outHumidity':  'gauge',
    'maxSolarRad':  'gauge',
    'altimeter':    'gauge',
    'heatindex':    'gauge',
    'radiation':    'gauge',
    'inDewpoint':   'gauge',
    'inTemp':       'gauge',
    'barometer':    'gauge',
    'windchill':    'gauge',
    'dewpoint':     'gauge',
    # 'windrun':
    'rain':         'gauge',
    'humidex':      'gauge',
    'pressure':     'gauge',
    # ET':
    'rainRate':     'gauge',
    # 'usUnits':
    'appTemp':      'gauge',
    'UV':           'gauge',
    # dateTime
    'windDir':      'gauge',
    'outTemp':      'gauge',
    'windSpeed':    'gauge',
    'inHumidity':   'gauge',
    'windGust':     'gauge',
    'windGustDir':  'gauge',
    'cloudbase':    'gauge'
}

__version__ = '0.1.0'

import weewx
import weewx.restx
import weeutil.weeutil

import requests

import Queue
import sys
import syslog

class PromPush(weewx.restx.StdRESTful):
    """

    sends weewx weather records to a prometheus pushgateway using the
    prometheus_client library

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
    DEFAULT_TIMEOUT = 10
    DEFAULT_MAX_TRIES = 3
    DEFAULT_RETRY_WAIT = 5

    def __init__(self, queue, manager_dict,
                 host=DEFAULT_HOST,
                 port=DEFAULT_PORT,
                 job=DEFAULT_JOB,
                 instance=DEFAULT_INSTANCE,
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

    def post_metrics(self, data):
        # post the weather stats to the prometheus push gw
        pushgw_url = 'http://' + self.host + ":" + self.port + "/metrics/job/" + self.job

        if self.instance is not "":
            pushgw_url += "/instance/" + self.instance

        res = requests.post(url=pushgw_url,
                            data=data,
                            headers={'Content-Type': 'application/octet-stream'})

        loginfo("prompush: post return code - %s" % res.status_code)


    def process_record(self, record, dbm):
        _ = dbm

        record_data = ''

        if self.skip_post:
            loginfo("-- prompush: skipping post")
        else:
            for key, val in record.iteritems():
                if weather_metrics.get(key):
                    # annotate the submission with the appropriate metric type
                    record_data += "# TYPE %s %s\n" % (str(key), weather_metrics[key])

                record_data += "%s %s\n" % (str(key), str(val))

        self.post_metrics(record_data)


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

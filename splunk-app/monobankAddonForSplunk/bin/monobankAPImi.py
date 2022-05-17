#!/usr/bin/env python
""" Splunk Modular Input for streaming transactions from
    Monobank API
"""

import sys
import os
import logging
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse
from urllib.error import HTTPError
import requests
import pytz
from splunklib import modularinput
from pythonjsonlogger import jsonlogger
from myutils import splunkutils


SPLUNK_MI_NAME = 'Costs Monobank API'
SPLUNK_MI_DESC = 'Streams events from Monobank'
if 'SPLUNK_HOME' in os.environ:
    SPLUNK_HOME_DIR = os.path.expandvars('$SPLUNK_HOME')
    log_subdirs = ['var', 'log']
    log_dir = os.path.join(SPLUNK_HOME_DIR)
    for log_subdir in log_subdirs:
        log_dir = os.path.join(log_dir, log_subdir)
    LOG_DIR = log_dir
else:
    log_dir = os.path.dirname(os.path.realpath(__file__))
    LOG_DIR = os.path.expandvars(log_dir)
INIT_DATE_FMT = '%Y-%m-%d'
TZ = pytz.timezone('Europe/Kiev') # since this is bank from Ukraine
REST_URI = 'https://api.monobank.ua/personal/statement/'


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """ Custom JSON logging """
    input_name = None

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        if not log_record.get('timestamp'):
            # this doesn't use record.created, so it is slightly off
            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['timestamp'] = now
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname
        # add custom static field in log message
        log_record['input_name'] = self.input_name
        log_record['function'] = str(sys._getframe(10).f_code.co_name)


class CostsModularInput(modularinput.Script):
    """ Modular input Script class for reading costs source data """

    def __init__(self):
        super().__init__()
        self.splunk_query = None
        self.splunk_args = None
        self.index = None
        self.source = None
        self.sourcetype = None
        self.mgmt_endpoint = None
        self.session_key = None

    def _set_params(self):
        self.splunk_query = '| tstats latest(_time) as timestamp where index=' + self.index + \
                ' sourcetype=' + self.sourcetype + \
                ' source=' + self.source + \
                '| append [| makeresults ] ' \
                '| eval timestamp = if(isnotnull(timestamp), timestamp, 0) ' \
                '| stats max(timestamp) as timestamp'
        self.splunk_args = {
            'mgmt_endpoint': self.mgmt_endpoint,
            'session_key': self.session_key
        }

    def _final_date(self):
        """ Do not get today's transactions """
        now_date = TZ.localize(datetime.now())
        final_date = TZ.localize(
            datetime.combine((now_date).date(), datetime.min.time())) - \
                    timedelta(seconds=1)
        return final_date

    def monobank(self, init_date, card_id, token):
        """ get Monobank transactions """
        src_sdate = init_date.split('-')
        init_datetime = datetime(int(src_sdate[0]),
                           int(src_sdate[1]), int(src_sdate[2]))
        tz_to_datetime = self._final_date()
        self._set_params()
        splunk_utils = splunkutils.ModularInput()
        splunk = splunkutils.Splunk()
        log.debug('Splunk checkpoint query', extra={'splunk_query': str(self.splunk_query)})
        splunk_latest_dt = splunk_utils.get_init_datetime(splunk, self.splunk_query,
                                                          self.splunk_args, init_datetime)
        tz_from_datetime = pytz.utc.localize(splunk_latest_dt)
        log.info('Init date: %s', str(tz_from_datetime))
        if tz_from_datetime <= tz_to_datetime:
            log.info('Getting events in time range: %s', str(tz_from_datetime) \
                 + ' - %s', str(tz_to_datetime))
        else:
            log.info('Not grabbing events today')
            return
        rest_from_timestamp = int((splunk_latest_dt - datetime(1970, 1, 1))
                                .total_seconds())
        rest_to_timestamp = int((tz_to_datetime - pytz.utc.localize(datetime(1970, 1, 1)))
                                .total_seconds())
        try:
            mono = requests.get(REST_URI +
                                str(card_id) + '/' + str(rest_from_timestamp) + '/' +
                                str(rest_to_timestamp),
                                headers={"X-Token":token})
        except HTTPError:
            log.exception('Non 2xx HTTP response code received')
        log.debug('Data received', extra={'data': str(mono.content)})
        for item in json.loads(mono.text):
            yield json.dumps(item)

    def get_scheme(self):
        """Creates modular input scheme.
        Returns:
          modularinput.Scheme object.
        """
        scheme = modularinput.Scheme(SPLUNK_MI_NAME)
        scheme.description = SPLUNK_MI_DESC
        scheme.use_external_validation = True

        card_id = modularinput.Argument('card_id')
        card_id.data_type = modularinput.Argument.data_type_number
        card_id.description = 'Monobank Card ID'
        card_id.required_on_create = True
        scheme.add_argument(card_id)

        token = modularinput.Argument('token')
        token.data_type = modularinput.Argument.data_type_string
        token.description = 'Monobank token'
        token.required_on_create = True
        scheme.add_argument(token)

        init_date = modularinput.Argument('init_date')
        init_date.data_type = modularinput.Argument.data_type_string
        init_date.description = 'Initial date for getting transactions list. Format: YYYY-MM-DD'
        init_date.required_on_create = True
        scheme.add_argument(init_date)

        log_level = modularinput.Argument('log_level')
        log_level.data_type = modularinput.Argument.data_type_string
        log_level.description = 'Log level (DEBUG|INFO)'
        log_level.required_on_create = True
        scheme.add_argument(log_level)

        return scheme

    def validate_input(self, validation_definition):
        """Checks user-provided params for the modular input.
        Raises:
          OSError if monitor_dir_name does not exist or is unreadable.
        """
        init_date = str(validation_definition.parameters['init_date'])
        try:
            datetime.strptime(init_date, INIT_DATE_FMT)
        except ValueError:
            log.exception('Incorrect date format, should be YYYY-MM-DD')
        log_level = str(validation_definition.parameters['log_level'])
        if log_level not in ('INFO', 'DEBUG'):
            log.exception('Incorrect log level format, should be INFO|DEBUG')

    def stream_events(self, inputs, event_writer):
        """Writes event objects to event_writer."""
        try:
            for input_name, input_item in inputs.inputs.items():
                CustomJsonFormatter.input_name = input_name
                log.info('Initializing modular input')
                log.setLevel(input_item['log_level'])
                if input_item['index'] == 'default':
                    input_item['index'] = 'main'
                else:
                    self.index = input_item['index']
                self.source = input_name
                self.sourcetype = input_item['sourcetype']
                self.session_key = self._input_definition.metadata['session_key']
                self.mgmt_endpoint = urlparse(
                    self._input_definition.metadata['server_uri'])
                event_count = 0
                for item in self.monobank(
                        input_item['init_date'], input_item['card_id'],
                        input_item['token']):
                    splunk_event = modularinput.Event(
                        data=item,
                        index=input_item['index'],
                        sourcetype=input_item['sourcetype']
                    )
                    event_writer.write_event(splunk_event)
                    event_count += 1
                log.info('Ingestion to Splunk complete', extra={'event_count': event_count})
        except Exception as exception:
            log.exception(exception)
            raise


if __name__ == '__main__':
    # set logger
    formatter = CustomJsonFormatter('%(timestamp)s %(level)s %(message)s')
    log = logging.getLogger()
    log_file_path = os.path.join(LOG_DIR, os.path.splitext(
        os.path.basename(__file__))[0]) + '.log.json'
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
    log.setLevel(logging.INFO)

    try:
        sys.exit(CostsModularInput().run(sys.argv))
    except Exception as exception:
        log.exception(str(exception))

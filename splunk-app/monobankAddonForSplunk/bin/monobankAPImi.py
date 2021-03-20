#!/usr/bin/env python
""" Splunk Modular Input for streaming transactions from
    Monobank API
"""

import sys
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse
from urllib.error import HTTPError
import requests
import pytz
from splunklib import modularinput
from myutils import splunkutils, mylogger


class CostsModularInput(modularinput.Script):
    """ Modular input Script class for reading costs source data """
    tz = pytz.timezone('Europe/Kiev') # since this is bank from Ukraine
    rest_endpoint = 'https://api.monobank.ua/personal/statement/'
    init_date_fmt = '%Y-%m-%d'
    fmt = '%Y-%m-%d %H:%M:%S'

    def _set_params(self):
        self.splunk_query = '| tstats latest(_time) as timestamp where index=' + str(self.index) + \
                ' sourcetype=' + self.source_type + \
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
        now_date = self.tz.localize(datetime.now())
        final_date = self.tz.localize(
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
        log.debug(str(self.splunk_query))
        splunk_latest_dt = splunk_utils.get_init_datetime(splunk, self.splunk_query,
                                                          self.splunk_args, init_datetime)
        tz_from_datetime = pytz.utc.localize(splunk_latest_dt)
        log.info('Init date: ' + str(tz_from_datetime))
        if tz_from_datetime <= tz_to_datetime:
            log.info('Getting events ' + str(tz_from_datetime) + ' - ' + str(tz_to_datetime))
        else:
            log.info('Not grabbing events today')
            return
        rest_from_timestamp = int((splunk_latest_dt - datetime(1970, 1, 1))
                                .total_seconds())
        rest_to_timestamp = int((tz_to_datetime - pytz.utc.localize(datetime(1970, 1, 1)))
                                .total_seconds())
        try:
            mono = requests.get(self.rest_endpoint +
                                str(card_id) + '/' + str(rest_from_timestamp) + '/' +
                                str(rest_to_timestamp),
                                headers={"X-Token":token})
        except HTTPError:
            raise Exception('Non 2xx HTTP response code: ' + str(HTTPError))
        log.debug(str(mono.content))
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

    def _validate_date(self, date_text):
        try:
            datetime.strptime(date_text, self.init_date_fmt)
        except ValueError:
            raise ValueError('Incorrect date format, should be YYYY-MM-DD')

    def _validate_log_level(self, log_level):
        if not log_level in ('INFO', 'DEBUG'):
            raise ValueError('Incorrect log level format, should be INFO|DEBUG')

    def validate_input(self, validation_definition):
        """Checks user-provided params for the modular input.
        Raises:
          OSError if monitor_dir_name does not exist or is unreadable.
        """
        init_date = str(validation_definition.parameters['init_date'])
        self._validate_date(init_date)
        log_level = str(validation_definition.parameters['log_level'])
        self._validate_log_level(log_level)

    def stream_events(self, inputs, event_writer):
        """Writes event objects to event_writer."""
        try:
            for input_name, input_item in inputs.inputs.items():
                log.source = input_name
                log.setLevel(input_item['log_level'])
                if input_item['index'] == 'default':
                    self.index = 'main'
                else:
                    self.index = input_item['index']
                self.source_type = input_item['sourcetype']
                self.session_key = self._input_definition.metadata['session_key']
                self.mgmt_endpoint = urlparse(
                    self._input_definition.metadata['server_uri'])
                log.info('Initializing ' + input_name)
                event_count = 0
                self.source = input_name
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
                log.info('Total events ingested: ' + str(event_count))
        except Exception as exception:
            log.exception(exception)
            raise


if __name__ == '__main__':
    log = mylogger.Logger(level='INFO')
    SPLUNK_MI_NAME = 'Costs Monobank API'
    SPLUNK_MI_DESC = 'Streams events from Monobank'
    try:
        sys.exit(CostsModularInput().run(sys.argv))
    except Exception as exception:
        log.exception(str(exception))

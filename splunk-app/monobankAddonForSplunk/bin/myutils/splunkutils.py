import logging
from datetime import datetime, timedelta
import splunklib.client
import splunklib.results
from splunklib import modularinput
import os
import json


class Log:
    ew = None

    def __init__(self):
        pass

    def log(self, level, message):
        message = str(message)
        levels = {
            'DEBUG': 10,
            'INFO': 20,
            'WARN': 30,
            'ERROR': 40,
            'CRITICAL': 50
        }
        if self.ew is not None:
            self.ew.log(level, message)
        else:
            logging.log(levels[level], message)

    def debug(self, message=None):
        level = 'DEBUG'
        self.log(level, message)

    def info(self, message=None):
        level = 'INFO'
        self.log(level, message)

    def warn(self, message=None):
        level = 'WARN'
        self.log(level, message)

    def error(self, message=None):
        level = 'ERROR'
        self.log(level, message)


class Splunk:
    ew = None
    level = 'INFO'
    log_file = None
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(message)s')

    def __init__(self):
        pass

    def connect(self, mgmt_endpoint=None, scheme='https', host='localhost', port='8089',
                session_key=None, username='admin', password=None, app='search', owner='nobody'):
        """connects to splunk instance"""
        if mgmt_endpoint is not None:
            args = {
                'scheme': mgmt_endpoint.scheme,
                'host': mgmt_endpoint.hostname,
                'port': mgmt_endpoint.port,
                'token': session_key,
                'app': app,
                'owner': owner
            }
        else:
            args = {
                'scheme': scheme,
                'host': host,
                'port': port,
                'username': username,
                'password': password,
                'app': app,
                'owner': owner
            }
        service = splunklib.client.connect(**args)
        assert isinstance(service, splunklib.client.Service)
        return service

    def search(self, service, query, earliest=0, latest='now'):
        """performs oneshot search"""
        kwargs_search = {'count': 0,
                         'earliest_time': earliest,
                         'latest_time': latest,
                         'exec_mode': 'blocking'
                         }
        kwargs_options = {'count': '0'}
        job = service.jobs.create(query, **kwargs_search)
        results = splunklib.results.ResultsReader(job.results(**kwargs_options))

        return results


class ModularInput:

    def __init__(self):
        self.ew = None

    def create_dirs(self, work_dir, checkpoint=True):
        """
        Creates working directories
        :param work_dir:
        :return: created dirs paths
        """
        inbound_dir = os.path.join(work_dir, 'inbound')
        outbound_dir = os.path.join(work_dir, 'outbound')
        inbound_archive_dir = os.path.join(inbound_dir, 'archive')
        outbound_archive_dir = os.path.join(outbound_dir, 'archive')
        checkpoint_dir = os.path.join(work_dir, 'checkpoint')
        # create directories if not exists
        if not os.path.exists(inbound_archive_dir):
            os.makedirs(inbound_archive_dir)
        if not os.path.exists(outbound_archive_dir):
            os.makedirs(outbound_archive_dir)
        if not os.path.exists(checkpoint_dir):
            os.makedirs(checkpoint_dir)
        if checkpoint:
            return inbound_dir, outbound_dir, checkpoint_dir, \
                inbound_archive_dir, outbound_archive_dir
        else:
            return inbound_dir, outbound_dir, \
                inbound_archive_dir, outbound_archive_dir

    def write_events(self, file_name, outbound_dir,
                     events, index='main', source_type=None):
        """
        write results to proper destination based on where script runs
        :param file_name:
        :param outbound_dir:
        :param events:
        """
        file_type = file_name.split('.')[-1]
        file_name = file_name.replace(file_type, 'json')
        outbound_file_path = os.path.join(outbound_dir, file_name)
        count = 0
        with open(outbound_file_path, 'w') as json_file:
            for event in events:
                for item in json.loads(event):
                    if self.ew is not None:
                        # write event to Splunk
                        splunk_event = modularinput.Event(
                            data=json.dumps(item),
                            index=index,
                            sourcetype=source_type
                        )
                        self.ew.write_event(splunk_event)
                        count += 1
                    else:
                        # print event to STDOUT
                        print(item)
                json_file.write(event)
        json_file.close()
        return file_name, count

    def write_checkpoint(self, dir, name, checkpoint):
        """
        writes checkpoint to file
        :param dir:
        :param name:
        :param checkpoint:
        :return:
        """
        file_path = os.path.join(dir, name)
        with open(file_path, 'w') as file:
            file.write(str(checkpoint))
        file.close()
        return

    def read_checkpoint(self, dir, name):
        """
        reads checkpoint from file
        :param dir:
        :param name:
        :return: checkpoint:
        """
        file_path = os.path.join(dir, name)
        if not os.path.exists(file_path):
            return None
        with open(file_path, 'r') as file:
            checkpoint = file.readlines()
        file.close()
        return checkpoint

    def get_init_date(self, splunk, splunk_query, splunk_args, user_init_date_str):
        """Get latest logged timestamp from Splunk"""
        user_init_date_str_parts = user_init_date_str.split('-')
        user_init_datetime = datetime(int(user_init_date_str_parts[0]),
                                      int(user_init_date_str_parts[1]),
                                      int(user_init_date_str_parts[2]))
        splunk_earliest = float((user_init_datetime - datetime(1970, 1, 1))
                                .total_seconds())
        # Splunk connection
        service = splunk.connect(**splunk_args)
        results = splunk.search(service, splunk_query, splunk_earliest)
        timestamp = 0
        for event in results:
            timestamp = float(event['timestamp'])
        utc_last_datetime = datetime.fromtimestamp(timestamp)
        if float(timestamp) <= 0 or utc_last_datetime is None:
            start_datetime = user_init_datetime
        else:
            start_datetime = utc_last_datetime + timedelta(seconds=1)
        return start_datetime

    def get_init_datetime(self, splunk, splunk_query, splunk_args, user_init_datetime):
        """Get latest logged timestamp from Splunk"""
        splunk_earliest = float((user_init_datetime - datetime(1970, 1, 1))
                                .total_seconds())
        # Splunk connection
        service = splunk.connect(**splunk_args)
        results = splunk.search(service, splunk_query, splunk_earliest)
        timestamp = 0
        for event in results:
            timestamp = float(event['timestamp'])
        utc_last_datetime = datetime.fromtimestamp(timestamp)
        if float(timestamp) <= 0 or utc_last_datetime is None:
            start_datetime = user_init_datetime
        else:
            start_datetime = utc_last_datetime + timedelta(seconds=1)
        return start_datetime

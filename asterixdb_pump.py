import json
import logging
import logging.config
import os
import re
import shelve
import urllib.parse
from datetime import datetime, timedelta
from typing import List

import requests

from ini_parser import parse
from paths import PUMP_MANAGER_PERSISTENCE_PATH, RUNTIME_LOG_PATH, \
    PUMP_CONFIG_PATH, LOG_DIR, PERSISTENCE_DIR


def validate_url(url: str) -> True:
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, url)


class AsterixDBError(Exception):
    pass


class PumpManager:
    def __init__(self):
        self.update_time = parse(PUMP_CONFIG_PATH)['selection_query']['start_time']
        self.update_count = 0
        self.total_count = 0

        self.last_update_count = 0
        self.last_update_time = self.update_time

    def pump(self):
        try:
            config = parse(PUMP_CONFIG_PATH)
            dest = config['asterixdb_dest']
            src = config['asterixdb_src']
            query = config['selection_query']

            if eval(query['restart']):
                self.update_time = query['start_time']
                logging.info(f"RESTARTING from {self.update_time}")

            self.last_update_time = self.update_time
            self.last_update_count = self.update_count
            end_time = query['end_time']
            if end_time == "now":
                end_time = (datetime.utcnow()).strftime('%Y-%m-%dT%H:%M:%SZ')

            start_time = self.update_time
            while datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ') < datetime.strptime(end_time,
                                                                                          '%Y-%m-%dT%H:%M:%SZ'):
                period_end_time = (datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ') + timedelta(days=1)).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
                if datetime.strptime(period_end_time, '%Y-%m-%dT%H:%M:%SZ') > datetime.strptime(
                        end_time, '%Y-%m-%dT%H:%M:%SZ'):
                    period_end_time = end_time
                data = self.pull_from_asterixdb(scheme=src.get('scheme'), host=src.get('host'), port=src.get('port'),
                                                start_time=start_time, end_time=period_end_time)
                if eval(config['general']['persist_to_disk']):
                    self.persist_to_disk(data, config['general']['persistence_path'])
                self.push_to_asterixdb(data, dest.get('host'), dest.get('port'))
                self.last_update_time = self.update_time
                self.update_time = period_end_time
                self.update_count += len(data)
                logging.info(self)
                start_time = self.update_time
        except Exception as e:
            logging.error(e)

    @staticmethod
    def pull_from_asterixdb(scheme: str, host: str, port: int, start_time=None, end_time=None) -> List[str]:
        config = parse(PUMP_CONFIG_PATH)
        asterixdb_url = f"{scheme}://{host}:{port}/query/service"
        assert host and port and validate_url(
            asterixdb_url), f"The provided asterixdb url is not correct: {asterixdb_url}"

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        incremental = eval(config['selection_query']['incremental'])
        logging.info(f"RUNNING in {'incremental' if incremental else 'one time'} mode")

        sql = config['selection_query']['sql'].lower()
        logging.info(f"SQL read from config: \n{sql}")
        if incremental:
            assert "select" in sql, "NOT a SELECT query"
            assert 'where' in sql, "NO WHERE clause found"
            assert '<time-window>' in sql, "NO <time-window> found"
            sql = sql.replace("<time-window>",
                              f" AND t.create_at >= datetime({repr(start_time)}) AND t.create_at < datetime({repr(end_time)})")
            logging.info(f"MODIFIED incremental sql: \n{sql}")

        data = {
            "statement": sql,
            "format": "application/x-adm"}

        proxies = {
            'http': config['general']['proxy']
        } if config['general']['proxy'] else {}

        r = requests.post(asterixdb_url, data=urllib.parse.urlencode(data), headers=headers, proxies=proxies)
        if r.status_code != 200:
            raise AsterixDBError(f"AsterixDB returned non 200 response: {r.status_code}, {r.content}")
        return json.loads(r.content).get('results')

    @staticmethod
    def persist_to_disk(data: List[str], out):
        with open(out, 'a+') as file:
            for each in data:
                file.write(each)

    def push_to_asterixdb(self, data: List[str], host: str, port: int):

        from socket import socket

        sock1 = socket()
        logging.info(f"Opening Feed on {host}:{port}")
        sock1.connect((host, int(port)))

        logging.info(f"Ingesting data through Feed opened on {host}:{port}")

        for each in data:
            sock1.sendall(each.encode('utf-8'))
        sock1.close()
        logging.info(f"Done ingesting through Feed opened on {host}:{port}")

    def __str__(self) -> str:
        incremental = eval(parse(PUMP_CONFIG_PATH)['selection_query']['incremental'])
        log = ""
        if incremental:
            log += f"""
        Last pump time: {self.last_update_time}
        Current pump time: {self.update_time}"""

        log += f"""
        Last count: {self.last_update_count}
        Current count: {self.update_count}
        Updated count in this run: {self.update_count - self.last_update_count}"""
        return log


if __name__ == '__main__':

    format = '[%(asctime)s] [%(levelname)s] %(funcName)s(): %(message)s'
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)
    logging.basicConfig(format=format, level=logging.INFO, filename=RUNTIME_LOG_PATH, datefmt='%Y-%m-%dT%H:%M:%SZ')
    if not os.path.exists(PERSISTENCE_DIR):
        os.mkdir(PERSISTENCE_DIR)
    if eval(parse(PUMP_CONFIG_PATH)['general']['clean_history']):
        os.remove(PUMP_MANAGER_PERSISTENCE_PATH)
    store = shelve.open(PUMP_MANAGER_PERSISTENCE_PATH)
    if 'pump_manager' not in store:
        pump_manager = store['pump_manager'] = PumpManager()
    else:
        pump_manager = store['pump_manager']
    pump_manager.pump()
    store['pump_manager'] = pump_manager

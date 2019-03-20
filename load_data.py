from configparser import ConfigParser

import psycopg2
import pandas as pd
from sshtunnel import SSHTunnelForwarder


class LoadData:
    def __init__(self, config_name, db_name):
        self._config = None
        self.init_config(config_name)
        self._db_username = self._config['psql']['username']
        self._db_password = self._config['psql']['password']
        self._db_name = self._config[db_name]['db_name']
        self._schema_name = self._config[db_name]['schema']
        self._sqlhost = self._config['psql']['host']
        self._sqlport = self._config.getint('psql', 'port')
        self._con = None
        self._ssh_username = self._config['ssh']['user']
        self._ssh_password = self._config['ssh']['password']
        self._server_host = self._config['ssh']['host']
        self._ssh_port = self._config.getint('ssh', 'port')

    def init_config(self, config_name):
        """

        :param config_name:
        :return:
        """
        if self._config is not None:
            return
        self._config = ConfigParser()
        self._config.read(config_name)

    @property
    def con(self):
        """

        :return:
        """
        if self._con is not None:
            return self._con
        self._con = psycopg2.connect(dbname=self._db_name,
                                     user=self._db_username,
                                     password=self._db_password,
                                     host=self._sqlhost,
                                     port=self._sqlport)
        return self._con

    def query_db(self, query, params=None):
        """

        :param params:
        :param query:
        :return:
        """
        with SSHTunnelForwarder(
                (self._server_host, self._ssh_port),
                ssh_username=self._ssh_username,
                ssh_password=self._ssh_password,
                remote_bind_address=(self._sqlhost, self._sqlport)
        ) as server:
            server.start()
            _con = psycopg2.connect(dbname=self._db_name,
                                    user=self._db_username,
                                    password=self._db_password,
                                    host=self._sqlhost,
                                    port=server.local_bind_port)
            tr = pd.read_sql_query(query, _con, params=params)
        return tr

# -*- coding: utf-8 -*-
##############################################################################
# LICENSE
#
# This file is part of mss_transmeta.
# 
# If you use mss_transmeta in any program or publication, please inform and
# acknowledge its authors.
# 
# mss_transmeta is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# mss_transmeta is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with mss_dataserver. If not, see <http://www.gnu.org/licenses/>.
#
# Copyright 2022 Stefan Mertl
##############################################################################

''' General utility function.

'''
import configparser
import json
import logging
import logging.handlers
import os
import re
import time

import obspy


def get_logger_stream_handler(log_level = 'WARNING'):
    ''' Create a logging stream handler.

    Returns
    -------
    ch: logging.StreamHandler
        The logging filehandler.
    '''
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    formatter = logging.Formatter("#LOG# - %(asctime)s - %(process)d - %(levelname)s - %(name)s: %(message)s")
    ch.setFormatter(formatter)
    return ch


def get_logger_rotating_file_handler(filename = None,
                                     log_level = 'INFO',
                                     max_bytes = 1000,
                                     backup_count = 3):
    ''' Create a logging rotating file handler.

    Create a logging RotatingFileHandler and ad a Formatter to it.

    Parameters
    ----------
    filename: str
        The full path of the log file.

    log_level: str
        The logging log level.
        ['DEBUG', 'INFO', 'WARNING', 'ERROR']

    max_bytes: int
        The maximum filesize of the log file [bytes].
        If the file grows larger than this value, a new
        file is created.

    backup_count: int
       The number of rotating files to use.


    Returns
    -------
    ch: logging.handlers.RotatingFileHandler
        The logging filehandler.
    '''
    if not filename:
        return

    ch = logging.handlers.RotatingFileHandler(filename = filename,
                                              maxBytes = max_bytes,
                                              backupCount = backup_count)
    ch.setLevel(log_level)
    formatter = logging.Formatter("#LOG# - %(asctime)s - %(process)d - %(levelname)s - %(name)s: %(message)s")
    ch.setFormatter(formatter)
    return ch


def load_configuration(filename):
    ''' Load the configuration from a file.

    Load the configuration from a .ini file using configparser.
    The properties of the configuration file are documented in an 
    example .ini file in the *example* directory.

    Parameters
    ----------
    filename: str
        The full path to the configuration file.

    Returns
    -------
    config: dict
        A dictionary holding the configuration data.

    '''
    if not os.path.exists(filename):
        raise RuntimeError("The configuration filename {filename} doesn't exist.".format(filename = filename))
    parser = configparser.ConfigParser()
    parser.read(filename)

    config = {}
    config['config_filepath'] = filename
    config['seedlink'] = {}
    config['seedlink']['host'] = parser.get('seedlink', 'host').strip()
    config['seedlink']['port'] = int(parser.get('seedlink', 'port'))
    config['output'] = {}
    config['output']['data_dir'] = parser.get('output', 'data_dir').strip()
    config['log'] = {}
    config['log']['log_dir'] = parser.get('log', 'log_dir').strip()
    config['log']['loglevel'] = parser.get('log', 'loglevel').strip()
    config['log']['max_bytes'] = int(parser.get('log', 'max_bytes'))
    config['log']['backup_count'] = int(parser.get('log', 'backup_count'))
    config['project'] = {}
    config['project']['author_uri'] = parser.get('project', 'author_uri').strip()
    config['project']['agency_uri'] = parser.get('project', 'agency_uri').strip()
    config['database'] = {}
    config['database']['host'] = parser.get('database', 'host').strip()
    config['database']['username'] = parser.get('database', 'username').strip()
    config['database']['password'] = parser.get('database', 'password').strip()
    config['database']['dialect'] = parser.get('database', 'dialect').strip()
    config['database']['driver'] = parser.get('database', 'driver').strip()
    config['database']['database_name'] = parser.get('database', 'database_name').strip()
    config['process'] = {}
    config['process']['stations'] = json.loads(parser.get('process', 'stations'))
    config['process']['save_interval'] = int(parser.get('process', 'save_interval'))
    config['process']['clean_interval'] = int(parser.get('process', 'clean_interval'))

    return config


def task_timer(callback, interval, logger, stop_event):
    ''' A timer executing a task at regular intervals.

    Parameters
    ----------
    callback: function
        The function to be called after the given process_interval.
    '''
    logger.info('Starting the timer.')
    interval = int(interval)
    now = obspy.UTCDateTime()
    delay_to_next_interval = interval - (now.timestamp % interval)
    logger.info('Sleeping for %f seconds.', delay_to_next_interval)
    time.sleep(delay_to_next_interval)

    while not stop_event.is_set():
        try:
            logger.info('task_timer: Executing callback.')
            callback()
        except Exception as e:
            logger.exception(e)
            stop_event.set()

        now = obspy.UTCDateTime()
        delay_to_next_interval = interval - (now.timestamp % interval)
        logger.info('task_timer: Sleeping for %f seconds.',
                    delay_to_next_interval)
        time.sleep(delay_to_next_interval)

    logger.debug("Leaving the task_timer method.")


class AttribDict(dict):
    ''' A dictionary with object like attribute access.

    '''
    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise AttributeError("No such attribute: " + name)
        

class Version(object):
    ''' A version String representation.


    Parameters
    ----------
    version: str
        The version as a point-seperated string (e.g. 0.0.1).

    '''
    

    def __init__(self, version = '0.0.1'):
        ''' Initialize the instance.

        '''
        self.version = self.string_to_tuple(version)


    def __str__(self):
        ''' The string representation.
        '''
        return '.'.join([str(x) for x in self.version])


    def __eq__(self, c):
        ''' Test for equality.
        '''
        for k, cur_n in enumerate(self.version):
            if cur_n != c.version[k]:
                return False

        return True

    def __ne__(self, c):
        ''' Test for inequality.
        '''
        return not self.__eq__(c)


    def __gt__(self, c):
        ''' Test for greater than.
        '''
        for k, cur_n in enumerate(self.version):
            if cur_n > c.version[k]:
                return True
            elif cur_n != c.version[k]:
                return False

        return False


    def __lt__(self, c):
        ''' Test for less than.
        '''
        for k, cur_n in enumerate(self.version):
            if cur_n < c.version[k]:
                return True
            elif cur_n != c.version[k]:
                return False

        return False


    def __ge__(self, c):
        ''' Test for greater or equal.
        '''
        return self.__eq__(c) or self.__gt__(c)

    def __le__(self, c):
        ''' Test for less or equal.
        '''
        return self.__eq__(c) or self.__lt__(c)


    def string_to_tuple(self, vs):
        ''' Convert a version string to a tuple.

        Parameters
        ----------
        version: str
            The version as a point-seperated string (e.g. 0.0.1).

        Returns
        -------
        version_tuple: tuple
            The version string as a tuple.
        '''
        nn = vs.split('.')
        for k, x in enumerate(nn):
            if x.isdigit():
                nn[k] = int(x)
            else:
                tmp = re.split('[A-Za-z]', x)
                tmp = [x for x in tmp if x.isdigit()]
                if len(tmp) > 0:
                    nn[k] = int(tmp[0])
                else:
                    nn[k] = 0

        return tuple(nn)


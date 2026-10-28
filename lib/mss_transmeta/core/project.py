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
 # along with mss_transmeta. If not, see <http://www.gnu.org/licenses/>.
 #
 # Copyright 2019 Stefan Mertl
##############################################################################

''' MSS Dataserver Project.
'''

import logging
import os

#import sqlalchemy
#import sqlalchemy.ext.declarative
#import sqlalchemy.orm

#import mss_dataserver.geometry as mssds_geometry
#import mss_dataserver.geometry.db_inventory


class Project(object):
    ''' A project holds global configuration and settings.


    Parameters
    ----------
    kwargs: dict
        The dictionary created from the configuration file.


    Attributes
    ----------
    logger: logging.Logger
        The logger instance.

    project_config: dict
        The *project* configuration section.

    author_uri: String
        The Uniform Resource Identifier of the author.
  
    agency_uri: String
        The Uniform Resource Identifier of the author agency.

    config: dict
        The complete configuration dictionary (kwargs).

    process_config: dict
        The *process* configuration section.

    db_host: String
        The URL or IP of the database host.

    db_username: String
        The database user name.

    db_pwd: String
        The database password.

    db_dialect: String
        The dialect of the database.

    db_driver: String
        The driver of the database.

    db_database_name: String
        The name of the database.

    db_tables: list
        The tables loaded from the database.

    db_inventory: :class:`mss_dataserver.geometry.DbInventory`
        The geometry inventory of the project.

    inventory: :class:`mss_dataserer.geometry.DbInventory`
        A dynamic property returning db_inventory.

    event_library: :class:`mss_dataserver.event.core.Library`
        The event library of the project.

    detection_library: :class:`mss_dataserver.detection.Library`
        The detection library of the project.

    
    See Also
    --------
    :meth:`mss_dataserver.core.util.load_configuration`

    '''

    def __init__(self, **kwargs):
        ''' Initialize the instance.
        '''
        logger_name = __name__ + "." + self.__class__.__name__
        self.logger = logging.getLogger(logger_name)

        self.project_config = kwargs['project']
        self.author_uri = self.project_config['author_uri']
        self.agency_uri = self.project_config['agency_uri']

        # The complete configuration content.
        self.config = kwargs

        # The processing configuration.
        self.process_config = kwargs['process']

        # The database configuration.
        db_config = kwargs['database']
        self.db_host = db_config['host']
        self.db_username = db_config['username']
        self.db_pwd = db_config['password']
        self.db_dialect = db_config['dialect']
        self.db_driver = db_config['driver']
        self.db_database_name = db_config['database_name']

        # Check and create the output directories.
        output_dirs = [self.config['output']['data_dir']]
        for cur_dir in output_dirs:
            if not os.path.exists(cur_dir):
                os.makedirs(cur_dir)

        # The database connection state.
        self.db_engine = None
        self.db_metadata = None
        self.db_base = None
        self.db_session_class = None

        # A dictionary of the project database tables.
        self.db_tables = {}

        # The geometry inventory.
        self.db_inventory = None

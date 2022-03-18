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

import logging
import os
import threading

import obspy
import obspy.clients.seedlink.easyseedlink as easyseedlink


class TransMetaClient(easyseedlink.EasySeedLinkClient):
    ''' A custom Seedlink client.

    '''
    def __init__(self, project, server_url,
                 autoconnect = False):
        ''' Initialize the instance.
        '''
        easyseedlink.EasySeedLinkClient.__init__(self,
                                                 server_url = server_url,
                                                 autoconnect = autoconnect)

        # Configure the logger.
        logger_name = __name__ + "." + self.__class__.__name__
        self.logger = logging.getLogger(logger_name)

        # Set the logging level of obspy module.
        #logging.getLogger('obspy.clients.seedlink').setLevel(logging.WARNING)

        # The parent project.
        self.project = project
        
        # The station NSL codes which should be requested from the server.
        # None if all stations in the inventory should be requested.
        self.stations = None
        if len(self.project.process_config['stations']) > 0:
            self.stations = self.project.process_config['stations']

        # The directory where to save the incoming data.
        self.data_dir = self.project.config['output']['data_dir']

        # The incoming data.
        self.stream = obspy.Stream()

        # Locks for threadsave data handling.
        self.stream_lock = threading.Lock()

        # Get the recorder mappings.
        self.recorder_map = self.get_recorder_mappings(station_nsl = self.stations)


    @property
    def inventory(self):
        ''' The geometry inventory.
        '''
        if self.project:
            return self.project.inventory
        else:
            return None
        

    def on_data(self, trace):
        """ Override the on_data callback function.
        """
        self.logger.debug('Received trace:')
        self.logger.debug(str(trace))
        cur_nslc = self.recorder_map[tuple(trace.id.split('.'))]
        trace.stats.network = cur_nslc[0]
        trace.stats.station = cur_nslc[1]
        trace.stats.location = cur_nslc[2]
        trace.stats.channel = cur_nslc[3]
        self.logger.debug('Changed metadata:')
        self.logger.debug(str(trace))
        with self.stream_lock:
            self.stream.append(trace)

        
    def seedlink_connect(self):
        ''' Connect to the seedlink server.
        '''
        self.connect()

        for cur_mss in self.recorder_map:
            self.logger.debug('selecting stream: %s', cur_mss)
            self.select_stream(cur_mss[0],
                               cur_mss[1],
                               cur_mss[2] + cur_mss[3])

    def save_data(self):
        ''' Save the data to miniseed files.
        '''
        self.logger.info("Saving the data.")
        with self.stream_lock:
            export_stream = self.stream.copy()
            self.stream = obspy.Stream()

        export_stream.merge()
        export_stream.split()
        self.logger.debug(export_stream.__str__(extended = True))
        flush_mode = True

        cur_start = min([x.stats.starttime for x in export_stream])
        cur_end = max([x.stats.endtime for x in export_stream])
        cur_filename = 'msstm_mseed_export' + '_' + cur_start.isoformat().replace(':','') + '_' + cur_end.isoformat().replace(':', '') + '.msd'
        cur_filepath = os.path.join(self.data_dir,
                                    cur_filename)
        try:
            export_stream.write(cur_filepath,
                                format = "MSEED",
                                reclen = 512,
                                encoding = 'STEIM2',
                                flush = flush_mode)
        except NotImplementedError:
            self.logger.exception("Error when writing the miniseed file with masked data. Clearing the stream and #going on.")
        except ValueError:
            self.logger.info("Not enough data to write a miniseed record.")
            os.remove(cur_filepath)

        #for cur_trace in export_stream:
        #    cur_filename = cur_trace.id.replace('.', '_') + '_' + cur_trace.stats.starttime.isoformat().replace(':','') + '.msd'
        #    cur_filepath = os.path.join(self.data_dir,
        #                                cur_filename)
        #    try:
        #        export_trace = cur_trace.copy()
        #        export_trace.write(cur_filepath,
        #                           format = "MSEED",
        #                           reclen = 512,
        #                           encoding = 'STEIM2',
        #                           flush = flush_mode)
        #    except NotImplementedError:
        #        self.logger.exception("Error when writing the miniseed file with masked data. Clearing the stream and #going on.")
        #        continue
        #    except ValueError:
        #        self.logger.debug("Not enough data to write a miniseed record.")
        #        os.remove(cur_filepath)
        #        continue


    def get_recorder_mappings(self, station_nsl = None):
        ''' Get the mappings of the requested NSLC.

        Parameters
        ----------
        station_nsl: :obj:`list` of :obj:`str`
            The station NSL codes to process. If None, all available stations
            in the inventory are processed.

        Returns
        -------
        :obj:`dict`
            The matching NSLC codes of the MSS units relating their
            serial numbers to the actual station locations.
        '''
        recorder_map = {}
        if station_nsl is None:
            station_list = self.inventory.get_station()
        else:
            station_list = []
            for cur_nsl in station_nsl:
                cur_station = self.inventory.get_station(network = cur_nsl[0],
                                                         name = cur_nsl[1],
                                                         location = cur_nsl[2])
                if len(cur_station) > 1:
                    raise ValueError('There are more than one stations. This is not yet supported.')
                if len(cur_station) == 0:
                    raise ValueError('No station found for {0:s}. Check the input file.'.format(cur_nsl))
                cur_station = cur_station[0]
                station_list.append(cur_station)

        for station in station_list:
            for cur_channel in station.channels:
                stream_tb = cur_channel.get_stream(start_time = obspy.UTCDateTime())
                cur_loc = stream_tb[0].item.name.split(':')[0]
                cur_chan = stream_tb[0].item.name.split(':')[1]

                cur_key = ('XX',
                           stream_tb[0].item.serial,
                           cur_loc,
                           cur_chan)
                recorder_map[cur_key] = cur_channel.nslc

        self.logger.debug(recorder_map)
        return recorder_map
        

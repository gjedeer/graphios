#!/usr/bin/python -tt
#
# Copyright (C) 2011  Shawn Sterling <shawn@systemtemplar.org>
#
# With contributions from:
#
# Juan Jose Presa <juanjop@gmail.com>
# Ranjib Dey <dey.ranjib@gmail.com>
# Ryan Davis <https://github.com/ryepup>
# Alexey Diyan <alexey.diyan@gmail.com>
# Steffen Zieger <me@saz.sh>
# Nathan Bird <ecthellion@gmail.com>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#
# graphios: this program will read nagios host and service perfdata, and
# send it to a carbon server.
#
# The latest version of this code will be found on my github page:
# https://github.com/shawn-sterling

import os
import sys
import re
import logging
import logging.handlers
import time
import socket
import cPickle as pickle
import struct
from optparse import OptionParser


############################################################
##### You will likely need to change some of the below #####

# carbon server info
carbon_server = '127.0.0.1'

# carbon pickle receiver port (normally 2004)
carbon_port = 2004

# nagios spool directory
spool_directory = '/var/spool/nagios/graphios'

# graphios log info
log_file = '/var/log/nagios/graphios.log'
log_max_size = 25165824         # 24 MB
log_level = logging.INFO
#log_level = logging.DEBUG      # DEBUG is quite verbose

# How long to sleep between processing the spool directory
sleep_time = 15

# when we can't connect to carbon, the sleeptime is doubled until we hit max
sleep_max = 480

# test mode makes it so we print what we would add to carbon, and not delete
# any files from the spool directory. log_level must be DEBUG as well.
test_mode = False

# Character to use as replacement for invalid characters in metric names
replacement_character = '_'

# use service description as part of your carbon metric
# $GRAPHIOSPREFIX.$HOSTNAME.$SERVICEDESC.$GRAPHIOSPOSTFIX.$PERFDATA
use_service_desc = False

##### You should stop changing things unless you know what you are doing #####
##############################################################################

parser = OptionParser("""usage: %prog [options]
sends nagios performance data to carbon.
""")

parser.add_option('-v', "--verbose", action="store_true", dest="verbose",
                  help="sets logging to DEBUG level")
parser.add_option("--spool-directory", dest="spool_directory",
                  default=spool_directory,
                  help="where to look for nagios performance data")
parser.add_option("--log-file", dest="log_file",
                  default=log_file,
                  help="file to log to")

sock = socket.socket()
log = logging.getLogger('log')


def configure(opts):
    global spool_directory

    log_handler = logging.handlers.RotatingFileHandler(
        opts.log_file, maxBytes=log_max_size, backupCount=4)
    formatter = logging.Formatter(
        "%(asctime)s %(filename)s %(levelname)s %(message)s",
        "%B %d %H:%M:%S")
    log_handler.setFormatter(formatter)
    log.addHandler(log_handler)

    if opts.verbose:
        log.setLevel(logging.DEBUG)
        log.addHandler(logging.StreamHandler())
    else:
        log.setLevel(log_level)

    spool_directory = opts.spool_directory


def connect_carbon():
    """
        Connects to Carbon server
    """
    log.debug("Connecting to carbon at %s:%s", carbon_server, carbon_port)
    global sock
    sock = socket.socket()
    try:
        sock.connect((carbon_server, carbon_port))
        log.debug("connected")
        return True
    except Exception, ex:
        log.warning("Can't connect to carbon: %s:%s %s" % (carbon_server,
                                                           carbon_port, ex))
        return False


def send_carbon(carbon_list):
    """
        Sends a list to Carbon, we postpend every entry with a \n as per
        carbon documentation.
        If we can't connect to carbon, it sleeps, and doubles sleep_time
        until it hits sleep_max.
    """
    global sock
    global sleep_time
    messages = convert_pickle(carbon_list)
    try:
        for message in messages:
            sock.sendall(message)
        return True
    except Exception, ex:
        log.critical("Can't send message to carbon error:%s" % ex)
        while True:
            sock.close()
            if connect_carbon():
                sleep_time = 15     # reset sleep_time to 15
                return False
            else:
                if sleep_time < sleep_max:
                    sleep_time = sleep_time + sleep_time
                    log.warning("Carbon not responding. Increasing " +
                                "sleep_time to %s." % sleep_time)
                else:
                    log.warning("Carbon not responding. Sleeping %s" %
                                sleep_time)
            log.debug("sleeping %s" % sleep_time)
            time.sleep(sleep_time)
        return False


def convert_pickle(carbon_list):
    """
        Converts a list into pickle formatted messages and returns it
    """
    MAX_METRICS = 200
    pickle_list = []
    for metric in carbon_list:
        path, value, timestamp = metric.strip().rsplit(' ', 2)
        path = re.sub(r"\s+", replacement_character, path)
        metric_tuple = (path, (timestamp, value))
        pickle_list.append(metric_tuple)
    for pickle_list_chunk in chunks(pickle_list, MAX_METRICS):
        payload = pickle.dumps(pickle_list_chunk)
        header = struct.pack("!L", len(payload))
        message = header + payload
    return message


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def process_host_data(file_name, delete_after=0):
    """
        processes a file loaded with nagios host data, and sends info to
        a carbon server. If delete_after is 1, we delete the file when we
        are done with it.

        When nagios has host perf data, that is from check_host_alive, which
        is the check_icmp plugin. The perf data looks like this:

rta=1.066ms;5.000;10.000;0; pl=0%;5;10;; rtmax=4.368ms;;;; rtmin=0.196ms;;;;

        We send to graphite:
        (GRAPHITEPREFIX).HOSTNAME.(GRAPHITEPOSTFIX).perf value TIMET

        Which for me I set the prefix to:
        monitoring.domain.com.nagiosXX.pingto.

        I leave the postfix blank (for this example)

        So if i'm checking db01, from nagios host nagios01 my carbon lines
        would be:

        monitoring.domain.com.nagios01.pingto.db01.rta 1.0.66 timet
        monitoring.domain.com.nagios01.pingto.db01.pl 0 timet
        monitoring.domain.com.nagios01.pingto.db01.rtmax 4.368 timet
        monitoring.domain.com.nagios01.pingto.db01.rtmin 0.196 timet
    """
    try:
        host_data_file = open(file_name, "r")
        file_array = host_data_file.readlines()
        host_data_file.close()
    except Exception, ex:
        log.critical("Can't open file:%s error: %s" % (file_name, ex))
        sys.exit(2)
    graphite_lines = []
    for line in file_array:
        variables = line.split('\t')
        carbon_string = ""
        carbon_dict = {}
        for var in variables:
            (var_name, value) = var.split('::')
            carbon_dict[var_name] = value
        if validate_host_dict(carbon_dict):
            carbon_string = build_carbon_metric(carbon_dict)
        if carbon_string:
            graphite_lines.extend(process_nagios_perf_data(
                                  carbon_string, carbon_dict['HOSTPERFDATA'],
                                  carbon_dict['TIMET']))
    handle_file(file_name, graphite_lines, test_mode, delete_after)


def validate_host_time(carbon_dict):
    valid = True
    try:
        if carbon_dict['TIMET'] == "":
            log.debug('TIMET was ""')
            valid = False
        if carbon_dict['HOSTNAME'] == "":
            log.debug('HOSTNAME was ""')
            valid = False
    except KeyError:
        log.debug('HOSTNAME or TIMET was not in carbon_dict')
        valid = False
    return valid


def validate_host_dict(carbon_dict):
    valid = validate_host_time(carbon_dict)
    try:
        if carbon_dict['HOSTPERFDATA'] == "":
            log.debug('HOSTPERFDATA was ""')
            valid = False
    except KeyError:
        log.debug('HOSTPERFDATA was not in carbon_dict')
        valid = False
    return valid


def handle_file(file_name, graphite_lines, test_mode, delete_after):
    """
        if we are test mode we just print the graphite lines.
        if the graphite data gets into carbon, and delete_after is set
        we remove the file.
        if the graphite_lines has a length of 0, there was no graphite
        data, and we remove the file.
    """
    if test_mode and len(graphite_lines) > 0:
        log.debug("graphite_lines:%s" % graphite_lines)
    else:
        if len(graphite_lines) > 0:
            if send_carbon(graphite_lines):
                if delete_after:
                    log.debug("removing file, %s" % file_name)
                    try:
                        os.remove(file_name)
                    except Exception, ex:
                        log.critical("couldn't remove file %s error:%s" % (
                            file_name, ex))
            else:
                log.warning("message not sent to graphite, file not deleted.")
        else:
            # file didn't have any graphite data in it, delete it.
            if delete_after:
                try:
                    os.remove(file_name)
                except Exception, ex:
                    log.critical("couldn't remove file %s error:%s" % (
                        file_name, ex))


def process_nagios_perf_data(carbon_string, perf_data, time_stamp):
    """
        given the nagios perfdata, and some variables we return a list of
        carbon formatted strings.

        nagios perfdata represented as a list of perf strings like following:
            label=value[UOM];[warn];[crit];[min];[max]
        We want to scrape the label=value and get rid of everything else.

        UOM can be: s, %, B(kb,mb,tb), c

        graphios assumes that you have modified your plugin to always use
        the same value. If your plugin does not support this, you can use
        check_mp to force your units to be consistent. Graphios plain
        ignores the UOM.
    """
    graphite_lines = []
    log.debug('perfdata:%s' % perf_data)
    matches = re.finditer(
        r'(?P<perfdata>(?P<label>.*?)=(?P<value>[-0-9\.]+)\S*\s?)',
        perf_data)
    parsed_perfdata = [match.groupdict() for match in matches]
    log.debug('parsed_perfdata:%s' % parsed_perfdata)
    for perf_string in parsed_perfdata:
        label = re.sub(r'[\s\.:\\]', replacement_character,
                       perf_string['label'])
        value = perf_string['value']
        new_line = "%s%s %s %s" % (carbon_string, label, value, time_stamp)
        log.debug("new line = %s" % new_line)
        graphite_lines.append(new_line)
    return graphite_lines


def process_service_data(file_name, delete_after=0):
    """
        processes a file loaded with nagios service data, and sends info to
        a carbon server. If delete_after is 1, we delete the file when we
        are done with it.

        here is what we send to carbon:

        (GRAPHITEPREFIX).HOSTNAME.(GRAPHITEPOSTFIX).perf value timet

        So, our example service will be
        'MySQL Connection Time' where the perfdata looks like this:
        connection_time=0.0213s;1;5

        Let's say this is checked on host db01, and it is run from nagios01.

        We set our graphiteprefix to be:
        monitoring.domain.com.nagios01.mysql

        the graphitepostfix in this case to be left blank

        Giving us a final carbon metric of:
        monitoring.domain.com.nagios01.mysql.db01.connection_time 0.0213 timet

        Or let's say you have a plugin that gives the perf 'load=3.4;5;6;;'

        In this case I want my carbon data to be:

        hostname.domain.com.nagios.load

        So I set the _graphitepostfix to 'domain.com.nagios'
    """
    try:
        log.debug("Starting on %r", file_name)
        service_data_file = open(file_name, "r")
        file_array = service_data_file.readlines()
        service_data_file.close()
    except Exception, ex:
        log.critical("Can't open file:%s error: %s" % (file_name, ex))
        sys.exit(2)
    graphite_lines = []
    for line in file_array:
        variables = line.split('\t')
        carbon_string = ""
        service_dict = {}
        for var in variables:
            var_name = ""
            value = ""
            if re.search("::", var):
                var_name = var.split('::')[0]
                value = var[len(var_name) + 2:]
            service_dict[var_name] = value
        if validate_service_dict(service_dict):
            carbon_string = build_carbon_metric(service_dict)
        if carbon_string:
            graphite_lines.extend(process_nagios_perf_data(
                                  carbon_string,
                                  service_dict['SERVICEPERFDATA'],
                                  service_dict['TIMET']))
    handle_file(file_name, graphite_lines, test_mode, delete_after)


def validate_service_dict(service_dict):
    valid = validate_host_time(service_dict)
    try:
        if service_dict['SERVICEPERFDATA'] == "":
            log.debug('SERVICEPERFDATA was ""')
            valid = False
        if not re.search("=", service_dict['SERVICEPERFDATA']):
            log.debug('no = in SERVICEPERFDATA')
            valid = False
    except KeyError:
        log.debug('SERVICEPERFDATA not found in config dict')
        valid = False
    return valid


def build_carbon_metric(carbon_dict):
    """
       builds the metric to send to carbon, returns empty string if
       there's insufficient data and we shouldn't forward to carbon.
    """
    graphite_prefix = carbon_dict.get('GRAPHITEPREFIX') or ""
    graphite_prefix = graphite_prefix.replace("$_HOSTGRAPHITEPREFIX$\n", "")
    graphite_prefix = fix_carbon_string(graphite_prefix)
    graphite_postfix = carbon_dict.get('GRAPHITEPOSTFIX') or ""
    graphite_postfix = graphite_postfix.replace("$_HOSTGRAPHITEPOSTFIX$\n", "")
    graphite_postfix = fix_carbon_string(graphite_postfix)
    host_name = carbon_dict['HOSTNAME']
    if (
        graphite_prefix == "" and graphite_postfix == "" and
        use_service_desc is False
    ):
        # if there is no prefix postfix and service desc is false, we have
        # no metric to build.
        return ""
    carbon_string = ""
    if graphite_prefix != "":
        carbon_string = "%s." % graphite_prefix
    host_name = host_name.replace('.', replacement_character)
    carbon_string = carbon_string + "%s." % host_name
    if use_service_desc:
        service_desc = carbon_dict.get('SERVICEDESC') or ""
        service_desc = fix_carbon_string(service_desc)
        if service_desc != "":
            carbon_string = carbon_string + "%s." % service_desc
    if graphite_postfix != "":
        carbon_string = carbon_string + "%s." % graphite_postfix
    return carbon_string


def fix_carbon_string(my_string):
    """
        takes a string and replaces whitespace and invalid carbon chars with
        the global replacement_character
    """
    invalid_chars = '~!@#$:;%^*()+={}[]|\/<>'
    my_string = re.sub("\s", replacement_character, my_string)
    for char in invalid_chars:
        my_string = my_string.replace(char, replacement_character)
    return my_string


def process_spool_dir(directory):
    """
        processes the files in the spool directory
    """
    log.debug("Processing spool directory %s", directory)
    num_files = 0
    perfdata_files = os.listdir(directory)
    for perfdata_file in perfdata_files:
        if (
            perfdata_file == "host-perfdata" or
            perfdata_file == "service-perfdata"
        ):
            continue
        file_dir = os.path.join(directory, perfdata_file)
        if re.match('host-perfdata\.', perfdata_file):
            process_host_data(file_dir, 1)
            num_files += 1
        if re.match('service-perfdata\.', perfdata_file):
            process_service_data(file_dir, 1)
            num_files += 1
    log.info("Processed %s files in %s", num_files, directory)


def main():
    """
        the main
    """
    global sock
    log.info("graphios startup.")
    try:
        connect_carbon()
        while True:
            process_spool_dir(spool_directory)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        log.info("ctrl-c pressed. Exiting graphios.")


if __name__ == '__main__':
    (options, args) = parser.parse_args()
    configure(options)
    main()

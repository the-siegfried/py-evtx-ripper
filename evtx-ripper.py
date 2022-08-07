#!/usr/bin/env python3

import concurrent.futures
import logging
import multiprocessing
import sys
import uuid
from configparser import ConfigParser
from optparse import OptionParser
from optparse import Values
from os import path
from os import curdir
from os import remove
from os import walk
from xml.dom import minidom

import Evtx.Evtx as EvtxProcessor

from xml_utils.xml2csv import XML2CSV
from xml_utils.xml2sql import XML2SQL


def configure():
    """ Configuration.
    Attempts to read and ingest the application config file if one can be
    found, else it creates a default one and uses that.

    :return: Returns config object or None.
    """
    # capture logger.
    logger = multiprocessing.get_logger()

    # Create the configparser object.
    config_object = ConfigParser()
    _prod = 'RIPPER_CONFIG'
    _test = 'RIPPER_TEST'
    _conf_name = _prod

    # Search for config file - if there isn't one create one with defaults.
    if not path.exists(path.join(curdir, "config.ini")):
        logger.info("Config file could not be found. Building default config.")

        config_object[_prod] = {
            "cores": 4,
            "csv": False,
            "sql": True,
            "sep": False,
            "input": "/tests/data/",
            "output": "/tests/output"
        }

        with open('config.ini', 'w') as conf:
            config_object.write(conf)

        logger.info("Default config built.")

    # Read in config and check it has the right section.
    config_object.read("config.ini")
    if config_object.has_section(_prod) or config_object.has_section(_test):
        _conf_name = _prod if not config_object.has_section(_test) else _test
    else:
        logger.error("Config file does not contain RIPPER_CONFIG!")
        return None

    # Parse configuration into a dict for use throughout the application.
    conf = {}
    for x in config_object.options(_conf_name):
        conf[x] = config_object.get(_conf_name, x)
    return conf


def collect_files(file_path, file_type):
    """ File collector.
    Walks the input path provided in order to find evtx files.

    :param file_path: File path to collect evtx files from.
    :param file_type: evtx filetype.
    :return: Returns 'evtx_files', list of discovered evtx file else
    returns None.
    """
    # Instantiate empty list for evtx files found.
    evtx_files = []

    logger = multiprocessing.get_logger()

    # Check to see if the input path exists.
    if path.isfile(file_path):
        evtx_files.append(path)
        return evtx_files
    elif path.exists(file_path):
        # Get list of EVTX files in path.
        for root, dirs, files in walk(file_path):
            for file in files:
                if file.endswith(file_type):
                    evtx_files.append("{}/{}".format(root, file))

        return evtx_files
    else:
        logger.fatal("Input path does not exist")
        return None


def evtx_to_xml(evtx):
    """ Windows evtx file to xml file parser.
    :param evtx:
        Windows evtx file path.
    :returns: None
    """
    # Get logger
    logger = multiprocessing.get_logger()

    success = False
    file_name = f"results_{uuid.uuid4()}.xml"
    # Create xml file handler and write header to file.
    xml_file_handler = open(file_name, "w")
    h_out = "<?xml version='1.0' encoding='utf-8' standalone='yes'" \
            " ?>\n<Events>"
    xml_file_handler.write(h_out)

    # hack to get the length of an xml with the xml deceleration.
    d_len = len(minidom.Document().toxml())

    interested_events = ['1000', '1001', '1002', '1106', '1107', '1115',
                         '1116', '258', '259', '102', '1102', '4624',
                         '4625', '4648', '4697', '4698', '4706', '4720',
                         '4724', '4728', '4732', '4735', '4740', '4756',
                         '4778', '4781', '4950', '4964', '104', '1125',
                         '1127', '1129', '4719', '7045']

    event_count = 0

    with EvtxProcessor.Evtx(evtx) as log:
        for chunk in log.chunks():
            for record in chunk.records():
                try:
                    evt = record.xml()
                    if evt is not None:
                        xml_doc = minidom.parseString(
                            evt.replace("\n", ""))
                        event_id = xml_doc.getElementsByTagName(
                            "EventID")[0].childNodes[0].nodeValue
                except UnicodeDecodeError as err:
                    logger.warning(f"WARN:: Failed to parse record "
                                   f"from {evtx}. REASON: {err}.")
                    continue

                if event_id not in interested_events:
                    continue
                event_count += 1
                xml_file_handler.write(xml_doc.toprettyxml()[d_len:])

    if event_count != 0:
        success = True

    return success, file_name


class Ripper:
    def __init__(self, options):
        """ Initiates EVTXRipper class
        :param options: command line arguments.
        """
        self.path = ""
        self.options = list
        self.processed_count = 0

        self.path = options.input
        self.options = options

    def process(self, f):
        """
        :param f:
        :return:
        """
        logger = multiprocessing.get_logger()
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler('evtx_ripper.log')
        logger.addHandler(fh)

        filename_w_ext = path.basename(f)
        filename, file_extension = path.splitext(filename_w_ext)
        logger.info("Processing {}".format(filename_w_ext))

        success, out_file = evtx_to_xml(f)
        if not success:
            logger.warning(f"WARN: No successful events found in file "
                           f"{filename_w_ext}. Proceeding to next file(s).")
            return

        out = path.abspath(path.join(curdir, out_file))

        if self.options.csv:
            csv = "{}/{}.{}".format(self.options.output, filename, "csv")
            xml_to_csv = XML2CSV(out, csv)
            xml_to_csv.convert(
                tag="Events"
            )
        elif self.options.sql:
            if self.options.sep:
                sql = f"{self.options.output}/{filename}.sql"
            else:
                sql = f"{self.options.output}/results.sql"

            xml_to_sql = XML2SQL(input_file=out, output_file=sql)
            xml_to_sql.convert()

        # Delete xml file.
        remove(out)


def main():
    """ """

    # Create logger object.
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler('evtx_ripper.log')
    logger.addHandler(fh)

    # Capture device core count.
    core_count = multiprocessing.cpu_count()

    # Handle options parsing.
    sample_msg = "\r\n" \
                 "Example:\r\n" \
                 "extv-ripper -d -i " \
                 "'C:/Users/abishop/Downloads/evtx_files' -o " \
                 "'C:/Users/abishop/sql_out'"

    parser = OptionParser()
    parser.add_option('-f', '--force', dest="force", default=False,
                      action="store_true")
    parser.add_option("-c", "--cores", dest="cores", type="int", default=4,
                      help="number of cores for processing.")
    parser.add_option("-v", "--csv", dest="csv", default=False,
                      action="store_true",
                      help="evtx2csv parser.")
    parser.add_option("-d", "--db", dest="sql", default=False,
                      action="store_true",
                      help="evtx2sql parser.")
    parser.add_option("-s", "--sep", dest="sep", default=False,
                      action="store_true",
                      help="separate output dbs.")
    parser.add_option("-i", "--input", dest="input",
                      help="input file or folder.")
    parser.add_option("-o", "--output", dest="output",
                      help="output path.")
    (opts, args) = parser.parse_args()

    if opts.force:
        if opts.csv is False and opts.sql is False:
            msg = "No output type selected. Please choose from the options " \
                  "displayed below:\r\n" \
                  "'-c' '--csv' for evtx2csv\r\n" \
                  "'-d' '--db' for evtx2sql\r\n{}".format(sample_msg)
            parser.error(msg)
            logger.warning(msg)

        if not opts.input:
            err = "Input not given"
            parser.error(err)
            logger.error(err)
        if not opts.output:
            err = "Output not given"
            parser.error(err)
            logger.error(err)

        if opts.cores >= core_count:
            logger.error("Number of cores given is equal to or greater than "
                         "the number of actual cores available. avail.{}"
                         "try reducing this number.".format(core_count))
            exit(1)
    else:
        # Use optparse.Values to parse configure() results.
        opts = Values(configure())

    logger.info("collecting Evtx files.")
    files = collect_files(opts.input, ".evtx")

    if files is None:
        logger.error("No file(s) found.")
        exit(1)
    logger.info("Files found: {}".format(len(files)))

    # Chunk up here. 18
    chunks = [files[x:x + opts.cores] for x in range(0, len(files),
                                                     opts.cores)]
    logger.info("Chunked up evtx files")

    c_len = len(chunks)
    count = 1
    for x in chunks:
        logger.info(
            "Processing chunk: {} of {} - There are {} files \n {}".format(
                count, c_len, len(x), x)
        )
        e = Ripper(opts)
        with concurrent.futures.ProcessPoolExecutor(opts.cores) as executor:
            executor.map(e.process, x)
        count += 1

    logger.info("Evtx file parsing complete...")
    sys.exit()


# # testing stub.
if __name__ == "__main__":
    main()

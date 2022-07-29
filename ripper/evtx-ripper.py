#!/usr/bin/env python3

import logging
import mmap
import os
import sys
import uuid
import concurrent.futures
import multiprocessing
from optparse import OptionParser
from xml.dom import minidom

import Evtx.Views
from Evtx.Evtx import FileHeader

from xml_utils.xml2csv import XML2CSV
from xml_utils.xml2sql import XML2SQL


def collect_files(path, file_type):
    """ File collector.
    Walks the input path provided in order to find evtx files.

    :param path: File path to collect evtx files from.
    :param file_type: evtx filetype.
    :return: Returns 'evtx_files', list of discovered evtx file else
    returns None.
    """
    # Instantiate empty list for evtx files found.
    evtx_files = []

    # Check to see if the input path exists.
    if os.path.isfile(path):
        evtx_files.append(path)
        return evtx_files
    elif os.path.exists(path):
        # Get list of EVTX files in path.
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(file_type):
                    evtx_files.append("{}/{}".format(root, file))

        return evtx_files
    else:
        logging.fatal("Input path does not exist")
        return None


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

    @staticmethod
    def evtx_to_xml(evtx):
        """ Windows evtx file to xml file parser.
        :param evtx:
            Windows evtx file path.
        :returns: None
        """
        f1 = uuid.uuid4()
        xml_file_handler = open("results_{}.xml".format(f1), "w")

        interested_events = ['1000', '1001', '1002', '1106', '1107', '1115',
                             '1116', '258', '259', '102', '1102', '4624',
                             '4625', '4648', '4697', '4698', '4706', '4720',
                             '4724', '4728', '4732', '4735', '4740', '4756',
                             '4778', '4781', '4950', '4964', '104', '1125',
                             '1127', '1129', '4719', '7045']

        with open(evtx, 'r') as f:
            buf = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            fh = FileHeader(buf, 0x00)
            h_out = "<?xml version='1.0' encoding='utf-8' standalone='yes'" \
                    " ?>\n<Events>"
            xml_file_handler.write(h_out)

            # hack to get the length of an xml with the xml deceleration.
            d_len = len(minidom.Document().toxml())

            for str_xml, record in Evtx.Views.evtx_file_xml_view(fh):
                xml_doc = minidom.parseString(str_xml.replace("\n", ""))
                event_id = xml_doc.getElementsByTagName(
                    "EventID")[0].childNodes[0].nodeValue
                if event_id not in interested_events:
                    continue
                xml_file_handler.write(xml_doc.toprettyxml()[d_len:])

            buf.close()
            end_tag = "</Events>"
            xml_file_handler.write(end_tag)
            xml_file_handler.close()
        return f1

    def process(self, f):
        """

        :param f:
        :return:
        """
        self.processed_count += 1

        # Create logger object.
        logger = logging.getLogger('evtx_ripper')
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler('evtx_ripper.log')
        logger.addHandler(fh)

        filename_w_ext = os.path.basename(f)
        filename, file_extension = os.path.splitext(filename_w_ext)
        print("Processing {}".format(filename_w_ext))

        out = self.evtx_to_xml(f)
        out = os.path.abspath(os.path.join(os.curdir,
                                           "results_{}.xml".format(out)))

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

        os.remove(out)


def main():
    # Create logger object.
    logger = logging.getLogger('evtx_ripper')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('evtx_ripper.log')
    logger.addHandler(fh)

    # Handle options parsing.
    sample_msg = "\r\n" \
                 "Example:\r\n" \
                 "extv-ripper -d -i " \
                 "'C:/Users/abishop/Downloads/evtx_files' -o " \
                 "'C:/Users/abishop/sql_out'"

    parser = OptionParser()
    parser.add_option("-c", "--cores", dest="cores", type="int", default=4,
                      help="number of cores for processing.")
    parser.add_option("-C", "--csv", dest="csv", default=False,
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

    if opts.csv is False and opts.sql is False:
        msg = "No parser type selected. Please choose from the options " \
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

    core_count = multiprocessing.cpu_count()
    if opts.cores >= core_count:
        logger.error("Number of cores given is equal to or greater than the "
                     "number of actual cores available. avail.{}"
                     "try reducing this number.".format(core_count))
        exit(1)

    logger.info("collecting Evtx files.")
    files = []
    if opts.evtx:
        files = collect_files(opts.input, ".evtx")
    else:
        files = collect_files(opts.input, ".log")
    if files is None:
        logging.error("No files found.")
        exit(1)
    logger.info("Files found: {}".format(len(files)))
    # chunk up here. 18
    chunks = [files[x:x + opts.cores] for x in range(0, len(files),
                                                     opts.cores)]
    logger.info("Chunked up evtx files")

    # create instance on ripper here
    c_len = len(chunks)
    count = 1
    for x in chunks:
        logger.info(
            "Processing chunk: {} of {} - There are {} files \n {}".format(
                count, c_len, len(x), x)
        )
        e = Ripper(opts)
        logger.info("")
        with concurrent.futures.ProcessPoolExecutor(opts.cores) as executor:
            executor.map(e.process, x)
        count += 1

    logger.info("Success!")
    sys.exit()


# # testing stub.
if __name__ == "__main__":
    main()

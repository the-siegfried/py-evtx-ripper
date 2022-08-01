import logging
import multiprocessing
import re
import xml.etree.ElementTree as ElemTree
from sqlite3 import Error
from sqlite3 import connect as connector


class XML2SQL:
    def __init__(self, input_file, output_file):
        """Initialize the class with the paths to the input xml file
        and the output sql file.

        :param input_file: input xml file path.
        :param output_file: output sql filename.
        """

        self.output_buffer = []
        self.sql_insert = None
        self.output = None
        self.num_insert = 0

        # Create logger object.
        self.logger = multiprocessing.get_logger()
        self.logger.info("I got logz")

        self.cursor = None

        # Open the xml file for iteration.
        self.context = ElemTree.iterparse(
            source=input_file, events=("start", "end"),
            parser=ElemTree.XMLParser(encoding='utf-8')
        )

        # Create output file handle.
        self.conn = connector(output_file)

        create_event_table_sql = """CREATE TABLE IF NOT EXISTS event (
                id varchar(32));"""

        # Create Event Table if it does not already exist.
        if self.conn is not None:
            try:
                cur = self.conn.cursor()
                cur.execute(create_event_table_sql)
            except Error as e:
                self.logger.error(e)
                print(e)

        self.cur = None

    def convert(self):
        """ Conversion function.
            Performs the heavy lifting to parse and insert events into the
            table and alters the table to handle previously undefined fields.
        :return: None
        """
        columns = []
        values = []
        self.cursor = self.conn.cursor()

        for event, element in self.context:
            if not element.tag.endswith("Event"):
                continue
            children = list(element)
            for child in children:
                title = re.sub('{.+}', '', child.tag)
                if title == "System":
                    for child_elem in child:
                        if child_elem.attrib:
                            for key in list(child_elem.attrib):
                                columns.append(
                                    f'{title.lower()}_{key.lower()}')

                                val = child_elem.attrib.get(key)
                                values.append('' if val is None else
                                              val.strip().replace('"', r'""'))

                        header = re.sub('{.+}', '', child_elem.tag)
                        columns.append(f'{title.lower()}_{header.lower()}')
                        txt = child_elem.text
                        if txt:
                            txt.strip().replace('"', r'""')   # noqa
                        values.append('' if txt is None else txt)
                elif title == "EventData":
                    for child_elem in child:
                        if child_elem.attrib:
                            for k in list(child_elem.attrib):
                                if k == "Name":
                                    columns.append("{}".format(
                                        child_elem.attrib.get(k)).lower())
                        data = '' if child_elem.text is None else \
                            child_elem.text.strip().replace('"', r'""')  # noqa
                        values.append(data)
                else:
                    msg = "[Fatal Error]:: Could not identify child " \
                          "element {}".format(child.tag)
                    print(msg)

            # Wake up cursor in order to get the table description.
            all_records = self.cursor.execute("select * from event limit 1")

            # Create list of new and existing columns.
            cols = list(map(lambda x: x[0], self.cursor.description))
            new_cols = [col for col in columns if col not in cols]

            for col in new_cols:
                if col == '':
                    continue
                # Insert new columns.
                self.cursor.execute(
                    "ALTER TABLE event ADD COLUMN {} TEXT".format(
                        col))
                self.conn.commit()
            # Insert events into database.
            if len(columns) >= 1 and len(values) >= 1:
                insert_record = "INSERT INTO event ({}) VALUES ({});".format(
                    ', '.join(columns), str(values)[1:-1])
                self.cursor.execute(insert_record)
                self.conn.commit()

            # Clear columns and values .
            columns = []
            values = []

        self.conn.close()


# Test Stub.
if __name__ == "__main__":
    import os

    # db = sqlite3.connect("results.sql")

    xml2sql = XML2SQL(input_file="{}/results.xml".format(os.curdir),
                      output_file="results.sql")
    xml2sql.convert()

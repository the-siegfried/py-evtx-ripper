import re
import codecs
import xml.etree.ElementTree as ET


class XML2CSV:
    def __init__(self, input_file, output_file, encoding="utf-8"):
        """
        Init XML2CSV class.

        :param input_file: path to XML input file.
        :param output_file: path of CSV output path.
        :param encoding: file encoding - default = utf-8
        """

        self.output_buffer = []
        self.output = None

        # Open the xml file for iteration.
        self.context = ET.iterparse(input_file, events=("start", "end"))

        # Open/Create output file handle.
        try:
            self.output = codecs.open(output_file, "w", encoding=encoding)
        except IOError:
            print("Failed to open output file")
            raise

    def convert(self, tag, delimiter=",", ignore=None, no_header=False,
                limit=-1, buffer_size=1000, quotes=True):
        """

        :param tag:
        :param delimiter:
        :param ignore:
        :param no_header:
        :param limit:
        :param buffer_size:
        :param quotes:
        :return:
        """

        if ignore is None:
            ignore = []

        items = []
        header_line = []
        processed_fields = []
        child_elem = None
        tagged = False
        n = 0

        # Iterate over the xml.
        for event, elem in self.context:
            if not elem.tag.endswith("Event"):
                continue

            children = elem.getchildren()
            for child in children:
                title = re.sub('{.+}', '', child.tag)
                if title == "System":
                    for child_elem in child:
                        if child_elem.attrib:
                            for key in list(child_elem.attrib):
                                header_line.append(f'{title}_{key}')
                                val = elem.attrib.get(key)
                                items.append('' if val is None else
                                             val.strip().replace('"', r'""'))
                        header = re.sub('{.+}', '', child_elem.tag)
                        header_line.append(f'{title}_{header}')
                        value = child_elem.text.strip().replace('"', r'""')     # noqa
                        items.append('' if child_elem.text is None else value)
                    processed_fields.append(child_elem.tag)
                elif title == "EventData":
                    header_line.append("{}_Data".format(title))
                    record = {}
                    header = ''
                    data = ''
                    for child_elem in child:
                        if child_elem.attrib:
                            for k in list(child_elem.attrib):
                                if k == "Name":
                                    header = child_elem.attrib.get(k)
                        data = '' if child_elem.text is None else \
                            child_elem.text.strip().replace('"', r'""')     # noqa
                        record[header] = data
                    items.append(str(record))
                else:
                    msg = "[Fatal Error]:: Could not identify child " \
                          "element {}".format(child.tag)
                    print(msg)

            if header_line and not tagged:
                self.output.write(delimiter.join(header_line) + '\n')
            tagged = True

            # Send the csv to buffer.
            if quotes:
                self.output_buffer.append(
                    r'"' + (r'"' + delimiter + r'"').join(items) + r'"')
            else:
                self.output_buffer.append(delimiter.join(items))
            items = []
            n += 1

            # Halt if the specified limit has been hit.
            if n == limit:
                break

            # Flush buffer to disk.
            if len(self.output_buffer) > buffer_size:
                self._write_buffer()

            elem.clear()  # Discard element and recover memory.

        self._write_buffer()  # Write rest of the buffer to file.
        self.output.close()

        return n

    def _write_buffer(self):
        """ Write Buffer
            Writes records from buffer to the output file.
        :return: None
        """

        self.output.write('\n'.join(self.output_buffer) + '\n')
        self.output_buffer = []

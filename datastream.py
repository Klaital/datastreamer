import datetime
import sys
import math


def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


# print_header_handler returns a handler to be used with DataStreamer.
# It prints the header as a CSV/TSV (or any other separator)
def print_header_handler(file_handle, separator='\t'):
    def f(header):
        file_handle.write(separator.join([i[0] for i in header]))
        file_handle.write('\n')

    return f


# print_row_handler returns a handler to be used with DataStreamer.
# It prints each row as a CSV/TSV (or any other separator)
def print_row_handler(file_handle, separator='\t'):
    def f(row):
        file_handle.write(separator.join(str(field) for field in row))
        file_handle.write('\n')

    return f


class DataStreamer:
    def __init__(self, dbconn, row_handler, header_handler=None):
        self.header_handler = header_handler
        self.row_handler = row_handler
        self.db = dbconn

    def run_query(self, query, params):
        # keep track of what row number we're processing overall
        overall_rows_handled = 0

        # Run the query one row at a time
        # Track how many rows are processed, and bytes read, per second
        rows_handled = 0
        bytes_handled = 0
        start_time = datetime.datetime.now()  # Track how long the whole process took
        window_start = datetime.datetime.now()  # Track how many bytes/rows were processed in a single time interval
        self.db.execute(query, params)

        row = self.db.fetchone()
        if self.header_handler is not None:
            self.header_handler(self.db.description)
        while row is not None:
            row_end_time = datetime.datetime.now()
            rows_handled += 1
            overall_rows_handled += 1
            bytes_handled += sys.getsizeof(row)
            if row_end_time - window_start > datetime.timedelta(seconds=1):
                # TODO: calculate bytes/s more precisely using the actual window duration rather than assuming exactly 1 second.
                # \r causes this line to rewrite itself each time
                sys.stdout.write("\rline {0}: {1} rows/s\t{2} bytes/s".format(overall_rows_handled, rows_handled,
                                                                              convert_size(bytes_handled)))
                # Reset the stats
                rows_handled = 0
                bytes_handled = 0
                window_start = datetime.datetime.now()

            # Run user's code for each row
            self.row_handler(row)
            row = self.db.fetchone()

        sys.stdout.write("\n {0} rows processed in {1} seconds\n".format(overall_rows_handled, (datetime.datetime.now() - start_time).total_seconds()))

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

class DataStreamer:
    def __init__(self, dbconn, row_handler):
        self.row_handler = row_handler
        self.db = dbconn

    def run_query(self, query, params):
        # Run the query one row at a time
        # Track how many rows are processed, and bytes read, per second
        rows_handled = 0
        bytes_handled = 0
        start_time = datetime.datetime.now()    # Track how long the whole process took
        window_start = datetime.datetime.now()  # Track how many bytes/rows were processed in a single time interval
        cursor = self.db.execute(query, params)

        row = cursor.fetchone()
        while row is not None:
            row_end_time = datetime.datetime.now()
            rows_handled += 1
            bytes_handled += sys.getsizeof(row)
            if row_end_time - window_start > datetime.timedelta(seconds=1):
                # \r causes this line to rewrite itself each time
                # TODO: calculate bytes/s more precisely using the actual window duration rather than assuming exactly 1 second.
                sys.stdout.write("\r{0} rows/s\t{1} bytes/s".format(rows_handled, convert_size(bytes_handled)))
                # Reset the stats
                rows_handled = 0
                bytes_handled = 0
                window_start = datetime.datetime.now()

            # Run user's code for each row
            self.row_handler(row)

import sqlite3
from datastream import DataStreamer, print_header_handler, print_row_handler

con = sqlite3.connect('database.db')
cur = con.cursor()
row_count = 0

f_out = open('test.csv', 'w')

header_handler = print_header_handler(f_out, ',')
row_handler = print_row_handler(f_out, ',')

streamer = DataStreamer(cur, row_handler, header_handler)
streamer.run_query("SELECT * FROM ids", ())

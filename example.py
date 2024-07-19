
import sqlite3
import uuid
from datastream import DataStreamer

con = sqlite3.connect('database.db')
cur = con.cursor()
row_count = 0


def counter(db_row):
    global row_count
    row_count += 1

streamer = DataStreamer(cur, counter)
streamer.run_query("SELECT * FROM ids", ())


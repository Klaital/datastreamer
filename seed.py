import sqlite3
import uuid

con = sqlite3.connect('database.db')
cur = con.cursor()
cur.execute("CREATE TABLE ids (id INTEGER PRIMARY KEY, log VARCHAR(255))")
con.commit()
for i in range(1000000000):
    newid = uuid.uuid4()
    cur.execute("INSERT INTO ids VALUES (?, ?)", (i, str(newid)))
    con.commit()


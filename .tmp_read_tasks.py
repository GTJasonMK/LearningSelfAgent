import json
import sqlite3
conn = sqlite3.connect(r'E:\Code\LearningSelfAgent\backend\data\agent.db')
conn.row_factory = sqlite3.Row
for row in conn.execute('select id, title, status from tasks order by id desc limit 3'):
    print(json.dumps(dict(row), ensure_ascii=False))

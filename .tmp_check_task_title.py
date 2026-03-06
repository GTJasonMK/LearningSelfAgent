import sqlite3
expected = '请你帮我收集最近三个月的黄金的价格数据，单位元/克，并保存为csv文件'
conn = sqlite3.connect(r'E:\Code\LearningSelfAgent\backend\data\agent.db')
row = conn.execute('select title from tasks where id=1').fetchone()
actual = row[0] if row else None
print('MATCH=' + ('1' if actual == expected else '0'))
print('LEN=' + str(len(actual or '')))

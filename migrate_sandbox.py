import sqlite3, os
db = "pwbot_overshoot.db"
if os.path.exists(db):
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE positions SET is_sandbox=1 WHERE city IN ('Tel Aviv','Sao Paulo') AND (is_sandbox IS NULL OR is_sandbox=0)")
        print("Migracion sandbox OK")
else:
    print("DB no existe aun")

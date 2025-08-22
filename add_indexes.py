import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "baseball.db")

def add_indexes():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_pitching_playerid ON lahman_pitching(playerid)",
        "CREATE INDEX IF NOT EXISTS idx_batting_playerid ON lahman_batting(playerid)", 
        "CREATE INDEX IF NOT EXISTS idx_awards_playerid ON lahman_awardsplayers(playerid)",
        "CREATE INDEX IF NOT EXISTS idx_war_bbref ON jeffbagwell_war(key_bbref)",
        "CREATE INDEX IF NOT EXISTS idx_people_playerid ON lahman_people(playerid)",
        "CREATE INDEX IF NOT EXISTS idx_allstar_playerid ON lahman_allstarfull(playerid)"
    ]
    
    for index_sql in indexes:
        cursor.execute(index_sql)
        print(f"Created: {index_sql}")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    add_indexes()

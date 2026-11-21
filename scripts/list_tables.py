import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.utils.database import db
df = db.fetch_dataframe("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
print(df.to_string())

import pandas as pd
import re

df = pd.DataFrame({'attractionName': ['Superman Roller Coaster', 'Water Ride', 'Indoor dark ride', None]})
coaster_patterns = [r"coaster", r"express"]
coaster_regex = re.compile("|".join(coaster_patterns), re.IGNORECASE)

# test str.contains
df['is_coaster'] = df['attractionName'].str.contains(coaster_regex).fillna(False).astype(int)
print(df)

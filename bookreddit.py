from psaw import PushshiftAPI
from datetime import datetime


api = PushshiftAPI()

query = '"Lock Every Door" OR "lock every door" AND Riley Sager"'
gen = api.search_comments(q=query)
cache=[]

for c in gen:
    cache.append(c)
    if len(cache) >= 100:
        break

for c in cache:
    print(c.body)
    print(datetime.fromtimestamp(c.created_utc).strftime('%c'))

import praw

reddit = praw.Reddit(
    client_id="AEOyGS56szqF-niro5cP7Q",
    client_secret="6DcNZPWE_2wn0INNSmxVrL1_TAKO1g",
    password="Ab@12345",
    user_agent="testscript by /u/Low_Boysenberry7068",  
    username="Low_Boysenberry7068",
)
import json
import pprint
data = reddit.subreddit("all").hot(limit=1)
row = next(data)
p = row.comments
print(p.list())
# data = vars(next(data))

result = []
for item in data:
    try:
        print(item.parent_id)
    except Exception as e:
        print("no parent")
    result.append({
        'id': item.id, 
        'url': item.permalink, 
        'text': item.selftext, 
        'likes': item.score, 
        'dataType': "0", 
        'community': item.subreddit.display_name,
        'username': item.author.name,
        'timestamp': item.created_utc
    })
    print(result)
import requests
import json
from fake_useragent import UserAgent

# headers = UserAgent().random
headers = UserAgent().random
# print(headers)
url="https://www.lfd.uci.edu/~gohlke/pythonlibs/"
print("4")
req = requests.get(url, headers={"User-Agent": headers}, timeout=12)
data = ''
print("3")

print(req.text)
print(data)

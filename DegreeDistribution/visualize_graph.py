import json

file = open('output/part-00000')
data = file.read()
data = data.replace("\'", "\"")
data = json.loads(data)
print(data)

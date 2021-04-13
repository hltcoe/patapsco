import json

with open('en_mini_dataset.jsonl') as fp:
    for line in fp:
        data = json.loads(line)
        print(f"002 0 {data['id']} 0") 

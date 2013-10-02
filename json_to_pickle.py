import pickle, json
r = open("seeds.json", 'r')
w = open("seeds.txt", 'w')
w.write(pickle.dumps(json.loads(r.read())))

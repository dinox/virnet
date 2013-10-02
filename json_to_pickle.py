import pickle, json


def main():
    r = open("seeds.json", 'r')
    w = open("seeds.txt", 'w')
    w.write(pickle.dumps(json.loads(r.read())))

if __name__ == "__main__":
    main()

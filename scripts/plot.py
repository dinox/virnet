#!/usr/bin/env python
import os, sys, getopt

def prepare_latency_file(filename):
    # get start time
    f = open(filename, "r")
    first = f.readline().replace("[", "").replace("]", "").split(",")
    starttime = float(first[4])
    f.close()

    # process file
    f = open(filename, "r")
    out = open("tmp.dat", "w")
    for line in f:
        values = line.replace("[", "").replace("]", "").split(",")
        t = (float(values[4]) - starttime) / 60
        l = float(values[2])*1000
        a = float(values[3])*1000
        msg = str(t) + " " + str(l) + " " + str(a)
        out.write(msg + "\n")
        print(msg)
    f.close()
    out.close()

def main(argv):
    filename = "latency.log"
    try:
        opts, args = getopt.getopt(argv,"f",["file="])
    except getopt.GetoptError:
        print('plot.py --file <filename>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('plot.py --file <filename>')
            sys.exit()
        elif opt in ("-f", "--file"):
            filename = arg
        else: 
            print('plot.py --file <filename>')
            sys.exit(2)
    prepare_latency_file(filename)


if __name__ == "__main__":
        main(sys.argv[1:])

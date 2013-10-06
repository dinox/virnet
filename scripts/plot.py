#!/usr/bin/env python
import os, sys, getopt

def prepare_latency_file(filename, nodeID):
    # get start time
    f = open(filename, "r")
    first = f.readline().replace("[", "").replace("]", "").split(",")
    starttime = float(first[4])
    f.close()

    # process file
    f = open(filename, "r")
    out = open("tmp.dat", "w")
    total = 0
    skipped = 0
    for line in f:
        values = line.replace("[", "").replace("]", "").split(",")
        if str(nodeID) == str(values[1].strip()):
            t = (float(values[4]) - starttime) / 60
            l = float(values[2])*1000
            a = float(values[3])*1000
            msg = str(t) + " " + str(l) + " " + str(a)
            out.write(msg + "\n")
        else:
            skipped = skipped + 1
        total = total + 1
    f.close()
    out.close()
    print("Skipped " + str(skipped) + " of total " + str(total))

def produce_plot(output):
    os.system("gnuplot latency_timeline.p")
    os.system("rm tmp.dat")
    os.system("mv out.pdf " + output)
    os.system("evince " + output + " &")

def main(argv):
    filename = "latency.log"
    nodeID = 0
    output = "latency.pdf"
    helpmessage = "Usage: plot.py [OPTION]... \n\t-f, --file [filename]\tInput file" + \
    "name (latency.log from the overlay network)\n\t-i, --id [nodeID]\t nodeID that" + \
    "is selected to plot the latency\n\t-o, --out [filename]\t Output filename" + \
    "(pdf)"
    try:
        opts, args = getopt.getopt(argv,"f:i:o:",["file=", "id=", "out="])
    except getopt.GetoptError:
        print(helpmessage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(helpmessage)
            sys.exit()
        elif opt in ("-f", "--file"):
            filename = arg
        elif opt in ("-i", "--id"):
            nodeID = arg
        elif opt in ("-o", "--out"):
            output = arg
        else: 
            print(helpmessage)
            sys.exit(2)
    print("Plot latency to node" + str(nodeID))
    prepare_latency_file(filename, nodeID)
    produce_plot(output)

if __name__ == "__main__":
        main(sys.argv[1:])

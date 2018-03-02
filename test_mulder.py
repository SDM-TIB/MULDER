#!/usr/bin/env python3.5

from multiprocessing import Process, Queue, active_children

from mulder.molecule.MTManager import ConfigFile
from mulder.mediator.decomposition.MediatorDecomposer import MediatorDecomposer
from mulder.mediator.planner.MediatorPlanner import MediatorPlanner
from mulder.mediator.planner.MediatorPlanner import contactSource as contactsparqlendpoint
import sys, os, signal, getopt

from time import time

__author__ = 'kemele'


def nexttime(time1):
    global t1
    global tn
    global c1
    global cn

    time2 = time() - time1
    tn = time2


def conclude(res, p2, printResults, traces=True):

    signal.signal(12, onSignal2)
    global t1
    global tn
    global c1
    global cn
    ri = res.get()

    if printResults:
        if (ri == "EOF"):
            nexttime(time1)
            print("Empty set.")
            printInfo()
            return

        while ri != "EOF":
            cn = cn + 1
            if cn == 1:
                time2 = time() - time1
                t1 = time2
                c1 = 1

            print(ri)
            if traces:
                nexttime(time1)
                printtraces()
            ri = res.get(True)

        nexttime(time1)
        printInfo()

    else:
        if ri == "EOF":
            nexttime(time1)
            printInfo()
            return

        while ri != "EOF":
            cn = cn + 1
            if cn == 1:
                time2 = time() - time1
                t1 = time2
                c1 = 1

            if traces:
                nexttime(time1)
                printtraces()
            ri = res.get(True)

        nexttime(time1)
        printInfo()


def printInfo():
    global tn
    if tn == -1:
        tn = time() - time1
    l = (qname + "\t" + str(dt) + "\t" + str(pt) + "\t" + str(t1) + "\t" + str(tn) + "\t" + str(c1) + "\t" + str(cn))

    print (l)


def printtraces():
    global tn
    if tn == -1:
        tn = time() - time1
    l = (qname + "," + "MULDER," + str(cn) + "," + str(tn))

    print (l)


def onSignal1(s, stackframe):
    cs = active_children()
    for c in cs:
      try:
        os.kill(c.pid, s)
      except OSError as ex:
        continue
    sys.exit(s)


def onSignal2(s, stackframe):
    printInfo()
    sys.exit(s)


def get_options(argv):
    try:
        opts, args = getopt.getopt(argv, "h:q:c:s:")
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    query = None
    '''
    Supported output formats:
        - json (default)
        - nt
        - SPARQL-UPDATE (directly store to sparql endpoint)
    '''
    config = 'config/config.json'
    isstring = -1
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt == "-q":
            query = arg
        elif opt == "-c":
            config = arg

    if not query:
        usage()
        sys.exit(1)

    # TODO: validate file path and sparql-endpoint capability (Update capability)

    return query, config, isstring


def usage():
    usage_str = ("Usage: {program} -q <query>  \n"
                 "-c <path/to/config.json> \n"
                 "-s <isstring> \n "
                 "where \n"
                 "\t<query> - SPARQL QUERY  \n"
                 "\t<path/to/config.json> - path to configuration file  \n"
                 "\t<isstring> - (Optional) set if <query> is sent as string: \n"
                 "\t\tavailable values 1 or -1. -1 is default, meaning query is from file\n")

    print (usage_str.format(program=sys.argv[0]),)


if __name__ == '__main__':

    # query, config, isstring = get_options(sys.argv[1:])
    # if isstring == 1:
    #     queryss = query.decode()
    # else:
    #     queryss = open(query).read()
    #
    queryss = open('queries/doi').read()
    config = 'config/pubs.json'

    config = ConfigFile(config)
    tempType = "MULDER"
    joinstarslocally = False

    global time1
    global qname
    global t1
    global tn
    global c1
    global cn
    global dt
    global pt
    c1 = 0
    cn = 0
    t1 = -1
    tn = -1
    dt = -1
    qname = "Q"
    time1 = time()
    dc = MediatorDecomposer(queryss, config, tempType, joinstarslocally)
    print ('Query:', queryss)

    quers = dc.decompose()
    dt = time() - time1
    #print type(quers)
    print ("Decomposed qeury:", quers)
    print ("=======================================================")
    if quers is None:
        print ("Query decomposer returns None")
        exit()

    planner = MediatorPlanner(quers, True, contactsparqlendpoint, None, config)
    plan = planner.createPlan()
    pt = time() - time1
    print ("Query execution Plan:", plan)

    output = Queue()
    #plan.execute(output)
    print ("*+*+*+*+*+*+*+*+*+*+*+*+Result*+*+*+*+*+*+*+++++")
    i = 0
    p2 = Process(target=plan.execute, args=(output,))
    p2.start()
    p3 = Process(target=conclude, args=(output, p2, True, False))
    p3.start()
    signal.signal(12, onSignal1)

    while True:
        if p2.is_alive() and not p3.is_alive():
            try:
                os.kill(p2.pid, 9)
            except Exception as ex:
                continue
            break
        elif not p2.is_alive() and not p3.is_alive():
            break


    # while True:
    #     r = output.get()
    #     i += 1
    #     #print r
    #     if r == "EOF":
    #         print "END of results ...."
    #         break
    #     #print "total: ", i
#!/usr/bin/env python3.5

__author__ = 'kemele'

import getopt
import string

#from ontario.mediator.planner import Plan
#from ontario.mediator.planner.Plan import contactSource, contactMetaWrapper
#from ontario.molecule.Config import Arango
#from ontario.molecule.MTManager import Arango

import sys, os, signal

from multiprocessing import Process, Queue, active_children, Manager
from time import time
import logging


from mulder.molecule.MTManager import ConfigFile
from mulder.mediator.decomposition.MediatorDecomposer import MediatorDecomposer
from mulder.mediator.planner.MediatorPlanner import MediatorPlanner
from mulder.mediator.planner.MediatorPlanner import contactSource

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('.decompositions.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def runQuery(queryfile, configfile, tempType, isEndpoint, res, qplan, adaptive, withoutCounts, printResults, result_folder, joinlocally=False):

    '''if isEndpoint:
        contact = contactSource
    else:
        contact = contactWrapper
    '''
    query = open(queryfile).read()
    pos = queryfile.rfind("/")
    qu = queryfile[pos+1:]
    pos2 = qu.rfind(".")

    if pos2 > -1:
        qu = qu[:pos2]
    global qname
    global t1
    global tn
    global c1
    global cn
    global dt
    global pt
    global resulttime
    global resulttrace

    resulttime = open(result_folder+"/results.csv", 'aw+')
    # resulttime.write("qname,decompositionTime,planningTime,firstResult,overallTime,moreResults,cardinality")
    resulttrace = open(result_folder + "/traces.csv", 'aw+')
    # resulttrace.write("qname,cardinality,time")

    c1 = 0
    cn = 0
    t1 = -1
    tn = -1
    dt = -1
    global time1
    qname = qu
    endpointType = 'V'
    logger.info("Query: " + qname)

    config = ConfigFile(configfile)

    time1 = time()

    mdq = MediatorDecomposer(query, config, tempType, joinstarslocally=joinlocally)
    new_query = mdq.decompose()

    dt = time() - time1

    if new_query is None: # if the query could not be answered by the endpoints
        time2 = time() - time1
        t1 = time2
        tn = time2
        pt = time2
        printInfo()
        printtraces()
        resulttrace.close()
        resulttime.close()
        return

    planner = MediatorPlanner(new_query, adaptive, contactSource, endpointType, config)
    plan = planner.createPlan()

    logger.info("Plan:")
    logger.info(plan)

    pt = time() - time1
    p2 = Process(target=plan.execute, args=(res,))
    p2.start()
    p3 = Process(target=conclude, args=(res, p2, printResults))
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


def conclude(res, p2, printResults, traces=True):

    signal.signal(12, onSignal2)
    global t1
    global tn
    global c1
    global cn
    global time1

    ri = res.get()

    if printResults:
        if ri == "EOF":
            nexttime(time1)
            print ("Empty set.")
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
            printtraces()
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

    global resulttime
    global resulttrace

    resulttrace.close()
    resulttime.close()
    p2.terminate()


def nexttime(time1):
    global tn

    time2 = time() - time1
    tn = time2


def printInfo():
    global tn
    global resulttime
    global cn
    if tn == -1:
        tn = time() - time1
    lr = (qname + "\t" + str(dt) + "\t" + str(pt) + "\t" + str(t1) + "\t" + str(tn) + "\t" + str(c1) + "\t" + str(cn))

    print(lr)
    resulttime.write('\n' + lr)
    logger.info(lr)


def printtraces():
    global tn
    global resulttrace
    global cn

    if tn == -1:
        tn = time() - time1
    l = (qname + "," + str(cn) + "," + str(tn))

    #print l
    resulttrace.write('\n' + l)


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
        opts, args = getopt.getopt(argv, "h:c:q:t:s:r:j:p:")
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    configfile = None
    queryfile = None
    #buffersize = 1638400
    tempType = "MULDER"
    isEndpoint = True
    plan = "b"
    adaptive = True
    withoutCounts = False
    printResults = False
    planonly = False
    joinlocally = False
    result_folder = './'
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt == "-c":
            configfile = arg
        elif opt == "-q":
            queryfile = arg
        elif opt == "-t":
            tempType = arg
        elif opt == "-s":
            isEndpoint = arg == "True"
        elif opt == '-r':
            result_folder = arg
        elif opt == '-p':
            planonly = eval(arg)
        elif opt == '-j':
            joinlocally = eval(arg)

    if not configfile or not queryfile:
        usage()
        sys.exit(1)

    return (configfile, queryfile, tempType, isEndpoint, plan, adaptive, withoutCounts, printResults, result_folder, joinlocally)


def usage():
    usage_str = ("Usage: {program} -c <config.json_file>  -q <query_file> -t "
                 +"<templateType> -s <isEndpoint>  -r <result_folder>"
                 +"\n where \n<isEndpoint> - a boolean value "
                 +"\n<result_folder> - an existing folder to store results.csv and traces.csv"
                  "\n")
    print(usage_str.format(program=sys.argv[0]),)


def main(argv):
    res = Queue()
    time1 = time()
    (configfile, queryfile, buffersize, isEndpoint, plan, adaptive, withoutCounts, printResults, result_folder, joinlocally) = get_options(argv[1:])
    try:
        runQuery(queryfile, configfile, buffersize, isEndpoint, res, plan, adaptive, withoutCounts, printResults, result_folder, joinlocally)
    except Exception as ex:
        print (ex)


if __name__ == '__main__':
    main(sys.argv)

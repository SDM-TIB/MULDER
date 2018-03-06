#!/usr/bin/env python3.5

__author__ = 'kemele'

import getopt
import string
import sys, os, signal
import json

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


def runQuery(query, configfile, tempType, isEndpoint, res, qplan, adaptive, withoutCounts, printResults, result_folder):

    '''if isEndpoint:
        contact = contactSource
    else:
        contact = contactWrapper
    '''

    endpointType = 'V'
    logger.info("Query: " + query)

    config = ConfigFile(configfile)

    mdq = MediatorDecomposer(query, config, tempType)
    new_query = mdq.decompose()
    if new_query is None: # if the query could not be answered by the endpoints
        print ("EOF")
        return

    logger.info(new_query)

    planner = MediatorPlanner(new_query, adaptive, contactSource, endpointType, config)
    plan = planner.createPlan()

    logger.info("Plan:")
    logger.info(plan)

    plan.execute(res)
    while True:
        r = res.get()
        print(json.dumps(r))
        if r == "EOF":
            break


def get_options(argv):
    try:
        opts, args = getopt.getopt(argv, "h:c:q:t:s:r:")
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    configfile = None
    queryfile = None
    tempType = "MULDER"
    isEndpoint = True
    plan = "b"
    adaptive = True
    withoutCounts = False
    printResults = False
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

    if not configfile or not queryfile:
        usage()
        sys.exit(1)

    return (configfile, queryfile, tempType, isEndpoint, plan, adaptive, withoutCounts, printResults, result_folder)


def usage():
    usage_str = ("Usage: {program} -c <config.json_file>  -q <query>\n")
    print (usage_str.format(program=sys.argv[0]),)


def main(argv):
    res = Queue()
    (configfile, queryfile, buffersize, isEndpoint, plan, adaptive, withoutCounts, printResults, result_folder) = get_options(argv[1:])
    try:
        runQuery(queryfile, configfile, buffersize, isEndpoint, res, plan, adaptive, withoutCounts, printResults, result_folder)
    except Exception as ex:
        print (ex)


if __name__ == '__main__':
    main(sys.argv)

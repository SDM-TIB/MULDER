#!/usr/bin/env python

from flask import Flask, request, session
from flask.json import jsonify
import sys
import os
import json
import getopt
from time import time
from multiprocessing import Process, Queue, active_children

from mulder.molecule.MTManager import ConfigFile
from mulder.mediator.decomposition.MediatorDecomposer import MediatorDecomposer
from mulder.mediator.planner.MediatorPlanner import MediatorPlanner
from mulder.mediator.planner.MediatorPlanner import contactSource as clm
import logging
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.INFO)
    fileHandler = logging.FileHandler("{0}.log".format('ontario'))
    fileHandler.setLevel(logging.INFO)
    fileHandler.setFormatter(logFormatter)

    logger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)



__author__ = 'kemele'

app = Flask(__name__)
configuration = None
tempType = "MULDER"
configfile = '/home/kemele/git/MULDER/config/config.json'


@app.route("/sparql", methods=['POST', 'GET'])
def sparql():
    if request.method == 'GET' or request.method == 'POST':
        try:
            query = request.args.get("query", '')
            # query = query.replace('\n', ' ').replace('\r', ' ')
            print('query:', query)
            logger.info(query)

            global configuration
            # if session['configuration'] is None:
            #     configuration = ConfigFile(configfile)
            # else:
            #     configuration = session.get('configuration')
            if configuration is None:
                configuration = ConfigFile(configfile)
            if query is None or len(query) == 0:
                return jsonify({"result": [], "error": "cannot read query"})
            start = time()
            dc = MediatorDecomposer(query, configuration, tempType)
            quers = dc.decompose()
            print ("Mediator Decomposer: \n", quers)
            logger.info(quers)
            if quers is None:
                print("Query decomposer returns None")
                return jsonify({"result": []})

            res = []
            planner = MediatorPlanner(quers, True, clm, None, configuration)
            plan = planner.createPlan()
            print ("Mediator Planner: \n", plan)
            logger.info(plan)
            output = Queue()

            plan.execute(output)
            i = 0
            first = 0
            vars = []
            while True:
                r = output.get()
                if i == 0:
                    first = time() - start
                if r == "EOF":
                    print ("END of results ....")
                    break

                vars = [k for k in r.keys()]
                # print(r)
                res.append(r)
                i += 1

            total = time() - start
            return jsonify(vars=vars, result=res, execTime=total, firstResult=first, totalRows=i)
        except Exception as e:
            print ("Exception: ", e)
            print ({"result": [], "error": e})
            return jsonify({"result": [], "error": str(e)})
    else:
        return jsonify({"result": [], "error": "Invalid HTTP method used. Use GET "})


def usage():
    usage_str = ("Usage: {program} -c <path_to_config>  "
                 + "\n where \n<path_to_config> "
                 + " is configuration file for Ontario "
                 + "\n")

    print(usage_str.format(program=sys.argv[0]),)


def get_options(argv):
    try:
        opts, args = getopt.getopt(argv, "h:c:")
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    configfile = None
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt == "-c":
            configfile = arg
    if not configfile:
        #usage()
        #sys.exit(1)
        configfile = '/data/config.json'

    return configfile


if __name__ == "__main__":

    mapping = ""
    tempType = "MULDER"
    argv = sys.argv

    configfile = get_options(argv[1:])
    try:
        conf = ConfigFile(configfile)
    except:
        conf = ConfigFile("/home/kemele/git/MULDER/config/config.json")
        print("The default DBpedia template is loaded")
    port = 5000
    # config = json.load(open(configfile))
    configuration = conf
    app.run(port=port, host="0.0.0.0")

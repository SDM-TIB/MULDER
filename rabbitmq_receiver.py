#!/usr/bin/env python3
import pika
import sys
import json
import os
import getopt
from time import time
from multiprocessing import Process, Queue, active_children

from mulder.molecule.MTManager import ConfigFile
from mulder.mediator.decomposition.MediatorDecomposer import MediatorDecomposer
from mulder.mediator.planner.MediatorPlanner import MediatorPlanner
from mulder.mediator.planner.MediatorPlanner import contactSource as clm
from mulder.molecule.create_rdfmts import create_rdfmts

import logging
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.INFO)
    fileHandler = logging.FileHandler("{0}/{1}.log".format('/data', 'ontario'))
    fileHandler.setLevel(logging.INFO)
    fileHandler.setFormatter(logFormatter)

    logger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)


def run_program(m):
    print("Executing job ", m)
    logger.info("Executing job " + str(m['jobID']))
    producer(m)


def open_output_connection():
    credentials = pika.PlainCredentials('guest', 'test12')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ['RABBITMQ_IP'], port=os.environ['RABBITMQ_PORT'], credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='iasis.ontario.queue', durable=True)

    return connection, channel


def produce_output(message):
    connection, channel = open_output_connection()
    channel.basic_publish(exchange='iasis.ui.queue',
                          routing_key='iasis.ontario.routingkey',
                          body=message)
    # channel.basic_publish(exchange='iasis.ontario.output.direct',
    #                       routing_key='iasis.ontario.output.routingkey',
    #                       body="EOF")
    connection.close()


def producer(message):

    if 'endpoints' in message:
        endpoints = message['endpoints']
        logger.info("Create RDF_MT command received for endpoints: " + ", ".join(endpoints))

        outputfile, molecules, status = create_rdfmts(endpoints, "/data/iasiskgnew-templates.json")

        if status == 0:
            confile = json.load(open("/data/config.json"))
            found = False
            for c in confile["MoleculeTemplates"]:
                if c['path'] == outputfile:
                    found = True
            if not found:
                config = {"type": "filepath",  "path": outputfile}
                confile['MoleculeTemplates'].append(config)
                with open('/data/config.json', 'w+') as f:
                    json.dump(confile, f)

            dd = json.dumps({'jobID': message['jobID'], 'componentName': "iasis_ontario", "result": molecules, "msg": "RDF_MTs created successfully!"})

            logger.info("RDF-MTs created successfully!")
            logger.info(dd)

            produce_output(dd)

            return
        else:
            msg = "Endpoint list is empty!" if status == -1 else \
                        "None of the endpoints can be accessed. Please check if you write URLs properly!"
            dd = json.dumps({'jobID': message['jobID'], 'componentName': "iasis_ontario", "result": [], "msg": msg})
            produce_output(dd)
            return

    if 'query' in message:
        query = message["query"]
        logger.info("Query received: " + str(query))
        configuration = ConfigFile('/data/config.json')
        if query is None:
            dd = json.dumps({'jobID': message['jobID'], 'componentName': "iasis_ontario", "result": [], "msg": "cannot read query"})
            produce_output(dd)
            return

        dc = MediatorDecomposer(query, configuration, "MULDER")
        quers = dc.decompose()
        print("Mediator Decomposer: \n", quers)
        logger.info("Decomposition: " + str(quers))
        if quers is None:
            print("Query decomposer returns None")
            dd =json.dumps({'jobID': message['jobID'], 'componentName': "iasis_ontario", "result": [], "msg":"Query do not produce any result in this KG!"})
            produce_output(dd)
            return

        planner = MediatorPlanner(quers, True, clm, None, configuration)
        plan = planner.createPlan()

        print("Mediator Planner: \n", plan)
        logger.info("Plan:" + str(plan))

        output = Queue()
        plan.execute(output)
        connection, channel = open_output_connection()
        i = 0
        while True:
            r = output.get()
            if r == "EOF":
                print("END of results ....")
                channel.basic_publish(exchange='iasis.ontario.output.direct',
                                      routing_key='iasis.ontario.output.routingkey',
                                      body=json.dumps({'jobID': message['jobID'], 'componentName': "iasis_ontario", 'result': r}))
                break
            dd = json.dumps({'jobID': message['jobID'], 'componentName': "iasis_ontario", 'result': r})
            i += 1
            channel.basic_publish(exchange='iasis.ontario.output.direct',
                                  routing_key='iasis.ontario.output.routingkey',
                                  body=dd)

        logger.info("Total of " + str(i) + " results produced!")
        print("Total of ", str(i), " results produced!")
        connection.close()


def callback(ch, method, properties, body):
    body = body.decode('utf-8')
    body = json.loads(body)
    logger.info("Job %r has been concluded " % body)
    run_program(body)


def consumer(fcallback):
    credentials = pika.PlainCredentials('guest', 'test12')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ['RABBITMQ_IP'], port=os.environ['RABBITMQ_PORT'], credentials=credentials))
    channel = connection.channel()

    channel.exchange_declare(exchange='iasis.ui.queue', exchange_type='direct', durable=True, auto_delete=False)

    queue_name = 'iasis.ontario.queue'
    channel.queue_declare(queue=queue_name, durable=True, auto_delete=False)
    channel.queue_bind(exchange='iasis.ui.queue', queue=queue_name, routing_key='iasis.ontario.routingkey')

    channel.basic_consume(fcallback,  queue=queue_name, no_ack=True)

    logger.info("MULDER is waiting messages via iasis.ontario.queue .. ")
    channel.start_consuming()


def main():
    consumer(callback)


if __name__ == '__main__':
    main()

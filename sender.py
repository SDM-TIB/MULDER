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


def run_program(m):
    print("Executing job ", m)
    producer(m)


def producer(message):
    # os.environ['LOCAL_IP']
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))
    channel = connection.channel()
    channel.queue_declare(queue='iasis.ontario.queue')
    channel.basic_publish(exchange='iasis.ontario.direct',
                          routing_key='iasis.ontario.routingkey',
                          body=message)
    print(" Orchestrator sent: ", message)
    connection.close()


def callback(ch, method, properties, body):
    body = body.decode('utf-8')
    if body == "EOF":
        exit(0)

    # os.system("echo 'message from the ochestrator " + str(body) + "\n'")

    # print(body)
    body = json.loads(body)
    print(body)
    if body['result'] == "EOF":
        exit(0)
    print(body)
    # print(" [x] %r job %r has been concluded" % (method.routing_key, body))


def consumer(fcallback):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))
    channel = connection.channel()
    channel.exchange_declare(exchange='iasis.orchestratort.direct',
                             exchange_type='direct')

    queue_name = 'iasis.mulder.output.queue'
    channel.queue_declare(queue=queue_name)

    channel.queue_bind(exchange='iasis.orchestrator.direct',
                       queue=queue_name,
                       routing_key='iasis.orchestrator.routingkey')

    channel.basic_consume(fcallback,
                          queue=queue_name,
                          no_ack=True)
    print('I am ochestrator [*] Waiting for executed jobs. To exit press CTRL+C')
    channel.start_consuming()


def main():
    msg = {"JobID": 0, "name":"query1", "query": "SELECT DISTINCT * WHERE {?c a <http://project-iasis.eu/vocab/Drug> . ?c <http://project-iasis.eu/vocab/sameAs> ?o. ?o <http://dbpedia.org/ontology/thumbnail> ?image}  LIMIT 100"}
    rdfmts = {"JobID": 0, "name":"query1", "endpoints": ["http://iasiskgstore:8890/sparql"]}
    producer(json.dumps(msg))
    consumer(callback)


if __name__ == '__main__':
    main()

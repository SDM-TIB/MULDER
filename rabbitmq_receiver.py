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
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ['RABBITMQ_IP'], port=os.environ['RABBITMQ_PORT']))
    channel = connection.channel()
    channel.queue_declare(queue='iasis.mulder.output.queue')
    query = message["query"]
    configuration = ConfigFile('/data/config.json')
    if query is None:
        dd =  json.dumps({"result": [], "error": "cannot read query"})
        channel.basic_publish(exchange='iasis.mulder.output.direct',
                              routing_key='iasis.mulder.output.routingkey',
                              body=dd)
        channel.basic_publish(exchange='iasis.mulder.output.direct',
                              routing_key='iasis.mulder.output.routingkey',
                              body="EOF")
        return
    dc = MediatorDecomposer(query, configuration, "MULDER")
    quers = dc.decompose()
    print("Mediator Decomposer: \n", quers)
    if quers is None:
        print("Query decomposer returns None")
        dd =json.dumps({"result": []})
        channel.basic_publish(exchange='iasis.mulder.output.direct',
                              routing_key='iasis.mulder.output.routingkey',
                              body=dd)
        channel.basic_publish(exchange='iasis.mulder.output.direct',
                              routing_key='iasis.mulder.output.routingkey',
                              body="EOF")
        return

    res = []
    planner = MediatorPlanner(quers, True, clm, None, configuration)
    plan = planner.createPlan()
    print("Mediator Planner: \n", plan)
    output = Queue()

    plan.execute(output)
    while True:
        r = output.get()
        if r == "EOF":
            print("END of results ....")
            channel.basic_publish(exchange='iasis.mulder.output.direct',
                                  routing_key='iasis.mulder.output.routingkey',
                                  body="EOF")
            break
        dd = json.dumps(r)
        channel.basic_publish(exchange='iasis.mulder.output.direct',
                              routing_key='iasis.mulder.output.routingkey',
                              body=dd)

    connection.close()


def callback(ch, method, properties, body):
    # os.system("echo 'message from the ochestrator " + str(body) + "\n'")

    body = body.decode('utf-8')
    # print(body)
    body = json.loads(body)
    print(" [x] %r job %r has been concluded" % (method.routing_key, body))
    run_program(body)


def consumer(fcallback):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ['RABBITMQ_IP'], port=os.environ['RABBITMQ_PORT']))
    channel = connection.channel()

    channel.exchange_declare(exchange='iasis.mulder.query.direct', exchange_type='direct')

    queue_name = 'iasis.mulder.query.queue'
    channel.queue_declare(queue=queue_name)
    channel.queue_bind(exchange='iasis.mulder.query.direct', queue=queue_name, routing_key='iasis.mulder.query.routingkey')

    channel.basic_consume(fcallback,  queue=queue_name, no_ack=True)

    print('I am ochestrator [*] Waiting for executed jobs. To exit press CTRL+C')
    channel.start_consuming()


def main():
    consumer(callback)


if __name__ == '__main__':
    main()

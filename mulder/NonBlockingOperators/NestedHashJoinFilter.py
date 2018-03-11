'''
NestedHashJoin.py

Implements a depending operator, similar to block nested join and symmetric
hash join

Autor: Maribel Acosta
Autor: Maria-Esther Vidal
Autor: Gabriela Montoya
Date: January 29th, 2014

'''
from multiprocessing import Queue, Process
from time import time
import string, sys
from multiprocessing.queues import Empty
from mulder.Operators.Join import Join
#from ontario.mediator.decomposer.Tree import Leaf, Node
from mulder.common.parser import queryParser as qp
from mulder.NonBlockingOperators.OperatorStructures import Table, Partition, Record
from mulder.NonBlockingOperators.NestedHashJoin import NestedHashJoin

WINDOW_SIZE = 10


class NestedHashJoinFilter(Join):

    def __init__(self, vars):
        self.left_table = dict()
        self.right_table = dict()
        self.qresults    = Queue()
        self.vars        = vars
        self.left_queue = Queue()
        self.right_operator = None
        self.qresults = Queue()

    def instantiate(self, d):
        newvars = self.vars - set(d.keys())
        return NestedHashJoin(newvars)

    def instantiateFilter(self, d, filter_str):
        newvars = self.vars - set(d)
        return NestedHashJoin(newvars)

    def execute(self, left_queue, right_operator, out, processqueue=Queue()):
        self.left_queue = left_queue
        self.right_operator = right_operator
        self.qresults = out
        tuple1 = None
        tuple2 = None
        right_queues = dict()
        filter_bag = []
        queue = Queue()
        finalqueue = Queue()
        counter = 0
        try:

            while not(tuple1 == "EOF") or (len(right_queues) > 0):
                try:
                    tuple1 = self.left_queue.get(False)
                    # Try to get and process tuple from left queue
                    if not(tuple1 == "EOF"):
                        instance = self.probeAndInsert1(tuple1, self.right_table, self.left_table, time(), self.qresults)
                        if instance: # the join variables have not been used to
                                     # instanciate the right_operator
                            filter_bag.append(tuple1)

                        if len(filter_bag) >= WINDOW_SIZE:
                            new_right_operator = self.makeInstantiation(filter_bag, self.right_operator)
                            # resource = self.getResource(tuple1)
                            # queue = Queue()
                            # right_queues[count] = queue
                            if "Nested" in new_right_operator.__class__.__name__:
                                new_right_operator.execute(self.right_operator.left_queue, self.right_operator.right_operator, queue, processqueue)
                            else:
                                new_right_operator.execute(queue, processqueue)
                            filter_bag = []
                            counter += 1
                            p = Process(target=self.processBatch, args=(queue, finalqueue, self.qresults,))
                            p.start()
                            processqueue.put(p.pid)

                    else:
                        if (len(filter_bag) > 0):
                            #print "here", len(filter_bag), filter_bag
                            new_right_operator = self.makeInstantiation(filter_bag, self.right_operator)
                            #resource = self.getResource(tuple1)
                            # queue = Queue()
                            # right_queues[count] = queue
                            if "Nested" in new_right_operator.__class__.__name__:
                                new_right_operator.execute(self.right_operator.left_queue, self.right_operator.right_operator, queue, processqueue)
                            else:
                                new_right_operator.execute(queue, processqueue)
                            filter_bag = []
                            counter += 1
                            p = Process(target=self.processBatch, args=(queue, finalqueue, self.qresults,))
                            p.start()
                            processqueue.put(p.pid)

                except Empty:
                        pass
                except Exception as e:
                        #print "Unexpected error:", sys.exc_info()[0]
                        pass
        except Exception as e:
            pass

        finished = 0
        while finished < counter:
            eof = finalqueue.get(True)
            if eof == 'EOF':
                finished += 1

        self.qresults.put("EOF")
        return

    def processBatch(self, inqueue, finalqueue, out):
        try:
            tuple1 = None
            while tuple1 != "EOF":
                try:
                    tuple1 = inqueue.get(False)
                    if tuple1 == "EOF":
                        finalqueue.put("EOF")
                        break
                    else:
                        resource = self.getResource(tuple1)
                        for v in self.vars:
                            del tuple1[v]
                        self.probeAndInsert2(resource, tuple1, self.left_table, self.right_table, time(), out)
                except Empty:
                    pass
        except Exception as e:
            pass
        return

    def processResults(self, inqueue, finalqueue, out):
        try:
            counter = -1
            eofcount = 0
            while counter != -2 and counter == -1 or eofcount != counter:
                tuple1 = None

                while tuple1 != "EOF":
                    try:
                        tuple1 = inqueue.get(False)
                        if tuple1 == "EOF":
                            eofcount += 1
                        else:
                            resource = self.getResource(tuple1)
                            for v in self.vars:
                                del tuple1[v]
                            self.probeAndInsert2(resource, tuple1, self.left_table, self.right_table, time(), out)
                    except Empty:
                        pass
                    if counter == -1:
                        try:
                            counter = finalqueue.get(False)
                        except Empty:
                            pass
                        if eofcount == counter:
                            break
        except Exception:
            # This catch:
            # Empty: in tuple2 = self.right.get(False), when the queue is empty.
            # TypeError: in att = att + tuple[var], when the tuple is "EOF".
            # print "Unexpected error:", sys.exc_info()
            pass

        out.put("EOF")
        return

    def getResource(self, tuple):
        resource = ''
        for var in self.vars:
            resource = resource + tuple[var]
        return resource

    def makeInstantiation(self, filter_bag, operators):
        filter_str = ''
        new_vars = ['?' + v for v in self.vars]  # TODO: this might be $
        filter_str = " . ".join(map(str, operators.tree.service.filters))
        # print "making instantiation join filter", filter_bag
        # When join variables are more than one: FILTER ( )
        if len(self.vars) >= 1:
            filter_str += ' . FILTER (__expr__)'

            or_expr = []
            for tuple in filter_bag:
                and_expr = []
                for var in self.vars:
                    # aux = "?" + var + "==" + tuple[var]
                    v = tuple[var]
                    if v.find("http") == 0:  # uris must be passed between < .. >
                        v = "<" + v + ">"
                    else:
                        v = '"' + v + '"'
                    v = "?" + var + "=" + v
                    and_expr.append(v)

                or_expr.append('(' + ' && '.join(and_expr) + ')')
            filter_str = filter_str.replace('__expr__', ' || '.join(or_expr))
        new_operator = operators.instantiateFilter(set(new_vars), filter_str)
        # print "type(new_operator)", type(new_operator)
        return new_operator

    def probeAndInsert1(self, tuple, table1, table2, time, out):
        #print "in probeAndInsert1", tuple
        record = Record(tuple, time, 0)
        r = self.getResource(tuple)
        #print "resource", r, tuple
        if r in table1:
            records =  table1[r]
            for t in records:
                if t.ats > record.ats:
                    continue
                x = t.tuple.copy()
                x.update(tuple)
                out.put(x)
        p = table2.get(r, [])
        i = (p == [])
        p.append(record)
        table2[r] = p
        return i

    def probeAndInsert2(self, resource, tuple, table1, table2, time, out):
        #print "probeAndInsert2", resource, tuple
        record = Record(tuple, time, 0)
        if resource in table1:
            records =  table1[resource]
            for t in records:
                if t.ats > record.ats:
                    continue
                x = t.tuple.copy()
                x.update(tuple)
                out.put(x)
        p = table2.get(resource, [])
        p.append(record) 
        table2[resource] = p

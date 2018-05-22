#!/usr/bin/env python3.5

import urllib
import urllib.parse as urlparse
import http.client as htclient
from http import HTTPStatus
import requests
import json
import pprint
import os
import random
import sys, getopt, os
from multiprocessing import Queue, Process
from multiprocessing.queues import Empty

metas = [    'http://www.w3.org/ns/sparql-service-description',
             'http://www.openlinksw.com/schemas/virtrdf#',
             'http://www.w3.org/2000/01/rdf-schema#',
             'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
             'http://www.w3.org/2002/07/owl#',
             'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType',
             'nodeID://']


def get_rdfs_ranges(referer, server, path, p, limit=-1):

    RDFS_RANGES = " SELECT DISTINCT ?range  WHERE{ <" + p + "> rdfs:range ?range. }"

    reslist = []
    if limit == -1:
        limit = 50
        offset = 0
        numrequ = 0
        while True:
            query_copy = RDFS_RANGES + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactSource(query_copy, referer, server, path)
            numrequ += 1
            if card == -2:
                limit = limit / 2
                if limit == 0:
                    break
                continue
            if card > 1:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
    else:
        reslist, card = contactSource(RDFS_RANGES, referer, server, path)

    ranges = []

    for r in reslist:
        skip = False
        for m in metas:
            if m in r['range']:
                skip = True
                break
        if not skip:
            ranges.append(r['range'])

    return ranges


def find_instance_range(referer, server, path, t, p, limit=-1):

    INSTANCE_RANGES = " SELECT DISTINCT ?r WHERE{ ?s a <" + t + ">. ?s <" + p + "> ?pt.  ?pt a ?r  } "
    reslist = []
    if limit == -1:
        limit = 50
        offset = 0
        numrequ = 0
        while True:
            query_copy = INSTANCE_RANGES + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactSource(query_copy, referer, server, path)
            numrequ += 1
            if card == -2:
                limit = limit / 2
                if limit == 0:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
    else:
        reslist, card = contactSource(INSTANCE_RANGES, referer, server, path)

    ranges = []

    for r in reslist:
        skip = False
        for m in metas:
            if m in r['r']:
                skip = True
                break
        if not skip:
            ranges.append(r['r'])

    return ranges


def get_concepts(endpoint, limit=-1, outqueue=Queue()):
    """
    Entry point for extracting RDF-MTs of an endpoint.
    Extracts list of rdf:Class concepts and predicates of an endpoint
    :param endpoint:
    :param limit:
    :return:
    """
    query = "SELECT DISTINCT ?t WHERE{ ?s a ?t } "
    referer = endpoint
    if 'https' in endpoint:
        server = endpoint.split("https://")[1]
    else:
        server = endpoint.split("http://")[1]

    (server, path) = server.split("/", 1)
    reslist = []
    if limit == -1:
        limit = 50
        offset = 0
        numrequ = 0
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactSource(query_copy, referer, server, path)
            # print "cardinality:", card
            numrequ += 1
            # print 'number of requests: ', numrequ
            if card == -2:
                limit = limit / 2
                # print ('limit:', limit)
                if limit == 0:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
    else:
        reslist, card = contactSource(query, referer, server, path)

    results = []

    toremove = []
    # [toremove.append(r) for v in metas for r in reslist if v in r['t']]
    for r in reslist:
        for m in metas:
            if m in str(r['t']):
                toremove.append(r)

    for r in toremove:
        reslist.remove(r)

    for r in reslist:

        t = r['t']
        # print t, '\n', 'getting predicates ...'
        preds = get_predicates(referer, server, path, t)
        # print 'getting ranges ...'
        for p in preds:
            rn = {"t": t}
            pred = p['p']
            rn['p'] = pred
            rn['range'] = get_rdfs_ranges(referer, server, path, pred)
            rn['r'] = find_instance_range(referer, server, path, t, pred)
            results.append(rn)
            outqueue.put(rn)

    outqueue.put('EOF')

    return results


def get_predicates(referer, server, path, t, limit=-1):
    """
    Get list of predicates of a class t

    :param referer: endpoint
    :param server: server address of an endpoint
    :param path:  path in an endpoint (after server url)
    :param t: RDF class Concept extracted from an endpoint
    :param limit:
    :return:
    """
    query = " SELECT DISTINCT ?p WHERE{ ?s a <" + t + ">. ?s ?p ?pt. } "
    reslist = []
    if limit == -1:
        limit = 50
        offset = 0
        numrequ = 0
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactSource(query_copy, referer, server, path)
            numrequ += 1
            # print "predicates card:", card
            if card == -2:
                limit = limit / 2
                # print "setting limit to: ", limit
                if limit == 0:
                    rand_inst_res = get_preds_of_random_instances(referer, server, path, t)
                    existingpreds = [r['p'] for r in reslist]
                    for r in rand_inst_res:
                        if r not in existingpreds:
                            reslist.append({'p': r})
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
    else:
        reslist, card = contactSource(query, referer, server, path)

    return reslist


def get_preds_of_random_instances(referer, server, path, t, limit=-1):

    """
    get a union of predicated from 'randomly' selected 10 entities from the first 100 subjects returned

    :param referer: endpoint
    :param server:  server name
    :param path: path
    :param t: rdf class concept of and endpoint
    :param limit:
    :return:
    """
    query = " SELECT DISTINCT ?s WHERE{ ?s a <" + t + ">. } "
    reslist = []
    if limit == -1:
        limit = 50
        offset = 0
        numrequ = 0
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactSource(query_copy, referer, server, path)
            numrequ += 1
            # print "rand predicates card:", card
            if card == -2:
                limit = limit / 2
                # print "rand setting limit to: ", limit
                if limit == 0:
                    break
                continue
            if numrequ == 100:
                break
            if card > 0:
                import random
                rand = random.randint(0, card-1)
                inst = res[rand]
                inst_res = get_preds_of_instance(referer, server, path, inst['s'])
                inst_res = [r['p'] for r in inst_res]
                reslist.extend(inst_res)
                reslist = list(set(reslist))
            if card < limit:
                break
            offset += limit
    else:
        reslist, card = contactSource(query, referer, server, path)

    return reslist


def get_preds_of_instance(referer, server, path, inst, limit=-1):
    query = " SELECT DISTINCT ?p WHERE{ <" + inst + "> ?p ?pt. } "
    reslist = []
    if limit == -1:
        limit = 100
        offset = 0
        numrequ = 0
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactSource(query_copy, referer, server, path)
            numrequ += 1
            # print "inst predicates card:", card
            if card == -2:
                limit = limit / 2
                # print "inst setting limit to: ", limit
                if limit == 0:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
    else:
        reslist, card = contactSource(query, referer, server, path)

    return reslist


def getResults(query, endpoint, limit=-1):
    referer = endpoint
    if 'https' in endpoint:
        server = endpoint.split("https://")[1]
    else:
        server = endpoint.split("http://")[1]
    (server, path) = server.split("/", 1)
    reslist = []
    if limit == -1:
        limit = 50
        offset = 0
        numrequ = 0
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            # print query_copy
            res, card = contactSource(query_copy, referer, server, path)
            # print "cardinality:", card
            numrequ += 1
            # print 'number of requests: ', numrequ
            if card == -2:
                limit = limit / 2
                # print 'limit:', limit
                if limit == 0:
                    break
                continue
            if card > 1:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
    else:
        reslist, card = contactSource(query, referer, server, path)

    types = set()

    toremove =[]
    for r in reslist:
        ifmetas = [True for v in metas if v in r['t']]
        if True in ifmetas:
            toremove.append(r)
            continue
        p = r['p']
        r['range'] = get_rdfs_ranges(referer, server, path, p)
        r['r'] = find_instance_range(referer, server, path, r['t'], p)

    for r in toremove:
        reslist.remove(r)

    return reslist


def contactSource(query, referer, server, path):
    # Formats of the response.
    json = "application/sparql-results+json"
    if '0.0.0.0' in server:
        server = server.replace('0.0.0.0', 'localhost')
    # Build the query and header.
    # params = urllib.urlencode({'query': query})
    params = urlparse.urlencode({'query': query, 'format': json})
    headers = {"Accept": "*/*", "Referer": referer, "Host": server}

    # js = "application/sparql-results+json"
    # params = {'query': query, 'format': js}
    try:
        resp = requests.get(referer, params=params, headers=headers)
        if resp.status_code == HTTPStatus.OK:
            res = resp.text
            reslist = []
            try:
                res = res.replace("false", "False")
                res = res.replace("true", "True")
                res = eval(res)
            except Exception as ex:
                print("EX processing res", ex)

            if type(res) is dict:
                if "results" in res:
                    for x in res['results']['bindings']:
                        for key, props in x.items():
                            # Handle typed-literals and language tags
                            suffix = ''
                            if props['type'] == 'typed-literal':
                                if isinstance(props['datatype'], bytes):
                                    suffix = "^^<" + props['datatype'].decode('utf-8') + ">"
                                else:
                                    suffix = "^^<" + props['datatype'] + ">"
                            elif "xml:lang" in props:
                                suffix = '@' + props['xml:lang']
                            try:
                                if isinstance(props['value'], bytes):
                                    x[key] = props['value'].decode('utf-8') + suffix
                                else:
                                    x[key] = props['value'] + suffix
                            except:
                                x[key] = props['value'] + suffix

                            if isinstance(x[key], bytes):
                                x[key] = x[key].decode('utf-8')

                    reslist = res['results']['bindings']
                    return reslist, len(reslist)
                else:
                    return res['boolean'], 1

        else:
            print("Endpoint->", referer, resp.reason, resp.status_code, query)

    except Exception as e:
        print("Exception during query execution to", referer, ': ', e)

    return None, -2


def get_links(endpoint1, rdfmt1, endpoint2, rdfmt2, q):
    # print 'between endpoints:', endpoint1, ' --> ', endpoint2
    for c in rdfmt1:
        for p in c['predicates']:
            reslist = get_external_links(endpoint1, c['rootType'], p['predicate'], endpoint2, rdfmt2)
            if len(reslist) > 0:
                reslist = [r+"@"+endpoint2 for r in reslist]
                c['linkedTo'].extend(reslist)
                c['linkedTo'] = list(set(c['linkedTo']))
                p['range'].extend(reslist)
                p['range'] = list(set(p['range']))
                # print 'external links found for ', c['rootType'], '->', p['predicate'], reslist
    q.put('EOF')


def get_external_links(endpoint1, rootType, pred, endpoint2, rdfmt2):
    query = 'SELECT DISTINCT ?o  WHERE {?s a <' + rootType + '> ; <' + pred + '> ?o . FILTER (isIRI(?o))}'
    referer = endpoint1
    if 'https' in endpoint1:
        server = endpoint1.split("https://")[1]
    else:
        server = endpoint1.split("http://")[1]
    (server, path) = server.split("/", 1)
    reslist = []
    limit = 50
    offset = 0
    numrequ = 0
    checked_inst = []
    links_found = []
    print("Checking external links: ", endpoint1, rootType, pred, ' in ', endpoint2)
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    while True:
        query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
        res, card = contactSource(query_copy, referer, server, path)
        numrequ += 1
        if card == -2:
            limit = limit / 2
            if limit == 0:
                break

            continue
        if numrequ == 100:
            break
        if card > 0:
            # rand = random.randint(0, card - 1)
            # inst = res[rand]
            #
            # if inst['o'] in checked_inst:
            #     offset += limit
            #     continue
            for inst in res:
                for c in rdfmt2:
                    if c['rootType'] in links_found:
                        continue
                    exists = link_exist(inst['o'], c['rootType'], endpoint2)
                    checked_inst.append(inst['o'])
                    if exists:
                        reslist.append(c['rootType'])
                        links_found.append(c['rootType'])
                        print(rootType, ',', pred, '->', c['rootType'])
            reslist = list(set(reslist))

        if card < limit:
            break

        offset += limit

    return reslist


def link_exist(s, c, endpoint):

    query = "ASK {<" + s + '>  a  <' + c + '> } '
    referer = endpoint
    if 'https' in endpoint:
        server = endpoint.split("https://")[1]
    else:
        server = endpoint.split("http://")[1]
    (server, path) = server.split("/", 1)
    res, card = contactSource(query, referer, server, path)
    if res is None:
        print('bad request on, ', s, c)
    if card > 0:
        if res:
            print("ASK result", res, endpoint)
        return res

    return False


def read_rdfmts(folder):
    files = os.listdir(folder)
    # print 'The following files are being combined:'
    pprint.pprint(files)

    molecules = {}
    # print 'Number of molecules in:'
    for m in files:
        with open(folder + '/' + m) as f:
            rdfmt = json.load(f)
            key = rdfmt[0]['wrappers'][0]['url']
            molecules[key] = rdfmt
            # print '-->', m, '=', len(rdfmt)
    # print 'Total number of endpoints: ', len(molecules)

    return molecules


def combine_single_source_descriptions(rdfmts):

    # print 'The following RDF-MTs are being combined:'
    pprint.pprint(rdfmts.keys())
    molecule_dict = {}
    molecules_tomerge = {}
    molecules = []
    # print 'Number of molecules in:'
    for rdfmt in rdfmts:
        for m in rdfmts[rdfmt]:
            if m['rootType'] in molecule_dict:
                molecules_tomerge[m['rootType']] = [molecule_dict[m['rootType']]]
                molecules_tomerge[m['rootType']].append(m)
                del molecule_dict[m['rootType']]
                continue

            molecule_dict[m['rootType']] = m

        # molecules.extend(rdfmts[rdfmt])
        # print '-->', rdfmt, '=', len(rdfmts[rdfmt])
    for m in molecule_dict:
        molecules.append(molecule_dict[m])

    for root in molecules_tomerge:
        mols = molecules_tomerge[root]
        res = {'rootType': root,
               'linkedTo': [],
               'wrappers': [],
               'predicates': []}
        for m in mols:
            res['wrappers'].append(m['wrappers'][0])
            res['linkedTo'].extend(m['linkedTo'])
            res['linkedTo'] = list(set(res['linkedTo']))
            predicates = {}
            for p in m['predicates']:
                if p['predicate'] in predicates:
                    predicates[p['predicate']]['range'].extend(p['range'])
                    predicates[p['predicate']]['range'] = list(set(predicates[p['predicate']]['range']))
                else:
                    predicates[p['predicate']] = p

            for p in predicates:
                res['predicates'].append(predicates[p])

        molecules.append(res)

    # print 'Total number of molecules: ', len(molecules)

    return molecules


def extractMTLs(endpoint, outqueue=Queue()):
    rdfmolecules = {}
    res = get_concepts(endpoint)
    molecules = {}
    for row in res:
        if row['t'] in molecules:
            found = False
            for p in molecules[row['t']]['predicates']:
                if p['predicate'] == row['p']:
                    ranges = []
                    if 'range' in row and len(row['range']) > 0:
                        ranges.extend(row['range'])
                    if 'r' in row and len(row['r']) > 0:
                        ranges.extend(row['r'])
                    ranges = list(set(ranges))
                    pranges = p['range']
                    pranges.append(ranges)
                    pranges = list(set(pranges))
                    p['range'] = pranges

                    links = molecules[row['t']]['linkedTo']
                    links.append(ranges)
                    links = list(set(links))

                    molecules[row['t']]['linkedTo'] = links

                    found = True

            if not found:
                ranges = []
                if 'range' in row and len(row['range']) > 0:
                    ranges.extend(row['range'])
                if 'r' in row and len(row['r']) > 0:
                    ranges.extend(row['r'])
                ranges = list(set(ranges))

                molecules[row['t']]['predicates'].append({'predicate': row['p'], 'range': ranges})
                molecules[row['t']]['linkedTo'].extend(ranges)
                molecules[row['t']]['linkedTo'] = list(set(molecules[row['t']]['linkedTo']))

            # this should be changed if the number of endpoints are more than one, Note: index 0
            if row['p'] not in molecules[row['t']]['wrappers'][0]['predicates']:
                molecules[row['t']]['wrappers'][0]['predicates'].append(row['p'])
        else:
            molecules[row['t']] = {'rootType': row['t'],
                                   'linkedTo': [],
                                   'wrappers': [{'url': endpoint,
                                                 'urlparam': "",
                                                 'wrapperType': "SPARQLEndpoint",
                                                 'predicates': [row['p']]}
                                                ]
                                   }
            found = False
            molecules[row['t']]['predicates'] = [{'predicate': row['p'],
                                                  'range': []}]
            ranges = []
            if 'range' in row and len(row['range']) > 0:
                ranges.extend(row['range'])
            if 'r' in row and len(row['r']) > 0:
                ranges.extend(row['r'])
            ranges = list(set(ranges))

            molecules[row['t']]['predicates'] = [{'predicate': row['p'],
                                                  'range': ranges}]

            molecules[row['t']]['linkedTo'].extend(ranges)
            molecules[row['t']]['linkedTo'] = list(set(molecules[row['t']]['linkedTo']))

    print('=========================================================================')
    print('----------------------', endpoint, '-------------------------------------')
    print('=========================================================================')

    pp.pprint(molecules)

    rdfmols = []
    for m in molecules:
        outqueue.put(molecules[m])
        rdfmols.append(molecules[m])

    outqueue.put('EOF')
    rdfmolecules[endpoint] = rdfmols
    return rdfmolecules


def get_single_source_rdfmts(enpointmaps, outqueue=Queue()):
    rdfmolecules = {}
    queues = {}

    for endpoint in enpointmaps:
        rdfmolecules[endpoint] = []
        queue = Queue()
        queues[endpoint] = queue
        p = Process(target=extractMTLs, args=(endpoint, queue, ))
        p.start()

    toremove = []
    while len(queues) > 0:
        for endpoint in queues:
            try:
                queue = queues[endpoint]
                r = queue.get(False)
                if r != 'EOF':
                    outqueue.put({endpoint: r})
                    rdfmolecules[endpoint].append(r)
                else:
                    toremove.append(endpoint)
            except Empty:
                pass
        for r in toremove:
            if r in queues:
                del queues[r]

    outqueue.put('EOF')

    for endpoint in rdfmolecules:
        rdfmols = rdfmolecules[endpoint]
        try:
            with open(enpointmaps[endpoint], 'w+') as f:
                json.dump(rdfmols, f)
                f.close()
        except Exception as e:
            print("WARN: exception while writing single source molecules:", endpoint, e)

    return rdfmolecules


def get_options(argv):
    try:
        opts, args = getopt.getopt(argv, "h:e:o:p:f:")
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    endpointfile = None
    '''
    Supported output formats:
        - json (default)
        - nt
        - SPARQL-UPDATE (directly store to sparql endpoint)
    '''
    outputType = 'json'
    pathToOutput = './'
    isFromFile = False
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt == "-e":
            endpointfile = arg
        elif opt == "-o":
            outputType = arg
        elif opt == "-p":
            pathToOutput = arg
        elif opt == '-f':
            isFromFile = True

    if not endpointfile or not outputType or outputType.lower() not in ['json', 'nt', 'sparql-update']:
        usage()
        sys.exit(1)

    # TODO: validate file path and sparql-endpoint capability (Update capability)

    return endpointfile, outputType, pathToOutput, isFromFile


def usage():
    usage_str = ("Usage: {program} -e <endpoints-file>  "
                 "-o <output-type> "
                 "-p <output-path> "
                 "-f <isFromFile> \n "
                 "where \n"
                 "\t<endpoints-file> - a text file containing a list of endpoint URLs OR folder name of single source MTs if -f flag is set\n"
                 "\t<output-type> - type of output; available options: json, nt, sparql-update  \n"
                 "\t<output-path> - output filename or URL for sparql endpoint with update support\n"
                 "\t<isFromFile>  - set this flag = 1 if single source MTs are already stored in a folder")

    print (usage_str.format(program=sys.argv[0]),)


def endpointsAccessible(endpoints):
    ask = "ASK {?s ?p ?o}"
    found = False
    for e in endpoints:
        referer = e
        if 'https' in e:
            server = e.split("https://")[1]
        else:
            server = e.split("http://")[1]
        (server, path) = server.split("/", 1)
        val, c = contactSource(ask, referer, server, path)
        if c == -2:
            print(e, '-> is not accessible. Hence, will not be included in the federation!')
        if val:
            found = True
        else:
            print(e, "-> is returning empty results. Hence, will not be included in the federation!")

    return found


if __name__ == "__main__":
    pp = pprint.PrettyPrinter(indent=2)
    endpointfile, outputType, pathToOutput, isFromFile = get_options(sys.argv[1:])
    #endpointfile, outputType, pathToOutput, isFromFile = "endpoint.txt", 'json', "motivtest.json", False
    if not isFromFile:
        with open(endpointfile, 'r') as f:
            endpoints = f.readlines()
            if len(endpoints) == 0:
                print("Endpoints file should have at least one url")
                sys.exit(1)

            endpoints = [e.strip('\n') for e in endpoints]
            if not endpointsAccessible(endpoints):
                print("None of the endpoints can be accessed. Please check if you write URLs properly!")
                sys.exit(1)

        rdfmts = {}
        emaps = {}
        for e in endpoints:
            print("Parsing: ", e)
            val = e.replace('/', '_').replace(':', '_')
            emaps[e] = val

        rdfmts = get_single_source_rdfmts(emaps)
    else:
        rdfmts = read_rdfmts(endpointfile)

    # TODO: NestedHashJoinFilter to find links between datasets
    eofflags = list()
    for endpoint1 in rdfmts:
        for endpoint2 in rdfmts:
            if endpoint1 == endpoint2:
                continue
            q = Queue()
            eofflags.append(q)
            print("Finding inter-links between:", endpoint1, ' and ', endpoint2, ' .... ')
            print("==============================//=========//===============================")
            p = Process(target=get_links, args=(endpoint1, rdfmts[endpoint1], endpoint2, rdfmts[endpoint2], q,))
            p.start()
            #get_links(endpoint1, rdfmts[endpoint1], endpoint2, rdfmts[endpoint2])

    while len(eofflags) > 0:
        for q in eofflags:
            eof = q.get()
            if eof == 'EOF':
                eofflags.remove(q)
                break

    molecules = combine_single_source_descriptions(rdfmts)
    print("Inter-link extraction finished!")
    print("Total Number of molecules =", len(molecules))
    print("Total Number of endpoints =", len(rdfmts))

    with open(pathToOutput, 'w+') as f:
        json.dump(molecules, f)
        f.close()

    exit(0)

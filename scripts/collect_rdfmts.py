#!/usr/bin/env python

import urllib
import httplib
import json
import pprint
import os
import random

XTYPES = """
        SELECT DISTINCT ?t ?p ?r ?range

        WHERE{
         ?s a ?t.
         ?s ?p ?pt.
         optional{ ?p rdfs:range ?range.
          filter (!regex(?range, 'http://www.w3.org/ns/sparql-service-description', 'i')
                && !regex(?range, 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances', 'i')
                && !regex(?range, 'http://www.openlinksw.com/schemas/virtrdf#', 'i')
                && !regex(?range, 'http://www.w3.org/2000/01/rdf-schema#', 'i')
                && !regex(?range, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#','i')
                && !regex(?range, 'http://www.w3.org/2002/07/owl#', 'i')
                && !regex(?range, 'nodeID://', 'i')
              )}
         optional {?pt a ?r. filter (!regex(?r, 'http://www.w3.org/ns/sparql-service-description', 'i')
                && !regex(?r, 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances', 'i')
                && !regex(?r, 'http://www.openlinksw.com/schemas/virtrdf#', 'i')
                && !regex(?r, 'http://www.w3.org/2000/01/rdf-schema#', 'i')
                && !regex(?r, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#','i')
                && !regex(?r, 'http://www.w3.org/2002/07/owl#', 'i')
                && !regex(?r, 'nodeID://', 'i')
                 )}
        filter (   !regex(?t, 'http://www.w3.org/ns/sparql-service-description', 'i')
                && !regex(?t, 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances', 'i')
                && !regex(?t, 'http://www.openlinksw.com/schemas/virtrdf#', 'i')
                && !regex(?t, 'http://www.w3.org/2000/01/rdf-schema#', 'i')
                && !regex(?t, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#','i')
                && !regex(?t, 'http://www.w3.org/2002/07/owl#', 'i')
                && !regex(?t, 'nodeID://', 'i')
               )
        }

    """

metas = ['http://www.w3.org/ns/sparql-service-description',
         'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances',
         'http://www.openlinksw.com/schemas/virtrdf#',
         'http://www.w3.org/2000/01/rdf-schema#',
         'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
         'http://www.w3.org/2002/07/owl#', 'nodeID://' ]

TYPES = """
        SELECT DISTINCT ?t ?p 
        WHERE{
         ?s a ?t.
         ?s ?p ?pt.
        }

    """


def get_rdfs_ranges(referer, server, path, p, limit=-1):

    RDFS_RANGES = " SELECT DISTINCT ?range  WHERE{ <" + p + "> rdfs:range ?range. }"

    reslist = []
    if limit == -1:
        limit = 10
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
    metas = ['http://www.w3.org/ns/sparql-service-description',
             'http://www.openlinksw.com/schemas/virtrdf#',
             'http://www.w3.org/2000/01/rdf-schema#',
             'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
             'http://www.w3.org/2002/07/owl#', 'nodeID://']
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
        limit = 10
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
    metas = ['http://www.w3.org/ns/sparql-service-description',
             'http://www.openlinksw.com/schemas/virtrdf#',
             'http://www.w3.org/2000/01/rdf-schema#',
             'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
             'http://www.w3.org/2002/07/owl#', 'nodeID://']
    for r in reslist:
        skip = False
        for m in metas:
            if m in r['r']:
                skip = True
                break
        if not skip:
            ranges.append(r['r'])

    return ranges


def get_concepts(endpoint, limit=-1):
    """
    Entry point for extracting RDF-MTs of an endpoint.
    Extracts list of rdf:Class concepts and predicates of an endpoint
    :param endpoint:
    :param limit:
    :return:
    """
    query = "SELECT DISTINCT ?t WHERE{ ?s a ?t } "
    referer = endpoint
    server = endpoint.split("http://")[1]
    (server, path) = server.split("/", 1)
    reslist = []
    if limit == -1:
        limit = 10
        offset = 0
        numrequ = 0
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactSource(query_copy, referer, server, path)
            print "cardinality:", card
            numrequ += 1
            print 'number of requests: ', numrequ
            if card == -2:
                limit = limit / 2
                print 'limit:', limit
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

    results = []
    metas = ['http://www.w3.org/ns/sparql-service-description',
             'http://www.openlinksw.com/schemas/virtrdf#',
             'http://www.w3.org/2000/01/rdf-schema#',
             'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
             'http://www.w3.org/2002/07/owl#',
             'nodeID://']
    toremove = []
    [toremove.append(r) for v in metas for r in reslist if v in r['t']]

    for r in toremove:
        reslist.remove(r)

    for r in reslist:

        t = r['t']
        print t, '\n', 'getting predicates ...'
        preds = get_predicates(referer, server, path, t)
        print 'getting ranges ...'
        for p in preds:
            rn = {"t": t}
            pred = p['p']
            rn['p'] = pred
            rn['range'] = get_rdfs_ranges(referer, server, path, pred)
            rn['r'] = find_instance_range(referer, server, path, t, pred)
            results.append(rn)

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
        limit = 10
        offset = 0
        numrequ = 0
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactSource(query_copy, referer, server, path)
            numrequ += 1
            print "predicates card:", card
            if card == -2:
                limit = limit / 2
                print "setting limit to: ", limit
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
        limit = 10
        offset = 0
        numrequ = 0
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactSource(query_copy, referer, server, path)
            numrequ += 1
            print "rand predicates card:", card
            if card == -2:
                limit = limit / 2
                print "rand setting limit to: ", limit
                if limit == 0:
                    break
                continue
            if numrequ == 10:
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
            print "inst predicates card:", card
            if card == -2:
                limit = limit / 2
                print "inst setting limit to: ", limit
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


# optional {?pt a ?r. }  ?r
def getResults(query, endpoint, limit=-1):
    referer = endpoint
    server = endpoint.split("http://")[1]
    (server, path) = server.split("/", 1)
    reslist = []
    if limit == -1:
        limit = 10
        offset = 0
        numrequ = 0
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            print query_copy
            res, card = contactSource(query_copy, referer, server, path)
            print "cardinality:", card
            numrequ += 1
            print 'number of requests: ', numrequ
            if card == -2:
                limit = limit / 2
                print 'limit:', limit
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
    metas = ['http://www.w3.org/ns/sparql-service-description',
             'http://www.openlinksw.com/schemas/virtrdf#',
             'http://www.w3.org/2000/01/rdf-schema#',
             'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
             'http://www.w3.org/2002/07/owl#', 'nodeID://']
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


def contactSource(qeury, referer, server, path):
    json = "application/sparql-results+json"
    params = urllib.urlencode({'query': qeury, 'format': json})
    headers = {"User-Agent": "mulder", "Accept": "*/*", "Referer": referer, "Host": server}
    try:
        conn = httplib.HTTPConnection(server)
        conn.request("GET", "/" + path + "?" + params, None, headers=headers)
        response = conn.getresponse()
        reslist = []
        if response.status == httplib.OK:
            res = response.read()
            res = res.replace("false", "False")
            res = res.replace("true", "True")
            res = eval(res)

            if type(res) is dict:
                if "results" in res:
                    for x in res['results']['bindings']:
                        for key, props in x.iteritems():
                            '''suffix =''
                            if props['type'] == 'typed-literal':
                                suffix = "^^<" + props['datatype'].encode("utf-8") + ">"
                            elif "xml:lang" in props:
                                suffix = '@' + props['xml:lang']
                            '''
                            x[key] = props['value'].encode('utf-8')
                    reslist = res['results']['bindings']

            return reslist, len(reslist)
        else:
            print response.reason, response.status, qeury
    except Exception as e:
        print "Exception during query execution to", server, ': ', e.message
    return None, -2


def get_links(endpoint1, rdfmt1, endpoint2, rdfmt2):
    print '============================================================'
    print 'between endpoints:', endpoint1, ' --> ', endpoint2
    for c in rdfmt1:
        for p in c['predicates']:
            reslist = get_external_links(endpoint1, c['rootType'], p['predicate'], endpoint2, rdfmt2)
            if len(reslist) > 0:

                c['linkedTo'].extend(reslist)
                c['linkedTo'] = list(set(c['linkedTo']))
                p['range'].extend(reslist)
                p['range'] = list(set(p['range']))
                print 'external links found for ', c['rootType'], '->', p['predicate'], reslist


def get_external_links(endpoint1, rootType, pred, endpoint2, rdfmt2):
    query = 'SELECT DISTINCT ?o  WHERE {?s a <' + rootType + '> ; <' + pred + '> ?o . FILTER (isIRI(?o))}'
    referer = endpoint1
    server = endpoint1.split("http://")[1]
    (server, path) = server.split("/", 1)
    reslist = []
    limit = 10
    offset = 0
    numrequ = 0
    checked_inst = []
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
            rand = random.randint(0, card - 1)
            inst = res[rand]
            if inst['o'] in checked_inst:
                offset += limit
                continue
            for c in rdfmt2:

                exists = link_exist(inst['o'], c['rootType'], endpoint2)
                if exists:
                    reslist.append(c['rootType'])
                    print inst['o'], ',', c['rootType']
            reslist = list(set(reslist))
        if card < limit:
            break

        offset += limit

    return reslist


def link_exist(s, c, endpoint):

    query = "ASK {<" + s + '>  a  <' + c + '> } '
    referer = endpoint
    server = endpoint.split("http://")[1]
    (server, path) = server.split("/", 1)
    res, card = contactSource(query, referer, server, path)
    if card > 0:
        if res[0]:
            print "ASK result", res, endpoint
        return res[0]
    if res is None:
        print 'bad request on, ', s, c
    return False


def read_rdfmts(folder):
    files = os.listdir(folder)
    print 'The following files are being combined:'
    pprint.pprint(files)

    molecules = {}
    print 'Number of molecules in:'
    for m in files:
        with open(folder + '/' + m) as f:
            rdfmt = json.load(f)
            key = rdfmt[0]['wrappers'][0]['url']
            molecules[key] = rdfmt
            print '-->', m, '=', len(rdfmt)
    print 'Total number of endpoints: ', len(molecules)

    return molecules


def combine_single_source_descriptions(rdfmts):

    print 'The following RDF-MTs are being combined:'
    pprint.pprint(rdfmts.keys())
    molecule_dict = {}
    molecules_tomerge = {}
    molecules = []
    print 'Number of molecules in:'
    for rdfmt in rdfmts:
        for m in rdfmts[rdfmt]:
            if m['rootType'] in molecule_dict:
                molecules_tomerge[m['rootType']] = [molecule_dict[m['rootType']]]
                molecules_tomerge[m['rootType']].append(m)
                del molecule_dict[m['rootType']]

                continue

            molecule_dict[m['rootType']] = m

        #molecules.extend(rdfmts[rdfmt])
        print '-->', rdfmt, '=', len(rdfmts[rdfmt])
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

    print 'Total number of molecules: ', len(molecules)

    return molecules


def get_single_source_rdfmts(enpointmaps):
    rdfmolecules = {}
    for endpoint in enpointmaps:

        #res = getResults(query, endpoint, limit=-1)
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
                ranges =[]
                if 'range' in row and len(row['range']) > 0:
                    ranges.extend(row['range'])
                if 'r' in row and len(row['r']) > 0:
                    ranges.extend(row['r'])
                ranges = list(set(ranges))

                molecules[row['t']]['predicates'] = [{'predicate': row['p'],
                                                      'range':ranges}]

                molecules[row['t']]['linkedTo'].extend(ranges)
                molecules[row['t']]['linkedTo'] = list(set(molecules[row['t']]['linkedTo']))

        pp.pprint(molecules)
        rdfmols = []
        for m in molecules:
            rdfmols.append(molecules[m])

        rdfmolecules[endpoint] = rdfmols
        try:
            with open(enpointmaps[endpoint], 'w+') as f:
                json.dump(rdfmols, f)
        except Exception as e:
            print "exception while writing single source molecules:", endpoint, e.message

    return rdfmolecules


if __name__ == "__main__":

    pp = pprint.PrettyPrinter(indent=2)
    #query = TYPES
    enpointmaps = {"http://0.0.0.0:7100/sparql": '../templates/iasis/goa-rdfmts.json',
                   "http://0.0.0.0:7102/sparql": '../templates/iasis/reactome-rdfmts.json',
                   "http://0.0.0.0:7101/sparql": '../templates/iasis/kegg-rdfmts.json'}

    enpointmaps = {"http://dbpedia.org/sparql": "../templates/dbpedia-template.json"}
    rdfmts = get_single_source_rdfmts(enpointmaps)
    exit()
    for endpoint1 in rdfmts:
        print endpoint1
        for endpoint2 in rdfmts:
            if '18890' in endpoint2 or '18894' in endpoint2:
                continue
            if endpoint1 == endpoint2:
                continue
            get_links(endpoint1, rdfmts[endpoint1], endpoint2, rdfmts[endpoint2])

    molecules = combine_single_source_descriptions(rdfmts)
    print "Number of endpoints=", len(molecules)
    with open('../templates/iasis/rdfmts-goa-reactome-kegg.json', 'w+') as f:
        json.dump(molecules, f)
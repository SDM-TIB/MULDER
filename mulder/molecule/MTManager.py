
__author__ = 'kemele'
import abc
import json


class Config(object):
    def __init__(self, configfile):
        self.configfile = configfile
        self.metadata = self.getAll()
        self.wrappers = self.getWrappers()
        self.predidx = self.createPredicateIndex()
    @abc.abstractmethod
    def getAll(self):
        return

    @abc.abstractmethod
    def getWrappers(self):
        return None

    def createPredicateIndex(self):
        pidx = {}
        for m in self.metadata:
            preds = self.metadata[m]['predicates']
            for p in preds:
                if p['predicate'] not in pidx:
                    pidx[p['predicate']] = set()
                    pidx[p['predicate']].add(m)
                else:
                    pidx[p['predicate']].add(m)

        return pidx

    def findbypreds(self, preds):
        res = []
        for p in preds:
            if p in self.predidx:
                res.append(self.predidx[p])
        if len(res) != len(preds):
            return []
        for r in res[1:]:
            res[0] = res[0].intersection(r)

        mols = list(res[0])
        return mols

    # def findbypreds(self, preds):
    #     mols = []
    #
    #     for m in self.metadata:
    #         found = True
    #         for p in preds:
    #             mps = [pm['predicate'] for pm in self.metadata[m]['predicates']]
    #             if p not in mps:
    #                 found = False
    #                 break
    #         if found:
    #             mols.append(m)
    #     return mols

    def findbypred(self, pred):
        mols = []
        for m in self.metadata:
            mps = [pm['predicate'] for pm in self.metadata[m]['predicates']]
            if pred in mps:
                mols.append(m)
        return mols

    def findMolecule(self, molecule):
        if molecule in self.metadata:
            return self.metadata[molecule]
        else:
            return None


class ArangoConfig(object):
    def __init__(self, url, database="OntarioMoleculeTemplates", moleculesCollection="MoleculeTemplates",
                 secured=False, username="", password=""):
        self.url = url
        self.database = database
        self.mtsCollection = moleculesCollection
        self.secured = secured
        self.username = username
        self.password = password

    def __repr__(self):
        return self.url


class ConfigFile(Config):
    def getAll(self):
        return self.readJsonFile(self.configfile)

    def getWrappers(self):
        with open(self.configfile) as f:
            conf = json.load(f)
        wrappers = {}
        if "WrappersConfig" in conf:
            if "MappingFolder" in conf['WrappersConfig']:
                self.mappingFolder = conf['WrappersConfig']['MappingFolder']

            for w in conf['WrappersConfig']:
                wrappers[w] = conf['WrappersConfig'][w]

        return wrappers

    def readJsonFile(self, configfile):
        try:
            with open(configfile) as f:
                conf = json.load(f)

            if 'MoleculeTemplates' not in conf:
                return None
            moleculetemps = conf['MoleculeTemplates']
            meta = {}
            for mt in moleculetemps:
                if mt['type'] == 'filepath':
                    with open(mt['path']) as f:
                        mts = json.load(f)

                    for m in mts:
                        if m['rootType'] in meta:
                            # linkedTo
                            links = meta[m['rootType']]['linkedTo']
                            links.extend(m['linkedTo'])
                            meta[m['rootType']]['linkedTo'] = list(set(links))

                            # predicates
                            preds = meta[m['rootType']]['predicates']
                            mpreds = m['predicates']
                            ps = {p['predicate']: p for p in preds}
                            for p in mpreds:
                                if p['predicate'] in ps and len(p['range']) > 0:
                                    ps[p['predicate']]['range'].extend(p['range'])
                                    ps[p['predicate']]['range'] = list(set(ps[p['predicate']]['range']))
                                else:
                                    ps[p['predicate']] = p

                            meta[m['rootType']]['predicates'] = []
                            for p in ps:
                                meta[m['rootType']]['predicates'].append(ps[p])

                            # wrappers
                            wraps = meta[m['rootType']]['wrappers']
                            wrs = {w['url']+w['wrapperType']: w for w in wraps}
                            mwraps = m['wrappers']
                            for w in mwraps:
                                key = w['url'] + w['wrapperType']
                                if key in wrs:
                                    wrs[key]['predicates'].extend(wrs['predicates'])
                                    wrs[key]['predicates'] = list(set(wrs[key]['predicates']))

                            meta[m['rootType']]['wrappers'] = []
                            for w in wrs:
                                meta[m['rootType']]['wrappers'].append(wrs[w])
                        else:
                            meta[m['rootType']] = m
            f.close()
            return meta
        except Exception as e:
            print("Exception while reading molecule templates file:", e)
            return None


#
# class Arango(Config):
#     """
#     Creates a configuration object for molecule templates
#     """
#     def getAll(self):
#         conn, config = self.getArangoDB(self.configfile)
#
#         '''
#         Load everything from molecule templates catalog
#         :return: list of all molecule templates in the database
#         '''
#         db = conn[config.database]
#         molquery = 'FOR u IN ' + config.mtsCollection + ' RETURN u'
#         mtResult = db.AQLQuery(molquery, rawResults=True, batchSize=2000, bindVars={})
#         mts = mtResult.response['result']
#         meta = {}
#         for m in mts:
#             meta[m['rootType']] = m
#         return meta
#
#     def getArangoDB(self, configfile):
#         '''
#         Read arangodb connection and configuration from config json file
#         :param configfile: json file specifying arangodb server info and database information:
#             e.g.,  arangodb: {url: "", database: "", MTsCollection: "", secured:"True/False", username: "", password: ""}
#         :return: connection and ArangoConfig objects
#         '''
#         conff = self.loadArangoConfig(configfile)
#         if conff.secured:
#             conns = Connection(arangoURL=conff.url, username=conff.username, password=conff.password)
#         else:
#             conns = Connection(arangoURL=conff.url)
#         return conns, conff
#
#     def loadArangoConfig(self, configfile):
#         '''
#         Loads configuration from json file
#         :param configfile:
#            e.g.,  arangodb: {url: "", database: "", MTsCollection: "", secured:"True/False", username: "", password: ""}
#         :return: ArangoConfig object
#         '''
#         with open(configfile) as mfile:
#             config = json.load(mfile)
#         config = config['arangodb']
#         conff = ArangoConfig(config['url'], config['database'], config['MTsCollection'], config['secured'],
#                              config['username'], config['password'])
#         return conff
#

#
# if __name__ == "__main__":
#     print "hello"
#     arr = ConfigFile("/home/kemele/git/Ontario/config/bsbm.json")
#     print "initialized"
#     preds = ["http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature"]
#     print arr.findbypreds(preds)
#     exit()
#     for t in arr.metadata:
#         print t
#         print "\t", arr.metadata[t]['linkedTo']
#         print '\t', arr.metadata[t]['predicates']
#         print '\t', arr.metadata[t]['wrappers']
#     print len(arr.metadata)

# MULDER


Installing MULDER
=================

MULDER runs on Debian GNU/Linux and OS X and Python 2.7

1. Download MULDER
    Clone using git:

    `$ git clone https://github.com/SDM-TIB/MULDER.git`

2. Go to MULDER folder:

    `$ cd MULDER`

3. Run:

    `pip install -r requirements.txt`

4. Install MULDER:

    `python setup.py install`

Configure MULDER
================

1. Create endpoints list `endpoints.txt`

    Example:

    ```http://dbpedia.org/sparql
     http://biotea.linkeddata.es/sparql
     http://colil.dbcls.jp/sparql```

2. Run RDF molecule template extractor in `scripts` folder:

    `scripts$ python collect_rdfmts.py -e endpoints.txt -o json -p 'templates/mytemplates.json'`

3. Create configuration file, `config.json` in `config` folder:

    Example:

    ```{
      "MoleculeTemplates": [
        {
           "type": "filepath",
           "path":"templates/mytemplates.json"
             }
          ]
       }
       ```

4. Now MULDER is ready to "investigate" :)


About supported endpoints
------------------------

MULDER currently supports endpoints that answer queries either on JSON.
Expect hard failures if you intend to use MULDER on endpoints that answer in any other format.


Running MULDER
===============

Once you installed MULDER and the Molecule Templates are ready with config.json,
you can start running MULDER using
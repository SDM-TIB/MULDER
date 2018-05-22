# MULDER


Installing MULDER
=================

MULDER runs on Debian GNU/Linux and OS X and Python 3.5

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

    ```
     http://biotea.linkeddata.es/sparql
     http://colil.dbcls.jp/sparql
    ```

2. Run RDF molecule template extractor in `scripts` folder:

    `scripts$ python3.5 collect_rdfmts.py -e endpoints.txt -o json -p 'templates/mytemplates.json'`

3. Create configuration file, `config.json` in `config` folder:

    Example:

    ```
      {
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
you can start running MULDER using the following script:

    $ python3.5 test_mulder.py -p <planonly> -q <query> -c <path/to/config.json> -s <isstring>
    
 where:

 - `<query>`:               - SPARQL QUERY
 - `<path/to/config.json>`: - path to configuration file
 - `<isstring>`:            - (Optional) set if <query> is sent as string: available values 1 or -1. -1 is default, meaning query is from file
 - `<planonly>`:            - (Optional) if set True, then only execution plan is generated and showed. If False (default), then the generated plan will be executed, too.

 Running experiments:
 ===================

 `$./runQueries.sh <path/to/queries-dir> <path/to/config.json> <path/to/results-folder> errors.txt <planonlyTorF>  &`

 OR

 `$ python3.5 start_experiment.py -c <path/to/config.json> -q <query-file> -r <path/to/results-folder> -t 'MULDER' -s True -p <planonly> `
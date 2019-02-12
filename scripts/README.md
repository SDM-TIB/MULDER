Running the script
=================

Requirements:
------------
- Python 3.5
- list of SPARQL endpoint URLs in endpoints.txt file

Running
-----------

    ```
    ./create_rdfmts.py -s <endpoints-file>  -o <output-filename.json>  
    ```

- `endpoints-file`: path to endpointURLs.txt file
- `output-filename.json`: path to the output file. e.g., `/path/to/fed1-templates.json`


Example
-----------

`endpoints.txt` file:
    
    ```
    http://localhost:11001/sparql
    http://localhost:`1102/sparql
    ```
    
Running script:
    ```
    ./create_rdfmts.py -s endpoints.txt -o myfed-temps.json
    ```
    
`myfed-temps.json` contains:

    ```
    {
    "predicates": [
      {
        "range": [],
        "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      },
      {
        "range": [],
        "predicate": "http://schema.org/name"
      }
    ],
    "rootType": "http://purl.org/dc/dcmitype/Collection",
    "linkedTo": [],
    "wrappers": [
      {
        "wrapperType": "SPARQLEndpoint",
        "predicates": [
          "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
          "http://schema.org/name"
        ],
        "urlparam": "",
        "url": "http://localhost:4001/sparql"
      }
    ]
    },  
    {
    "predicates": [
      {
        "range": [],
        "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      },
      {
        "range": [],
        "predicate": "http://schema.org/availability"
      },
      {
        "range": [],
        "predicate": "http://schema.org/license"
      },
      {
        "range": [],
        "predicate": "http://bibframe.org/vocab/subtitle"
      },
      {
        "range": [],
        "predicate": "http://bibframe.org/vocab/doi"
      },
      {
        "range": [],
        "predicate": "http://purl.org/dc/terms/identifier"
      },
      {
        "range": [],
        "predicate": "http://schema.org/dateCreated"
      },
      {
        "range": [],
        "predicate": "http://schema.org/datePublished"
      },
      {
        "range": [],
        "predicate": "http://schema.org/duration"
      },
      {
        "range": [],
        "predicate": "http://schema.org/noOfItems"
      },
      {
        "range": [],
        "predicate": "http://schema.org/thumbnailUrl"
      },
      {
        "range": [],
        "predicate": "http://schema.org/author"
      },
      {
        "range": [],
        "predicate": "http://schema.org/producer"
      },
      {
        "range": [],
        "predicate": "http://schema.org/publisher"
      },
      {
        "range": [],
        "predicate": "http://schema.org/url"
      },
      {
        "range": [],
        "predicate": "http://purl.org/dc/terms/subject"
      },
      {
        "range": [
          "http://purl.org/dc/dcmitype/Collection"
        ],
        "predicate": "http://purl.org/dc/terms/isPartOf"
      },
      {
        "range": [],
        "predicate": "http://purl.org/dc/terms/language"
      },
      {
        "range": [],
        "predicate": "http://schema.org/name"
      },
      {
        "range": [],
        "predicate": "http://www.w3.org/2000/01/rdf-schema#seeAlso"
      },
      {
        "range": [],
        "predicate": "http://bibframe.org/vocab/partNumber"
      },
      {
        "range": [],
        "predicate": "http://schema.org/alternateName"
      },
      {
        "range": [],
        "predicate": "http://schema.org/keywords"
      },
      {
        "range": [],
        "predicate": "http://schema.org/contributor"
      },
      {
        "range": [],
        "predicate": "http://schema.org/description"
      }
    ],
    "rootType": "http://schema.org/Movie",
    "linkedTo": [
      "http://purl.org/dc/dcmitype/Collection"
    ],
    "wrappers": [
      {
        "wrapperType": "SPARQLEndpoint",
        "predicates": [
          "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
          "http://schema.org/availability",
          "http://schema.org/license",
          "http://bibframe.org/vocab/subtitle",
          "http://bibframe.org/vocab/doi",
          "http://purl.org/dc/terms/identifier",
          "http://schema.org/dateCreated",
          "http://schema.org/datePublished",
          "http://schema.org/duration",
          "http://schema.org/noOfItems",
          "http://schema.org/thumbnailUrl",
          "http://schema.org/author",
          "http://schema.org/producer",
          "http://schema.org/publisher",
          "http://schema.org/url",
          "http://purl.org/dc/terms/subject",
          "http://purl.org/dc/terms/isPartOf",
          "http://purl.org/dc/terms/language",
          "http://schema.org/name",
          "http://www.w3.org/2000/01/rdf-schema#seeAlso",
          "http://bibframe.org/vocab/partNumber",
          "http://schema.org/alternateName",
          "http://schema.org/keywords",
          "http://schema.org/contributor",
          "http://schema.org/description"
        ],
        "urlparam": "",
        "url": "http://localhost:4001/sparql"
      }
    ]
  }
    
    ```    
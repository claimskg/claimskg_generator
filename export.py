import getopt
import sys

import pandas
from SPARQLWrapper import SPARQLWrapper
from ruamel import yaml

from generator import ClaimsKGGenerator
from vsm.embeddings import Embeddings


def usage():
    f = open('exporter_help_text.txt', 'r')
    print(f.read())
    f.close()


if __name__ == '__main__':
    argv = sys.argv[1:]
    options = {'output': "output.ttl", 'format': "turtle", 'resolve': True, 'threshold': 0.3,
               'model-uri': "http://data.gesis.org/claimskg/", 'include-body': False, 'reconcile': -1.0,
               'caching': False}

    if len(argv) == 0:
        print('You must pass some parameters. Use \"-h\" to display the present help information.')
        usage()
        exit()

    if len(argv) == 1 and argv[0] == '-h':
        usage()
        exit()

    configuration_dict = config_dict = yaml.load(open("configuration.yaml", "r"))

    try:
        opts, args = getopt.getopt(argv, "",
                                   ("input=", "output=", "format=", "model-uri=", "resolve", "threshold=",
                                    "include-body", "reconcile=", "caching"))

        for opt, arg in opts:
            if opt == '--input':
                options['input'] = arg
            elif opt == '--output':
                options['output'] = arg
            elif opt == '--format':
                options['format'] = arg
            elif opt == '--model-uri':
                options['model_uri'] = arg
            elif opt == "--resolve":
                options['resolve'] = True
            elif opt == "--include-body":
                options['include-body'] = True
            elif opt == "--threshold":
                options['threshold'] = float(arg)
            elif opt == "--reconcile":
                options['reconcile'] = float(arg)
            elif opt == "--caching":
                options['caching'] = True

    except:
        print('Arguments parser error')
        usage()
        exit()


    if "input" not in options.keys():
        print("Missing mandatory parameter --input")
        usage()
        exit()

    sparql_wrapper = None
    if options['resolve']:
        sparql_wrapper = SPARQLWrapper("https://dbpedia.org/sparql/")

    print("Loading data...")
    pandas_frame = pandas.read_csv(options['input'])

    theta = options['reconcile']
    embeddings = None
    if theta > 0:
        embeddings = Embeddings.load_from_file_lazy(configuration_dict['embeddings_path'])

    generator = ClaimsKGGenerator(model_uri=options['model-uri'],
                                  sparql_wrapper=sparql_wrapper, include_body=options['include-body'],
                                  threshold=options['threshold'], resolve=options['resolve'],
                                  use_caching=options['caching'])

    print("Generating model from CSV data...")
    generator.generate_model(pandas_frame)

    if theta > 0:
        print("Serializing pre-reconciliation graph to {file} ...".format(file=options["output"]))
    else:
        print("Serializing graph to {file} ...".format(file=options["output"]))
    output = generator.export_rdf(options['format'])
    file = open(options['output'], "w")
    file.write(output.decode("utf-8"))

    if theta > 0:
        print("Reconciling claims...")
        generator.reconcile_claims(embeddings, theta, 0.2, 0.2, 0.1, 0.1, 0.4)

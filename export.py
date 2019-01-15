import csv
import getopt
import sys

from SPARQLWrapper import SPARQLWrapper
from ruamel import yaml

import claimskg
from claimskg.generator import ClaimsKGGenerator
from claimskg.vsm.embeddings import MagnitudeEmbeddings


def usage():
    f = open('exporter_help_text.txt', 'r')
    print(f.read())
    f.close()


if __name__ == '__main__':
    argv = sys.argv[1:]

    if len(argv) == 0:
        print('You must pass some parameters. Use \"-h\" to display the present help information.')
        usage()
        exit()

    if len(argv) == 1 and argv[0] == '-h':
        usage()
        exit()

    configuration_dict = config_dict = yaml.load(open("configuration.yaml", "r"), Loader=yaml.Loader)
    options = {'output': "output.ttl", 'format': "turtle", 'resolve': True, 'threshold': 0.3,
               'model-uri': "http://data.gesis.org/claimskg/", 'include-body': False, 'reconcile': -1.0,
               'caching': False, 'seed': None, 'sample': None, 'mappings-file': "./mappings.csv",
               'embeddings-type': "MagnitudeEmbeddings", 'embeddings-path': None}

    # Overriding hard-coded defaults with values from configuration file
    for (key, value) in configuration_dict.items():
        options[key] = value

    try:
        opts, args = getopt.getopt(argv, "",
                                   ("input=", "output=", "format=", "model-uri=", "resolve", "threshold=",
                                    "include-body", "reconcile=", "caching", "sample=", "seed=", "mappings-file="))

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
            elif opt == "--sample":
                options['sample'] = int(arg)
            elif opt == "--seed":
                options['seed'] = int(arg)
            elif opt == "--mappings-file":
                options['mappings-file'] = arg

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

    print()
    print("Loading data...")
    csv.field_size_limit(sys.maxsize)
    # pandas_frame = pandas.read_csv(options['input'], sep=',', skipinitialspace=True, quotechar='"', escapechar='"', engine="python",encoding="utf-8")

    dataset_rows = []
    with open(options['input']) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"', dialect=csv.unix_dialect)
        for row in csv_reader:
            dataset_rows.append(row)
    theta = options['reconcile']
    embeddings = None
    if theta > 0 and options['embeddings-path']:
        print("Loading embeddings...")
        class_name = options['embeddings-type']
        embeddings_class = getattr(claimskg.vsm.embeddings, class_name)
        # embeddings = MagnitudeEmbeddings(options['embeddings-path'])
        embeddings = embeddings_class(options['embeddings-path'])

    generator = ClaimsKGGenerator(model_uri=options['model-uri'],
                                  sparql_wrapper=sparql_wrapper, include_body=options['include-body'],
                                  threshold=options['threshold'], resolve=options['resolve'],
                                  use_caching=options['caching'])

    print()
    print("Generating model from CSV data...")
    generator.generate_model(dataset_rows)

    if theta > 0:
        print()
        print("Reconciling claims...")
        generator.reconcile_claims(embeddings, theta=theta, keyword_weight=1, link_weight=1, text_weight=1,
                                   entity_weight=1, mappings_file_path=options['mappings-file'],
                                   samples=options['sample'], seed=options['seed'])

    print()
    print("\nSerializing graph...")
    output = generator.export_rdf(options['format'])
    file = open(options['output'], "w")

    print()
    print("Writing to {file} ...\t\t\t".format(file=options["output"]))
    file.write(output.decode("utf-8"))
    file.flush()
    file.close()

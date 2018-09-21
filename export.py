import getopt
import sys

import pandas
from SPARQLWrapper import SPARQLWrapper

from generator import ClaimsKGGenerator


def usage():
    f = open('exporter_help_text.txt', 'r')
    print(f.read())
    f.close()


if __name__ == '__main__':
    argv = sys.argv[1:]
    options = {'output': "output.ttl", 'format': "turtle", 'resolve': True, 'threshold': 0.3,
               'model-uri': "http://data.gesis.org/claimskg/public/"}

    if len(argv) == 0:
        print('You must pass some parameters. Use \"-h\" to display the present help information.')
        usage()
        exit()

    if len(argv) == 1 and argv[0] == '-h':
        usage()
        exit()

    spotter = None

    try:
        opts, args = getopt.getopt(argv, "",
                                   ("input=", "output=", "format=", "model-uri=", "resolve", "threshold="))

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

            elif opt == "--threshold":
                options['threshold'] = float(arg)

    except:
        print('Arguments parser error')
        usage()
        exit()

    if "input" not in options.keys():
        print("Missing mandatory parameter --input")
        usage()
        exit()

    spotter = None
    sparql_wrapper = None
    if options['resolve']:
        sparql_wrapper = SPARQLWrapper("https://dbpedia.org/sparql/")

    print("Loading data...")
    pandas_frame = pandas.read_csv(options['input'])

    generator = ClaimsKGGenerator(model_uri=options['model-uri'],
                                  sparql_wrapper=sparql_wrapper)

    print("Generating model from CSV data...")
    generator.generate_model(pandas_frame)

    print("Serializing graph to {file} ...".format(file=options["output"]))
    output = generator.export_rdf(options['format'])
    file = open(options['output'], "w")
    file.write(output.decode("utf-8"))

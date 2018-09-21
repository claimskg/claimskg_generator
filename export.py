import getopt
import sys

import pandas

from generator import ClaimsKGGenerator

if __name__ == '__main__':
    argv = sys.argv[1:]
    options = {'output': "output.ttl", 'format': "turtle", 'model_uri': ""}

    if len(argv) == 0:
        print('You must pass some parameters. Use \"-h\" to help.')
        exit()

    if len(argv) == 1 and argv[0] == '-h':
        f = open('exporter_help_text.txt', 'r')
        print(f.read())
        f.close()

        exit()

    try:
        opts, args = getopt.getopt(argv, "", ("input=", "output=", "format=", "model_uri"))

        for opt, arg in opts:
            if opt == '--input':
                options['input'] = arg

            elif opt == '--output':
                options['output'] = arg

            elif opt == '--format':
                options['format'] = arg

            elif opt == 'model_uri':
                options['model_uri'] = arg

    except:
        print('Arguments parser error, try -h')
        exit()

    pandas_frame = pandas.read_csv(options['input'])

    generator = ClaimsKGGenerator(model_uri=options['model_uri'])
    generator.generate_model(pandas_frame)
    output = generator.export_rdf(options['format'])
    file = open(options['output'], "w")
    file.write(output.decode("utf-8"))

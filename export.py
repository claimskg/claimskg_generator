import cgi
import getopt
import json
import sys
import urllib.parse

import pandas
import rdflib
from rdflib import Graph
from rdflib import URIRef, Literal
from rdflib.namespace import NamespaceManager
from rdflib.namespace import RDF

namespace_manager = NamespaceManager(Graph())

rdf_pref = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema#")
namespace_manager.bind('rdfs', rdf_pref, override=False)

schema_pref = rdflib.Namespace("http://schema.org/")
namespace_manager.bind('schema', schema_pref, override=False)

claim_review_class_uri = URIRef(schema_pref['ClaimReview'])
schema_organization_class_uri = URIRef(schema_pref['Organization'])
schema_thing_class_uri = URIRef(schema_pref['Thing'])

claim_reviewed_property_uri = rdflib.term.URIRef(schema_pref['claimReviewed'])
schema_url_property_uri = rdflib.term.URIRef(schema_pref['url'])
schema_name_property_uri = rdflib.term.URIRef(schema_pref['name'])
schema_date_published_property_uri = rdflib.term.URIRef(schema_pref['datePublished'])
schema_language_preperty_uri = rdflib.term.URIRef(schema_pref['language'])
schema_author_property_uri = rdflib.term.URIRef(schema_pref['author'])
schema_same_as_property_uri = rdflib.term.URIRef(schema_pref['sameAs'])


english_literal = Literal("english")

nif_pref = rdflib.Namespace("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#")
namespace_manager.bind('nif', nif_pref, override=False)

its_pref = rdflib.Namespace("https://www.w3.org/2005/11/its/rdf#")
namespace_manager.bind('itsrdf', its_pref, override=False)

# dbpedia_pref = rdflib.Namespace("")
# namespace_manager.bind("dbpedia",dbpedia_pref,override=False)

claimskg_pref = rdflib.Namespace("http://claimskg.gesis.org/website_placeholder/")
namespace_manager.bind('claimskg', claimskg_pref, override=False)
namespace_manager.bind('base', claimskg_pref, override=True)


def export_rdf(pandas_dataframe, options):
    print()
    uri_name_unique_counter = 0
    entity_counter = 0
    output_graph = rdflib.Graph()
    output_graph.namespace_manager = namespace_manager
    for column, row in pandas_dataframe.iterrows():
        uri_name_unique_counter += 1
        sys.stdout.write("{current}/{total} [{percent}%]                                                            \r"
                         .format(current=uri_name_unique_counter, total=len(pandas_dataframe),
                                 percent=round((uri_name_unique_counter / float(len(pandas_dataframe))) * 100, 2)))

        ###################
        # schema_claimreview
        ###################
        claimreview_instance = URIRef(claimskg_pref['claimreview' + "/" + str(uri_name_unique_counter)])
        output_graph.add((claimreview_instance, RDF.type, claim_review_class_uri))
        output_graph.add(
            (claimreview_instance, claim_reviewed_property_uri, Literal(row['extra_title'])))
        output_graph.add(
            (claimreview_instance, schema_url_property_uri, Literal(row['claimReview_url'])))

        output_graph.add(
            (claimreview_instance, schema_date_published_property_uri,
             Literal(row['claimReview_datePublished'])))
        output_graph.add((claimreview_instance, schema_language_preperty_uri, english_literal))

        ####################
        # schema_organization
        ####################
        author_name = row['claimReview_author_name'].lower().replace(" ", "_")
        organization = URIRef(claimskg_pref['organization' + "/" + author_name])
        output_graph.add((organization, RDF.type, schema_organization_class_uri))

        output_graph.add(
            (organization, schema_name_property_uri, Literal(row['claimReview_author_name'])))
        output_graph.add((organization, schema_url_property_uri, Literal(row['claimReview_author_url'])))

        output_graph.add((claimreview_instance, schema_author_property_uri, organization))

        ####################
        # schema_creativework
        ####################
        creative_work_author_value = str(row['creativeWork_author_name']).lower().replace(" ", "_")
        creative_work_author = URIRef(claimskg_pref["creativework_author" + "/" + creative_work_author_value])

        output_graph.add((creative_work_author, RDF.type, schema_thing_class_uri))

        output_graph.add(
            (creative_work_author, schema_name_property_uri , Literal(row['creativeWork_author_name'])))

        # Todo: Reconcile author entities with DBPedia
        output_graph.add((creative_work_author, schema_same_as_property_uri, Literal("dbpedia:link")))



        creativework = URIRef(urllib.parse.quote_plus("creativework_" + row['claimReview_author_name']))
        output_graph.add((creativework, schema_date_published_property_uri,
                          Literal(row['creativeWork_datePublished'])))
        output_graph.add(
            (creativework, rdflib.term.URIRef(schema_pref['citation']), Literal(row['claimReview_author_url'])))
        output_graph.add((creativework, rdflib.term.URIRef(schema_pref['author']), creative_work_author))

        output_graph.add((claimreview_instance, rdflib.term.URIRef(schema_pref['itemReviewed']), creativework))

        ####################
        # schema_reviewRating
        ####################
        # print
        if type(row['rating_alternateName']) == type(1.0):
            str_ = ""
        else:
            str_ = cgi.escape(row['rating_alternateName']).encode('ascii', 'xmlcharrefreplace')

        rating = rdflib.term.URIRef(claimskg_pref["rating"] + "/" + str(uri_name_unique_counter))
        # rating = rdflib.term.URIRef("https://schema.org/reviewRating")

        # Having defined the things and the edge weights, now assemble the graph
        output_graph.add((rating, RDF.type, rdflib.term.URIRef("https://schema.org/Rating")))
        output_graph.add((rating, rdflib.term.URIRef(schema_pref['alternateName_normalized']),
                          Literal(row['rating_alternateName_normalized'])))
        output_graph.add((rating, rdflib.term.URIRef(schema_pref['alternateName_original']), Literal(str_)))
        output_graph.add((rating, rdflib.term.URIRef(schema_pref['bestRating']), Literal(row['rating_bestRating'])))
        output_graph.add((rating, rdflib.term.URIRef(schema_pref['ratingValue']), Literal(row['rating_ratingValue'])))
        output_graph.add((rating, rdflib.term.URIRef(schema_pref['worstRating']), Literal(row['rating_worstRating'])))

        output_graph.add((claimreview_instance, rdflib.term.URIRef(schema_pref['reviewRating']), rating))

        ##############
        ### mentions 
        #############
        if json.loads(row[u'extra_entities_claimReview_claimReviewed']):
            for i in json.loads(row[u'extra_entities_claimReview_claimReviewed']):
                entity_counter += 1
                nif = rdflib.term.URIRef(urllib.parse.quote_plus("mention_" + str(entity_counter)))
                output_graph.add((nif, rdflib.term.URIRef(nif_pref['isString']), Literal(i['mention'])))
                output_graph.add((nif, rdflib.term.URIRef(nif_pref['beginIndex']), Literal(i['start'])))
                output_graph.add((nif, rdflib.term.URIRef(nif_pref['endInder']), Literal(i['end'])))
                output_graph.add((nif, rdflib.term.URIRef(its_pref['taConfidence']), Literal(i['linkProbability'])))
                output_graph.add((nif, rdflib.term.URIRef(its_pref['taIdentRef']), Literal(i['entity'])))

                output_graph.add((claimreview_instance, rdflib.term.URIRef(schema_pref['mentions']), nif))

    return output_graph.serialize(format=options['format'], encoding='utf-8')


if __name__ == '__main__':
    argv = sys.argv[1:]
    options = {}

    options['output'] = "output.ttl"
    options['format'] = "turtle"

    if len(argv) == 0:
        print('You must pass some parameters. Use \"-h\" to help.')
        exit()

    if len(argv) == 1 and argv[0] == '-h':
        f = open('exporter_help_text.txt', 'r')
        print(f.read())
        f.close()

        exit()

    try:
        opts, args = getopt.getopt(argv, "", ("input=", "output=", "format="))

        for opt, arg in opts:
            if opt == '--input':
                options['input'] = arg

            elif opt == '--output':
                options['output'] = arg

            elif opt == '--format':
                options['format'] = arg

    except:
        print('Arguments parser error, try -h')
        exit()

    pandas_frame = pandas.read_csv(options['input'])
    output = export_rdf(pandas_frame, options)
    file = open(options['output'], "w")
    file.write(output.decode("utf-8"))

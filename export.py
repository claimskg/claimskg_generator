from rdflib import URIRef, BNode, Literal
from rdflib.namespace import RDF,DC, FOAF
from rdflib.namespace import Namespace, NamespaceManager
from rdflib import Graph
import pandas as pd
import cgi
import rdflib
import json
import urllib
import sys,getopt

namespace_manager = NamespaceManager(Graph())

rdf_pref=rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema#")
namespace_manager.bind('rdfs', rdf_pref, override=False)

schema_pref=rdflib.Namespace("http://schema.org/")
namespace_manager.bind('schema', schema_pref, override=False)

nif_pref=rdflib.Namespace("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#")
namespace_manager.bind('nif', nif_pref, override=False)

its_pref=rdflib.Namespace("https://www.w3.org/2005/11/its/rdf#")
namespace_manager.bind('itsrdf', its_pref, override=False)


def export_rdf(pdf,options):
    print 
    index_=0
    index_ent=0
    g = rdflib.Graph()
    g.namespace_manager = namespace_manager
    for index, row in pdf.iterrows():
        index_+=1
        print str(index_) + "/" + str(len(pdf))

        ###################
        #schema_claimreview
        ###################


        claimreview = URIRef(urllib.quote_plus("claimreview"+str(index_)))
        g.add( (claimreview, rdflib.term.URIRef(schema_pref['claimReviewed']), Literal(row['extra_title']) ) )
        g.add( (claimreview, rdflib.term.URIRef(schema_pref['url']), Literal(row['claimReview_url']) ) )
        g.add( (claimreview, rdflib.term.URIRef(schema_pref['datePublished']), Literal(row['claimReview_datePublished']) ) )
        g.add( (claimreview, rdflib.term.URIRef(schema_pref['language']), Literal( "english") ))
        

        ####################
        #schema_organization
        ####################
        organization = URIRef(urllib.quote_plus("organization_"+row['claimReview_author_name']))
        g.add( (organization, rdflib.term.URIRef(schema_pref['name']), Literal(row['claimReview_author_name']) ) )
        g.add( (organization, rdflib.term.URIRef(schema_pref['url']), Literal(row['claimReview_author_url']) ) )

        g.add( (claimreview, rdflib.term.URIRef(schema_pref['author']), organization ))
        

        ####################
        #schema_creativework
        ####################
        creativework_author = URIRef(urllib.quote_plus("creativework_author_"+str(row['creativeWork_author_name'])))
        g.add( (creativework_author, rdflib.term.URIRef(schema_pref['name']), Literal(row['creativeWork_author_name']) ) )
        g.add( (creativework_author, rdflib.term.URIRef(schema_pref['sameAs']), Literal("dbpedia:link") ) )

        creativework = URIRef(urllib.quote_plus("creativework_"+row['claimReview_author_name']))
        g.add( (creativework, rdflib.term.URIRef(schema_pref['datePublished']), Literal(row['creativeWork_datePublished']) ) )
        g.add( (creativework, rdflib.term.URIRef(schema_pref['citation']), Literal(row['claimReview_author_url']) ) )
        g.add( (creativework, rdflib.term.URIRef(schema_pref['author']), creativework_author ) )

        g.add( (claimreview, rdflib.term.URIRef(schema_pref['itemReviewed']), creativework ))

        ####################
        #schema_reviewRating
        ####################
        #print 
        if (type(row['rating_alternateName'])==type(1.0)):
            str_=""
        else:
            str_=cgi.escape(row['rating_alternateName']).encode('ascii', 'xmlcharrefreplace')

        rating = rdflib.term.URIRef(urllib.quote_plus("rating_"+str(index_)))
        #rating = rdflib.term.URIRef("https://schema.org/reviewRating") 
        
        
        # Having defined the things and the edge weights, now assemble the graph
        g.add( (rating, RDF.type, rdflib.term.URIRef("https://schema.org/Rating") ) )
        g.add( (rating,  rdflib.term.URIRef(schema_pref['alternateName_normalized']),  Literal(row['rating_alternateName_normalized']) ))
        g.add( (rating,  rdflib.term.URIRef(schema_pref['alternateName_original']),  Literal(str_) ))
        g.add( (rating,  rdflib.term.URIRef(schema_pref['bestRating']),  Literal(row['rating_bestRating']) ))
        g.add( (rating,  rdflib.term.URIRef(schema_pref['ratingValue']),  Literal(row['rating_ratingValue']) ))
        g.add( (rating,  rdflib.term.URIRef(schema_pref['worstRating']),  Literal(row['rating_worstRating']) ))
        
        g.add( (claimreview, rdflib.term.URIRef(schema_pref['reviewRating']), rating ) )
        

        ##############
        ### mentions 
        #############
        if json.loads(row[u'extra_entities_claimReview_claimReviewed']):
            for i in json.loads( row[u'extra_entities_claimReview_claimReviewed']):
                index_ent+=1
                nif= rdflib.term.URIRef(urllib.quote_plus("mention_"+str(index_ent)))
                g.add( (nif, rdflib.term.URIRef(nif_pref['isString']), Literal(i['mention'])) )
                g.add( (nif, rdflib.term.URIRef(nif_pref['beginIndex']), Literal(i['start'])) )
                g.add( (nif, rdflib.term.URIRef(nif_pref['endInder']), Literal(i['end'])) )
                g.add( (nif, rdflib.term.URIRef(its_pref['taConfidence']), Literal(i['linkProbability'])) )
                g.add( (nif, rdflib.term.URIRef(its_pref['taIdentRef']), Literal(i['entity'])) )

                g.add( (claimreview, rdflib.term.URIRef(schema_pref['mentions']), nif ) )





    return g.serialize(format=options['format'])



if __name__ == '__main__':
    argv = sys.argv[1:]
    options={}

    options['output']="output.rdf"
    options['format']="turtle"

    if len(argv) == 0:
        print('You must pass some parameters. Use \"-h\" to help.')
        exit()

    if len(argv) == 1 and argv[0] == '-h':
        f = open('exporter_help_text.txt', 'r')
        print f.read()
        f.close()

        exit()

    try:
        opts, args = getopt.getopt(argv, "", ("input=", "output=", "format="))

        
        
        for opt,arg in opts:
            if opt == '--input':
                options['input']= arg

            elif opt == '--output':
                options['output']= arg

            elif opt == '--format':
                options['format'] = arg

    except:
        print('Arguments parser error, try -h')
        exit()


    pdf = pd.read_csv(options['input'])
    out=export_rdf(pdf,options)
    file = open(options['output'], "w")
    file.write(out)
    
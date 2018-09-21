import sys
import html

import rdflib
from pandas import json
from rdflib import URIRef, Literal, Graph
from rdflib.namespace import NamespaceManager, RDF

from util import TypedCounter


class ClaimsKGGenerator:

    def __init__(self, model_uri):
        self._graph = rdflib.Graph()

        self.model_uri = model_uri
        self._namespace_manager = NamespaceManager(Graph())

        self._claimskg_prefix = rdflib.Namespace(model_uri)
        self._namespace_manager.bind('claimskg', self._claimskg_prefix, override=False)
        self._namespace_manager.bind('base', self._claimskg_prefix, override=True)

        self.counter = TypedCounter()

        self._rdf_prefix = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema#")
        self._namespace_manager.bind('rdfs', self._rdf_prefix, override=False)

        self._schema_prefix = rdflib.Namespace("http://schema.org/")
        self._namespace_manager.bind('schema', self._schema_prefix, override=False)

        self._schema_claim_review_class_uri = URIRef(self._schema_prefix['ClaimReview'])
        self._schema_creative_work_class_uri = URIRef(self._schema_prefix['CreativeWork'])
        self._schema_organization_class_uri = URIRef(self._schema_prefix['Organization'])
        self._schema_thing_class_uri = URIRef(self._schema_prefix['Thing'])
        self._schema_rating_class_uri = URIRef(self._schema_prefix['Rating'])

        self._schema_claim_reviewed_property_uri = URIRef(self._schema_prefix['claimReviewed'])
        self._schema_url_property_uri = URIRef(self._schema_prefix['url'])
        self._schema_name_property_uri = URIRef(self._schema_prefix['name'])
        self._schema_date_published_property_uri = URIRef(self._schema_prefix['datePublished'])
        self._schema_language_preperty_uri = URIRef(self._schema_prefix['language'])
        self._schema_author_property_uri = URIRef(self._schema_prefix['author'])
        self._schema_same_as_property_uri = URIRef(self._schema_prefix['sameAs'])
        self._schema_citation_preperty_uri = URIRef(self._schema_prefix['citation'])
        self._schema_item_reviewed_property_uri = URIRef(self._schema_prefix['itemReviewed'])
        self._schema_alternate_name_normalized_property_uri = URIRef(self._schema_prefix['alternateName_normalized'])
        self._schema_mentions_property_uri = URIRef(self._schema_prefix['mentions'])

        self._english_literal = Literal("english")

        self._nif_prefix = rdflib.Namespace("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#")
        self._namespace_manager.bind('nif', self._nif_prefix, override=False)

        self._nif_RFC5147String_class_uri = URIRef(self._nif_prefix['RFC5147String'])
        self._nif_context_class_uri = URIRef(self._nif_prefix['Context'])

        self._nif_source_url_property_uri = URIRef(self._nif_prefix['sourceUrl'])
        self._nif_begin_index_property_uri = URIRef(self._nif_prefix["beginIndex"])
        self._nif_end_index_property_uri = URIRef(self._nif_prefix["endIndex"])
        self._nif_is_string_property_uri = URIRef(self._nif_prefix["isString"])

        self._its_prefix = rdflib.Namespace("https://www.w3.org/2005/11/its/rdf#")
        self._namespace_manager.bind('itsrdf', self._its_prefix, override=False)

        self.its_ta_confidence_property_uri = URIRef(self._its_prefix['taConfidence'])
        self.its_ta_ident_ref_property_uri = URIRef(self._its_prefix['taIdentRef'])

    def _create_schema_claim_review(self, row):
        claimreview_instance = URIRef(
            self._claimskg_prefix['claimreview' + "/" + str(self.counter.count(self._schema_claim_review_class_uri))])
        self._graph.add((claimreview_instance, RDF.type, self._schema_claim_review_class_uri))
        self._graph.add(
            (claimreview_instance, self._schema_claim_reviewed_property_uri, Literal(row['extra_title'])))
        self._graph.add(
            (claimreview_instance, self._schema_url_property_uri, Literal(row['claimReview_url'])))

        self._graph.add(
            (claimreview_instance, self._schema_date_published_property_uri,
             Literal(row['claimReview_datePublished'])))
        self._graph.add((claimreview_instance, self._schema_language_preperty_uri, self._english_literal))

        return claimreview_instance

    def _create_organization(self, row):
        author_name = row['claimReview_author_name'].lower().replace(" ", "_")
        organization = URIRef(self._claimskg_prefix['organization' + "/" + author_name])
        self._graph.add((organization, RDF.type, self._schema_organization_class_uri))

        self._graph.add(
            (organization, self._schema_name_property_uri, Literal(row['claimReview_author_name'])))
        self._graph.add((organization, self._schema_url_property_uri, Literal(row['claimReview_author_url'])))

        return organization

    def _create_creative_work(self, row):
        creative_work = URIRef(self._claimskg_prefix["creativework" + "/" + row['claimReview_author_name']])
        self._graph.add((creative_work, RDF.type, self._schema_creative_work_class_uri))

        self._graph.add((creative_work, self._schema_date_published_property_uri,
                          Literal(row['creativeWork_datePublished'])))

        self._graph.add(
            (creative_work, self._schema_citation_preperty_uri, Literal(row['claimReview_author_url'])))

        # Creative work author instantiation

        creative_work_author_value = str(row['creativeWork_author_name']).lower().replace(" ", "_")
        creative_work_author = URIRef(self._claimskg_prefix["creativework_author" + "/" + creative_work_author_value])

        self._graph.add((creative_work_author, RDF.type, self._schema_thing_class_uri))

        self._graph.add(
            (creative_work_author, self._schema_name_property_uri, Literal(row['creativeWork_author_name'])))

        self._graph.add((creative_work, self._schema_author_property_uri, creative_work_author))

        # Todo: Reconcile author entities with DBPedia
        self._graph.add((creative_work_author, self._schema_same_as_property_uri, Literal("dbpedia:link")))

        return creative_work

    def _create_review_rating(self, row):
        if type(row['rating_alternateName']) == type(1.0):
            escaped_alternate_rating_name = ""
        else:
            escaped_alternate_rating_name = html.escape(row['rating_alternateName']).encode('ascii',
                                                                                            'xmlcharrefreplace')

        rating = rdflib.term.URIRef(
            self._claimskg_prefix["rating" + "/" + str(self.counter.count(self._schema_rating_class_uri))])

        # Having defined the things and the edge weights, now assemble the graph
        self._graph.add((rating, RDF.type, self._schema_rating_class_uri))

        self._graph.add((rating, self._schema_alternate_name_normalized_property_uri,
                   Literal(row['rating_alternateName_normalized'])))

        self._graph.add(
            (rating, rdflib.term.URIRef(self._schema_prefix['alternateName_original']),
             Literal(escaped_alternate_rating_name)))
        self._graph.add(
            (rating, rdflib.term.URIRef(self._schema_prefix['bestRating']), Literal(row['rating_bestRating'])))
        self._graph.add(
            (rating, rdflib.term.URIRef(self._schema_prefix['ratingValue']), Literal(row['rating_ratingValue'])))
        self._graph.add(
            (rating, rdflib.term.URIRef(self._schema_prefix['worstRating']), Literal(row['rating_worstRating'])))

        return rating

    def _create_mention(self, mention_entry):
        mention = rdflib.term.URIRef(
            self._claimskg_prefix["mention" + "/" + str(self.counter.count(self._nif_context_class_uri))])

        self._graph.add((mention, RDF.type, self._nif_context_class_uri))
        self._graph.add((mention, RDF.type, self._nif_RFC5147String_class_uri))

        self._graph.add((mention, self._nif_is_string_property_uri, Literal(mention_entry['mention'])))
        self._graph.add((mention, self._nif_begin_index_property_uri, Literal(mention_entry['start'])))
        self._graph.add((mention, self._nif_end_index_property_uri, Literal(mention_entry['end'])))

        # TODO: Fix values so that they aren't diplayed in scientific notation
        self._graph.add(
            (mention, self.its_ta_confidence_property_uri, Literal(self._format_confidence_score(mention_entry))))
        self._graph.add((mention, self.its_ta_ident_ref_property_uri, Literal(mention_entry['entity'])))
        return mention

    @staticmethod
    def _format_confidence_score(mention_entry):
        value = float(mention_entry['linkProbability'])
        rounded_to_two_decimals = round(value, 2)
        return str(rounded_to_two_decimals)

    def generate_model(self, pandas_dataframe):
        row_counter = 0

        self._graph.namespace_manager = self._namespace_manager
        total_entry_count = len(pandas_dataframe)
        for column, row in pandas_dataframe.iterrows():
            row_counter += 1

            # Progress animation with the old carriage return with no new line trick
            print("Generating model from CSV data...")
            sys.stdout.write(
                "{current}/{total} [{percent}%]                                                            \r"
                    .format(current=row_counter, total=total_entry_count,
                            percent=round((row_counter / float(total_entry_count)) * 100, 2)))

            ###################
            # schema_claimreview
            ###################

            claimreview_instance = self._create_schema_claim_review(row)

            ####################
            # schema_organization
            ####################

            organization = self._create_organization(row)
            self._graph.add((claimreview_instance, self._schema_author_property_uri, organization))

            ####################
            # schema_creativework
            ####################

            creative_work = self._create_creative_work(row)
            self._graph.add((claimreview_instance, self._schema_item_reviewed_property_uri, creative_work))

            ####################
            # schema_reviewRating
            ####################

            rating = self._create_review_rating(row)
            self._graph.add((claimreview_instance, rdflib.term.URIRef(self._schema_prefix['reviewRating']), rating))

            ##############
            ### mentions
            #############
            if json.loads(row[u'extra_entities_claimReview_claimReviewed']):
                for mention_entry in json.loads(row[u'extra_entities_claimReview_claimReviewed']):
                    mention = self._create_mention(mention_entry)
                    self._graph.add((claimreview_instance, self._schema_mentions_property_uri, mention))

    def export_rdf(self, format):
        return self._graph.serialize(format=format, encoding='utf-8')

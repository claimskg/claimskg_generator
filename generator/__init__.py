import html
import sys
import uuid
from typing import List

import rdflib
from SPARQLWrapper import SPARQLWrapper
from pandas.io import json
from rdflib import URIRef, Literal, Graph
from rdflib.namespace import NamespaceManager, RDF

from util import TypedCounter
from util.sparql.sparql_offset_fetcher import SparQLOffsetFetcher


def _row_string_value(row, key):
    value = row[key]
    if not isinstance(value, str):
        value = ""
    return str(value)


def _row_string_values(row, keys: List[str]):
    return [_row_string_value(row, key) for key in keys]


_normalized_rating_values = {
    "FALSE": 1,
    "MIXTURE": 2,
    "TRUE": 3,
    "OTHER": -1
}


class ClaimsKGURIGenerator:
    def __init__(self, base_uri):
        self.base_uri = base_uri
        self._claimskg_prefix = rdflib.Namespace(base_uri)

    def creative_work_uri(self, row):
        uuid_key = "".join(_row_string_values(row, ["creativeWork_author_name", "creativeWork_author_sameAs",
                                                    "creativeWork_datePublished", "extra_body"]))
        return URIRef(self._claimskg_prefix["creative_work/" + str(
            uuid.uuid5(namespace=uuid.NAMESPACE_URL, name=uuid_key))])

    def claim_review_uri(self, row):
        uuid_key = "".join(_row_string_values(row, ["claimReview_author_name", "claimReview_author_url",
                                                    "claimReview_claimReviewed", "claimReview_datePublished",
                                                    "claimReview_url"]))
        return URIRef(self._claimskg_prefix["claim_review/" + str(
            uuid.uuid5(namespace=uuid.NAMESPACE_URL, name=uuid_key))])

    def organization_uri(self, row):
        uuid_key = "".join(_row_string_values(row, ["claimReview_author_name"])).lower().replace(" ", "_")
        return URIRef(self._claimskg_prefix["organization/" + uuid_key])

    def claimskg_organization_uri(self):
        return URIRef(self._claimskg_prefix[self.base_uri + "/organization/claimskg"])

    def creative_work_author_uri(self, row):
        uuid_key = "".join(_row_string_values(row, ["creativeWork_author_name", "creativeWork_author_sameAs"]))
        return URIRef(self._claimskg_prefix["creative_work_author/" + str(uuid.uuid5(namespace=uuid.NAMESPACE_URL,
                                                                                     name=uuid_key))])

    def create_original_rating_uri(self, row):
        uuid_key = "_".join(
            _row_string_values(row, ["claimReview_author_name", "rating_alternateName"])).lower().replace(" ", "_")
        return URIRef(self._claimskg_prefix["rating/original/" + uuid_key])

    def create_normalized_rating_uri(self, row):
        uuid_key = "claimskg_" + _row_string_value(row, "rating_alternateName_normalized").lower()
        return URIRef(self._claimskg_prefix["rating/normalized/" + uuid_key])

    def mention_uri(self, begin, end, text, ref, confidence):
        uuid_key = str(begin) + str(end) + str(text) + str(ref) + str(round(confidence, 2))
        return URIRef(
            self._claimskg_prefix["mention/" + str(uuid.uuid5(namespace=uuid.NAMESPACE_URL, name=uuid_key))])


class ClaimsKGGenerator:

    def __init__(self, model_uri, sparql_wrapper=None, threshold=0.3, include_body: bool = False, resolve: bool = True):
        self._graph = rdflib.Graph()

        self._sparql_wrapper = sparql_wrapper  # type: SPARQLWrapper
        self._uri_generator = ClaimsKGURIGenerator(model_uri)
        self._threshold = threshold
        self._include_body = include_body
        self._resolve = resolve

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
        self._schema_language_class_uri = URIRef(self._schema_prefix['Language'])

        self._schema_claim_reviewed_property_uri = URIRef(self._schema_prefix['claimReviewed'])
        self._schema_url_property_uri = URIRef(self._schema_prefix['url'])
        self._schema_name_property_uri = URIRef(self._schema_prefix['name'])
        self._schema_date_published_property_uri = URIRef(self._schema_prefix['datePublished'])
        self._schema_in_language_preperty_uri = URIRef(self._schema_prefix['inLanguage'])
        self._schema_author_property_uri = URIRef(self._schema_prefix['author'])
        self._schema_same_as_property_uri = URIRef(self._schema_prefix['sameAs'])
        self._schema_citation_preperty_uri = URIRef(self._schema_prefix['citation'])
        self._schema_item_reviewed_property_uri = URIRef(self._schema_prefix['itemReviewed'])
        self._schema_alternate_name_property_uri = URIRef(self._schema_prefix['alternateName'])
        self._schema_description_property_uri = URIRef(self._schema_prefix['description'])
        self._schema_rating_value_property_uri = URIRef(self._schema_prefix['ratingValue'])
        self._schema_mentions_property_uri = URIRef(self._schema_prefix['mentions'])
        self._schema_keywords_property_uri = URIRef(self._schema_prefix['keywords'])
        self._schema_headline_property_uri = URIRef(self._schema_prefix['headline'])
        self._schema_review_body_property_uri = URIRef(self._schema_prefix['reviewBody'])

        self._english_uri = URIRef(self._claimskg_prefix["language/English"])
        self._graph.add((self._english_uri, RDF.type, self._schema_language_class_uri))
        self._graph.add((self._english_uri, self._schema_alternate_name_property_uri, Literal("eng")))
        self._graph.add((self._english_uri, self._schema_description_property_uri, Literal("English")))

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
        claim_review_instance = self._uri_generator.claim_review_uri(row)
        self._graph.add((claim_review_instance, RDF.type, self._schema_claim_review_class_uri))

        self._graph.add(
            (claim_review_instance, self._schema_claim_reviewed_property_uri,
             Literal(_row_string_value(row, "claimReview_claimReviewed"))))

        headline_value = _row_string_value(row, "extra_title")

        if len(headline_value) > 0:
            self._graph.add(
                (claim_review_instance, self._schema_headline_property_uri, Literal(headline_value)))

        # Include body only if the option is enabled

        body_value = _row_string_value(row, "extra_body")
        if self._include_body and len(body_value) > 0:
            self._graph.add((claim_review_instance, self._schema_review_body_property_uri, Literal(body_value)))

        self._graph.add(
            (claim_review_instance, self._schema_url_property_uri, URIRef(row['claimReview_url'])))

        self._graph.add(
            (claim_review_instance, self._schema_date_published_property_uri,
             Literal(row['claimReview_datePublished'])))
        self._graph.add((claim_review_instance, self._schema_in_language_preperty_uri, self._english_uri))

        keywords = row['extra_tags']
        if isinstance(keywords, str) and len(keywords) > 0:
            self._graph.add((claim_review_instance, self._schema_keywords_property_uri, Literal(keywords)))

        return claim_review_instance

    def _create_organization(self, row):
        organization = self._uri_generator.organization_uri(row)
        self._graph.add((organization, RDF.type, self._schema_organization_class_uri))

        self._graph.add(
            (organization, self._schema_name_property_uri, Literal(row['claimReview_author_name'])))

        author_url = _row_string_value(row, 'claimReview_author_url')
        if len(author_url) > 0:
            self._graph.add((organization, self._schema_url_property_uri, URIRef(author_url)))

        return organization

    def _create_claims_kg_organization(self):
        organization = self._uri_generator.claimskg_organization_uri()
        self._graph.add((organization, RDF.type, self._schema_organization_class_uri))

        self._graph.add(
            (organization, self._schema_name_property_uri, Literal("ClaimsKG")))

        self._graph.add((organization, self._schema_url_property_uri, URIRef(self.model_uri)))

    def _create_creative_work(self, row):
        creative_work = self._uri_generator.creative_work_uri(row)
        self._graph.add((creative_work, RDF.type, self._schema_creative_work_class_uri))

        date_published_value = _row_string_value(row, "creativeWork_datePublished")
        if len(date_published_value) > 0:
            self._graph.add((creative_work, self._schema_date_published_property_uri, Literal(date_published_value)))

        links = row['extra_refered_links']
        if not isinstance(links, float):
            links = links[1:-1].split(",")
            for link in links:
                if len(link) > 0 and link.strip() != "#":
                    self._graph.add(
                        (creative_work, self._schema_citation_preperty_uri, Literal(link.strip())))

        # Creative work author instantiation

        author_value = _row_string_value(row, "creativeWork_author_name")

        if len(author_value) > 0:
            creative_work_author = self._uri_generator.creative_work_author_uri(row)

            self._graph.add((creative_work_author, RDF.type, self._schema_thing_class_uri))

            self._graph.add(
                (creative_work_author, self._schema_name_property_uri, Literal(author_value)))
            self._graph.add((creative_work, self._schema_author_property_uri, creative_work_author))

        # Todo: Reconcile author entities with DBPedia
        # self._graph.add((creative_work_author, self._schema_same_as_property_uri, Literal("dbpedia:link")))

        return creative_work

    def _create_review_rating(self, row):
        if isinstance(row['rating_alternateName'], float):
            escaped_alternate_rating_name = ""
        else:
            escaped_alternate_rating_name = html.escape(row['rating_alternateName']).encode('ascii',
                                                                                            'xmlcharrefreplace')

        original_rating = self._uri_generator.create_original_rating_uri(row)
        self._graph.add((original_rating, RDF.type, self._schema_rating_class_uri))
        self._graph.add(
            (original_rating, self._schema_alternate_name_property_uri,
             Literal(escaped_alternate_rating_name)))
        self._graph.add(
            (original_rating, self._schema_rating_value_property_uri, Literal(int(row['rating_ratingValue']))))
        organization = self._uri_generator.organization_uri(row)
        self._graph.add((original_rating, self._schema_author_property_uri, organization))

        normalized_rating = self._uri_generator.create_normalized_rating_uri(row)
        self._graph.add((normalized_rating, RDF.type, self._schema_rating_class_uri))
        self._graph.add(
            (normalized_rating, self._schema_alternate_name_property_uri,
             Literal(row['rating_alternateName_normalized'])))

        self._graph.add(
            (normalized_rating, self._schema_rating_value_property_uri,
             Literal(_normalized_rating_values[_row_string_value(row, "rating_alternateName_normalized")])))

        claimskg_org = self._uri_generator.claimskg_organization_uri()
        self._graph.add((normalized_rating, self._schema_author_property_uri, claimskg_org))

        return original_rating, normalized_rating

    def _create_mention(self, mention_entry):
        rho_value = float(mention_entry['linkProbability'])
        if rho_value > self._threshold:

            text = mention_entry['mention']
            start = mention_entry['start']
            end = mention_entry['end']
            entity_uri = self.resolve_entity_identifier(mention_entry['entity'])

            mention = self._uri_generator.mention_uri(start, end, text, entity_uri, rho_value)

            self._graph.add((mention, RDF.type, self._nif_context_class_uri))
            self._graph.add((mention, RDF.type, self._nif_RFC5147String_class_uri))

            self._graph.add((mention, self._nif_is_string_property_uri, Literal(text)))
            self._graph.add((mention, self._nif_begin_index_property_uri, Literal(int(start))))
            self._graph.add((mention, self._nif_end_index_property_uri, Literal(int(end))))

            # TODO: Fix values so that they aren't displayed in scientific notation
            self._graph.add(
                (mention, self.its_ta_confidence_property_uri, Literal(self._format_confidence_score(mention_entry))))

            self._graph.add((mention, self.its_ta_ident_ref_property_uri, URIRef(entity_uri)))

            return mention
        else:
            return None

    def resolve_entity_identifier(self, identifier):
        if self._sparql_wrapper is not None and self._resolve:
            fetcher = SparQLOffsetFetcher(self._sparql_wrapper, 10000, """            
                        ?concept dbo:wikiPageID {id}.
                        """.format(id=identifier),
                                          "?concept")
            result = fetcher.fetch_all()
            if len(result) > 0:
                uri = result[0]['concept']['value']
            else:
                uri = "tagme://" + str(identifier)
        else:
            uri = "tagme://" + str(identifier)

        return uri

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

            original, normalized = self._create_review_rating(row)
            self._graph.add((claimreview_instance, rdflib.term.URIRef(self._schema_prefix['reviewRating']), original))
            self._graph.add((claimreview_instance, rdflib.term.URIRef(self._schema_prefix['reviewRating']), normalized))

            ##############
            ### mentions
            #############
            # For claim review
            if json.loads(row[u'extra_entities_claimReview_claimReviewed']):
                for mention_entry in json.loads(row[u'extra_entities_claimReview_claimReviewed']):
                    mention = self._create_mention(mention_entry)
                    if mention:
                        self._graph.add((claimreview_instance, self._schema_mentions_property_uri, mention))

            # For Creative Work
            if json.loads(row[u'extra_entities_body']):
                for mention_entry in json.loads(row[u'extra_entities_claimReview_claimReviewed']):
                    mention = self._create_mention(mention_entry)
                    if mention:
                        self._graph.add((creative_work, self._schema_mentions_property_uri, mention))

    def export_rdf(self, format):
        return self._graph.serialize(format=format, encoding='utf-8')

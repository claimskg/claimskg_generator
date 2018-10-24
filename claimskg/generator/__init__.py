import datetime
import html
import re
import uuid
from typing import List
from urllib.parse import urlparse

import rdflib
from SPARQLWrapper import SPARQLWrapper
from pandas.io import json
from rdflib import URIRef, Literal, Graph
from rdflib.namespace import NamespaceManager, RDF, OWL, XSD
from tqdm import tqdm

import claimskg.generator.ratings
from claimskg.generator.statistics import ClaimsKGStatistics
from claimskg.reconciler import FactReconciler
from claimskg.util import TypedCounter
from claimskg.util.sparql.sparql_offset_fetcher import SparQLOffsetFetcher

_is_valid_url_regex = re.compile(
    r'^(?:http|ftp)s?://'  # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

source_uri_dict = {
    '': '',
    'snopes': "http://www.snopes.com",
    'politifact': "http://www.politifact.com",
    'africacheck': "https://africacheck.org",
    'truthorfiction': "https://www.truthorfiction.com",
    'checkyourfact': "http://checkyourfact.com",
    'factscan': "http://factscan.ca",
}


def _row_string_value(row, key):
    value = row[key]
    if not isinstance(value, str):
        str_value = str(value)
        if "nan" == str_value:
            value = ""
        else:
            value = str_value
    return value


def _row_string_values(row, keys: List[str]):
    return [_row_string_value(row, key) for key in keys]


class ClaimLogicalView:
    def __init__(self):
        self.review_entities = []
        self.claim_entities = []
        self.keywords = []
        self.links = []
        self.text_fragments = []
        self.claimreview_author = ""
        self.creative_work_author = ""
        self.creative_work_uri = None
        self.claim_date = None
        self.review_date = None
        self.has_body_text = False
        self.has_headline = False


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
        return URIRef(self._claimskg_prefix["organization/claimskg"])

    def creative_work_author_uri(self, row):
        uuid_key = "".join(_row_string_values(row, ["creativeWork_author_name", "creativeWork_author_sameAs"]))
        return URIRef(self._claimskg_prefix["creative_work_author/" + str(uuid.uuid5(namespace=uuid.NAMESPACE_URL,
                                                                                     name=uuid_key))])

    def create_original_rating_uri(self, row):
        uuid_key = "_".join(
            _row_string_values(row, ["claimReview_author_name", "rating_alternateName",
                                     "rating_ratingValue"])).lower().replace(" ", "_").replace("\n", "_")
        return URIRef(self._claimskg_prefix["rating/original/" + uuid_key])

    def create_normalized_rating_uri(self, normalized_rating):
        rating_name = str(normalized_rating.name)
        uuid_key = "claimskg_" + rating_name
        return URIRef(self._claimskg_prefix["rating/normalized/" + uuid_key])

    def mention_uri(self, begin, end, text, ref, confidence, source_text_content):
        uuid_key = str(begin) + str(end) + str(text) + str(ref) + str(round(confidence, 2)) + source_text_content
        return URIRef(
            self._claimskg_prefix["mention/" + str(uuid.uuid5(namespace=uuid.NAMESPACE_URL, name=uuid_key))])


def _normalize_text_fragment(text: str):
    return text.replace("\"\"", "\"").replace("\"", "'")


class ClaimsKGGenerator:

    def __init__(self, model_uri, sparql_wrapper=None, threshold=0.3, include_body: bool = False, resolve: bool = True,
                 use_caching: bool = False):
        self._graph = rdflib.Graph()

        self._sparql_wrapper = sparql_wrapper  # type: SPARQLWrapper
        self._uri_generator = ClaimsKGURIGenerator(model_uri)
        self._threshold = threshold
        self._include_body = include_body
        self._resolve = resolve
        self._use_caching = use_caching

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

        self._namespace_manager.bind('owl', OWL, override=True)

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
        self._schema_text_property_uri = URIRef(self._schema_prefix['text'])

        self._iso1_language_tag = "en"
        self._iso3_language_tag = "eng"

        self._english_uri = URIRef(self._claimskg_prefix["language/English"])
        self._graph.add((self._english_uri, RDF.type, self._schema_language_class_uri))
        self._graph.add((self._english_uri, self._schema_alternate_name_property_uri, Literal(self._iso1_language_tag)))
        self._graph.add((self._english_uri, self._schema_name_property_uri, Literal("English")))

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

        self._logical_view_claims = []  # type: List[ClaimLogicalView]

    def _create_schema_claim_review(self, row, claim: ClaimLogicalView):
        claim_review_instance = self._uri_generator.claim_review_uri(row)
        self._graph.add((claim_review_instance, RDF.type, self._schema_claim_review_class_uri))

        # claim_reviewed_value = _normalize_text_fragment(_row_string_value(row, "claimReview_claimReviewed"))
        # self._graph.add(
        #     (claim_review_instance, self._schema_claim_reviewed_property_uri,
        #      Literal(claim_reviewed_value,
        #              lang=self._iso1_language_tag)))

        headline_value = _row_string_value(row, "extra_title")

        if len(headline_value) > 0:
            self._graph.add(
                (claim_review_instance, self._schema_headline_property_uri,
                 Literal(headline_value, lang=self._iso1_language_tag)))
            claim.text_fragments.append(headline_value)
            claim.has_headline = True

        # Include body only if the option is enabled

        body_value = _row_string_value(row, "extra_body")
        if len(body_value) > 0:
            claim.has_body_text = True
            claim.text_fragments.append(_normalize_text_fragment(body_value))
            if self._include_body:
                self._graph.add((claim_review_instance, self._schema_review_body_property_uri,
                                 Literal(body_value, lang=self._iso1_language_tag)))

        self._graph.add(
            (claim_review_instance, self._schema_url_property_uri, URIRef(row['claimReview_url'])))

        review_date = row['claimReview_datePublished']
        self._graph.add(
            (claim_review_instance, self._schema_date_published_property_uri,
             Literal(review_date, datatype=XSD.date)))
        claim.review_date = datetime.datetime.strptime(review_date, "%Y-%m-%d").date()
        self._graph.add((claim_review_instance, self._schema_in_language_preperty_uri, self._english_uri))

        return claim_review_instance

    def _create_organization(self, row, claim):
        organization = self._uri_generator.organization_uri(row)
        self._graph.add((organization, RDF.type, self._schema_organization_class_uri))

        claim.claimreview_author = row['claimReview_author_name']

        self._graph.add(
            (organization, self._schema_name_property_uri,
             Literal(row['claimReview_author_name'], lang=self._iso1_language_tag)))

        author_name = _row_string_value(row, 'claimReview_author_name')
        if len(author_name) > 0:
            self._graph.add((organization, self._schema_url_property_uri, URIRef(source_uri_dict[author_name])))

        return organization

    def _create_claims_kg_organization(self):
        organization = self._uri_generator.claimskg_organization_uri()
        self._graph.add((organization, RDF.type, self._schema_organization_class_uri))

        self._graph.add(
            (organization, self._schema_name_property_uri, Literal("ClaimsKG")))

        self._graph.add((organization, self._schema_url_property_uri, URIRef(self.model_uri)))

    def _create_creative_work(self, row, claim: ClaimLogicalView):
        creative_work = self._uri_generator.creative_work_uri(row)
        self._graph.add((creative_work, RDF.type, self._schema_creative_work_class_uri))

        date_published_value = _row_string_value(row, "creativeWork_datePublished")
        if len(date_published_value) > 0:
            self._graph.add((creative_work, self._schema_date_published_property_uri,
                             Literal(date_published_value, datatype=XSD.date)))
            claim.claim_date = datetime.datetime.strptime(date_published_value, "%Y-%m-%d").date()

        keywords = row['extra_tags']
        if isinstance(keywords, str) and len(keywords) > 0:
            self._graph.add((creative_work, self._schema_keywords_property_uri,
                             Literal(keywords, lang=self._iso1_language_tag)))
            for keyword in keywords.split(";"):
                claim.keywords.append(keyword.strip())

        links = row['extra_refered_links']
        author_url = _row_string_value(row, 'claimReview_author_url')
        if not isinstance(links, float):
            links = links[1:-1].split(",")
            for link in links:
                stripped_link = link.strip()
                if len(stripped_link) > 0 and stripped_link[0] != "#" and re.match(_is_valid_url_regex,
                                                                                   link.strip()) and link.strip() != \
                        source_uri_dict[
                            author_url]:
                    claim.links.append(link)
                    try:
                        parsed_url = urlparse(link.strip())
                        self._graph.add(
                            (creative_work, self._schema_citation_preperty_uri,
                             URIRef(
                                 parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path + "?" +
                                 parsed_url.query.replace("|", "%7C").replace("^", "%5E").replace("\\", "%5C").replace(
                                     "{", "%7B").replace("}", "%7D").replace("&", "%26").replace("=", "%3D"))))
                    except:
                        pass
        # Creative work author instantiation

        author_value = _row_string_value(row, "creativeWork_author_name")
        claim.creative_work_author = author_value

        claim_reviewed_value = _normalize_text_fragment(_row_string_value(row, "claimReview_claimReviewed"))
        self._graph.add(
            (creative_work, self._schema_text_property_uri,
             Literal(claim_reviewed_value,
                     lang=self._iso1_language_tag)))

        if len(author_value) > 0:
            creative_work_author = self._uri_generator.creative_work_author_uri(row)

            self._graph.add((creative_work_author, RDF.type, self._schema_thing_class_uri))

            self._graph.add(
                (creative_work_author, self._schema_name_property_uri,
                 Literal(author_value, lang=self._iso1_language_tag)))
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
        rating_value = int(row['rating_ratingValue'])
        original_rating = self._uri_generator.create_original_rating_uri(row)
        self._graph.add((original_rating, RDF.type, self._schema_rating_class_uri))
        self._graph.add(
            (original_rating, self._schema_alternate_name_property_uri,
             Literal(escaped_alternate_rating_name)))

        if rating_value >= 0:
            self._graph.add(
                (original_rating, self._schema_rating_value_property_uri,
                 Literal(rating_value, datatype=XSD.integer)))

        organization = self._uri_generator.organization_uri(row)
        self._graph.add((original_rating, self._schema_author_property_uri, organization))

        normalized_rating_enum = ratings.normalize(_row_string_value(row, "claimReview_author_name").lower(),
                                                   _row_string_value(row, "rating_alternateName").lower())
        normalized_rating = self._uri_generator.create_normalized_rating_uri(normalized_rating_enum)
        self._graph.add((normalized_rating, RDF.type, self._schema_rating_class_uri))
        self._graph.add(
            (normalized_rating, self._schema_alternate_name_property_uri,
             Literal(str(normalized_rating_enum.name), lang=self._iso1_language_tag)))

        self._graph.add(
            (normalized_rating, self._schema_rating_value_property_uri,
             Literal(normalized_rating_enum.value,
                     datatype=XSD.integer)))

        claimskg_org = self._uri_generator.claimskg_organization_uri()
        self._graph.add((normalized_rating, self._schema_author_property_uri, claimskg_org))

        return original_rating, normalized_rating


def _create_mention(self, mention_entry, claim: ClaimLogicalView, in_review):
    rho_value = float(mention_entry['linkProbability'])
    if rho_value > self._threshold:

        text = mention_entry['mention']
        start = mention_entry['start']
        end = mention_entry['end']
        entity_uri = self.resolve_entity_identifier(mention_entry['entity'])

        mention = self._uri_generator.mention_uri(start, end, text, entity_uri, rho_value,
                                                  ",".join(claim.text_fragments))

        self._graph.add((mention, RDF.type, self._nif_context_class_uri))
        self._graph.add((mention, RDF.type, self._nif_RFC5147String_class_uri))

        self._graph.add((mention, self._nif_is_string_property_uri,
                         Literal(text, lang=self._iso1_language_tag)))
        self._graph.add((mention, self._nif_begin_index_property_uri, Literal(int(start), datatype=XSD.integer)))
        self._graph.add((mention, self._nif_end_index_property_uri, Literal(int(end), datatype=XSD.integer)))

        # TODO: Fix values so that they aren't displayed in scientific notation
        self._graph.add(
            (mention, self.its_ta_confidence_property_uri,
             Literal(float(self._format_confidence_score(mention_entry)), datatype=XSD.float)))

        self._graph.add((mention, self.its_ta_ident_ref_property_uri, URIRef(entity_uri)))
        if in_review:
            claim.review_entities.append(entity_uri)
        else:
            claim.claim_entities.append(entity_uri)

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
    one_per_cent = int(total_entry_count * 0.01)
    self.global_statistics = ClaimsKGStatistics()
    self.per_source_statistics = {}

    progress_bar = tqdm(total=len(pandas_dataframe))

    for index, row in pandas_dataframe.iterrows():
        row_counter += 1

        if row_counter % one_per_cent == 0:
            progress_bar.update(one_per_cent)

        logical_claim = ClaimLogicalView()  # Instance holding claim raw information for mapping generation
        source_site = _row_string_value(row, 'claimReview_author_name')
        if source_site not in self.per_source_statistics.keys():
            self.per_source_statistics[source_site] = ClaimsKGStatistics()

        claim_review_instance = self._create_schema_claim_review(row, logical_claim)

        organization = self._create_organization(row, logical_claim)
        self._graph.add((claim_review_instance, self._schema_author_property_uri, organization))

        creative_work = self._create_creative_work(row, logical_claim)
        self._graph.add((claim_review_instance, self._schema_item_reviewed_property_uri, creative_work))
        logical_claim.creative_work_uri = creative_work

        original, normalized = self._create_review_rating(row)
        self._graph.add((claim_review_instance, rdflib.term.URIRef(self._schema_prefix['reviewRating']), original))
        self._graph.add(
            (claim_review_instance, rdflib.term.URIRef(self._schema_prefix['reviewRating']), normalized))

        # For claim review mentions
        if json.loads(row[u'extra_entities_claimReview_claimReviewed']):
            for mention_entry in json.loads(row[u'extra_entities_claimReview_claimReviewed']):
                mention = self._create_mention(mention_entry, logical_claim, True)
                if mention:
                    self._graph.add((creative_work, self._schema_mentions_property_uri, mention))

        # For Creative Work mentions
        if json.loads(row[u'extra_entities_body']):
            for mention_entry in json.loads(row[u'extra_entities_body']):
                mention = self._create_mention(mention_entry, logical_claim, False)
                if mention:
                    self._graph.add((claim_review_instance, self._schema_mentions_property_uri, mention))

        self._logical_view_claims.append(logical_claim)
        self.global_statistics.compute_stats_for_review(logical_claim)
        self.per_source_statistics[source_site].compute_stats_for_review(logical_claim)

    progress_bar.close()


def export_rdf(self, format):
    graph_serialization = self._graph.serialize(format=format, encoding='utf-8')
    print("\nGlobal dataset statistics")
    self.global_statistics.output_stats()

    print("\nPer source site statistics")

    for site in self.per_source_statistics.keys():
        print("\n\n{site} statistics...".format(site=site))
        self.per_source_statistics[site].output_stats()
    return graph_serialization


def reconcile_claims(self, embeddings, theta, keyword_weight,
                     link_weight, text_weight, entity_weight, mappings_file_path=None, seed=None, samples=None):
    reconciler = FactReconciler(embeddings, self._use_caching, mappings_file_path, self._logical_view_claims, theta,
                                keyword_weight, link_weight, text_weight, entity_weight, seed=seed, samples=samples)
    mappings = reconciler.generate_mappings()

    for mapping in mappings:
        if mapping is not None and mapping[1] is not None and mapping[1] != (None, None):
            source = mapping[1][0]
            target = mapping[1][1]
            self.global_statistics.count_mapping()
            self._graph.add((source.creative_work_uri, OWL.sameAs, target.creative_work_uri))

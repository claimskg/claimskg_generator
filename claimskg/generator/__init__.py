import datetime
import html
import itertools
import re
import uuid
from logging import getLogger
from typing import List
from urllib.parse import urlparse

import rdflib
from SPARQLWrapper import SPARQLWrapper
from pandas.io import json
from rdflib import URIRef, Literal, Graph
from rdflib.extras.external_graph_libs import rdflib_to_networkx_multidigraph
from rdflib.namespace import NamespaceManager, RDF, OWL, XSD, Namespace, RDFS
from tqdm import tqdm

import claimskg.generator.ratings
from claimskg.generator.skosthesaurusmatcher import SkosThesaurusMatcher
from claimskg.generator.statistics import ClaimsKGStatistics
from claimskg.reconciler import FactReconciler
from claimskg.util import TypedCounter

logger = getLogger()

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
    'factcheck_aap': "https://factcheck.aap.com.au",
    'factuel_afp_fr': "https://factuel.afp.com/",
    "factcheck_afp": "https://factcheck.afp.com/",
    "fullfact": "https://fullfact.org/"
}


def _row_string_value(row, key):
    value = row[key]
    if not value:
        value = ""
    return value


def _row_string_values(row, keys: List[str]):
    return [_row_string_value(row, key) for key in keys]


class ClaimLogicalView:
    def __init__(self):
        self.review_entities = []
        self.review_entity_categories = []
        self.claim_entities = []
        self.claim_entity_categories = []
        self.keywords = set()
        self.keywords_thesoz = set()
        self.keywords_unesco = set()
        self.keywords_dbpedia = set()
        self.keywords_thesoz_dbpedia = set()
        self.keywords_unesco_dbpedia = set()
        self.links = []
        self.text_fragments = []
        self.claimreview_author = ""
        self.creative_work_author = ""
        self.creative_work_uri = None
        self.claim_review_url = None
        self.claim_date = None
        self.review_date = None
        self.has_body_text = False
        self.has_headline = False
        self.title = ""
        self.normalized_rating = ""


class ClaimsKGURIGenerator:
    def __init__(self, base_uri):
        self.base_uri = base_uri
        self._claimskg_prefix = rdflib.Namespace(base_uri)

    def creative_work_uri(self, row):
        uuid_key = "".join(_row_string_values(row, ["creativeWork_author_name", "creativeWork_author_sameAs",
                                                    "creativeWork_datePublished", "claimReview_claimReviewed"]))
        return URIRef(self._claimskg_prefix["creative_work/" + str(
            uuid.uuid5(namespace=uuid.NAMESPACE_URL, name=uuid_key))])

    def claim_review_uri(self, row):
        uuid_key = "".join(
            _row_string_values(row, ["claimReview_author_name", "claimReview_author_url", "claimReview_datePublished",
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

    def keyword_uri(self, keyword):
        uuid_key = keyword
        return URIRef(self._claimskg_prefix["keyword/" + str(uuid.uuid5(namespace=uuid.NAMESPACE_URL,
                                                                        name=uuid_key))])

    def create_original_rating_uri(self, row):
        uuid_key = "_".join(
            _row_string_values(row, ["claimReview_author_name", "rating_alternateName",
                                     "rating_ratingValue"])).lower().replace(" ", "_").replace("\n", "_") \
            .replace("[", "").replace("]", "").replace("'", "").replace(",", ",").replace("\\", "").strip() \
            .replace("/", "").replace("<", "").replace(">", "")
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
        self.thesoz = SkosThesaurusMatcher(self._graph, thesaurus_path="claimskg/data/thesoz-komplett.xml",
                                           skos_xl_labels=True, prefix="http://lod.gesis.org/thesoz/")
        self._graph = self.thesoz.get_merged_graph()

        self.unesco = SkosThesaurusMatcher(self._graph, thesaurus_path="claimskg/data/unesco-thesaurus.xml",
                                           skos_xl_labels=False, prefix="http://vocabularies.unesco.org/thesaurus/")

        self._graph = self.unesco.get_merged_graph()

        self._graph.load("claimskg/data/dbpedia_categories_lang_en_skos.ttl", format="turtle")

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

        self._rdfs_prefix = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema#")
        self._namespace_manager.bind('rdfs', self._rdfs_prefix, override=False)

        self._schema_prefix = rdflib.Namespace("http://schema.org/")
        self._namespace_manager.bind('schema', self._schema_prefix, override=False)

        self._namespace_manager.bind('owl', OWL, override=True)

        self._dbo_prefix = rdflib.Namespace("http://dbpedia.org/ontology/")
        self._namespace_manager.bind("dbo", self._dbo_prefix, override=False)

        self._dbr_prefix = rdflib.Namespace("http://dbpedia.org/resource/")
        self._namespace_manager.bind("dbr", self._dbr_prefix, override=False)

        self._dbc_prefix = rdflib.Namespace("http://dbpedia.org/resource/Category_")
        self._namespace_manager.bind("dbc", self._dbr_prefix, override=False)

        self._dcat_prefix = rdflib.Namespace("http://www.w3.org/ns/dcat#")
        self._namespace_manager.bind("dcat", self._dcat_prefix, override=False)

        self._dct_prefix = rdflib.Namespace("http://purl.org/dc/terms/")
        self._namespace_manager.bind("dct", self._dct_prefix, override=False)

        self._foaf_prefix = rdflib.Namespace("http://xmlns.com/foaf/0.1/")
        self._namespace_manager.bind("foaf", self._foaf_prefix, override=False)

        self._vcard_prefix = rdflib.Namespace("http://www.w3.org/2006/vcard/ns#")
        self._namespace_manager.bind("vcard", self._vcard_prefix, override=False)

        self._adms_prefix = Namespace("http://www.w3.org/ns/adms#")
        self._namespace_manager.bind("adms", self._adms_prefix, override=False)

        self._skos_prefix = Namespace("http://www.w3.org/2004/02/skos/core#")
        self._namespace_manager.bind("skos", self._skos_prefix, override=False)

        self._owl_same_as = URIRef(OWL['sameAs'])

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
        self._creative_works_index = []

        self.keyword_uri_set = set()

        self.global_statistics = ClaimsKGStatistics()
        self.per_source_statistics = {}

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

        claim_review_url = row['claimReview_url']
        claim.claim_review_url = claim_review_url

        if claim_review_url is not None:
            self._graph.add(
                (claim_review_instance, self._schema_url_property_uri, URIRef(row['claimReview_url'])))

        review_date = row['claimReview_datePublished']
        if review_date:
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

    def _reconcile_keyword_annotations(self, claim, keyword_uri, keyword, matching_annotations, type="thesoz"):
        for annotation in matching_annotations:
            self._graph.add((keyword_uri, URIRef(self._dct_prefix["about"]), URIRef(annotation[0])))
            if type == "thesoz":
                claim.keywords_thesoz.add(keyword)
            else:
                claim.keywords_unesco.add(keyword)

    def _reconcile_keyword_mention_with_annotations(self, claim, mention, dbpedia_entity, keyword,
                                                    matching_annotations, type="thesoz"):
        start = mention['begin']
        end = mention['end']
        for matching_annotation in matching_annotations:
            if start == matching_annotation[2] and end == matching_annotation[3]:
                if type == "thesoz":
                    claim.keywords_thesoz_dbpedia.add(keyword)
                elif type == "unesco":
                    claim.keywords_unesco_dbpedia.add(keyword)
                self._graph.add(
                    (URIRef(dbpedia_entity), OWL.sameAs, URIRef(matching_annotation[0])))

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
            keyword_mentions = self._process_json(row['extra_entities_keywords'])
            if not keyword_mentions:
                keyword_mentions = []
            if ";" in keywords:
                keyword_list = keywords.split(";")
            else:
                keyword_list = keywords.split(",")

            for keyword in keyword_list:
                keyword = keyword.strip()
                keyword_uri = self._uri_generator.keyword_uri(keyword)
                if keyword_uri not in self.keyword_uri_set:
                    self._graph.add((keyword_uri, RDF.type, self._schema_thing_class_uri))
                    self._graph.add(
                        (keyword_uri, self._schema_name_property_uri, Literal(keyword, lang=self._iso1_language_tag)))
                    thesoz_matching_annotations = self.thesoz.find_keyword_matches(keyword)
                    unesco_matching_annotations = self.unesco.find_keyword_matches(keyword)
                    self._reconcile_keyword_annotations(claim, keyword_uri, keyword, thesoz_matching_annotations)
                    self._reconcile_keyword_annotations(claim, keyword_uri, keyword, unesco_matching_annotations,
                                                        type="unesco")
                    for mention in keyword_mentions:
                        if keyword.lower().strip() in mention['text'].lower().strip():
                            self.keyword_uri_set.add(keyword_uri)
                            mention_instance, dbpedia_entity = self._create_mention(mention, claim, False)
                            if mention_instance:
                                claim.keywords_dbpedia.add(keyword)
                                self._graph.add((keyword_uri, self._schema_mentions_property_uri, mention_instance))

                                self._reconcile_keyword_mention_with_annotations(claim, mention, dbpedia_entity,
                                                                                 keyword, thesoz_matching_annotations)
                                self._reconcile_keyword_mention_with_annotations(claim, mention, dbpedia_entity,
                                                                                 keyword, unesco_matching_annotations,
                                                                                 type="unesco")
                claim.keywords.add(keyword.strip())

                self._graph.add((creative_work, self._schema_keywords_property_uri, keyword_uri))

        links = row['extra_refered_links']
        author_url = _row_string_value(row, 'claimReview_author_url')
        if links:
            links = links[1:-1].split(",")
            for link in links:
                stripped_link = link.strip()
                if len(stripped_link) > 0 and stripped_link[0] != "#" and re.match(_is_valid_url_regex,
                                                                                   link.strip()) and link.strip() != \
                        source_uri_dict[
                            author_url]:
                    link = link.strip().replace("\\", "").replace(
                        "%20TARGET=prayer>adultery</A>%20was%20made%20public.%20</p>%0A", "").replace("\"", "").replace(
                        "<img%20src=?", "").replace(">", "").replace("</", "").replace("<", "")

                    parsed_url = urlparse(link)
                    is_correct = (all([parsed_url.scheme, parsed_url.netloc, parsed_url.path])
                                  and len(parsed_url.netloc.split(".")) > 1 and "<img" not in link)
                    if is_correct:
                        claim.links.append(link)
                        # try:
                        self._graph.add(
                            (creative_work, self._schema_citation_preperty_uri,
                             URIRef(
                                 parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path + "?" +
                                 parsed_url.query.replace("|", "%7C").replace("^", "%5E").replace("\\", "%5C").replace(
                                     "{", "%7B").replace("}", "%7D").replace("&", "%26").replace("=", "%3D"))))
                    # except :
                    #     pass
        # Creative work author instantiation

        author_value = _row_string_value(row, "creativeWork_author_name")
        claim.creative_work_author = author_value

        claim_reviewed_value = _normalize_text_fragment(_row_string_value(row, "claimReview_claimReviewed"))
        claim.title = claim_reviewed_value
        self._graph.add(
            (creative_work, self._schema_text_property_uri,
             Literal(claim_reviewed_value,
                     lang=self._iso1_language_tag)))

        if len(author_value) > 0:
            creative_work_author = self._uri_generator.creative_work_author_uri(row)
            self._graph.add((creative_work_author, RDF.type, self._schema_thing_class_uri))

            author_mentions = self._process_json(row['extra_entities_author'])
            if not author_mentions:
                author_mentions = []
            for mention in author_mentions:
                entity_uri = mention['entity'].replace(" ", "_")
                mention_instance = self._dbr_prefix[entity_uri]
                if mention_instance:
                    self._graph.add((creative_work_author, self._schema_mentions_property_uri, mention_instance))

            self._graph.add(
                (creative_work_author, self._schema_name_property_uri,
                 Literal(author_value, lang=self._iso1_language_tag)))
            self._graph.add((creative_work, self._schema_author_property_uri, creative_work_author))

        # Todo: Reconcile author entities with DBPedia
        # self._graph.add((creative_work_author, self._schema_same_as_property_uri, Literal("dbpedia:link")))
        self._creative_works_index.append(creative_work)
        return creative_work

    def _create_review_rating(self, row, claim):

        original_rating = self._uri_generator.create_original_rating_uri(row)

        rating_alternate_name = row['rating_alternateName']
        if rating_alternate_name:
            escaped_alternate_rating_name = html.escape(row['rating_alternateName']).encode('ascii',
                                                                                            'xmlcharrefreplace')
            self._graph.add(
                (original_rating, self._schema_alternate_name_property_uri,
                 Literal(escaped_alternate_rating_name)))

        self._graph.add((original_rating, RDF.type, self._schema_rating_class_uri))

        rating_value = row['rating_ratingValue'].replace("[", "").replace("]", "").replace("'", "").replace(",",
                                                                                                            "").strip()

        if rating_value and len(rating_value) > 0:
            value = float(rating_value)
            self._graph.add(
                (original_rating, self._schema_rating_value_property_uri,
                 Literal(value, datatype=XSD.float)))

        organization = self._uri_generator.organization_uri(row)
        self._graph.add((original_rating, self._schema_author_property_uri, organization))

        normalized_rating_enum = ratings.normalize(_row_string_value(row, "claimReview_author_name").lower(),
                                                   _row_string_value(row, "rating_alternateName").lower())
        claim.normalized_rating = normalized_rating_enum.name
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
        rho_value = float(mention_entry['score'])
        if rho_value > self._threshold:

            text = mention_entry['text']
            start = mention_entry['begin']
            end = mention_entry['end']
            entity_uri = mention_entry['entity'].replace(" ", "_")
            categories = mention_entry['categories']
            if len(categories) > 0:
                categories = categories[0].split(",")

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

            self._graph.add((mention, self.its_ta_ident_ref_property_uri, self._dbr_prefix[entity_uri]))
            if in_review:
                claim.review_entities.append(entity_uri)
                for category in categories:
                    claim.review_entity_categories.append(category)
            else:
                claim.claim_entities.append(entity_uri)

                for category in categories:
                    claim.claim_entity_categories.append(category)

            for category in categories:
                category = category.replace(" ", "_")
                self._graph.add((mention, URIRef(self._dct_prefix["about"]), URIRef(self._dbc_prefix[category])))

            return mention, self._dbr_prefix[entity_uri]
        else:
            return None, None

    @staticmethod
    def _format_confidence_score(mention_entry):
        value = float(mention_entry['score'])
        rounded_to_two_decimals = round(value, 2)
        return str(rounded_to_two_decimals)

    def create_contact_vcard(self):
        atchechmedjiev_contact_vcard = URIRef(self._claimskg_prefix['atchechmedjiev_contact_vcard'])
        self._graph.add((atchechmedjiev_contact_vcard, RDF.type, URIRef(self._vcard_prefix['Individual'])))
        self._graph.add(
            (atchechmedjiev_contact_vcard, self._vcard_prefix['hasEmail'],
             URIRef("mailto:andon.tchechmedjiev@mines-ales.fr")))
        self._graph.add(
            (atchechmedjiev_contact_vcard, self._vcard_prefix['fn'], Literal("Andon Tchechmedjiev")))

        return atchechmedjiev_contact_vcard

    def add_dcat_metadata(self):
        claimskg = rdflib.term.URIRef(self._claimskg_prefix['claimskg'])
        self._graph.add((claimskg,
                         RDF.type,
                         rdflib.term.URIRef(self._dcat_prefix['Dataset'])))
        self._graph.add((claimskg, rdflib.term.URIRef(self._dct_prefix['title']),
                         Literal("ClaimsKG")))
        self._graph.add((claimskg, rdflib.term.URIRef(self._dct_prefix['description']),
                         Literal("ClaimsKG: A Live Knowledge Graph ofFact-Checked Claims")))

        self._graph.add((claimskg, rdflib.term.URIRef(self._dct_prefix['issued']),
                         rdflib.term.Literal("2019-04-10", datatype=XSD.date)))

        self._graph.add((claimskg, rdflib.term.URIRef(self._dct_prefix['modified']),
                         rdflib.term.Literal(datetime.datetime.now(), datatype=XSD.date)))

        doi_org = URIRef(self._claimskg_prefix['doi_org_instance'])
        self._graph.add((
            doi_org, RDF.type, URIRef(self._foaf_prefix['Organization'])
        ))
        self._graph.add((doi_org, RDFS.label, Literal("International DOI Foundation")))
        self._graph.add((doi_org, self._foaf_prefix['homepage'], URIRef("https://www.doi.org/")))

        identifier = URIRef(self._claimskg_prefix['doi_identifier'])
        self._graph.add((identifier, RDF.type, self._adms_prefix['Identifier']))
        self._graph.add((identifier, self._skos_prefix['notation'], URIRef("https://doi.org/10.5281/zenodo.2628745")))
        self._graph.add((identifier, self._adms_prefix['schemaAgency'], Literal("International DOI Foundation")))
        self._graph.add((identifier, self._dct_prefix['creator'], doi_org))

        self._graph.add((claimskg, rdflib.term.URIRef(self._dct_prefix['identifier']),
                         rdflib.term.Literal("10.5281/zenodo.2628745")))

        self._graph.add((claimskg, rdflib.term.URIRef(self._dct_prefix['language']),
                         rdflib.term.URIRef("http://id.loc.gov/vocabulary/iso639-1/en")))

        self._graph.add((claimskg, rdflib.term.URIRef(self._dct_prefix['accrualPeriodicity']),
                         URIRef("http://purl.org/linked-data/sdmx/2009/code#freq-M")))

        self._graph.add((claimskg, rdflib.term.URIRef(self._dcat_prefix['keyword']),
                         Literal("Claims")))
        self._graph.add((claimskg, rdflib.term.URIRef(self._dcat_prefix['keyword']),
                         Literal("Facts")))
        self._graph.add((claimskg, rdflib.term.URIRef(self._dcat_prefix['keyword']),
                         Literal("Fact-checking")))
        self._graph.add((claimskg, rdflib.term.URIRef(self._dcat_prefix['keyword']),
                         Literal("Knowledge Graphs")))

        self._graph.add((claimskg, rdflib.term.URIRef(self._dcat_prefix['contactPoint']),
                         self.create_contact_vcard()))

        # SPARQL Distribution
        sparql_claimskg_distribution = URIRef(self._claimskg_prefix['sparql_claimskg_distribution'])
        self._graph.add((sparql_claimskg_distribution, RDF.type, self._dcat_prefix['Distribution']))
        self._graph.add((sparql_claimskg_distribution, self._dct_prefix['title'], Literal("SPARQL endpoint")))
        self._graph.add(
            (sparql_claimskg_distribution, self._dct_prefix['description'], Literal("The ClaimsKG SPARQL endpoint"))
        )

        self._graph.add((sparql_claimskg_distribution, rdflib.term.URIRef(self._dct_prefix['issued']),
                         rdflib.term.Literal("2019-04-10", datatype=XSD.date)))

        self._graph.add((sparql_claimskg_distribution, rdflib.term.URIRef(self._dct_prefix['modified']),
                         rdflib.term.Literal(datetime.datetime.now(), datatype=XSD.date)))

        licence_document = URIRef("https://creativecommons.org/licenses/by/4.0/")
        self._graph.add((licence_document, RDF.type, self._dct_prefix['LicenseDocument']))

        self._graph.add((sparql_claimskg_distribution, rdflib.term.URIRef(self._dct_prefix['license']),
                         licence_document))

        self._graph.add((sparql_claimskg_distribution, rdflib.term.URIRef(self._dcat_prefix['accessURL']),
                         Literal("https://data.gesis.org/claimskg/sparql")))

        # Source code distribution
        sourcecode_claimskg_distribution = URIRef(self._claimskg_prefix['sourcecode_claimskg_distribution'])
        self._graph.add((sourcecode_claimskg_distribution, RDF.type, self._dcat_prefix['Distribution']))
        self._graph.add((sourcecode_claimskg_distribution, self._dct_prefix['title'], Literal("SPARQL endpoint")))
        self._graph.add(
            (sourcecode_claimskg_distribution, self._dct_prefix['description'],
             Literal("The ClaimsKG Github repository group"))
        )

        self._graph.add((sourcecode_claimskg_distribution, rdflib.term.URIRef(self._dct_prefix['issued']),
                         rdflib.term.Literal("2019-04-10", datatype=XSD.date)))

        self._graph.add((sourcecode_claimskg_distribution, rdflib.term.URIRef(self._dct_prefix['modified']),
                         rdflib.term.Literal(datetime.datetime.now(), datatype=XSD.date)))

        self._graph.add((sourcecode_claimskg_distribution, rdflib.term.URIRef(self._dct_prefix['license']),
                         licence_document))

        self._graph.add((sourcecode_claimskg_distribution, rdflib.term.URIRef(self._dcat_prefix['accessURL']),
                         Literal("https://github.com/claimskg")))

    def generate_model(self, dataset_rows):
        row_counter = 0

        self._graph.namespace_manager = self._namespace_manager
        total_entry_count = len(dataset_rows)

        self.add_dcat_metadata()

        progress_bar = tqdm(total=total_entry_count)

        for row in dataset_rows:
            row_counter += 1
            progress_bar.update(1)

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

            original, normalized = self._create_review_rating(row, logical_claim)
            self._graph.add((claim_review_instance, rdflib.term.URIRef(self._schema_prefix['reviewRating']), original))
            self._graph.add(
                (claim_review_instance, rdflib.term.URIRef(self._schema_prefix['reviewRating']), normalized))

            # For claim review mentions
            entities_json = row['extra_entities_claimReview_claimReviewed']  # type: str
            loaded_json = self._process_json(entities_json)
            if loaded_json:
                for mention_entry in loaded_json:
                    mention, dbpedia_entity = self._create_mention(mention_entry, logical_claim, True)
                    if mention:
                        self._graph.add((creative_work, self._schema_mentions_property_uri, mention))

            # For Creative Work mentions
            body_entities_json = row['extra_entities_body']
            loaded_body_json = self._process_json(body_entities_json)
            if loaded_body_json:
                for mention_entry in loaded_body_json:
                    mention, dbpedia_entity = self._create_mention(mention_entry, logical_claim, False)
                    if mention:
                        self._graph.add((claim_review_instance, self._schema_mentions_property_uri, mention))

            self._logical_view_claims.append(logical_claim)
            self.global_statistics.compute_stats_for_review(logical_claim)
            self.per_source_statistics[source_site].compute_stats_for_review(logical_claim)

        progress_bar.close()

    def _process_json(self, json_string):
        loaded_json = []
        if json_string:
            json_string = re.sub("\",\"\"", ",\"", json_string)
            json_string = re.sub('"\n\t\"', "", json_string)
            json_string = re.sub('}\]\[\]', '}]', json_string)

            if json_string == "[[][]]":
                loaded_json = []
            else:
                try:
                    loaded_json = json.loads(json_string)
                except ValueError:
                    loaded_json = None
        return loaded_json

    def export_rdf(self, format):

        print("\nGlobal dataset statistics")
        self.global_statistics.output_stats()

        print("\nPer source site statistics")

        for site in self.per_source_statistics.keys():
            print("\n\n{site} statistics...".format(site=site))
            self.per_source_statistics[site].output_stats()
        graph_serialization = self._graph.serialize(format=format, encoding='utf-8')
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
                self._graph.add((source.creative_work_uri, OWL.sameAs, target.creative_work_uri))

    def materialize_indirect_claim_links(self):
        mdg = rdflib_to_networkx_multidigraph(self._graph)

    def align_duplicated(self):
        count = len(self._logical_view_claims)
        total = int(count * (count - 1) / 2)
        result = [pair for
                  pair in
                  tqdm(itertools.combinations(range(count), 2), total=total) if
                  self.compare_claim_titles(self._logical_view_claims[pair[0]], self._logical_view_claims[
                      pair[1]])]

        for pair in result:
            self._graph.add(
                (self._creative_works_index[pair[0]], self._owl_same_as, self._creative_works_index[pair[1]]))

            self.global_statistics.count_mapping()
            self.per_source_statistics[self._logical_view_claims[pair[0]].claimreview_author].count_mapping()

    def compare_claim_titles(self, claim_a, claim_b):
        return self._normalize_label(claim_a.title) == self._normalize_label(claim_b.title)

    def _normalize_label(self, label):
        return label.strip().lower().replace("\"", "").replace("'", "")

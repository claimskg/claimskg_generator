from logging import getLogger

from rdflib import Graph, Namespace

from claimskg.reconciler.dictionary import StringDictionaryLoader
from claimskg.reconciler.recognizer.intersection_recognizers import IntersStemConceptRecognizer

logger = getLogger()


class SkosThesaurusMatcher:
    def __init__(self, claimskg_graph: Graph, thesaurus_path="claimskg/data/thesoz-komplett.xml", skos_xl_labels=True,
                 prefix="http://lod.gesis.org/thesoz/"):
        self.claimskg_graph = claimskg_graph
        self.graph = Graph()
        logger.info("Loading thesaurus into ClaimsKG graph... [{}]".format(thesaurus_path))
        self.graph.load(thesaurus_path)

        string_entries = []

        if skos_xl_labels:
            query = """SELECT ?x ?lf WHERE {
                ?x a skos:Concept;
                skosxl:prefLabel ?l.
                ?l skosxl:literalForm ?lf.
                FILTER(lang(?lf)='en' || lang(?lf)='fr')
            }
            """
            pref_labels = self.graph.query(query, initNs={'skos': Namespace("http://www.w3.org/2004/02/skos/core#"),
                                                          'skosxl': Namespace("http://www.w3.org/2008/05/skos-xl#")})
        else:
            query = """SELECT ?x ?lf WHERE {
                 ?x a skos:Concept;
                 skos:prefLabel ?lf.
                 FILTER(lang(?lf)='en' || lang(?lf)='fr')
             }
             """
            pref_labels = self.graph.query(query, initNs=dict(skos=Namespace("http://www.w3.org/2004/02/skos/core#")))

        for result in pref_labels:
            string_entries.append((str(result[0]), str(result[1])))

        if skos_xl_labels:
            query = """SELECT ?x ?lf WHERE {
                ?x a skos:Concept;
                skosxl:prefLabel ?l.
                ?l skosxl:literalForm ?lf.
                FILTER(lang(?lf)='en' || lang(?lf)='fr')
            }
        """
            alt_labels = self.graph.query(query, initNs=dict(skos=Namespace("http://www.w3.org/2004/02/skos/core#"),
                                                             skosxl=Namespace("http://www.w3.org/2008/05/skos-xl#")))
        else:
            query = """SELECT ?x ?lf WHERE {
            ?x a skos:Concept;
            skos:altLabel ?lf.
            FILTER(lang(?lf)='en' || lang(?lf)='fr')

        }
        """
            alt_labels = self.graph.query(query, initNs=dict(skos=Namespace("http://www.w3.org/2004/02/skos/core#")))

        for result in alt_labels:
            string_entries.append((str(result[0]), str(result[1])))
        dictionary_loader = StringDictionaryLoader(string_entries)
        dictionary_loader.load()

        self.concept_recognizer = IntersStemConceptRecognizer(dictionary_loader,
                                                              "claimskg/data/stopwordsen.txt",
                                                              "claimskg/data/termination_termsen.txt")
        self.concept_recognizer.initialize()

    def get_merged_graph(self):
        return self.claimskg_graph + self.graph

    def find_keyword_matches(self, keyword):
        matching_annotations = self.concept_recognizer.recognize(keyword)
        return_annotations = set()
        for matching_annotation in matching_annotations:
            delta = matching_annotation.end - matching_annotation.start
            if len(keyword) == delta:
                return_annotations.add((matching_annotation.concept_id, matching_annotation.matched_text,
                                        matching_annotation.start, matching_annotation.end))
        return return_annotations

import sys
from datetime import timedelta
from typing import List

import redis
from nltk.corpus import stopwords
from nltk.tokenize import TreebankWordTokenizer
from redis import StrictRedis
from scipy.spatial import distance

import similarity as sim
from vsm.embeddings import Embeddings

_stop_words = set(stopwords.words('english'))
import re

stopword_pattern = re.compile(r'\b(' + r'|'.join(_stop_words) + r')\b\s*')


def _merge_and_normalise_strings(strings):
    return stopword_pattern.sub("", " ".join(strings).strip().lower())


class ClaimLogicalView:
    def __init__(self):
        self.entities = []
        self.keywords = []
        self.links = []
        self.text_fragments = []
        self.author = ""
        self.creative_work_uri = None
        self.claim_date = None
        self.review_date = None


class FactReconciler:
    def __init__(self, embeddings: Embeddings, caching: bool):
        self._embeddings = embeddings
        self._caching = caching
        if caching:
            self.redis = redis.StrictRedis()
        else:
            self.redis = None

    def generate_mappings(self, claims: List[ClaimLogicalView], theta: float, entity_weight, keyword_weight,
                          link_weight, author_weight, text_weight):
        mappings = []
        # Iterate over pairs of claims on the upper diagonal of the grid (order doesn't matter)
        # Compute the score for each evaluated pair and create a new mapping if the score is
        # above the threshold (theta)

        futures = []

        total = len(claims) * (len(claims) - 1) / 2
        one_tenth_per_cent = int(total * 0.0001)
        completed = 0
        mapped = 0
        computed = 0
        index_a = len(claims) - 1

        while index_a >= 0:
            claim_a = claims[index_a]
            index_b = index_a
            while index_b >= 0:
                claim_b = claims[index_b]
                if claim_a != claim_b and not self.pruning_criterion(claim_a, claim_b):
                    if completed % one_tenth_per_cent == 0:
                        sys.stdout.write(
                            "{current}/{total} [{percent}%] (M {mapped}) (C {computed})                                        \r".format(
                                current=completed, total=total,
                                percent=round((completed / float(total)) * 100, 2), mapped=mapped, computed=computed))
                    score = self.claim_similarity(claim_a, claim_b, entity_weight,
                                                  keyword_weight, link_weight,
                                                  author_weight, text_weight)
                    computed += 1
                    if score > theta:
                        mapped += 1
                        mappings.append((claim_a.creative_work_uri, claim_b.creative_work_uri))
                        print("<{a}> owl:sameAs <{b}>".format(a=claim_a.creative_work_uri, b=claim_b.creative_work_uri))
                completed += 1
                index_b -= 1
            index_a -= 1

        return mappings

    def pruning_criterion(self, claim_a, claim_b):
        # Date criterion
        if claim_a.claim_date is not None and claim_b.claim_date is not None \
                and claim_a.claim_date != claim_b.claim_date:
            return True
        elif claim_a.review_date is not None and claim_b.review_date is not None \
                and claim_a.review_date - claim_b.review_date > timedelta(days=3):
            return True
        else:
            return False

    def claim_similarity(self, claim_a: ClaimLogicalView, claim_b: ClaimLogicalView, entity_weight, keyword_weight,
                         link_weight, author_weight, text_weight):

        key = str(claim_a.creative_work_uri) + str(claim_b.creative_work_uri)
        if self._caching and self.redis.exists(key):
            score = float(self.redis.get(key))
        else:
            entity_similarity = FactReconciler._entity_similarity(claim_a, claim_b)
            keyword_similarity = FactReconciler._keyword_similarity(claim_a, claim_b)
            link_similarity = FactReconciler._link_similarity(claim_a, claim_b)
            author_similarity = FactReconciler._author_similarity(claim_a, claim_b)
            text_similarity = FactReconciler._text_fragment_similarity(claim_a, claim_b, self._embeddings, self.redis)
            score = sim.geometric_mean_aggregation([
                (entity_similarity, entity_weight),
                (keyword_similarity, keyword_weight),
                (link_similarity, link_weight),
                (author_similarity, author_weight),
                (text_similarity, text_weight)])
            if self._caching:
                self.redis.set(key, score)

        return score

    @staticmethod
    def _entity_similarity(first, other):
        overlap = sim.compute_hard_overlap(first.entities, other.entities)
        return sim.jaccard(overlap, len(first.entities) + len(other.entities))

    @staticmethod
    def _keyword_similarity(first, other):
        overlap = sim.compute_hard_overlap(first.keywords, other.keywords)
        return sim.jaccard(overlap, len(first.keywords) + len(other.keywords))

    @staticmethod
    def _link_similarity(first, other):
        overlap = sim.compute_hard_overlap(first.links, other.links)
        return sim.jaccard(overlap, len(first.links) + len(other.links))

    @staticmethod
    def _author_similarity(first, other):
        if len(first.author) == 0 and len(other.author) == 0 or first.author == other.author:
            return 1
        else:
            return 0

    @staticmethod
    def _text_fragment_similarity(first, other, embeddings: Embeddings, redis: StrictRedis):

        tokenizer = TreebankWordTokenizer()
        string_a = _merge_and_normalise_strings(first.text_fragments).lower()
        string_b = _merge_and_normalise_strings(other.text_fragments).lower()
        string_a_tokens = tokenizer.tokenize(string_a)
        string_b_tokens = tokenizer.tokenize(string_b)

        key_1 = str(first.creative_work_uri)
        key_2 = str(other.creative_work_uri)

        if redis is not None and redis.exists(key_1):
            vector_a = Embeddings.load_vector_from_cache(key_1, redis)
        else:
            vector_a = embeddings.arithmetic_mean_bow_embedding(string_a_tokens)
            if redis is not None:
                Embeddings.cache_vector(key_1, vector_a, redis)

        if redis is not None and redis.exists(key_2):
            vector_b = Embeddings.load_vector_from_cache(key_2, redis)
        else:
            vector_b = embeddings.arithmetic_mean_bow_embedding(string_b_tokens)
            if redis is not None:
                Embeddings.cache_vector(key_2, vector_b, redis)

        return 1 - distance.cosine(vector_a, vector_b)

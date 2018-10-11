import itertools
from datetime import timedelta

import numpy
import redis
from nltk.corpus import stopwords
from tqdm import tqdm

from claimskg import similarity as sim
from claimskg.similarity import cached_embedding_text_thematic_similarity
from claimskg.vsm.embeddings import Embeddings

_stop_words = set(stopwords.words('english'))
import re

stopword_pattern = re.compile(r'\b(' + r'|'.join(_stop_words) + r')\b\s*')


def _merge_and_normalise_strings(strings):
    return re.sub(r'\[.*?\]|\(.*?\)|\W', ' ', stopword_pattern.sub("", " ".join(strings).strip().lower()))


def _process_pairwise_sample(sample_size, collection, seed, callback, *args):
    count = len(collection)
    generator_list = itertools.combinations(range(count), 2)
    iterlen = int(count * (count - 1) / 2)
    progress_bar = tqdm(total=sample_size)

    random = numpy.random
    if seed is not None:
        random.seed(seed)

    num = 0
    inds = random.random(iterlen) <= (sample_size * 1.0 / (iterlen))
    results = []
    iterator = iter(generator_list)
    gotten = 0
    while gotten < sample_size and num < iterlen - 1:
        b = next(iterator)
        if inds[num]:
            results.append(callback((collection[b[0]], collection[b[1]]), *args))
            gotten += 1
            progress_bar.update(1)
        num += 1
        if num == iterlen:
            num = 0
            iterator = iter(generator_list)
            inds = numpy.random.random(iterlen) <= ((sample_size - gotten) * 1.0 / iterlen / 3)
    print("Post")
    progress_bar.close()
    return results


class FactReconciler:
    def __init__(self, embeddings: Embeddings, caching: bool, mappings_file_path: str, claims, theta: float,
                 keyword_weight,
                 link_weight, text_weight, entity_weight, seed=None, samples=None):
        self._embeddings = embeddings
        self._caching = caching
        if caching:
            self._redis = redis.StrictRedis()
        else:
            self._redis = None

        if mappings_file_path is not None:
            self.output_file = open(mappings_file_path, "w")
        else:
            self.output_file = None

        self.claims = claims

        self.theta = theta
        self.keyword_weight = keyword_weight
        self.link_weight = link_weight
        self.text_weight = text_weight
        self.entity_weight = entity_weight
        self.claim_count = len(claims)
        self.seed = seed
        self.samples = samples

    # if self.output_file is not None:
    # self.output_file.write(
    # self._generate_claim_mapping_string_description(claim_a, claim_b))
    def generate_mappings(self):

        if self.output_file is not None:
            self.output_file.write(FactReconciler._generate_claim_mapping_output_header())

        # Iterate over pairs of claims on the upper diagonal of the grid (order doesn't matter)
        # Compute the score for each evaluated pair and create a new mapping if the score is
        # above the threshold (theta)

        # one_tenth_per_cent = int(total * 0.0001)
        # completed = 0
        # mapped = 0
        # computed = 0
        # index_a = len(self.claims) - 1

        if self.samples is not None:
            result = _process_pairwise_sample(self.samples, self.claims, self.seed, self._evaluate_mapping)
        else:
            result = [self._evaluate_mapping((self.claims[pair[0]], self.claims[pair[1]])) for pair in
                      tqdm(itertools.combinations(range(len(self.claims)), 2))]

        print(len(result))
        mappings = [x for x in result if x is not None]
        mappings = [
            (x, y) for x, y in mappings if (x, y) != (None, None)
        ]
        print(len(mappings))

        if self.output_file is not None:
            for mapping in mappings:
                if mapping is not None and mapping[1] is not None and mapping[1] != (None, None):
                    self.output_file.write(
                        FactReconciler._generate_claim_mapping_string_description(mapping[0], mapping[1][0],
                                                                                  mapping[1][1]))
            self.output_file.flush()
            self.output_file.close()

        return mappings

    @staticmethod
    def _generate_claim_mapping_output_header():
        return "\"Score\", \"CW Author A\",\"CW Author B\",\"CR Author A\",\"CR Author B\",\"Text Fragments A\"," \
               "\"Text Fragments B\",\"Entities A\",\"Entities B\",\"Keywords A\",\"Keywords B\",\"Citations A\"," \
               "\"Citations B\",\"URI A\",\"URI B\"\n"

    @staticmethod
    def _generate_claim_mapping_string_description(score, claim_a, claim_b):
        return "{score},\"{cwa_a}\",\"{cwa_b}\",\"{cra_a}\",\"{cra_b}\",\"{tf_a}\",\"{tf_b}\",\"{ent_a}\",\"{ent_b}\"," \
               "\"{kw_a}\",\"{kw_b}\",\"{cit_a}\",\"{cit_b}\", \"{uri_a}\",\"{uri_b}\",\n" \
            .format(
            # Claim A fields
            uri_a=claim_a.creative_work_uri, ent_a=",".join(claim_a.entities), kw_a=",".join(claim_a.keywords),
            cit_a=",".join(claim_a.links), cwa_a=claim_a.creative_work_author, cra_a=claim_a.claimreview_author,
            tf_a=claim_a.text_fragments[0].replace("\"", "''"),
            # Claim B fields
            uri_b=claim_b.creative_work_uri, ent_b=",".join(claim_b.entities), kw_b=",".join(claim_b.keywords),
            cit_b=",".join(claim_b.links), cwa_b=claim_b.creative_work_author, cra_b=claim_b.claimreview_author,
            tf_b=claim_b.text_fragments[0].replace("\"", "''"),
            score=score)

    @staticmethod
    def _pruning_criterion(claim_a, claim_b):
        prune = False
        author_score = FactReconciler.author_match(claim_a, claim_b)
        entity_score = sim.compute_hard_overlap(claim_a.entities, claim_b.entities)
        num_entities_a = len(claim_a.entities)
        num_entities_b = len(claim_b.entities)

        # Date criterion
        if claim_a.claim_date is not None and claim_b.claim_date is not None \
                and claim_a.claim_date != claim_b.claim_date:
            prune = True
        elif claim_a.review_date is not None and claim_b.review_date is not None \
                and claim_a.review_date - claim_b.review_date > timedelta(days=1):
            prune = True
        elif author_score != 1:
            prune = True
        elif num_entities_a != 0 and num_entities_b != 0 and entity_score <= 0.00001:
            prune = True
        elif num_entities_a == 0 and num_entities_b > 0 or num_entities_a > 0 and num_entities_b == 0:
            prune = True

        return prune

    @staticmethod
    def author_match(first, other):
        if first.creative_work_author == other.creative_work_author:
            return 1
        else:
            return 0

    def _evaluate_mapping(self, pair):
        result = None
        score = None
        claim_a = pair[0]
        claim_b = pair[1]
        if claim_a != claim_b and not FactReconciler._pruning_criterion(claim_a, claim_b):
            score = self._claim_similarity(claim_a, claim_b)

            if score > self.theta:
                result = pair

        return score, result

    def _claim_similarity(self, claim_a, claim_b):
        if len(claim_a.keywords) == 0 and len(claim_b.keywords) == 0:
            keyword_similarity = None
        else:
            keyword_similarity = sim.jaccard(claim_a.keywords, claim_b.keywords)

        link_similarity = sim.jaccard(claim_a.links, claim_b.links)
        if len(claim_a.entities) == 0 and len(claim_b.entities) == 0:
            entity_similarity = sim.jaccard(claim_a.entities, claim_b.entities)
        else:
            entity_similarity = None

        text_similarity = cached_embedding_text_thematic_similarity(
            _merge_and_normalise_strings(claim_a.text_fragments).lower(),
            _merge_and_normalise_strings(claim_b.text_fragments).lower(),
            self._embeddings, self._redis)

        score = sim.geometric_mean_aggregation([
            (entity_similarity, self.entity_weight),
            (keyword_similarity, self.keyword_weight),
            (link_similarity, self.link_weight),
            (text_similarity, self.text_weight)])

        return score

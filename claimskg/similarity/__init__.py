import hashlib
import math as math
from typing import List, Tuple

from nltk import TreebankWordTokenizer, collections
from nltk.corpus import stopwords
from nltk.util import everygrams
from redis import StrictRedis
from scipy.spatial import distance
import sent2vec

from claimskg.vsm.embeddings import Embeddings

tokenizer = TreebankWordTokenizer()

_stop_words = stopwords.words('english')


def compute_hard_overlap(collection_a: List[str], collection_b: List[str]):
    overlap_count = 0
    index_a = 0
    while index_a < len(collection_a):
        item_a = collection_a[index_a]
        index_b = 0
        while index_b < len(collection_b):
            item_b = collection_b[index_b]
            if item_a == item_b:
                overlap_count += 1
            index_b += 1
        index_a += 1
    return overlap_count


def tverski_ratio(alpha: float, beta: float, gamma: float, overlap_count: float, difference_a: float,
                  difference_b: float):
    contrast = tverski_contrast(alpha, beta, gamma, overlap_count, difference_a, difference_b)
    if contrast == 0:
        return 0
    else:
        return alpha * overlap_count / contrast


def tverski_contrast(alpha: float, beta: float, gamma: float, overlap_count: float, difference_a: float,
                     difference_b: float):
    return alpha * overlap_count - beta * difference_a - gamma * difference_b


def jaccard_count(overlap_count: float, union_count: float):
    if union_count == 0:
        return 0
    else:
        return overlap_count / union_count


def jaccard(collection_a, collection_b):
    overlap = compute_hard_overlap(collection_a, collection_b)
    return jaccard_count(overlap, len(collection_a) + len(collection_b))


def geometric_mean_aggregation(weighted_values: List[Tuple[float, float]]):
    length = len(weighted_values)
    overall_product = 1
    for (v, w) in weighted_values:
        if v is not None:
            if v < 0.00001:
                v = 0.00001
            overall_product *= math.pow(v, w)
    return math.pow(overall_product, 1.0 / float(length))


def arithmetic_mean_aggregation(weighted_values: List[Tuple[float, float]]):
    length = len(weighted_values)
    overall_sum = 0.0
    for (v, w) in weighted_values:
        overall_sum += v * w

    return overall_sum / float(length)


def cached_embedding_text_thematic_similarity(string_a, string_b, embeddings: Embeddings, redis: StrictRedis):
    string_a_tokens = [token for token in tokenizer.tokenize(string_a) if
                       token.isprintable() and token not in _stop_words]
    string_b_tokens = [token for token in tokenizer.tokenize(string_b) if
                       token.isprintable() and token not in _stop_words]

    one_grams_a = list(everygrams(string_a_tokens, 1, 1))  # .sort(key=lambda x: x[1])[:10]
    one_grams_b = list(everygrams(string_b_tokens, 1, 1))  # .sort(key=lambda x: x[1])[:10]
    one_grams_a = [word[0] for word, count in collections.Counter(one_grams_a).most_common(20)]
    one_grams_b = [word[0] for word, count in collections.Counter(one_grams_b).most_common(20)]

    key_1 = hashlib.md5(string_a.encode('utf-8')).hexdigest()
    key_2 = hashlib.md5(string_b.encode('utf-8')).hexdigest()

    if redis is not None and redis.exists(key_1):
        vector_a = Embeddings.load_vector_from_cache(key_1, redis)
    else:
        vector_a = embeddings.arithmetic_mean_bow_embedding(one_grams_a)
        if redis is not None:
            Embeddings.cache_vector(key_1, vector_a, redis)

    if redis is not None and redis.exists(key_2):
        vector_b = Embeddings.load_vector_from_cache(key_2, redis)
    else:
        vector_b = embeddings.arithmetic_mean_bow_embedding(one_grams_b)
        if redis is not None:
            Embeddings.cache_vector(key_2, vector_b, redis)

    return 1 - distance.cosine(vector_a, vector_b)


def cached_embedding_text_sentence_similarity_sent2vec(string_a, string_b, sent2vec, redis: StrictRedis):

    key_1 = hashlib.md5(string_a.encode('utf-8')).hexdigest()
    key_2 = hashlib.md5(string_b.encode('utf-8')).hexdigest()

    if redis is not None and redis.exists(key_1):
        vector_a = Embeddings.load_vector_from_cache(key_1, redis)
    else:
        vector_a = sent2vec.embed_sentence(string_a)
        if redis is not None:
            Embeddings.cache_vector(key_1, vector_a, redis)

    if redis is not None and redis.exists(key_2):
        vector_b = Embeddings.load_vector_from_cache(key_2, redis)
    else:
        vector_b = sent2vec.embed_sentence(string_b)
        if redis is not None:
            Embeddings.cache_vector(key_2, vector_b, redis)

    try:
        value = 1 - distance.cosine(vector_a, vector_b)
    except ValueError:
        value = 0

    return value
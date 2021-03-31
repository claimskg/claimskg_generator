import hashlib
from abc import ABC, abstractmethod
import sent2vec
from typing import List

import numpy
from nltk import TreebankWordTokenizer, everygrams, collections
from nltk.corpus import stopwords
from numpy.core.multiarray import ndarray
from redis import StrictRedis
from scipy.spatial import distance
from sklearn.decomposition import SparsePCA

tokenizer = TreebankWordTokenizer()

_stop_words = stopwords.words('english')


class Embeddings(ABC):
    def __init__(self, redis=None):
        self.redis = redis

    @abstractmethod
    def word_vector(self, word: str) -> ndarray:
        yield None

    @abstractmethod
    def dim(self):
        pass

    @staticmethod
    def vector_similarity(vector_1, vector_2):
        return 1 - distance.cosine(vector_1, vector_2)

    def word_similarity(self, word_1: str, word_2: str):
        vector_1 = self.word_vector(word_1)
        vector_2 = self.word_vector(word_2)
        return self.vector_similarity(vector_1, vector_2)

    def sentence_similarity(self, string_a, string_b, sample=None):
        vector_a = self.sentence_vector(string_a, sample)
        vector_b = self.sentence_vector(string_b, sample)
        return self.vector_similarity(vector_a, vector_b)

    def sentence_vector(self, sentence: str, sample=None) -> ndarray:
        tokens = [token for token in tokenizer.tokenize(sentence) if
                  token.isprintable() and token not in _stop_words]

        if sample:
            tokens = list(everygrams(tokens, 1, 1))  # .sort(key=lambda x: x[1])[:10]
            tokens = [word[0] for word, count in collections.Counter(tokens).most_common(sample)]
        key = hashlib.md5(sentence.encode('utf-8')).hexdigest()
        if self.redis is not None and self.redis.exists(key):
            vector = Embeddings.load_vector_from_cache(key, self.redis)
        else:
            vector = self.arithmetic_mean_bow_embedding(tokens)
            if self.redis is not None:
                Embeddings.cache_vector(key, vector, self.redis)

        return vector

    def arithmetic_mean_bow_embedding(self, tokens: List[str]):
        vectors = []
        for token in tokens:
            vectors.append(self.word_vector(token))

        return Embeddings.arithmetic_mean_aggregation(vectors)

    def geometric_mean_bow_embedding(self, tokens: List[str]):
        vectors = []
        for token in tokens:
            vectors.append(self.word_vector(token))

        return Embeddings.geometric_mean_aggregation(vectors)

    @staticmethod
    def arithmetic_mean_aggregation(vectors: List[ndarray]):
        length = len(vectors)
        if length > 0:
            sum_vector = vectors[0]
        else:
            sum_vector = numpy.identity(100)
            length = 1
        for vector in vectors[1:]:
            sum_vector = numpy.add(sum_vector, vector)

        return sum_vector / length

    @staticmethod
    def geometric_mean_aggregation(vectors: List[ndarray]):
        length = len(vectors)
        if length > 0:
            product_vector = vectors[0]
        else:
            product_vector = numpy.identity(100)
            length = 1

        for vector in vectors[1:]:
            numpy.multiply(product_vector, vector)

        return numpy.power(product_vector, (1.0 / float(length)))

    # def textual_context_embedding(self, context_tokens: List[str], target_token_index: int, context_radius: int):
    #     left_context = context_tokens[min(0, target_token_index - context_radius):target_token_index]
    #     right_context = context_tokens[
    #                     target_token_index + 1: max(target_token_index + context_radius, len(context_tokens) - 1)]
    #     # left_embedding = self.directional_context_embedding(left_context)
    #     # right_embedding = self.directional_context_embedding(right_context)
    #     pass

    # @memory.cache
    def directional_context_embedding(self, context: List[str]) -> ndarray:
        transformer = SparsePCA(n_components=1, max_iter=10000, n_jobs=4)
        if len(context) > 2:
            w1 = self.word_vector(context[0])
            n = w1.size
            return transformer.fit_transform(
                numpy.kron(w1, self.directional_context_embedding(context[1:]).transpose()).reshape(n, n))
        elif len(context) == 2:
            w1 = self.word_vector(context[0])
            w2 = self.word_vector(context[1])
            n = w1.size
            return transformer.fit_transform(numpy.kron(w1, w2.transpose()).reshape(n, n))

    @staticmethod
    def cache_vector(key, vector, redis: StrictRedis):
        serialized = vector.ravel().tostring()
        redis.set(key, serialized)

    @staticmethod
    def load_vector_from_cache(key, redis: StrictRedis):
        value = redis.get(key)
        return numpy.fromstring(value)


class LazyDenseEmbeddings(Embeddings):

    def __init__(self, vocab_file, vectors_file, use_cache: bool, redis: StrictRedis = None):
        """
        Usage: todo
        :param vocab_file:
        :param vectors_file:
        """
        super(Embeddings, self).__init__(redis)

        with open(vocab_file, "r", encoding="utf-8") as vocab_file:
            labels = vocab_file.read().splitlines()

        with open(vectors_file, "r") as vectors_file:
            content = ""
            while True:
                block = vectors_file.read(1024 * (1 << 20))
                if not block:
                    break
                content += block
            lines = content.splitlines()

        self._use_cache = use_cache
        self._zero_vec = None
        self._vector_index = {}

        self._load(labels, lines)
        self._dim = None

    def __init__(self, embeddings_file: str, use_cache: bool, redis: StrictRedis = None):
        """
        Usage: todo
        :param embeddings_file:
        :param use_cache
        """
        super(Embeddings, self).__init__(redis)
        labels = []
        vector_lines = []
        with open(embeddings_file, "r", encoding="utf-8") as embedding_file:
            lines = embedding_file.read().splitlines()
            for line in lines:
                parts = line.split(" ")
                labels.append(parts[0])
                vector_lines.append(" ".join(parts[1:]))

        self._load(labels, vector_lines)
        self._vector_index = {}
        self._use_cache = use_cache
        self._zero_vec = None
        self._dim = None

    def dim(self):
        if not self._dim:
            first = next(iter(self._vector_dictionary.values()))
            self._dim = len(first.split(" "))
        return self._dim

    def _load(self, dimensions, line_list):
        dim_index = 0
        self._vector_dictionary = {}
        while dim_index < len(dimensions):
            word = dimensions[dim_index]
            self._vector_dictionary[word] = line_list[dim_index]
            dim_index += 1

    def word_vector(self, word: str) -> ndarray:
        vec = None
        if self._use_cache:
            try:
                vec = self._vector_index[word]
            except KeyError:
                pass

        if not vec:
            if word in self._vector_dictionary.keys():
                line = self._vector_dictionary[word]
                vec = numpy.fromstring(line, sep=" ")
            else:
                vec = self._zero_vector()

        if self._use_cache:
            self._vector_index[word] = vec

        return vec

    def _zero_vector(self):
        if not self._zero_vec:
            self._zero_vec = numpy.zeros((self.dim(),))
        return self._zero_vec


class DenseEmbeddings(Embeddings):

    def __init__(self, vocab_file, vectors_file, redis: StrictRedis = None):
        super(Embeddings, self).__init__(redis)
        with open(vocab_file, "r") as vocab_file:
            self._dimensions = vocab_file.readlines()
        self._vsm = numpy.loadtxt(vectors_file)
        self._dim = None

    def word_vector(self, word: str) -> ndarray:
        try:
            index = self._dimensions.index(word)
            return self._vsm[index, :]
        except ValueError:
            return numpy.zeros((self.dim(),))

    def dim(self):
        if not self._dim:
            self._dim = len(self._vsm[0])
        return self._dim


# class MagnitudeEmbeddings(Embeddings):
#
#     def __init__(self, embeddings_file):
#         super(Embeddings, self).__init__()
#         self.model = Magnitude(embeddings_file)
#
#     def word_vector(self, word: str) -> ndarray:
#         return self.model.query(word)
#
#     def sentence_vector(self, sentence: str, sample=False) -> ndarray:
#         tokens = tokenizer.tokenize(sentence)
#         return self.model.query(tokens)
#
#     def dim(self):
#         return self.model.dim()
#
#     def word_similarity(self, word_1: str, word_2: str):
#         return self.model.similarity(word_1, word_2)
#
#     def sentence_similarity(self, string_a, string_b, sample=None):
#         one_grams_a = tokenizer.tokenize(string_a)
#         string_b_tokens = tokenizer.tokenize(string_b)
#
#         if sample:
#             one_grams_a = list(everygrams(one_grams_a, 1, 1))
#             one_grams_a = [word[0] for word, count in collections.Counter(one_grams_a).most_common(sample)]
#
#         pairwise_similarities = self.model.similarity(one_grams_a, string_b_tokens)
#         average_similarity_vector = self.arithmetic_mean_aggregation(pairwise_similarities)
#         return average_similarity_vector.mean()

class Sent2VecEmbeddings(Embeddings):
    def __init__(self, embeddings_file):
        super(Embeddings, self).__init__()
        self.model = Sent2vecModel()
        self.model.load_model(embeddings_file)

    def word_vector(self, word: str) -> ndarray:
        return self.model.embed_sentence(word)

    def dim(self):
        return self.model.get_emb_size()

    def sentence_vector(self, sentence: str, sample=None) -> ndarray:
        return self.model.embed_sentence(sentence)

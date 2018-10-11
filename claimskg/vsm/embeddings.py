from typing import List

import numpy
from numpy.core.multiarray import ndarray
from redis import StrictRedis
from sklearn.decomposition import SparsePCA


class Embeddings:
    def __init__(self, dimensions: List[str], matrix: ndarray = None, line_list: List[str] = None):

        if line_list is None:
            line_list = list()
        if matrix is not None:
            self._vsm = matrix  # type: ndarray
            self._lazy = False
        elif len(line_list) > 0:
            self._lazy = True
            dim_index = 0
            self._vector_dictionary = {}
            while dim_index < len(dimensions):
                word = dimensions[dim_index]
                self._vector_dictionary[word] = line_list[dim_index]
                dim_index += 1
        self._dimensions = dimensions  # type: List[str]

        self.vector_index = {}

    def vector(self, word: str) -> ndarray:
        if self._lazy and word in self.vector_index.keys():
            return self.vector_index[word]

        if self._lazy:
            if word in self._vector_dictionary.keys():
                line = self._vector_dictionary[word]
                vec = numpy.fromstring(line, sep=" ")
                self.vector_index[word] = vec
                return vec
            else:
                first = next(iter(self._vector_dictionary.values()))
                n = len(first.split(" "))
                zeros = numpy.zeros((n,))
                self.vector_index[word] = zeros
                return zeros
        else:
            try:
                index = self._dimensions.index(word)
                return self._vsm[index, :]
            except ValueError:
                n = len(self._vsm[0])
                return numpy.zeros((n,))

    @staticmethod
    def load_from_files(vectors_file, vocab_file):
        with open(vocab_file, "r") as vocab_file:
            labels = vocab_file.readlines()
        vsm_array = numpy.loadtxt(vectors_file)
        return Embeddings(matrix=vsm_array, dimensions=labels)

    @staticmethod
    def load_from_files_lazy(vectors_file, vocab_file):
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

        return Embeddings(dimensions=labels, line_list=lines)

    # Works for GloVE text format
    @staticmethod
    def load_from_file_lazy(embedding_file):
        labels = []
        vectors = []
        with open(embedding_file, "r", encoding="utf-8") as embedding_file:
            lines = embedding_file.read().splitlines()
            for line in lines:
                parts = line.split(" ")
                labels.append(parts[0])
                vectors.append(" ".join(parts[1:]))

        return Embeddings(dimensions=labels, line_list=vectors)

    def arithmetic_mean_bow_embedding(self, tokens: List[str]):
        vectors = []
        for token in tokens:
            vectors.append(self.vector(token))

        return Embeddings.arithmetic_mean_aggregation(vectors)

    def geometric_mean_bow_embedding(self, tokens: List[str]):
        vectors = []
        for token in tokens:
            vectors.append(self.vector(token))

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

    def textual_context_embedding(self, context_tokens: List[str], target_token_index: int, context_radius: int):
        left_context = context_tokens[min(0, target_token_index - context_radius):target_token_index]
        right_context = context_tokens[
                        target_token_index + 1: max(target_token_index + context_radius, len(context_tokens) - 1)]
        left_embedding = self.directional_context_embedding(left_context)
        right_embedding = self.directional_context_embedding(right_context)
        pass

    # @memory.cache
    def directional_context_embedding(self, context: List[str]) -> ndarray:
        transformer = SparsePCA(n_components=1, max_iter=10000, n_jobs=4)
        if len(context) > 2:
            w1 = self.vector(context[0])
            n = w1.size
            return transformer.fit_transform(
                numpy.kron(w1, self.directional_context_embedding(context[1:]).transpose()).reshape(n, n))
        elif len(context) == 2:
            w1 = self.vector(context[0])
            w2 = self.vector(context[1])
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

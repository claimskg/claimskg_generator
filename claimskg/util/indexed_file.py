import numpy


class FileMappedEmbeddings:

    def __init__(self, path):
        self.file = open(path, "r")
        self.label_index = {}
        self.file_start_offsets = []
        self.dimensions = None

        current_start_offset = self.file.tell()
        vocab_index = 0
        while self.file.readable():
            line = self.file.readline()
            self.label_index[line.split(" ")[0]] = vocab_index
            self.file_start_offsets.append(current_start_offset)

            current_start_offset = self.file.tell()
            vocab_index += 1

    def vector(self, vocab_word):
        index = self.label_index[vocab_word]
        if index:
            self.file.seek(self.file_start_offsets[index])
            parts = self.file.readline().split(" ")
            if not self.dimensions:
                self.dimensions = len(parts[1:])
            text_vector = " ".join(parts[1:])
            return numpy.fromstring(text_vector, sep=" ")
        else:
            return numpy.zeros((self.dimensions,))

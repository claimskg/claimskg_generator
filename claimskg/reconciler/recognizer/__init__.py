from abc import ABC, abstractmethod
from typing import List

# (concept_id, concept, concept_start, concept_end, text[concept_start:concept_end],
# match_cursor - stop_count)
from claimskg.reconciler.dictionary import DictionaryLoader


class Annotation:
    def __init__(self, concept_id, start, end, matched_text, matched_length, label_key: str = None):
        self.concept_id = concept_id
        if label_key:
            self.label_key = label_key
        else:
            self.label_key = concept_id

        self.start = start
        self.end = end
        self.matched_text = matched_text
        self.matched_length = matched_length
        self.loaded_concept = None

    def __str__(self) -> str:
        return "Annotation ({})[{}-{} M{}] - {}".format(self.matched_text, self.start, self.end, self.matched_length,
                                                        self.concept_id)


class ConceptRecognizer(ABC):
    def __init__(self, stop_words_file, termination_terms_file, dictionary_loader: DictionaryLoader):
        self.stop_words = self._load_word_list(stop_words_file)
        self.termination_terms = self._load_word_list(termination_terms_file)
        self.dictionary_loader = dictionary_loader

    @staticmethod
    def _load_word_list(file):
        with open(file, "r", encoding="utf8") as word_file:
            words = [line.strip() for line in word_file.readlines()]
        return words

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def recognize(self, input_text) -> List[Annotation]:
        pass

from abc import ABC, abstractmethod
from typing import List, Set, Dict

# (concept_id, concept, concept_start, concept_end, text[concept_start:concept_end],
# match_cursor - stop_count)
import torch

from claimskg.reconciler.dictionary import DictionaryLoader


class Concept:
    def __init__(self, uri, labels: Set[str] = None, label_embeddings: Dict[str, torch.tensor] = None,
                 definition: str = ""):
        self.label_embeddings = label_embeddings
        if label_embeddings is None:
            self.label_embeddings = dict()
        self.uri = uri
        if labels is not None:
            self.labels = labels
        else:
            self.labels = set()
        self.definition = definition

    def add_label(self, label, label_embedding: torch.tensor = None):
        self.labels.add(label)
        if label_embedding is not None:
            self.label_embeddings[label] = label_embedding


class Annotation:
    def __init__(self, concept_id, start, end, matched_text, matched_length, label_key: str = None,
                 concept: Concept = None):
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
        self.confidence_score = 1
        self.concept = concept

    def __str__(self) -> str:
        return "Annotation ({})[{}-{} M{}] - {}".format(self.matched_text, self.start, self.end, self.matched_length,
                                                        self.concept_id)

    def __eq__(self, o) -> bool:
        if isinstance(o, Annotation):
            return 'concept_id' in o.__dict__ and self.concept_id == o.concept_id and self.start == o.start and self.end == o.end
        else:
            return False

    def __hash__(self) -> int:
        return hash((self.matched_text, self.start, self.end, self.concept_id))


class ConceptRecognizer(ABC):
    def __init__(self, stop_words_file, termination_terms_file, dictionary_loader: DictionaryLoader):
        self.stop_words = self._load_word_list(stop_words_file)
        self.termination_terms = self._load_word_list(termination_terms_file)
        self.dictionary_loader = dictionary_loader
        self.concept_index = dict()  # type: Dict[str,Concept]

    @staticmethod
    def _load_word_list(file) -> List[str]:
        with open(file, "r", encoding="utf8") as word_file:
            words = [line.strip() for line in word_file.readlines()]
        return words

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def recognize(self, input_text) -> Set[Annotation]:
        pass

from typing import List

import regex
from metaphone import doublemetaphone
from nltk import TreebankWordTokenizer
from nltk.tokenize import word_tokenize
from tqdm import tqdm

from claimskg.reconciler.dictionary import DictionaryLoader
from claimskg.reconciler.recognizer import ConceptRecognizer, Annotation


class InterDoubleMetaphoneConceptRecognizer(ConceptRecognizer):

    def __init__(self, dictionary_loader: DictionaryLoader, stop_words_file: str, termination_terms_file: str):
        super().__init__(stop_words_file, termination_terms_file, dictionary_loader)
        self.unigram_phone_index = dict()  # record the phone and give an Id
        self.concept_length_index = dict()  # record the phone and give the length of the expression

        self.punctuation_remove = regex.compile('\p{C}', regex.UNICODE)

    def initialize(self):
        print("Now loading the dictionary...")
        self.dictionary_loader.load()
        dictionary = self.dictionary_loader.dictionary  # type : List[DictionaryEntry]
        print("Now indexing the dictionary...")
        for entry in tqdm(dictionary):
            # we split concept ids from labels
            # fields = line.split("\t")
            label = entry.label
            concept_id = entry.id

            labels = [label]
            if entry.synonyms:
                labels.extend(entry.synonyms)

            self._load_concept_labels(concept_id, labels)

    def _load_concept_labels(self, concept_id, labels):
        label_index = 0
        for label in labels:
            normalized = self.punctuation_remove.sub(" ", label).replace("-", " ")
            # We tokenize the label
            tokens = word_tokenize(normalized)
            concept_token_count = 0
            # For each token
            key = str(concept_id) + ":::" + str(label_index)
            for token in tokens:
                # We skip words that belong to the stop list and words that contain non alphanumerical characters
                if token not in self.stop_words:
                    token_phone = doublemetaphone(token)[0]

                    # we create the dictionary entry if it did not exist before
                    if token_phone not in self.unigram_phone_index:
                        self.unigram_phone_index[
                            token_phone] = set()  # il va eut etre falloir creer une liste a la place
                    # if it already existed we add the concept id to the corresponding set
                    self.unigram_phone_index[token_phone].add(key)
                    concept_token_count += 1
            self.concept_length_index[key] = concept_token_count
            label_index += 1

    def recognize(self, text) -> List[Annotation]:

        annotations = []

        # We normalize the text (Remove all punctuation and replace with whitespace)

        normalized_input_text = self.punctuation_remove.sub(" ", text).replace("-", " ").lower()

        # We split the text into token spans (begin and end position from the start of the text)
        spans = TreebankWordTokenizer().span_tokenize(normalized_input_text)
        token_spans = [i for i in spans]

        # we iterate over tokens one by one until we reach the end of the text
        current_token_span_index = 0
        while current_token_span_index < len(token_spans):
            # we get the current token span
            currentSpan = token_spans[current_token_span_index]

            # we extract the string of the token from the text
            token = normalized_input_text[currentSpan[0]:currentSpan[1]]

            # if the word is a stoplist term or a termination term we skip it
            if token not in self.stop_words and token not in self.termination_terms:
                # We get the concept ids matching the phone of the current token
                token_phone = doublemetaphone(token)[0]
                concepts = self.concepts_from_phone(token_phone)

                # this is the start position of the first token of a matching sequence
                concept_start = currentSpan[0]
                # For now we have matched a single terms, so currently the end position will be that of the current
                # token
                concept_end = currentSpan[1]
                match_cursor = 1
                stop_count = 0
                while current_token_span_index + match_cursor < len(token_spans):

                    # We get the next token and position span
                    next_span = token_spans[current_token_span_index + match_cursor]
                    next_token = normalized_input_text[next_span[0]:next_span[1]]

                    # if the token is in the termination list the matching process ends here
                    if next_token in self.termination_terms:
                        break
                    # If the token is in the Stop list we skip it and increment the count of the skipped words
                    # We will need to subtract this from the total number of tokens for the concept
                    elif next_token in self.stop_words:
                        stop_count += 1
                    # Otherwise we try to find a match for the token phone in the dictionary index
                    else:
                        # we doublemetaphone the token's text
                        next_token_phone = doublemetaphone(next_token)[0]

                        # We try to find matching concepts and compute the intersection with previously identified
                        # concepts

                        next_concepts = self.concepts_from_phone(next_token_phone) & concepts

                        # if we find none we stop the matching here
                        if len(next_concepts) == 0:

                            break

                        else:
                            # if we find a match, then we update the current end position to that of the currently
                            # matching token and update the intersected matched concept buffer
                            concepts = next_concepts
                            concept_end = next_span[1]

                    # if we arrive here the current token has matched, we keep count of the current match length
                    match_cursor += 1

                # Once we get out of the loop we reconstruct the matches from the concepts remaining in the set
                # after successive intersections, if concepts is empty there was no match and so
                # Tokens.conceptsToAnnotationTokens will return an empty list otherwise we get a list of
                # AnnotationToken objects instances that we add to the list of identified concepts

                for concept in concepts:
                    key_parts = concept.split(":::")
                    concept_id = key_parts[0]
                    annotation = Annotation(concept_id, concept_start, concept_end, text[concept_start:concept_end],
                                            match_cursor - stop_count, label_key=concept)
                    annotations.append(annotation)

            current_token_span_index += 1
        # Here we filter the annotations to keep only those where the concept length matches the length of the
        # identified annotation
        return [annotation for annotation in annotations if
                annotation.matched_length == self.concept_length_index[annotation.label_key]]

    def concepts_from_phone(self, phone):
        if phone not in self.unigram_phone_index:
            return set()
        else:
            return self.unigram_phone_index[phone]

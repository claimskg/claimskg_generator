import re
from typing import Set, Tuple, List

import regex
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModel, AutoConfig

from claimskg.reconciler.dictionary import DictionaryLoader
from claimskg.reconciler.recognizer import ConceptRecognizer, Concept, Annotation


class IntersEmbeddingConceptRecognizer(ConceptRecognizer):

    def __init__(self, dictionary_loader: DictionaryLoader, stop_words_file: str, termination_terms_file: str,
                 tokenizer: AutoTokenizer, model: AutoModel, config: AutoConfig):
        super().__init__(stop_words_file, termination_terms_file, dictionary_loader)
        self.unigram_index = dict()  # record the stem and give an Id
        self.concept_token_vector_index = dict()
        self.concept_length_index = dict()  # record the stem and give the length of the expression
        self.model = model
        self.tokenizer = tokenizer
        self.config = config
        self.unk_token_id = self.tokenizer.unk_token_id
        self.cls_token_id = self.tokenizer.cls_token_id
        self.pad_token_id = self.tokenizer.pad_token_id
        self.eos_token_id = self.tokenizer.eos_token_id
        self.punctuation_regexp = regex.compile('\p{P}', re.UNICODE)
        self.stop_words = self._piece_wise_tokenize_token_list(self.stop_words)
        self.termination_terms = self._piece_wise_tokenize_token_list(self.termination_terms)

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

    def _piece_wise_tokenize_token_list(self, token_list):
        final_token_list = []

        for token in token_list:
            sub_tokens = self.tokenizer.tokenize(token)
            final_token_list.append(sub_tokens[-1])

        return self.tokenizer.convert_tokens_to_ids(final_token_list)

    def _load_concept_labels(self, concept_id, labels):
        label_index = 0
        for label in labels:
            inputs = self.tokenizer.encode_plus(label, add_special_tokens=True, max_length=512, pad_to_max_length=True,
                                                return_attention_mask=True)
            tokens = inputs['input_ids']
            att_masks = inputs['attention_mask']
            last_token = att_masks.index(0)

            for key in inputs.keys():
                inputs[key] = torch.tensor(inputs[key], dtype=torch.long)
                inputs[key] = inputs[key].reshape((1, len(inputs[key])))

            last_outputs, class_output = self.model(**inputs)
            token_vectors = last_outputs[:, 0:last_token - 1, :]

            concept = Concept(concept_id)
            concept.add_label(label, label_embedding=class_output)
            self.concept_index[concept_id] = concept
            # We tokenize the label
            concept_token_count = 0
            # For each token
            key = str(concept_id) + ":::" + str(label_index)
            last_token_index = tokens.index(0) - 1
            tokens = tokens[:last_token_index]
            token_index = 0
            for token in tokens:
                # We skip words that belong to the stop list and words that contain non alphanumerical characters
                # we create the dictionary entry if it did not exist before
                if token not in self.unigram_index:
                    self.unigram_index[token] = set()
                # if it already existed we add the concept id to the corresponding set
                self.unigram_index[token].add(key)

                if token not in self.concept_token_vector_index:
                    self.concept_token_vector_index[token] = dict()
                    token_vector = token_vectors[:, token_index, :]
                    new_shape = (token_vector.shape[1])
                    self.concept_token_vector_index[token][concept_id] = token_vector.reshape(new_shape)

                token_index += 1
                concept_token_count += 1
            self.concept_length_index[key] = concept_token_count
            label_index += 1

    def _tokens_to_spans(self, tokens, text: str, initial_start_offset=0):
        spans = []  # type: List[Tuple[int, int, str]]
        start_offset = initial_start_offset
        end_offset = initial_start_offset

        for current_token_index in range(len(tokens)):
            raw_token = tokens[current_token_index]
            if "#" in raw_token:
                raw_token = raw_token.replace("#", " ").strip()
            end_offset += len(raw_token)
            span = (start_offset, end_offset, text[start_offset:end_offset])
            spans.append(span)
            if current_token_index < len(tokens) - 1 and text[end_offset] == " ":
                offset = 0
                while end_offset + offset < len(text) and text[end_offset + offset + 1] == " ":
                    offset += 1
                start_offset = end_offset + offset + 1
                end_offset += offset + 1
            else:
                start_offset = end_offset
        return spans

    def recognize(self, text) -> Set[Annotation]:
        annotations = []

        tokens = self.tokenizer.tokenize(text)
        token_ids = self.tokenizer.convert_tokens_to_ids(tokens)

        len_leading_whitespaces = len(text) - len(text.lstrip())
        token_spans = self._tokens_to_spans(tokens, text, initial_start_offset=len_leading_whitespaces)

        print(tokens)
        print(token_ids)
        print(token_spans)

        # we iterate over tokens one by one until we reach the end of the text
        current_token_span_index = 0
        while current_token_span_index < len(token_spans):
            # we get the current token span
            current_span = token_spans[current_token_span_index]

            # we extract the string of the token from the text
            token = text[current_span[0]:current_span[1]]

            # if the word is a stoplist term or a termination term we skip it
            if token not in self.stop_words and token not in self.termination_terms:
                # We get the concept ids matching the stem of the current token
                # Double stemming ensures we come back to the most elementary root, ensure match between nouns and
                # adjectives with the same root
                concepts = self.concepts_from_stem(self.stem(token))

                # this is the start position of the first token of a matching sequence
                concept_start = current_span[0]
                # For now we have matched a single terms, so currently the end position will be that of the current
                # token
                concept_end = current_span[1]
                match_cursor = 1
                stop_count = 0
                while current_token_span_index + match_cursor < len(token_spans):

                    # We get the next token and position span
                    next_span = token_spans[current_token_span_index + match_cursor]
                    next_token = text[next_span[0]:next_span[1]]

                    # if the token is in the termination list the matching process ends here
                    if next_token in self.termination_terms:
                        break
                    # If the token is in the Stop list we skip it and increment the count of the skipped words
                    # We will need to subtract this from the total number of tokens for the concept
                    elif next_token in self.stop_words:
                        stop_count += 1
                    # Otherwise we try to find a match for the token stem's in the dictionary index
                    else:
                        # we stem the toke text
                        next_token_stem = self.stem(next_token)

                        # We try to find matching concepts and compute the intersection with previously identified
                        # concepts

                        next_concepts = self.concepts_from_stem(next_token_stem) & concepts

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
                                            match_cursor - stop_count, label_key=concept,
                                            concept=self.concept_index[concept_id])
                    annotations.append(annotation)

            current_token_span_index += 1
        # Here we filter the annotations to keep only those where the concept length matches the length of the
        # identified annotation
        return set([annotation for annotation in annotations if
                    annotation.matched_length == self.concept_length_index[annotation.label_key]])

    def concepts_from_stem(self, stem):
        if stem not in self.unigram_vector_index:
            return set()
        else:
            return self.unigram_vector_index[stem]

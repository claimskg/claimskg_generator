from abc import ABC, abstractmethod
from typing import List

import pandas
from tqdm import tqdm


class DictionaryEntry:
    def __init__(self, id: int, label: str, definition: str = None, source: str = None, language: str = None,
                 mappings: List[str] = None, cuis: List[str] = None, tuis: List[str] = None,
                 synonyms: List[str] = None):
        self.id = id
        self.label = label
        self.synonyms = synonyms
        self.definition = definition
        self.source = source
        self.language = language
        self.mappings = mappings
        self.cuis = cuis
        self.tuis = tuis

    def __str__(self):
        return "{[{id}] {label}}".format(id=self.id, label=self.label)


class DictionaryLoader(ABC):
    def __init__(self, dictionary_file):
        self.dictionary = []  # type: List[DictionaryEntry]
        self.dictionary_index = dict()  # type : Dict[int, DictionaryEntry]
        self.dictionary_file = dictionary_file
        self.reverse_index = dict()

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def save(self, output_file: str):
        pass

    def entry_from_index(self, id: int) -> DictionaryEntry:
        return self.dictionary_index[id]

    def index(self, label: str):
        return self.reverse_index[label]

    def size(self):
        return len(self.dictionary_index.values())

    def __iter__(self):
        return self.dictionary_index.__iter__()


class MgrepDictionaryLoader(DictionaryLoader):

    def load(self):
        data = pandas.read_csv(self.dictionary_file, delimiter="\t", encoding="utf8")
        for index, row in tqdm(data.iterrows()):
            id = int(row[0])
            if id in self.dictionary_index.keys():
                entry = self.dictionary_index[id]  # type : DictionaryEntry
                synonyms = entry.synonyms
                if not synonyms:
                    entry.synonyms = []
                entry.synonyms.append(row[1])
            else:
                entry = DictionaryEntry(id, row[1])
                self.dictionary.append(entry)
                self.dictionary_index[id] = entry
                self.reverse_index[row[1]] = id

    def save(self, output_file: str):
        with open(output_file, "w", encoding="utf8") as output:
            for key, value in self.dictionary_index.items():
                output.write("{id}\t{label}\n".format(id=key, label=value.label))
                if value.synonyms:
                    for synonym in value.synonyms:
                        output.write("{id}\t{label}\n".format(id=key, label=synonym))
            output.flush()


class StringDictionaryLoader(MgrepDictionaryLoader):

    def __init__(self, string_entries):
        super().__init__(None)
        self.dictionary_string_entries = string_entries

    def load(self):
        for string_entry in self.dictionary_string_entries:
            id = string_entry[0]
            if id in self.dictionary_index.keys():
                entry = self.dictionary_index[id]  # type : DictionaryEntry
                synonyms = entry.synonyms
                if not synonyms:
                    entry.synonyms = []
                entry.synonyms.append(string_entry[1])
            else:
                entry = DictionaryEntry(id, string_entry[1])
                self.dictionary.append(entry)
                self.dictionary_index[id] = entry
                self.reverse_index[string_entry[1]] = id


class SemaxoneDictionaryLoader(DictionaryLoader):

    def load(self):
        data = pandas.read_csv(self.dictionary_file, delimiter="\t", encoding="utf8")
        for index, row in data.iterrows():
            id = int(row["Id"])
            entry = DictionaryEntry(id=id, label=row["Lemme"], definition=row["Definition"],
                                    source=row["Origine"],
                                    language=row["Langue"])
            self.dictionary.append(entry)
            self.dictionary_index[id] = entry
            self.reverse_index[row["Lemme"]] = id

    def save(self, output_file: str):
        pass

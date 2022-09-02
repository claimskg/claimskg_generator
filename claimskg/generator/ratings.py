from enum import Enum
from typing import Dict


class NormalizedRatings(Enum):
    FALSE = 1
    MIXTURE = 2
    TRUE = 3
    OTHER = -1


_normalization_dictionary = {  # type: Dict[str, Dict[str,NormalizedRatings]]

    "politifact": {  # type: Dict[str,NormalizedRatings]
        'incorrect': NormalizedRatings.FALSE,
        'pants-fire': NormalizedRatings.FALSE,
        'pants on fire': NormalizedRatings.FALSE,
        'pants on fire!': NormalizedRatings.FALSE,
        'false': NormalizedRatings.FALSE,
        'mostly correct': NormalizedRatings.MIXTURE,
        'mostly false': NormalizedRatings.MIXTURE,
        'barely true': NormalizedRatings.MIXTURE,
        'half true': NormalizedRatings.MIXTURE,
        'half-true': NormalizedRatings.MIXTURE,
        'mostly true': NormalizedRatings.MIXTURE,
        'true': NormalizedRatings.TRUE,
        'correct': NormalizedRatings.TRUE
    },
    "snopes": {  # type: Dict[str,NormalizedRatings]
        'false': NormalizedRatings.FALSE,
        'legend': NormalizedRatings.FALSE,
        'mixture': NormalizedRatings.MIXTURE,
        'mixture:': NormalizedRatings.MIXTURE,
        'true': NormalizedRatings.TRUE,
        'mostly false': NormalizedRatings.MIXTURE,
        'mostly true': NormalizedRatings.MIXTURE,
        'partly true': NormalizedRatings.MIXTURE,
        'MIXTURE OF TRUE AND FALSE INFORMATION': NormalizedRatings.MIXTURE,
        'MIXTURE OF TRUE AND FALSE INFORMATION:': NormalizedRatings.MIXTURE,
        'MIXTURE OF ACCURATE AND  INACCURATE INFORMATION': NormalizedRatings.MIXTURE
    },
    "africacheck": {  # type: Dict[str,NormalizedRatings]
        'incorrect': NormalizedRatings.FALSE,
        'mostly-correct': NormalizedRatings.MIXTURE,
        'correct': NormalizedRatings.TRUE
    },
    "factscan": {  # type: Dict[str,NormalizedRatings]
        'false': NormalizedRatings.FALSE,
        'true': NormalizedRatings.TRUE,
        'Misleading': NormalizedRatings.OTHER
    },
    "truthorfiction": {  # type: Dict[str,NormalizedRatings]
        'fiction': NormalizedRatings.FALSE,
        'truth': NormalizedRatings.TRUE,
        'Mixed': NormalizedRatings.MIXTURE,
        'Reported as Fiction': NormalizedRatings.MIXTURE,
        'truth & misleading': NormalizedRatings.MIXTURE,
        'mostly truth': NormalizedRatings.MIXTURE,
        'Decontextualized': NormalizedRatings.MIXTURE,
        'Not True': NormalizedRatings.MIXTURE,
        'true': NormalizedRatings.TRUE,
        'Unknown': NormalizedRatings.OTHER,
        'Misattributed': NormalizedRatings.OTHER,
        'Disputed': NormalizedRatings.MIXTURE,
        'Outdated': NormalizedRatings.OTHER,
        'Incorrect Attribution': NormalizedRatings.OTHER,
        'Correct Attribution': NormalizedRatings.OTHER,
        'Commentary': NormalizedRatings.OTHER,
        'Reported as Truth': NormalizedRatings.TRUE,
        'Mostly Fiction': NormalizedRatings.FALSE,
        'Unproven': NormalizedRatings.OTHER,
        'Authorship Confirmed': NormalizedRatings.OTHER,
        'Truth! & Fiction! & Unproven!': NormalizedRatings.MIXTURE,
        'Depends on Where You Vote': NormalizedRatings.MIXTURE,
        'Unofficial': NormalizedRatings.OTHER,
        'Truth! But an Opinion!': NormalizedRatings.MIXTURE,
        'Truth! But Postponed!': NormalizedRatings.MIXTURE,
        'Pending Investigation!': NormalizedRatings.OTHER,
        
    },
    "checkyourfact": {  # type: Dict[str,NormalizedRatings]
        'false': NormalizedRatings.FALSE,
        'true': NormalizedRatings.TRUE,
        'mostly true': NormalizedRatings.MIXTURE,
        'true/false': NormalizedRatings.MIXTURE,
        'verdict false': NormalizedRatings.FALSE,
        'mostly truth': NormalizedRatings.MIXTURE,
        'misleading': NormalizedRatings.FALSE
    },
    "factcheck_aap": {
        "True": NormalizedRatings.TRUE,
        "False": NormalizedRatings.FALSE,
        "Mostly True": NormalizedRatings.MIXTURE,
        "Mostly False": NormalizedRatings.MIXTURE,
        "Somewhat True": NormalizedRatings.MIXTURE,
        "Somewhat False": NormalizedRatings.MIXTURE
    },
    "factual_afp": {
        'faux': NormalizedRatings.FALSE,
        'article satirique': NormalizedRatings.FALSE,
        'infondé': NormalizedRatings.FALSE,
        'montage': NormalizedRatings.FALSE,
        'trompeur': NormalizedRatings.MIXTURE,
        'parodie': NormalizedRatings.FALSE,
        'vrai': NormalizedRatings.TRUE,
        'Contexte manquant': NormalizedRatings.FALSE,
        'propos sortis de leur contexte': NormalizedRatings.FALSE,
        'manque de contexte': NormalizedRatings.FALSE,        
        "faux, ces photos montrent un couple britannique sans aucun lien de parenté et illustrent un article satirique": NormalizedRatings.FALSE,
        'faux, manque de contexte : vidéo tronquée': NormalizedRatings.FALSE,
        'totalement vrai': NormalizedRatings.TRUE,
        'plutôt vrai': NormalizedRatings.MIXTURE,        
        'trompeur': NormalizedRatings.MIXTURE,
        'plutôt faux': NormalizedRatings.MIXTURE,
        'presque': NormalizedRatings.MIXTURE,
        'mélangé': NormalizedRatings.MIXTURE,
        'Inexact': NormalizedRatings.MIXTURE,
        'Incertain': NormalizedRatings.MIXTURE,
        'Imprécis': NormalizedRatings.MIXTURE,
        'Exagéré': NormalizedRatings.MIXTURE,
        'Douteux': NormalizedRatings.MIXTURE,

    },
    #"factcheck_afp": {
        #'False': NormalizedRatings.FALSE,
       # 'Fake': NormalizedRatings.FALSE,
       # 'Mixed': NormalizedRatings.MIXTURE,
        #'Hoax': NormalizedRatings.FALSE,
        #'Falso': NormalizedRatings.FALSE,
        #'APRIL FOOL': NormalizedRatings.FALSE,
       # 'Misleading' : NormalizedRatings.MIXTURE
   # },
     "factcheck_afp": {
        'false': NormalizedRatings.FALSE,
        'partly false': NormalizedRatings.MIXTURE,
        'misleading': NormalizedRatings.FALSE,
        'satire': NormalizedRatings.FALSE,
        'missing context': NormalizedRatings.FALSE,
        'altered image': NormalizedRatings.OTHER,
        'not recommended' : NormalizedRatings.OTHER,
        'true' : NormalizedRatings.TRUE,
        'unproven': NormalizedRatings.OTHER,
        'no evidence': NormalizedRatings.OTHER,
        'photo out of context': NormalizedRatings.OTHER,
        'misattributed': NormalizedRatings.FALSE,
        'Outdated': NormalizedRatings.OTHER
    },
    "fullfact": {
        'true': NormalizedRatings.TRUE,
        'false': NormalizedRatings.FALSE,
        'mixture': NormalizedRatings.MIXTURE,
        'other': NormalizedRatings.OTHER
    },
    "eufactcheck": {
       
        'm': NormalizedRatings.MIXTURE,
        'f': NormalizedRatings.FALSE,
        't': NormalizedRatings.TRUE,        
        'u': NormalizedRatings.OTHER
        
    },
      "polygraph": {
        'misleading': NormalizedRatings.MIXTURE,
        'true': NormalizedRatings.TRUE,
        'false': NormalizedRatings.FALSE,       
        'unsubstantiated': NormalizedRatings.FALSE
        
    },
    "fatabyyano": {
        'false': NormalizedRatings.FALSE,
        'altered': NormalizedRatings.MIXTURE,      
        'partially false': NormalizedRatings.MIXTURE,
        'satire': NormalizedRatings.OTHER,
        'missing context': NormalizedRatings.OTHER,
        'true': NormalizedRatings.TRUE     
        
    },
    "factograph": {
        'не факт': NormalizedRatings.FALSE,
        'это так': NormalizedRatings.TRUE,
        'да, но': NormalizedRatings.MIXTURE,
        'Тak, но,': NormalizedRatings.MIXTURE,
        'пока не факт': NormalizedRatings.FALSE, # for now it should be false    
        'скорее, так,': NormalizedRatings.MIXTURE,# true but not 100%
        'не факт, но': NormalizedRatings.MIXTURE,# false but....
        'видимо, так': NormalizedRatings.TRUE,#seems to be true
        'не факт, увы,': NormalizedRatings.FALSE,
        'пока, скорее, так': NormalizedRatings.MIXTURE, # for now it should be truth
        'сомнительно': NormalizedRatings.MIXTURE, # we doubts its not true
        'искажение': NormalizedRatings.OTHER,#interpreted wrongly
        'скорее, так, но,': NormalizedRatings.MIXTURE,#seems to be true but..
        'это так, но,': NormalizedRatings.MIXTURE,#true..but        
        'правда': NormalizedRatings.TRUE,
        'пока сомнительно': NormalizedRatings.MIXTURE,# for now we dont think its true
        'скорее, правда': NormalizedRatings.MIXTURE,# seems to be true;NOT SURE
        'неправда': NormalizedRatings.FALSE,              
        'возможно, но,': NormalizedRatings.MIXTURE   # may be true but...
        
        
    },
    
    "vishvanews": {
        'false': NormalizedRatings.FALSE,
        'misleading': NormalizedRatings.MIXTURE,      
        'true': NormalizedRatings.TRUE,       
        
    }
}


def _standardize_name(original_name: str):
    return original_name.strip().lower().replace("!", "").replace(":", "").replace("-", " ")


def normalize(source_name, original_name) -> NormalizedRatings:
    """
        Generate a normalized rating from the original ratings on each respective site
    :param original_name:
    :return normalized_rating: NormalizedRating
    """
    try:
        source = _normalization_dictionary[source_name]
        print("source")
        print(source)
        print("original_name")
        print(original_name)
        normalized_value = source[_standardize_name(original_name)]
        print("normalized_value")
        print(normalized_value)
    except KeyError:
        normalized_value = NormalizedRatings.OTHER
    return normalized_value

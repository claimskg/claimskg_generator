from enum import Enum
from typing import Dict


class StatKeys(Enum):
    CLAIM_REVIEW = "ClaimReview"
    CREATIVE_WORK = "CreativeWork"
    ENTITY = "Total Number of Entities"
    KEYWORDS = "Keywords"
    KEWORDS_THESOZ = "Keywords reconciled with TheSoz"
    KEWORDS_UNESCO = "Keywords reconciled with Unesco"
    KEWORDS_DBPEDIA = "Keywords reconciled with DBPedia"
    KEWORDS_THESOZ_DBPEDIA = "Keywords reconciled with both TheSoz and DBPedia"
    KEWORDS_UNESCO_DBPEDIA = "Keywords reconciled with both UNESCO and DBPedia"
    ENTITIES_PER_REVIEW = "Entities per review"
    ENTITIES_PER_CLAIM = "Entities per claim"
    KEYWORDS_PER_REVIEW = "Keywords per review"
    CITATIONS_PER_CREATIVE_WORK = "Citations per CreativeWork"
    CLAIMS_WITHOUT_AUTHOR = "Claims reviews without author"
    CLAIM_MAPPINGS = "CreativeWork Mappings"
    CLAIMS_WITH_TEXT_PERCENT = "Claims with text"
    CLAIMS_WITH_AUTHOR_PERCENT = "Claims with author"
    CLAIMS_WITH_DATE_PERCENT = "Claims with date published"
    CLAIMS_WITH_CITATIONS_PERCENT = "Claims with at least one citations"
    CLAIMS_WITH_ENTITIES_PERCENT = "Claims with at least one entity mention"
    REVIEW_WITH_HEADLINE_PERCENT = "Reviews with a headline"
    REVIEW_WITH_KEYWORDS_PERCENT = "Reviews with at least one keyword"
    REVIEW_WITH_ENTITIES = "Reviews with at least one entity mention"
    FALSE_CLAIMS = "Claims rated as FALSE under our normalized scale"
    MIXTURE_CLAIMS = "Claims rated as MIXTURE under our normalized scale"
    TRUE_CLAIMS = "Claims rated as TRUE under our normalized scale"
    OTHER_CLAIMS = "Claims rated as OTHER under our normalized scale"


class ClaimsKGStatistics:
    def __init__(self):
        self.counts = {}  # type: Dict[StatKeys,int]

        for stat in StatKeys:
            self.counts[stat] = 0

    def _increment_statistic(self, key: StatKeys, value: float):
        self.counts[key] += value

    def compute_stats_for_review(self, claim):
        self._increment_statistic(StatKeys.CLAIM_REVIEW, 1)
        self._increment_statistic(StatKeys.CREATIVE_WORK, 1)
        if claim.creative_work_author is None or len(claim.creative_work_author) == 0:
            self._increment_statistic(StatKeys.CLAIMS_WITHOUT_AUTHOR, 1)

        self._increment_statistic(StatKeys.ENTITIES_PER_REVIEW, len(claim.review_entities))
        self._increment_statistic(StatKeys.ENTITIES_PER_CLAIM, len(claim.claim_entities))
        self._increment_statistic(StatKeys.ENTITY, len(claim.claim_entities) + len(claim.review_entities))

        self._increment_statistic(StatKeys.KEYWORDS, len(claim.keywords))
        self._increment_statistic(StatKeys.KEWORDS_THESOZ, len(claim.keywords_thesoz))
        self._increment_statistic(StatKeys.KEWORDS_UNESCO, len(claim.keywords_unesco))
        self._increment_statistic(StatKeys.KEWORDS_DBPEDIA, len(claim.keywords_dbpedia))
        self._increment_statistic(StatKeys.KEWORDS_THESOZ_DBPEDIA, len(claim.keywords_thesoz_dbpedia))
        self._increment_statistic(StatKeys.KEWORDS_UNESCO_DBPEDIA, len(claim.keywords_unesco_dbpedia))
        self._increment_statistic(StatKeys.KEYWORDS_PER_REVIEW, len(claim.keywords))

        self._increment_statistic(StatKeys.CITATIONS_PER_CREATIVE_WORK, len(claim.links))

        if claim.has_body_text:
            self._increment_statistic(StatKeys.CLAIMS_WITH_TEXT_PERCENT, 1)

        if len(claim.creative_work_author) > 0:
            self._increment_statistic(StatKeys.CLAIMS_WITH_AUTHOR_PERCENT, 1)

        if claim.claim_date:
            self._increment_statistic(StatKeys.CLAIMS_WITH_DATE_PERCENT, 1)

        if len(claim.links) > 0:
            self._increment_statistic(StatKeys.CLAIMS_WITH_CITATIONS_PERCENT, 1)

        if len(claim.claim_entities) > 0:
            self._increment_statistic(StatKeys.CLAIMS_WITH_ENTITIES_PERCENT, 1)

        if claim.has_headline:
            self._increment_statistic(StatKeys.REVIEW_WITH_HEADLINE_PERCENT, 1)

        if len(claim.keywords) > 0:
            self._increment_statistic(StatKeys.REVIEW_WITH_KEYWORDS_PERCENT, 1)
        if len(claim.review_entities) > 0:
            self._increment_statistic(StatKeys.REVIEW_WITH_ENTITIES, 1)

        if "TRUE" in claim.normalized_rating:
            self._increment_statistic(StatKeys.TRUE_CLAIMS, 1)
        elif "FALSE" in claim.normalized_rating:
            self._increment_statistic(StatKeys.FALSE_CLAIMS, 1)
        elif "MIXTURE" in claim.normalized_rating:
            self._increment_statistic(StatKeys.MIXTURE_CLAIMS, 1)
        elif "OTHER" in claim.normalized_rating:
            self._increment_statistic(StatKeys.OTHER_CLAIMS, 1)

    def count_mapping(self):
        self._increment_statistic(StatKeys.CLAIM_MAPPINGS, 1)

    def output_stats(self):
        self.counts[StatKeys.ENTITIES_PER_REVIEW] /= float(self.counts[StatKeys.CLAIM_REVIEW])
        self.counts[StatKeys.ENTITIES_PER_CLAIM] /= float(self.counts[StatKeys.CREATIVE_WORK])
        self.counts[StatKeys.KEYWORDS_PER_REVIEW] /= float(self.counts[StatKeys.CLAIM_REVIEW])
        self.counts[StatKeys.CITATIONS_PER_CREATIVE_WORK] /= float(self.counts[StatKeys.CREATIVE_WORK])

        self.counts[StatKeys.CLAIMS_WITH_TEXT_PERCENT] /= float(self.counts[StatKeys.CREATIVE_WORK])
        self.counts[StatKeys.CLAIMS_WITH_TEXT_PERCENT] *= 100.0

        self.counts[StatKeys.CLAIMS_WITH_AUTHOR_PERCENT] /= float(self.counts[StatKeys.CREATIVE_WORK])
        self.counts[StatKeys.CLAIMS_WITH_AUTHOR_PERCENT] *= 100.0

        self.counts[StatKeys.CLAIMS_WITH_DATE_PERCENT] /= float(self.counts[StatKeys.CREATIVE_WORK])
        self.counts[StatKeys.CLAIMS_WITH_DATE_PERCENT] *= 100.0

        self.counts[StatKeys.CLAIMS_WITH_CITATIONS_PERCENT] /= float(self.counts[StatKeys.CREATIVE_WORK])
        self.counts[StatKeys.CLAIMS_WITH_CITATIONS_PERCENT] *= 100.0

        self.counts[StatKeys.CLAIMS_WITH_ENTITIES_PERCENT] /= float(self.counts[StatKeys.CREATIVE_WORK])
        self.counts[StatKeys.CLAIMS_WITH_ENTITIES_PERCENT] *= 100.0

        self.counts[StatKeys.REVIEW_WITH_HEADLINE_PERCENT] /= float(self.counts[StatKeys.CLAIM_REVIEW])
        self.counts[StatKeys.REVIEW_WITH_HEADLINE_PERCENT] *= 100.0

        self.counts[StatKeys.REVIEW_WITH_KEYWORDS_PERCENT] /= float(self.counts[StatKeys.CLAIM_REVIEW])
        self.counts[StatKeys.REVIEW_WITH_KEYWORDS_PERCENT] *= 100.0

        self.counts[StatKeys.REVIEW_WITH_ENTITIES] /= float(self.counts[StatKeys.CLAIM_REVIEW])
        self.counts[StatKeys.REVIEW_WITH_ENTITIES] *= 100.0

        for stat in self.counts.keys():
            print("{name},{value}".format(name=stat.value, value=self.counts[stat]))

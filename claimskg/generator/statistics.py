from enum import Enum, auto
from typing import Dict

class StatType(Enum):
    COUNT = auto()
    AVERAGE = auto()


class StatKeys(Enum):
    CLAIM_REVIEW = "ClaimReview"
    CREATIVE_WORK = "CreativeWork"
    ENTITY = "Total Number of Entities"
    KEYWORDS = "Keyword"
    ENTITIES_PER_REVIEW = "Entities per review"
    KEYWORDS_PER_REVIEW = "Keywords per review"
    CITATIONS_PER_CREATIVE_WORK = "Citations per CreativeWork"
    CLAIMS_WITHOUT_AUTHOR = "Claims reviews without author"
    CLAIM_MAPPINGS = "CreativeWork Mappings"


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

        self._increment_statistic(StatKeys.ENTITIES_PER_REVIEW, len(claim.entities))
        self._increment_statistic(StatKeys.ENTITY, len(claim.entities))

        self._increment_statistic(StatKeys.KEYWORDS, len(claim.keywords))
        self._increment_statistic(StatKeys.KEYWORDS_PER_REVIEW, len(claim.keywords))

        self._increment_statistic(StatKeys.CITATIONS_PER_CREATIVE_WORK, len(claim.links))

    def count_mapping(self):
        self._increment_statistic(StatKeys.CLAIM_MAPPINGS, 1)

    def output_stats(self):
        self.counts[StatKeys.ENTITIES_PER_REVIEW] /= float(self.counts[StatKeys.CLAIM_REVIEW])
        self.counts[StatKeys.KEYWORDS_PER_REVIEW] /= float(self.counts[StatKeys.CLAIM_REVIEW])
        self.counts[StatKeys.CITATIONS_PER_CREATIVE_WORK] /= float(self.counts[StatKeys.CREATIVE_WORK])
        for stat in self.counts.keys():
            print("{name},{value}".format(name=stat.value, value=self.counts[stat]))

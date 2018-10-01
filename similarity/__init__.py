import math as math
from typing import List, Tuple


def compute_hard_overlap(collection_a: List[str], collection_b: List[str]):
    overlap_count = 0
    index_a = 0
    while index_a < len(collection_a):
        item_a = collection_a[index_a]
        index_b = 0
        while index_b < len(collection_b):
            item_b = collection_b[index_b]
            if item_a == item_b:
                overlap_count += 1
            index_b += 1
        index_a += 1
    return overlap_count


def tverski_ratio(alpha: float, beta: float, gamma: float, overlap_count: float, difference_a: float,
                  difference_b: float):
    contrast = tverski_contrast(alpha, beta, gamma, overlap_count, difference_a, difference_b)
    if contrast == 0:
        return 0
    else:
        return alpha * overlap_count / contrast


def tverski_contrast(alpha: float, beta: float, gamma: float, overlap_count: float, difference_a: float,
                     difference_b: float):
    return alpha * overlap_count - beta * difference_a - gamma * difference_b


def jaccard(overlap_count: float, union_count: float):
    if union_count == 0:
        return 0
    else:
        return overlap_count / union_count


def geometric_mean_aggregation(weighted_values: List[Tuple[float, float]]):
    length = len(weighted_values)
    overall_product = 1
    for (v, w) in weighted_values:
        if v < 0.00001:
            v = 0.00001
        overall_product *= math.pow(v, w)
    return math.pow(overall_product, 1.0 / float(length))


def arithmetic_mean_aggregation(weighted_values: List[Tuple[float, float]]):
    length = len(weighted_values)
    overall_sum = 0.0
    for (v, w) in weighted_values:
        overall_sum += v * w

    return overall_sum / float(length)

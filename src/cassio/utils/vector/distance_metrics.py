from typing import List

import numpy as np

# distance definitions. These all work batched in the first argument.
def distance_dotProduct(embeddingVectors: List[List[float]], referenceEmbeddingVector: List[float]) -> List[float]:
        """
        Given a list [emb_i] and a reference rEmb vector,
        return a list [distance_i] where each distance is
            distance_i = distance(emb_i, rEmb)
        At the moment only the dot product is supported
        (which for unitary vectors is the cosine difference).

        Not particularly optimized.
        """
        v1s = np.array(embeddingVectors, dtype=float)
        v2 = np.array(referenceEmbeddingVector, dtype=float)
        return list(np.dot(
            v1s,
            v2.T,
        ))


def distance_cosDifference(embeddingVectors: List[List[float]], referenceEmbeddingVector: List[float]) -> List[float]:
    v1s = np.array(embeddingVectors, dtype=float)
    v2 = np.array(referenceEmbeddingVector, dtype=float)
    return list(np.dot(
        v1s,
        v2.T,
    ) / (
        np.linalg.norm(v1s, axis=1)
        * np.linalg.norm(v2)
    ))


def distance_L1(embeddingVectors: List[List[float]], referenceEmbeddingVector: List[float]) -> List[float]:
        v1s = np.array(embeddingVectors, dtype=float)
        v2 = np.array(referenceEmbeddingVector, dtype=float)
        return list(np.linalg.norm(v1s - v2, axis=1, ord=1))


def distance_L2(embeddingVectors: List[List[float]], referenceEmbeddingVector: List[float]) -> List[float]:
        v1s = np.array(embeddingVectors, dtype=float)
        v2 = np.array(referenceEmbeddingVector, dtype=float)
        return list(np.linalg.norm(v1s - v2, axis=1, ord=2))


def distance_max(embeddingVectors: List[List[float]], referenceEmbeddingVector: List[float]) -> List[float]:
        v1s = np.array(embeddingVectors, dtype=float)
        v2 = np.array(referenceEmbeddingVector, dtype=float)
        return list(np.linalg.norm(v1s - v2, axis=1, ord=np.inf))


# The tuple is:
#   (
#       function,
#       sorting 'reverse' argument, nearest-to-farthest
#   )
# (i.e. True means that:
#     - in that metric higher is closer and that
#     - cutoff should be metric > threshold)
distanceMetricsMap = {
    'cos': (
        distance_cosDifference,
        True,
    ),
    'dot': (
        distance_dotProduct,
        True,
    ),
    'l1': (
        distance_L1,
        False,
    ),
    'l2': (
        distance_L2,
        False,
    ),
    'max': (
        distance_max,
        False,
    ),
}

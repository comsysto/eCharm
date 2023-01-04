from typing import Dict, List, Optional

from difflib import SequenceMatcher
import pandas as pd


def weighted_score_duplicates(
        current_station: pd.Series,
        duplicate_candidates: pd.DataFrame,
        score_threshold: float = 0.49,
        max_distance: int = 100,
        score_weights: Optional[Dict[str, float]] = None,
        ) -> pd.DataFrame:

    score_weights = (
        score_weights
        if score_weights
        else dict(operator=0.2, address=0.1, distance=0.7)
    )

    duplicate_candidates["operator_match"] = duplicate_candidates.operator.apply(
        lambda x: SequenceMatcher(None, current_station.operator, str(x)).ratio()
        if (current_station.operator is not None) & (x is not None)
        else 0.0
    )

    current_station_address = f"{current_station['street']},{current_station['town']}"
    duplicate_candidates["address"] = duplicate_candidates[
        ["street", "town"]
    ].apply(lambda x: f"{x['street']},{x['town']}", axis=1)
    duplicate_candidates["address_match"] = duplicate_candidates.address.apply(
        lambda x: SequenceMatcher(None, current_station_address, x).ratio()
        if (current_station_address != "None,None") & (x != "None,None")
        else 0.0,
    )

    operator_score = (
            score_weights["operator"] * duplicate_candidates["operator_match"]
    )
    address_score = score_weights["address"] * duplicate_candidates["address_match"]
    distance_score = 70  # TODO check if we still need a distance score, wanted to filter hierarchically
    # score_weights["distance"] * (
    #    1 - duplicate_candidates["distance_meter"] / max_distance
    # )
    duplicate_candidates["matching_score"] = (
            operator_score + address_score + distance_score
    )
    duplicate_candidates.loc[
        (duplicate_candidates.matching_score > score_threshold), "is_duplicate"
    ] = True
    return duplicate_candidates.loc[duplicate_candidates.is_duplicate, :]

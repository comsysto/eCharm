from typing import Dict, List, Optional

from difflib import SequenceMatcher
import pandas as pd


def attribute_match_thresholds_duplicates(
        current_station: pd.Series,
        duplicate_candidates: pd.DataFrame,
        score_threshold: float = 0.49,
        max_distance: int = 100,
        score_weights: Optional[Dict[str, float]] = None,
        ) -> pd.DataFrame:

    current_station_address = f"{current_station['street']},{current_station['town']}"

    print(f"Searching for duplicates to OSM station {current_station.source_id}, "
          f"operator: {current_station.operator}, "
          f"address: {current_station_address}"
          )

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
    distance_score = 0.7  # TODO check if we still need a distance score, wanted to filter hierarchically
    duplicate_candidates["distance_match"] = 1 - duplicate_candidates["distance"] / max_distance

    #for idx in range(duplicate_candidates.shape[0]):
    #    duplicate_candidate: pd.Series = duplicate_candidates.iloc[idx]

    def is_duplicate_by_score(duplicate_candidate):
        #print(f"Candidate: {duplicate_candidate}")
        if duplicate_candidate["address_match"] >= 0.7:
            is_duplicate = True
            print("duplicate according to address")
        elif duplicate_candidate["operator_match"] >= 0.7:
            is_duplicate = True
            print("duplicate according to operator")
        elif duplicate_candidate["distance_match"] >= 0.3:
            is_duplicate = True
            print("duplicate according to distance")
        else:
            is_duplicate = False
            print(f"no duplicate: {duplicate_candidate.data_source}, "
                  f"source id: {duplicate_candidate.source_id}, "
                  f"operator: {duplicate_candidate.operator}, "
                  f"distance: {duplicate_candidate.distance}")
        return is_duplicate

    # TODO: for all duplicates found via OSM, which has no address, run the check again against all candidates
    # so e.g. if we have a duplicate with address it can be matched to other data sources via this attribute

    duplicate_candidates["is_duplicate"] = duplicate_candidates.apply(is_duplicate_by_score, axis=1)

    return duplicate_candidates[duplicate_candidates["is_duplicate"]]


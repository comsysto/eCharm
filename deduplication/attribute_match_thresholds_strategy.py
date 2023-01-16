from typing import Dict, List, Optional

from difflib import SequenceMatcher
import pandas as pd


def attribute_match_thresholds_duplicates(
        current_station: pd.Series,
        duplicate_candidates: pd.DataFrame,
        station_id_name: str,
        max_distance: int = 100,
        ) -> pd.DataFrame:
    pd.options.mode.chained_assignment = None

    if (duplicate_candidates.empty):
        return pd.DataFrame()

    current_station_address = f"{current_station['street']},{current_station['town']}"

    print(f"### Searching for duplicates to station {current_station.source_id}, "
          f"operator: {current_station.operator}, "
          f"address: {current_station_address}"
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

    duplicate_candidates["distance_match"] = 1 - duplicate_candidates["distance"] / max_distance

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
                  f"address: {duplicate_candidate.address}, "
                  f"distance: {duplicate_candidate.distance}")
        return is_duplicate

    duplicate_candidates["is_duplicate"] = duplicate_candidates.apply(is_duplicate_by_score, axis=1)

    # for all duplicates found via OSM, which has most of the time no address info, run the check again against all candidates
    # so e.g. if we have a duplicate with address it can be matched to other data sources via this attribute
    duplicates = duplicate_candidates[duplicate_candidates["is_duplicate"]]
    for idx in range(duplicates.shape[0]):
        current_station: pd.Series = duplicates.iloc[idx]
        current_station_id = current_station[station_id_name]

        # new candidates are all existing candidates except the new current station
        # and all stations which are already marked as duplicate
        duplicate_candidates_new = duplicate_candidates[(duplicate_candidates[station_id_name] != current_station_id)
                                                        & (~duplicate_candidates["is_duplicate"])]
        # recursive call to the current method, but with reduced set of candidates and new current station
        # TODO: think of changing distance threshold in this 2nd call
        #  as coordinates of other sources are not as good as OSM coordinates
        duplicate_candidates_new = attribute_match_thresholds_duplicates(
            current_station=current_station,
            duplicate_candidates=duplicate_candidates_new,
            station_id_name=station_id_name
        )
        if not duplicate_candidates_new.empty:
            # merge with original candidates
            duplicate_candidates.update(duplicate_candidates_new)

    return duplicate_candidates


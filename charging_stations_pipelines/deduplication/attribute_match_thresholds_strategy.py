import logging
from difflib import SequenceMatcher

import pandas as pd

logger = logging.getLogger(__name__)


def attribute_match_thresholds_duplicates(
        current_station: pd.Series,
        duplicate_candidates: pd.DataFrame,
        station_id_name: str,
        max_distance: int = 100,
) -> pd.DataFrame:
    pd.options.mode.chained_assignment = None

    remaining_duplicate_candidates = duplicate_candidates[~duplicate_candidates["is_duplicate"].astype(bool)]
    if remaining_duplicate_candidates.empty:
        return duplicate_candidates

    logger.debug(f"### Searching for duplicates to station {current_station.source_id}, "
                 f"operator: {current_station.operator}, "
                 f"address: {current_station['address']}"
                 )
    logger.debug(f"{len(remaining_duplicate_candidates)} duplicate candidates")

    remaining_duplicate_candidates["operator_match"] = remaining_duplicate_candidates.operator.apply(
        lambda x: SequenceMatcher(None, current_station.operator, str(x)).ratio()
        if (current_station.operator is not None) & (x is not None)
        else 0.0
    )

    remaining_duplicate_candidates["address_match"] = remaining_duplicate_candidates.address.apply(
        lambda x: SequenceMatcher(None, current_station['address'], x).ratio()
        if (current_station['address'] != "None,None") & (x != "None,None")
        else 0.0,
    )

    # this is always the distance to the initial central charging station
    remaining_duplicate_candidates["distance_match"] = 1 - remaining_duplicate_candidates["distance"] / max_distance

    def is_duplicate_by_score(duplicate_candidate):
        if duplicate_candidate["address_match"] >= 0.7:
            is_duplicate = True
            logger.debug("duplicate according to address")
        elif duplicate_candidate["operator_match"] >= 0.7:
            is_duplicate = True
            logger.debug("duplicate according to operator")
        elif duplicate_candidate["distance_match"] >= 0.3:
            is_duplicate = True
            logger.debug("duplicate according to distance")
        else:
            is_duplicate = False
            logger.debug(f"no duplicate: {duplicate_candidate.data_source}, "
                         f"source id: {duplicate_candidate.source_id}, "
                         f"operator: {duplicate_candidate.operator}, "
                         f"address: {duplicate_candidate.address}, "
                         f"row id: {duplicate_candidate.name}, "
                         f"distance: {duplicate_candidate.distance}")
        return is_duplicate

    remaining_duplicate_candidates["is_duplicate"] = remaining_duplicate_candidates.apply(is_duplicate_by_score, axis=1)
    # update original candidates
    duplicate_candidates.update(remaining_duplicate_candidates)

    # for all duplicates found via OSM, which has most of the time no address info,
    # run the check again against all candidates
    # so e.g. if we have a duplicate with address it can be matched to other data sources via this attribute
    new_duplicates = remaining_duplicate_candidates[remaining_duplicate_candidates["is_duplicate"]]
    for idx in range(new_duplicates.shape[0]):
        current_station: pd.Series = new_duplicates.iloc[idx]

        # recursive call to the current method,
        # but with some candidates already marked as duplicate and new current station
        # TODO: think of changing distance threshold in this 2nd call
        #  as coordinates of other sources are not as good as OSM coordinates
        duplicate_candidates = attribute_match_thresholds_duplicates(
            current_station=current_station,
            duplicate_candidates=duplicate_candidates,
            station_id_name=station_id_name
        )

    return duplicate_candidates

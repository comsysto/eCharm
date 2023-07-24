from Levenshtein import ratio

from charging_stations_pipelines.deduplication.v2.types import Station

WEIGHT_DISTANCE = 0.2
WEIGHT_ADDRESS = 0.5
WEIGHT_OPERATOR = 0.3


def weighted_average(distribution, weights):
    return sum([distribution[i] * weights[i] for i in range(len(distribution))]) / sum(weights)


class RatioCalculator:

    @staticmethod
    def ratio(left, right) -> float:
        # the higher the value for ratio is the more similar are the stations

        ratio_address = RatioCalculator.ratio_address(left, right)

        ratio_operator = RatioCalculator.ratio_operator(left, right)
        if ratio_address < 0.5 and ratio_operator < 0.5:
            return 0.1

        ratio_distance = RatioCalculator.ratio_distance(left, right)

        # return weighted_average([ratio_address, ratio_operator],
        #                         [WEIGHT_ADDRESS, WEIGHT_OPERATOR])
        return weighted_average([ratio_distance, ratio_address, ratio_operator],
                                [WEIGHT_DISTANCE, WEIGHT_ADDRESS, WEIGHT_OPERATOR])

    @staticmethod
    def ratio_operator(left, right):
        if any([not s for s in [left.operator, right.operator]]):
            return 0.1

        return ratio(left.operator, right.operator)

    @staticmethod
    def ratio_address(left: Station, right: Station):
        if any([not s for s in [left.address, right.address]]):
            return 0.1

        return ratio(left.address, right.address)

    @staticmethod
    def ratio_distance(left, right):
        distance = left.point.distance(right.point)
        return 0.0001 ** distance

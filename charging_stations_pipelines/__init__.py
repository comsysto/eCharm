"""Toplevel package for data processing pipelines."""
from pathlib import Path
from typing import Final

from charging_stations_pipelines.shared import JSON

PROJ_ROOT: Final[Path] = Path(__file__).parents[1]
"""The root directory of the project."""

PROJ_DATA_DIR: Final[Path] = PROJ_ROOT / 'data'
"""The path to the data folder."""

PROJ_CONFIG_DIR: Final[Path] = PROJ_ROOT / 'config'
"""The path to the central project config folder."""

# FIXME: move all filter here,
# EUROPEAN_COUNTRIES: Final[list[str]] = ['AL', 'AD', 'AM', 'AT', 'BY', 'BE', 'BA', 'BG', 'CH', 'CY',
#                                         'CZ', 'DE', 'DK', 'EE', 'ES', 'FO', 'FI', 'FR', 'GB', 'GE',
#                                         'GI', 'GR', 'HU', 'HR', 'IE', 'IS', 'IT', 'LT', 'LU', 'LV',
#                                         'MC', 'MK', 'MT', 'NO', 'NL', 'PO', 'PT', 'RO', 'RU', 'SE',
#                                         'SI', 'SK', 'SM', 'TR', 'UA', 'VA']
EUROPEAN_COUNTRIES: Final[dict[str, str]] = dict([
    ("AD", "Andorra"),
    ("AL", "Albania"),
    ("AT", "Austria"),
    ("BA", "Bosnia and Herzegovina"),
    ("BE", "Belgium"),
    ("BG", "Bulgaria"),
    ("BY", "Belarus"),
    ("CH", "Switzerland"),
    ("CY", "Cyprus"),
    ("CZ", "Czech Republic"),
    ("DE", "Germany"),
    ("DK", "Denmark"),
    ("EE", "Estonia"),
    # Spain (mainland): To focus on the mainland part of Spain and exclude its islands like the Canary Islands and
    #   the Balearic Islands.
    ("ES", "Spain (mainland)"),
    ("FI", "Finland"),
    # France (land mass) or France métropolitaine: This term is used to specify the mainland European part of France,
    #   excluding its overseas regions and territories.
    ("FR", "France (land mass)"),
    ("GB", "United Kingdom"),
    ("GR", "Greece"),
    ("HR", "Croatia"),
    ("HU", "Hungary"),
    ("IE", "Ireland"),
    ("IS", "Iceland"),
    ("IT", "Italy"),
    ("LI", "Liechtenstein"),
    ("LT", "Lithuania"),
    ("LU", "Luxembourg"),
    ("LV", "Latvia"),
    ("MD", "Moldova"),
    ("ME", "Montenegro"),
    ("MK", "North Macedonia"),
    ("MT", "Malta"),
    ("NL", "Netherlands"),
    ("NO", "Norway"),
    ("PL", "Poland"),
    ("PT", "Portugal"),
    ("RO", "Romania"),
    ("RS", "Serbia"),
    ("RU", "Russia"),
    ("SE", "Sweden"),
    ("SI", "Slovenia"),
    ("SK", "Slovakia"),
    ("SM", "San Marino"),
    ("UA", "Ukraine"),
    ("VA", "Vatican City"),])
"""The mapping of European country codes to the corresponding area names."""

# FIXME: "France (land mass)" and "France métropolitaine"
# (land mass) / (mainland) / (continental)?
# List of ISO3166-1 codes and corresponding country names
#
#     Portugal (mainland): To specify the European mainland of Portugal and exclude its autonomous regions like Madeira and the Azores.
#
#     Italy (mainland): To refer to the mainland Italian peninsula, excluding its islands like Sicily and Sardinia.

#     Greece (mainland): To target the Greek mainland and leave out its numerous islands.
#
#     Croatia (mainland): To focus on the mainland part of Croatia and exclude its islands along the Adriatic coast.
#
#     Norway (mainland): To specify the Norwegian mainland and exclude the Svalbard archipelago and other overseas territories.
#
#     Finland (mainland): To refer to the mainland part of Finland and exclude the Åland Islands.
#
#     Sweden (mainland): To target the Swedish mainland and exclude islands like Gotland.
#
#     Denmark (mainland): To focus on the Jutland Peninsula (mainland Denmark) and exclude the Faroe Islands and Greenland.


# FIXME: 54 european countries from OCM
# """AD""","""Andorra"""
# """AL""","""Albania"""
# """AT""","""Austria"""
# """AX""","""Aland Islands"""
# """BA""","""Bosnia And Herzegovina"""
# """BE""","""Belgium"""
# """BG""","""Bulgaria"""
# """BY""","""Belarus"""
# """CH""","""Switzerland"""
# """CZ""","""Czech Republic"""
# """DE""","""Germany"""
# """DK""","""Denmark"""
# """EE""","""Estonia"""
# """ES""","""Spain"""
# """FI""","""Finland"""
# """FO""","""Faroe Islands"""
# """FR""","""France"""
# """GB""","""United Kingdom"""
# """GG""","""Guernsey"""
# """GI""","""Gibraltar"""
# """GR""","""Greece"""
# """HR""","""Croatia"""
# """HU""","""Hungary"""
# """IE""","""Ireland"""
# """IM""","""Isle Of Man"""
# """IS""","""Iceland"""
# """IT""","""Italy"""
# """JE""","""Jersey"""
# """LI""","""Liechtenstein"""
# """LT""","""Lithuania"""
# """LU""","""Luxembourg"""
# """LV""","""Latvia"""
# """MC""","""Monaco"""
# """MD""","""Moldova, Republic Of"""
# """ME""","""Montenegro"""
# """MK""","""Macedonia"""
# """MT""","""Malta"""
# """NL""","""Netherlands"""
# """NO""","""Norway"""
# """PL""","""Poland"""
# """PT""","""Portugal"""
# """RO""","""Romania"""
# """RS""","""Serbia"""
# """RU""","""Russian Federation"""
# """SE""","""Sweden"""
# """SI""","""Slovenia"""
# """SJ""","""Svalbard And Jan Mayen"""
# """SK""","""Slovakia"""
# """SM""","""San Marino"""
# """TR""","""Turkey"""
# """UA""","""Ukraine"""
# """VA""","""Holy See (Vatican City State)"""
# """XK""","""Kosovo"""

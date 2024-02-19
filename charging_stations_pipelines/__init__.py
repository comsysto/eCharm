from pathlib import Path
from typing import Final


class CountryInfo:
    def __init__(self, name: str, gov: bool, osm: bool, ocm: bool):
        self.name = name
        self.gov = gov
        self.osm = osm
        self.ocm = ocm


COUNTRIES: Final[dict[str, CountryInfo]] = dict(
    [
        ("AD", CountryInfo(name="Andorra", gov=False, osm=True, ocm=True)),
        ("AL", CountryInfo(name="Albania", gov=False, osm=True, ocm=True)),
        ("AT", CountryInfo(name="Austria", gov=True, osm=True, ocm=True)),
        ("BA", CountryInfo(name="Bosnia and Herzegovina", gov=False, osm=True, ocm=True)),
        ("BE", CountryInfo(name="Belgium", gov=False, osm=True, ocm=True)),
        ("BG", CountryInfo(name="Bulgaria", gov=False, osm=True, ocm=True)),
        ("BY", CountryInfo(name="Belarus", gov=False, osm=True, ocm=True)),
        ("CH", CountryInfo(name="Switzerland", gov=False, osm=True, ocm=True)),
        ("CY", CountryInfo(name="Cyprus", gov=False, osm=True, ocm=True)),
        ("CZ", CountryInfo(name="Czech Republic", gov=False, osm=True, ocm=True)),
        ("DE", CountryInfo(name="Germany", gov=True, osm=True, ocm=True)),
        ("DK", CountryInfo(name="Denmark", gov=False, osm=True, ocm=True)),
        ("EE", CountryInfo(name="Estonia", gov=False, osm=True, ocm=True)),
        ("ES", CountryInfo(name="Spain", gov=False, osm=True, ocm=True)),
        ("FI", CountryInfo(name="Finland", gov=False, osm=True, ocm=True)),
        ("FR", CountryInfo(name="France", gov=True, osm=True, ocm=True)),
        ("GB", CountryInfo(name="United Kingdom", gov=True, osm=True, ocm=True)),
        ("GR", CountryInfo(name="Greece", gov=False, osm=True, ocm=True)),
        ("HR", CountryInfo(name="Croatia", gov=False, osm=True, ocm=True)),
        ("HU", CountryInfo(name="Hungary", gov=False, osm=True, ocm=True)),
        ("IE", CountryInfo(name="Ireland", gov=False, osm=True, ocm=True)),
        ("IS", CountryInfo(name="Iceland", gov=False, osm=True, ocm=True)),
        ("IT", CountryInfo(name="Italy", gov=False, osm=True, ocm=True)),
        ("LI", CountryInfo(name="Liechtenstein", gov=False, osm=True, ocm=True)),
        ("LT", CountryInfo(name="Lithuania", gov=False, osm=True, ocm=True)),
        ("LU", CountryInfo(name="Luxembourg", gov=False, osm=True, ocm=True)),
        ("LV", CountryInfo(name="Latvia", gov=False, osm=True, ocm=True)),
        ("MC", CountryInfo(name="Monaco", gov=False, osm=True, ocm=True)),
        ("MD", CountryInfo(name="Moldova", gov=False, osm=True, ocm=True)),
        ("ME", CountryInfo(name="Montenegro", gov=False, osm=True, ocm=True)),
        ("MK", CountryInfo(name="North Macedonia", gov=False, osm=True, ocm=True)),
        ("MT", CountryInfo(name="Malta", gov=False, osm=True, ocm=True)),
        ("NL", CountryInfo(name="Netherlands", gov=False, osm=True, ocm=True)),
        ("NO", CountryInfo(name="Norway", gov=True, osm=True, ocm=True)),
        ("PL", CountryInfo(name="Poland", gov=False, osm=True, ocm=True)),
        ("PT", CountryInfo(name="Portugal", gov=False, osm=True, ocm=True)),
        ("RO", CountryInfo(name="Romania", gov=False, osm=True, ocm=True)),
        ("RS", CountryInfo(name="Serbia", gov=False, osm=True, ocm=True)),
        ("SE", CountryInfo(name="Sweden", gov=True, osm=True, ocm=True)),
        ("SI", CountryInfo(name="Slovenia", gov=False, osm=True, ocm=True)),
        ("SK", CountryInfo(name="Slovakia", gov=False, osm=True, ocm=True)),
        ("SM", CountryInfo(name="San Marino", gov=False, osm=True, ocm=True)),
        ("UA", CountryInfo(name="Ukraine", gov=False, osm=True, ocm=True)),
        ("VA", CountryInfo(name="Vatican City", gov=False, osm=True, ocm=False)),
        ("XK", CountryInfo(name="Kosovo", gov=False, osm=True, ocm=True)),
    ]
)

COUNTRY_CODES: list[str] = list(COUNTRIES.keys())
OSM_COUNTRY_CODES: list[str] = list({k: v for k, v in COUNTRIES.items() if v.osm}.keys())
OCM_COUNTRY_CODES: list[str] = list({k: v for k, v in COUNTRIES.items() if v.ocm}.keys())
GOV_COUNTRY_CODES: list[str] = list({k: v for k, v in COUNTRIES.items() if v.gov}.keys())

PROJ_ROOT: Final[Path] = Path(__file__).parents[1]
"""The root directory of the project."""

PROJ_DATA_DIR: Final[Path] = PROJ_ROOT / "data"
"""The path to the data folder."""

# New datasource for Austria...

...aka `api.e-control.at` aka `ladestellen.at` aka _Ladestellenverzeichnis_ aka _nationales Ladepunkteregister_.

## Caveats

1. registration required (https://admin.ladestellen.at) in order to access the API https://api.e-control.at/...
2. the API is limited to 1000 results per request, so we need to paginate
3. described in documentation usage via `Apikey: <...>` and `Domain: <...>` headers does not work (TODO contact
   the support). As workaround, I currently use `Authorization: Basic <base64 encoded username:password>` header.

## Data source

1. data import via https://api.e-control.at/charge/1.0/search/stations
2. number of datapoints (stations): 9444

#### API schema

Full API schema (JSON) - https://api.e-control.at/charge/1.0/v2/api-docs?group=public-api

1. Swagger UI for it - https://api.e-control.at/charge/1.0/swagger-ui.html?urls.primaryName=public-api
2. its copy is stored locally in this repo - [api-e-control.at-20231121.json](./api-e-control.at-20231121.json)

#### Point (charger point): station.points[]

```json
{
  "GeneralSearchPointDTO": {
    "title": "GeneralSearchPointDTO",
    "type": "object",
    "properties": {
      "public": {
        "type": "boolean"
      },
      "evseId": {
        "type": "string",
        "example": "AT*CTL*E12345*1",
        "description": "the EVSE ID"
      },
      "energyInKw": {
        "type": "number",
        "example": 22.0,
        "description": "the provided energy in kW"
      },
      "location": {
        "description": "the point's geo-location",
        "$ref": "#/definitions/GeneralSearchLocationDTO"
      },
      "roaming": {
        "type": "boolean",
        "example": true,
        "description": "roaming contracts accepted"
      },
      "freeOfCharge": {
        "type": "boolean",
        "example": true,
        "description": "load for free"
      },
      "priceInCentPerKwh": {
        "type": "number",
        "example": 0.0,
        "description": "the price in Euro Cent per kWh"
      },
      "priceInCentPerMin": {
        "type": "number",
        "example": 0.0,
        "description": "the price in Euro Cent per minute"
      },
      "status": {
        "type": "string",
        "example": [
          'AVAILABLE',
          'OCCUPIED',
          'RESERVED',
          'FAULTED',
          'UNAVAILABLE',
          'UNKNOWN'
        ],
        "description": "the point's status"
      },
      "authenticationModes": {
        "type": "array",
        "example": [
          'CASH',
          'RFID',
          'CREDIT_CARD',
          'DEBIT_CARD',
          'APP',
          'SMS',
          'NFC',
          'WEBSITE'
        ],
        "description": "how to authenticate yourself when loading",
        "items": {
          "type": "string"
        }
      },
      "connectorTypes": {
        "type": "array",
        "example": [
          'CCCS1',
          'CCCS2',
          'CG105',
          'CTESLA',
          'CTYPE1',
          'CTYPE2',
          'S309-1P-16A',
          'S309-1P-32A',
          'S309-3P-16A',
          'S309-3P-32A',
          'SBS1361',
          'SCEE-7-8',
          'STYPE2',
          'STYPE3',
          'OTHER1PHMAX16A',
          'OTHER1PHOVER16A',
          'OTHER3PH',
          'PAN',
          'WINDUCTIVE',
          'WRESONANT',
          'UNDETERMINED',
          'UNKNOWN'
        ],
        "description": "the loading-point's connectors",
        "items": {
          "type": "string"
        }
      },
      "vehicleTypes": {
        "type": "array",
        "example": [
          'CAR',
          'MOTORCYCLE',
          'BICYCLE',
          'BOAT',
          'TRUCK'
        ],
        "description": "which vehicle-types the loading-point can accommodate",
        "items": {
          "type": "string"
        }
      }
    }
  }
}
```

## Misc

E-Control is the government regulator for electricity and natural gas markets in Austria. It was founded in 2001 on the
basis of the Energy Liberalisation Act.

Das Ladestellenverzeichnis dient zudem im Sinne der EU-Richtlinie (Richtlinie 2014/94/EU) über den Aufbau der
Infrastruktur für alternative Kraftstoffe als nationales Referenzverzeichnis in dem jede öffentlich zugängliche
Ladestation aufgeführt sein soll.

Alle Betreiber öffentlich zugänglicher Ladepunkte in Österreich sind gesetzlich verpflichtet, die Positionen seiner
Ladestellen bzw. Ladepunkte in das Ladestellenverzeichnis zu melden (§22a E-Control-Gesetz).

1. API docs - https://api.e-control.at/charge/1.0/v2/api-docs?group=public-api, Swagger UI for
   it - https://api.e-control.at/charge/1.0/swagger-ui.html?urls.primaryName=public-api
2. Docs: As of definition OCPP 2.0 (reference to ‘OCPP-2.0_part2_specification.pdf’ section ‘2.23.
   ConnectorEnumType’) - https://www.openchargealliance.org/news/download-now-ocpp-201-part-2-edition-2/ ,
   root: https://www.openchargealliance.org/protocols/ocpp-201/
3. Login / Registration: https://admin.ladestellen.at

## Excerpt from the OCPP (Open Charge Point Protocol) docs / by openchargealliance.org

Source: https://www.openchargealliance.org/news/download-now-ocpp-201-part-2-edition-2/

### 3.22 ConnectorEnumType

Enumeration
Allowed values of ConnectorCode.

NOTE
This enumeration does not attempt to include every possible power connector type worldwide as an individual
type, but to specifically define those that are known to be in use (or likely to be in use) in the Charging Stations
using the OCPP protocol. In particular, many of the very large number of domestic electrical sockets designs in
use in many countries are excluded, unless there is evidence that they are or are likely to be approved for use on
Charging Stations in some jurisdictions (e.g. as secondary connectors for charging light EVs such as electric
scooters). These light connector types can be represented with the enumeration value Other1PhMax16A.
Similarly, any single phase connector not otherwise enumerated that is rated for 16A or over should be reported
as Other1PhOver16A. All 3 phase connector types not explicitly enumerated should be represented as Other3Ph.

ConnectorEnumType is used by: reserveNow:ReserveNowRequest

| Value                                                           | Depiction                                                                   | Description                                                                                                                                                                  |
|-----------------------------------------------------------------|-----------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [cCCS1](https://en.wikipedia.org/wiki/Combined_Charging_System) | ![](https://www.ladestellen.at/assets/appicons/electric/plugs/1_cCCS1.svg)  | Combined Charging System 1 (captive cabled) a.k.a. Combo 1                                                                                                                   |
| [cCCS2](https://en.wikipedia.org/wiki/Combined_Charging_System) | ![](https://www.ladestellen.at/assets/appicons/electric/plugs/2_cCCS2.svg)  | Combined Charging System 2 (captive cabled) a.k.a. Combo 2                                                                                                                   |
| [cG105](https://en.wikipedia.org/wiki/CHAdeMO)                  | ![](https://www.ladestellen.at/assets/appicons/electric/plugs/3_cG105.svg)  | JARI G105-1993 (captive cabled) a.k.a. **CHAdeMO**                                                                                                                           |
| [cTesla](https://en.wikipedia.org/wiki/Tesla_Supercharger)      |                                                                             | Tesla Connector (captive cabled)                                                                                                                                             |                                                                                                                                             
| [cType1](https://en.wikipedia.org/wiki/SAE_J1772)               | ![](https://www.ladestellen.at/assets/appicons/electric/plugs/5_cType1.svg) | IEC62196-2 Type 1 connector (captive cabled) a.k.a. J1772                                                                                                                    |           |                                                                                                         
| [cType2](https://en.wikipedia.org/wiki/Type_2_connector)        | ![](https://www.ladestellen.at/assets/appicons/electric/plugs/6_cType2.svg) | IEC62196-2 Type 2 connector (captive cabled) a.k.a. Mennekes connector                                                                                                       |
| s309-1P-16A                                                     |                                                                             | 16A 1 phase IEC60309 socket                                                                                                                                                  |
| s309-1P-32A                                                     |                                                                             | 32A 1 phase IEC60309 socket                                                                                                                                                  |
| s309-3P-16A                                                     |                                                                             | 16A 3 phase IEC60309 socket                                                                                                                                                  |
| s309-3P-32A                                                     |                                                                             | 32A 3 phase IEC60309 socket                                                                                                                                                  |
| sBS1361                                                         |                                                                             | UK domestic socket a.k.a. 13Amp                                                                                                                                              |
| sCEE-7-7                                                        | ![](https://www.ladestellen.at/assets/appicons/electric/plugs/9_sCEE.svg)   | CEE 7/7 16A socket. May represent 7/4 & 7/5 a.k.a **Schuko**                                                                                                                 |
| sType2                                                          |                                                                             | IEC62196-2 Type 2 socket a.k.a. Mennekes connector                                                                                                                           |
| [sType3](https://en.wikipedia.org/wiki/Type_3_connector)        | ![]()                                                                       | IEC62196-2 Type 2 socket a.k.a. Scame                                                                                                                                        |
| Other1PhMax16A                                                  |                                                                             | Other single phase (domestic) sockets not mentioned above, rated at no more than 16A. CEE7/17, AS3112,NEMA 5-15, NEMA 5-20, JISC8303, TIS166, SI 32, CPCS-CCC, SEV1011, etc. |                                                       
| Other1PhOver16A                                                 |                                                                             | Other single phase sockets not mentioned above (over 16A)                                                                                                                    |
| Other3Ph                                                        |                                                                             | Other 3 phase sockets not mentioned above. NEMA14-30, NEMA14-50.                                                                                                             |
| Pan                                                             |                                                                             | Pantograph connector                                                                                                                                                         |
| wInductive                                                      |                                                                             | Wireless inductively coupled connection (generic)                                                                                                                            |
| wResonant                                                       |                                                                             | Wireless resonant coupled connection (generic)                                                                                                                               |
| Undetermined                                                    |                                                                             | Yet to be determined (e.g. before plugged in)                                                                                                                                |
| Unknown                                                         |                                                                             | Unknown; not determinable                                                                                                                                                    |

Source of the images: https://www.ladestellen.at/#/electric - see drop-down list "Steckertypen"

## Glossary

**Ladestelle**: eine Ladestelle definiert sich über den Standort, als die Adresse. Alle Lademöglichkeiten,
die an derselben Adresse vorzufinden sind, bilden zusammen eine Ladestelle. (So wie alle Tankrüssel
an einer Adresse zusammen eine Tankstelle darstellen) [1]

**Ladepunkt**: ein Ladepunkt ist der elektrische Anschluss (Stecker) über den jeweils immer nur ein
Fahrzeug gleichzeitig geladen werden kann. Wenn also beispielsweise an einer Ladeeinrichtung
(Ladesäule) zwei Stecker vorhanden sind, an denen gleichzeitig auch zwei Fahrzeuge laden können,
dann sind das zwei Ladepunkte. Wenn aber etwas mehrere, unterschiedliche Stecker an einer
Ladeeinrichtung vorhanden sind, die nur der Stecktypauswahl dienen, von denen aber immer nur
einer gleichzeitig genutzt werden kann, dann sind diese mehrere Stecker dennoch nur ein
Ladepunkt [1]

**Wechselstrom = AC (Alternating Current)**: beim AC – Laden stehen Leistungen von 3,6 kW bis 43 kW bei den
Ladestationen zur Verfügung [1]

**Gleichstrom = DC (Direct Current)**: bei den sogenannten DC-Schnellladern stehen Leistungen von 20 kW bis bald 350 kW
zur Verfügung. Diese Ladeinfrastruktur findet man meist an sehr stark frequentierten Orten (Rastplätzen, Autobahn,
usw.). [1]

[1] ladestellen.at

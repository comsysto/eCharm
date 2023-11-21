# New datasource for Austria...

...aka `api.e-control.at` aka `ladestellen.at` aka _Ladestellenverzeichnis_ aka _nationales Ladepunkteregister_.

## Caveats

1. registration required (https://admin.ladestellen.at) in order to access the API https://api.e-control.at/...
2. the API is limited to 1000 results per request, so we need to paginate
3. described in documentation usage via `Apikey: <...>` and `Domain: <...>` headers does not work (TODO contact
   the support). As workaround I currently use `Authorization: Basic <base64 encoded username:password>` header.

## Data source

1. data import via https://api.e-control.at/charge/1.0/search/stations
2. number of datapoints (stations): 9444

### Schema

TDB

#### API schema

1. TBD URL
2. Station object section - TDB (add data as version number)
3. Point (charger point) object section - TDB

## Mapping

## Misc

E-Control is the government regulator for electricity and natural gas markets in Austria. It was founded in 2001 on the
basis of the Energy Liberalisation Act.

Das Ladestellenverzeichnis dient zudem im Sinne der EU-Richtline (Richtlinie 2014/94/EU) über den Aufbau der
Infrastruktur für alternative Kraftstoffe als nationales Referenzverzeichnis in dem jede öffentlich zugängliche
Ladestation aufgeführt sein soll.

Alle Betreiber öffentlich zugänglicher Ladepunkte in Österreich sind gesetzlich verpflichtet, die Positionen seiner
Ladestellen bzw. Ladepunkte in das Ladestellenverzeichnis zu melden (§22a E-Control-Gesetz).

1. API docs - https://api.e-control.at/charge/1.0/v2/api-docs?group=public-api, Swagger UI for
   it - https://api.e-control.at/charge/1.0/swagger-ui.html?urls.primaryName=public-api
2. Docs: As of definition OCPP 2.0 ((reference to ‘OCPP-2.0_part2_specification.pdf’ section ‘2.23.
   ConnectorEnumType’) - https://www.openchargealliance.org/news/download-now-ocpp-201-part-2-edition-2/ ,
   root: https://www.openchargealliance.org/protocols/ocpp-201/
3. Login / Registration: https://admin.ladestellen.at

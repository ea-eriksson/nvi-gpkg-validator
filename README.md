# nvi-gpkg-validator

Validate [NVI GeoPackage](https://www.sis.se/standardutveckling/tksidor/tk500599/sistk555/) data integrity and compliance with the standard: SIS/TS 199002:2023 Naturvärdesinventering (NVI).

## Background

The *SIS/TS 199002:2023 Naturvärdesinventering (NVI)* technical specification defines the exchange format for the standard: *SS 199000:2023 Naturvärdesinventering (NVI)*.
A [template NVI GeoPackage](https://www.sis.se/standardutveckling/tksidor/tk500599/sistk555/) has been provided by the Swedish Institute for Standards.
However, since there are few constraints when adding data to this template, it's still possible to create a GeoPackage that doesn't conform to the standard. The nvi-gpkg-validator script was therefore developed to help users ensure compliant NVI GeoPackages.

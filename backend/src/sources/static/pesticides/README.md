# Pesticide data

This data was requested from and granted by the The Danish Environmental Protection Agency.

It was downloaded on the 12th of december 2024.

## Note on updating the data

The data for the year 2023/2024 has its deadline on the 31st of march 2025. By then, it should be trivial to request the single sheet with the same columns as in the data already received.

## Column descriptions

With the data we also received a file describing the columns and units in the data.

### Columns

Variable                   |Danish description |English description
---------------------------|-------------------|--------------------
CompanyName                |Virksomhedsnavn
CompanyRegistrationNumber  |CVR nr.            |
StreetName                 |Vejnavn            |
StreetBuildingIdentifier   |vej nummer         |
FloorIdentifier            |etage              |
PostCodeIdentifier         |Postnummer         |Postal code
City                       |By                 |
AcreageSize                |Behandlet areal    |Acreage size treated
AcreageUnit                |Enhed              |
Name                       |Afgrødenavn        |Crop name
Code                       |Afgrødekode        |Crop code
PesticideName              |Navn pesticid
PesticideRegistrationNumber|Reg. nr. (pesticid)|Pesticide ID number
DosageQuantity             |Dosis              |
DosageUnit                 |Enhed dosis        |
NoPesticides               |Anvender pesticider

### Units

AcreageUnit   | Values
--------------|------------
1             | m2
2             | hektar

DosageUnit    | Values
--------------|------------
1             | Gram
2             | Kg
3             | Mililiter
4             | Liter
5             | Tablets

NoPesticides  | Values
--------------|-----------
FALSK         | Pesticide use
SAND          | NO pesticide use
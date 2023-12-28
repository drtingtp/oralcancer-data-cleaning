# Validation rules - dates

## 3 `DATEBIRTH` vs `ICNUMBER`
* Applies to rows with full I/C (12 digits)
* Full `ICNUMBER` should map to `DATEBIRTH` correctly

## 4 `DATESCREEN` vs `DATE REFERRED` vs `DATE REFERRED QUIT SER`
* `DATE REFERRED` (date referred for lesion management) should be later than `DATESCREEN`
* `DATE REFERRED QUIT SER` (date referred to quit smoking clinic) should be later than `DATESCREEN`

## 5 `DATE REFERRED` vs `DATE SEEN BY SPECIALIST`
* `DATE SEEN BY SPECIALIST` should be later than `DATE REFERRED`

## 6 `DATE REFERRED QUIT SER` vs `TARIKH TEMUJANJI QUIT SERVICE`
* `TARIKH TEMUJANJI QUIT SERVICE` should be later than `DATE REFERRED QUIT SER`

# Validation rules - others

## 1 `ICNUMBER` vs `GENDER`
* Applies to rows with full I/C (12 digits)
* Last digit of `ICNUMBER` is evaluated, if it ends with odd number `GENDER` should be `1`; if it ends with even number `GENDER` should be `2`

## 2 `LESION` vs `REFERAL TO SPECIALIST`
* (`LESION`, `REFERAL TO SPECIALIST`) should be either (`True`, `True`) or (`False`, `False`)

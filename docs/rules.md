# Validation rules

## General information

### INCLUSION_LESION_OR_HABIT
* Subject must have either lesion or habit
* No opportunistic screening
* [outbox] What if it is done at high-risk community but no lesion and no habit?

### IC_VS_GENDER
* Applies to rows with full I/C (12 digits)
* Last digit of `ICNUMBER` is evaluated, if it ends with odd number `GENDER` should be `1`; if it ends with even number `GENDER` should be `2`

### LESION_VS_TELEPHONE
* If `LESION` is True, subject must provide telephone number
* Valid telephone number is checked using regex `^(6?0[1-9])\d{7,9}$`
    * Start `^`
    * Optionally a digit `6`
    * Followed by a digit within the range [1-9]
    * Followed by 7 to 9 digits
    * End `$`

## Dates

### IC_VS_DATEBIRTH
* Applies to rows with full I/C (12 digits)
* Full `ICNUMBER` should map to `DATEBIRTH` correctly

### DATESCREEN_VS_DATEREFER
* `DATE REFERRED` (date referred for lesion management) should be later than `DATESCREEN`
* `DATE REFERRED QUIT SER` (date referred to quit smoking clinic) should be later than `DATESCREEN`

### DATEREFER_VS_DATE_SEEN_SPECIALIST
* `DATE SEEN BY SPECIALIST` should be later than `DATE REFERRED`

### DATEREFER_QUIT_VS_QUIT_APPT
* `TARIKH TEMUJANJI QUIT SERVICE` should be later than `DATE REFERRED QUIT SER`

## Lesion

### LESION_VS_REFER_SPECIALIST
* (`LESION`, `REFERAL TO SPECIALIST`) should be either (`True`, `True`) or (`False`, `False`)

### LESION_VS_LESION_COLS
[WIP]

### LESION_COLS_COMPLETENESS
[WIP]

## Habit

### HABIT_VS_HABIT_COLS

### TOBACCO_COMPLETENESS

### ALCOHOL_COMPLETENESS

### BETEL_COMPLETENESS

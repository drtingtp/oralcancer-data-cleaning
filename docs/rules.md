# Validation rules

## General information

### INCLUSION_LESION_OR_HABIT
* Subject must have either lesion or habit.
* No opportunistic screening.

### VALID_IC
* Should contain only 12 digits.
* First 6 digits should be map into a date.

### IC_VS_GENDER
* Applies to rows with full I/C (12 digits).
* Last digit of `ICNUMBER` is evaluated, if it ends with odd number `GENDER` should be `1`; if it ends with even number `GENDER` should be `2`.
* [According to Wikipedia](https://en.wikipedia.org/wiki/Malaysian_identity_card#Structure_of_the_National_Registration_Identity_Card_Number_(NRIC)), this is not true for early batches of MyKad, and the specification was never gazetted.

### LESION_VS_TELEPHONE
* If `LESION` is True, subject must provide telephone number.
* Valid telephone number is checked using regex `^(6?0[1-9])\d{7,9}$`:
    * Start `^`
    * Optionally a digit `6`
    * Followed by a digit within the range [1-9]
    * Followed by 7 to 9 digits
    * End `$`

## Dates

### IC_VS_DATEBIRTH
* Applies to rows with full I/C (12 digits).
* Full `ICNUMBER` should map to `DATEBIRTH` correctly.

### DATESCREEN_VS_DATEREFER
* `DATE REFERRED` (date referred for lesion management) should be later than `DATESCREEN`.
* `DATE REFERRED QUIT SER` (date referred to quit smoking clinic) should be later than `DATESCREEN`.

### DATEREFER_VS_DATE_SEEN_SPECIALIST
* `DATE SEEN BY SPECIALIST` should be later than `DATE REFERRED`.

### DATEREFER_QUIT_VS_QUIT_APPT
* valid_completeness: `DATE REFERRED QUIT SER` if filled, `TARIKH TEMUJANJI QUIT SERVICE` should be filled.
* valid_sequence: `TARIKH TEMUJANJI QUIT SERVICE` should be later than `DATE REFERRED QUIT SER`.

## Lesion

### LESION_VS_REFER_SPECIALIST
* (`LESION`, `REFERAL TO SPECIALIST`) should be either (`True`, `True`) or (`False`, `False`).

### LESION_VS_LESION_COLS
* If `LESION` is False, `lesion_count` should be `0`.
* If `LESION` is True, `lesion_count` should be more than `0`.

### LESION_COLS_COMPLETENESS
* Completeness check for lesion type, size and site - all must be filled if any one is filled.

## Habit

### HABIT_VS_HABIT_COLS
* `HABITS` if True, either one of `TOBACCO`, `BBETEL QUID CHEWING`, `ALCOHOL` columns should be "1- habit currently practiced" or "2 - past habit now has stopped (minimum 6 months)".

### TOBACCO_COMPLETENESS

### ALCOHOL_COMPLETENESS

### BETEL_COMPLETENESS

## Quit service

### READY_QUIT_VS_REFERRAL_QUIT
* 

## [WIP]REFERRAL_QUIT_TOBACCO_ONLY
* In WPKLP only: referral to quit services should only be subjects with tobacco habits and ready to quit.

### [WIP]REFERRAL_QUIT_VS_DATE_REFERRED_QUIT
* If `REFERRAL TO QUIT SERVICES` is True, `DATE REFERRED QUIT SER` should be filled.

### [WIP]ATTEND_FIRST_APPT_VS_INTERVENTION_STATUS



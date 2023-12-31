from enum import Enum, auto

# List: identifier columns
list_id_cols = ["DISTRICT", "LOCATION OF SCREENING", "DATESCREEN", "ICNUMBER"]


# Rule enums: ../docs/rules.md
class RuleEnum(Enum):
  # General information
  INCLUSION_LESION_OR_HABIT = auto()
  VALID_IC = auto()
  IC_VS_GENDER = auto()
  LESION_VS_TELEPHONE = auto()

  # Dates
  IC_VS_DATEBIRTH = auto()
  DATESCREEN_VS_DATEREFER = auto()
  DATEREFER_VS_DATE_SEEN_SPECIALIST = auto()
  DATEREFER_QUIT_VS_QUIT_APPT = auto()

  # Habit
  HABIT_VS_HABIT_COLS = auto()
  TOBACCO_TALLINESS = auto()
  ALCOHOL_TALLINESS = auto()
  BETEL_TALLINESS = auto()

  # History
  MEDIHIST_COMPLETENESS = auto()
  FAMIHISTCANCER_COMPLETENESS = auto()

  # Lesion
  LESION_VS_REFER_SPECIALIST = auto()
  LESION_VS_LESION_COLS = auto()
  LESION_COLS_COMPLETENESS = auto()

  # Additional details (Education and Occupation)
  LESION_VS_ADDITIONAL_DETAILS = auto()

  # Quit services (Appendix 5A)
  REFERRAL_QUIT_VS_READY_QUIT = auto()
  REFERRAL_QUIT_VS_DATE_REFERRED_VS_FIRST_APPT_DATE = auto()
  ATTEND_FIRST_APPT_NULL_CHECK = auto()
  ATTEND_FIRST_APPT_VS_INTERVENTION_STATUS = auto()

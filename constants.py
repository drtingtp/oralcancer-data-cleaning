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

  # Lesion
  LESION_VS_REFER_SPECIALIST = auto()
  LESION_VS_LESION_COLS = auto()
  LESION_COLS_COMPLETENESS = auto()

  # Habit
  HABIT_VS_HABIT_COLS = auto()
  REFERRAL_QUIT_VS_DATA_REFERRED_QUIT = auto()
  TOBACCO_TALLINESS = auto()
  ALCOHOL_TALLINESS = auto()
  BETEL_TALLINESS = auto()

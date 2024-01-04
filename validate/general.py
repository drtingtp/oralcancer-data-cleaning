import math
from datetime import date

import polars as pl

from constants import RuleEnum

from .store import store_data, ValidationStore
from .decorator import valid_ic

# constants
today = date.today()
this_year = date.today().year
this_year_p1 = math.floor(this_year / 100)  # first two digits
this_year_p2 = this_year % 100  # last two digits
habit_whitelist = [
  "Null|Null|Null",
  "0 - No such habit|Null|Null",
  "1- habit currently practiced|Yes|Yes",
  "1- habit currently practiced|Yes|No",
  "2 - past habit now has stopped (minimum 6 months)|Yes|Null",
]
intervention_status_whitelist = [
  "Null|Null",
  "TIDAK HADIR|II- GAGAL DATANG TEMUJANJI",
  "HADIR|I- SEDANG MENERIMA RAWATAN",
  "HADIR|III- GAGAL BERHENTI",
  "HADIR|IV- BERJAYA BERHENTI SELAMA 6 BULAN",
]
famihistcancer_whitelist = ["false|false|false", "true|true|true"]


# inclusion criteria
@store_data(RuleEnum.INCLUSION_LESION_OR_HABIT, ["LESION", "HABITS"])
def _validate_inclusion_lesion_or_habit(lf: pl.LazyFrame):
  """
  Rule: Subject must either have LESION or HABITS.
  """
  return lf.filter((pl.col("LESION") | pl.col("HABITS")) == False)


def _pipe_validate_ic(lf: pl.LazyFrame):
  """
  Pipe function for ValidationGeneral init.
  Add columns: datebirth_from_ic, valid_ic_digits, valid_ic_date, valid_ic.
  """
  # year_p1 is first two digits of birth year
  # year_p2 is second two digits of birth year
  return (
    lf.with_columns(  # slice first two digits as year_p2
      pl.when(pl.col("ICNUMBER").str.contains(r"^\d{12}$"))
      .then(True)
      .otherwise(False)
      .alias("valid_ic_digits"),
      pl.col("ICNUMBER").str.slice(0, 2).cast(pl.Int16).alias("year_p2"),
    )
    .with_columns(  # calculate first two digits of birth year from IC
      pl.when(pl.col("year_p2") > this_year_p2)
      .then(this_year_p1 - 1)
      .otherwise(this_year_p1)
      .cast(pl.Utf8)
      .alias("year_p1"),
    )
    .with_columns(
      pl.concat_str(  # concat into full date string
        pl.col("year_p1"),
        pl.col("ICNUMBER").str.slice(0, 2),
        pl.lit("-"),
        pl.col("ICNUMBER").str.slice(2, 2),
        pl.lit("-"),
        pl.col("ICNUMBER").str.slice(4, 2),
      ).alias("datebirth_from_ic")
    )
    .drop(["year_p1", "year_p2"])
    .with_columns(pl.col("datebirth_from_ic").str.to_date(strict=False))
    .with_columns(
      pl.when(pl.col("datebirth_from_ic").is_null())
      .then(False)
      .otherwise(True)
      .alias("valid_ic_date")
    )
    .with_columns(
      pl.all_horizontal(pl.col("valid_ic_digits"), pl.col("valid_ic_date")).alias(
        "valid_ic"
      )
    )
  )


# validate ic
@store_data(RuleEnum.VALID_IC, ["valid_ic", "valid_ic_digits", "valid_ic_date"])
def _validate_ic(lf: pl.LazyFrame):
  """
  Rule: Validate `ICNUMBER`
  - should have 12 digits
  - first 6 digits should map into date correctly
  """
  return lf.filter(pl.col("valid_ic") == False)


# date validation
@valid_ic
@store_data(
  RuleEnum.IC_VS_DATEBIRTH,
  ["DATEBIRTH", "datebirth_from_ic"],
)
def _validate_ic_datebirth(lf: pl.LazyFrame):
  """
  Rule: `ICNUMBER` should map to `DATEBIRTH` correctly.
  """
  return lf.filter(pl.col("DATEBIRTH") != pl.col("datebirth_from_ic"))


@store_data(RuleEnum.DATESCREEN_VS_DATEREFER, ["DATESCREEN", "DATE REFERRED QUIT SER"])
def _validate_date_r4(lf: pl.LazyFrame):
  """
  Rule: `DATESCREEN` should be before `DATE REFERRED` (OS/OMOP) and `DATE REFERRED QUIT SER` (Quit smoking)
  """
  return lf.filter(
    (pl.col("DATE REFERRED") < pl.col("DATESCREEN"))
    | (pl.col("DATE REFERRED QUIT SER") < pl.col("DATESCREEN"))
  )


@store_data(
  RuleEnum.DATEREFER_VS_DATE_SEEN_SPECIALIST,
  ["DATE REFERRED", "DATE SEEN BY SPECIALIST"],
)
def _validate_date_r5(lf: pl.LazyFrame):
  """
  Rule: `DATE REFERRED` (OS/OMOP) should be before `DATE SEEN BY SPECIALIST`
  """
  return lf.filter((pl.col("DATE SEEN BY SPECIALIST") < pl.col("DATE REFERRED")))


@store_data(
  RuleEnum.DATEREFER_QUIT_VS_QUIT_APPT,
  [
    "valid_completeness",
    "valid_date_sequence",
    "DATE REFERRED QUIT SER",
    "TARIKH TEMUJANJI QUIT SERVICE",
  ],
)
def _validate_date_r6(lf: pl.LazyFrame):
  """
  Rules:
  - valid_completeness: `DATE REFERRED QUIT SER` if filled, `TARIKH TEMUJANJI QUIT SERVICE` should be filled.
  - valid_sequence: `DATE REFERRED QUIT SER` should be before `TARIKH TEMUJANJI QUIT SERVICE`.
  """
  return (
    lf.with_columns(
      pl.col("DATE REFERRED QUIT SER", "TARIKH TEMUJANJI QUIT SERVICE"),
      pl.when(pl.col("DATE REFERRED QUIT SER").is_not_null())
      .then(True)
      .otherwise(False)
      .alias("date_referred_filled"),
      pl.when(pl.col("TARIKH TEMUJANJI QUIT SERVICE").is_not_null())
      .then(True)
      .otherwise(False)
      .alias("appt_date_filled"),
      pl.when(
        pl.col("TARIKH TEMUJANJI QUIT SERVICE") >= pl.col("DATE REFERRED QUIT SER")
      )
      .then(True)
      .otherwise(False)
      .alias("valid_date_sequence"),
    )
    .filter(pl.any_horizontal(["date_referred_filled", "appt_date_filled"]))
    .with_columns(
      (pl.col("date_referred_filled") == pl.col("appt_date_filled")).alias(
        "valid_completeness"
      )
    )
    .filter(~pl.all_horizontal("valid_completeness", "valid_date_sequence"))
  )


@valid_ic
@store_data(RuleEnum.IC_VS_GENDER, ["ICNUMBER", "GENDER CODE"])
def _validate_r1(lf: pl.LazyFrame):
  """
  Rule: `ICNUMBER` should tally with subject's `GENDER CODE`
  """
  return lf.with_columns(
    (pl.col("GENDER CODE").cast(pl.Int16) % 2).alias("R1_GENDER_mod"),
    (pl.col("ICNUMBER").str.slice(-1).cast(pl.Int16) % 2).alias("R1_IC_mod"),
  ).filter(pl.col("R1_GENDER_mod") != pl.col("R1_IC_mod"))


@store_data(RuleEnum.LESION_VS_REFER_SPECIALIST, ["LESION", "REFERAL TO SPECIALIST"])
def _validate_r2(lf: pl.LazyFrame):
  """
  Rule: `LESION` if True, `REFERAL TO SPECIALIST` should be True, vice versa for `LESION` == False
  """
  return lf.filter(pl.col("LESION") != pl.col("REFERAL TO SPECIALIST"))


@store_data(RuleEnum.LESION_VS_TELEPHONE, ["LESION", "TELEPHONE NO"])
def _validate_lesion_telephone(lf: pl.LazyFrame):
  """
  Rule: `LESION` if True, `TELEPHONE NO` should be filled and matches regex pattern `^(6?0[1-9])\\d{7,9}$`
  """
  return lf.with_columns("LESION", "TELEPHONE NO").filter(
    (pl.col("LESION") == True)
    & ~(pl.col("TELEPHONE NO").str.contains(r"^(6?0[1-9])\d{7,9}$"))
  )


@store_data(
  RuleEnum.HABIT_VS_HABIT_COLS,
  [
    "valid_completeness",
    "valid_habits",
    "HABITS",
    "TOBACCO",
    "BBETEL QUID CHEWING",
    "ALCOHOL",
  ],
)
def _validate_habit_vs_habit_cols(lf: pl.LazyFrame):
  """
  Rule:
  * valid_completeness: `HABITS` if True, all three `TOBACCO`, `BBETEL QUID CHEWING`, `ALCOHOL` should be filled.
  * valid_habits: `HABITS` if True, either one of `TOBACCO`, `BBETEL QUID CHEWING`, `ALCOHOL` should be
  `1- habit currently practiced` or `2 - past habit now has stopped (minimum 6 months)`.
  """
  str_habit_false = "0 - No such habit"
  return (
    lf.with_columns(
      pl.when(
        (pl.col("HABITS") == True)
        & (
          pl.any_horizontal(
            pl.col("TOBACCO", "BBETEL QUID CHEWING", "ALCOHOL").is_null()
          )
        )
      )
      .then(False)
      .otherwise(True)
      .alias("valid_completeness")
    )
    .with_columns(
      pl.when(pl.col("TOBACCO").is_null() | pl.col("TOBACCO").eq(str_habit_false))
      .then(False)
      .otherwise(True)
      .alias("has_tobacco"),
      pl.when(
        pl.col("BBETEL QUID CHEWING").is_null()
        | pl.col("BBETEL QUID CHEWING").eq(str_habit_false)
      )
      .then(False)
      .otherwise(True)
      .alias("has_betel"),
      pl.when(pl.col("ALCOHOL").is_null() | pl.col("ALCOHOL").eq(str_habit_false))
      .then(False)
      .otherwise(True)
      .alias("has_alcohol"),
    )
    .with_columns(
      pl.when(
        pl.col("HABITS")
        == pl.any_horizontal(["has_tobacco", "has_betel", "has_alcohol"])
      )
      .then(True)
      .otherwise(False)
      .alias("valid_habits")
    )
    .filter(pl.col("valid_habits") == False)
  )


@store_data(RuleEnum.TOBACCO_TALLINESS, ["invalid_combination"])
def _validate_tobacco(lf: pl.LazyFrame):
  habit_cols = ["TOBACCO", "TOBACCO_ADVISED", "TOBACCO QUIT"]
  return (
    lf.with_columns(pl.col(habit_cols).fill_null("Null"))
    .with_columns(
      pl.concat_str(pl.col(habit_cols), separator="|").alias("invalid_combination")
    )
    .filter(~pl.col("invalid_combination").is_in(habit_whitelist))
  )


@store_data(RuleEnum.BETEL_TALLINESS, ["invalid_combination"])
def _validate_betel(lf: pl.LazyFrame):
  habit_cols = [
    "BBETEL QUID CHEWING",
    "BBETEL QUID CHEWING ADVISED",
    "BBETEL QUID CHEWING QUIT",
  ]
  return (
    lf.with_columns(pl.col(habit_cols).fill_null("Null"))
    .with_columns(
      pl.concat_str(pl.col(habit_cols), separator="|").alias("invalid_combination")
    )
    .filter(~pl.col("invalid_combination").is_in(habit_whitelist))
  )


@store_data(RuleEnum.ALCOHOL_TALLINESS, ["invalid_combination"])
def _validate_alcohol(lf: pl.LazyFrame):
  habit_cols = [
    "ALCOHOL",
    "ALCOHOL ADVISED",
    "ALCOHOL QUIT",
  ]
  return (
    lf.with_columns(pl.col(habit_cols).fill_null("Null"))
    .with_columns(
      pl.concat_str(pl.col(habit_cols), separator="|").alias("invalid_combination")
    )
    .filter(~pl.col("invalid_combination").is_in(habit_whitelist))
  )


@store_data(
  RuleEnum.REFERRAL_QUIT_VS_READY_QUIT, ["TOBACCO QUIT", "REFERRAL TO QUIT SERVICES"]
)
def _validate_referral_quit_vs_ready_quit(lf: pl.LazyFrame):
  return lf.with_columns(
    pl.when(pl.col("TOBACCO QUIT") == "Yes")
    .then(True)
    .otherwise(False)
    .alias("tobacco_quit")
  ).filter(
    (pl.col("REFERRAL TO QUIT SERVICES") == True) & (pl.col("tobacco_quit") == False)
  )


@store_data(
  RuleEnum.REFERRAL_QUIT_VS_DATE_REFERRED_VS_FIRST_APPT_DATE,
  ["REFERRAL TO QUIT SERVICES", "has_referred_date", "has_appt_date"],
)
def _validate_referral_quit_vs_date_referral_quit(lf: pl.LazyFrame):
  """
  Rule: If `REFERRAL TO QUIT SERVICES` is True, `DATE REFERRED QUIT SER` and `TARIKH TEMUJANJI QUIT SERVICE`
  should be filled, and vice versa.
  """
  return lf.with_columns(
    pl.when(pl.col("DATE REFERRED QUIT SER").is_null())
    .then(False)
    .otherwise(True)
    .alias("has_referred_date"),
    pl.when(pl.col("TARIKH TEMUJANJI QUIT SERVICE").is_null())
    .then(False)
    .otherwise(True)
    .alias("has_appt_date"),
  ).filter(
    (pl.col("REFERRAL TO QUIT SERVICES") != pl.col("has_referred_date"))
    | (pl.col("REFERRAL TO QUIT SERVICES") != pl.col("has_appt_date"))
  )


@store_data(
  RuleEnum.ATTEND_FIRST_APPT_NULL_CHECK,
  ["TARIKH TEMUJANJI QUIT SERVICE", "HADIR QUIT SERVICES"],
)
def _validate_attend_first_appt_null_check(lf: pl.LazyFrame):
  """
  Rule: If the first appointment date for quit service is earlier than date of data validation, `HADIR QUIT SERVICES` should be filled.
  """
  return lf.filter(
    (pl.col("TARIKH TEMUJANJI QUIT SERVICE") < today)
    & pl.col("HADIR QUIT SERVICES").is_null()
  )


@store_data(RuleEnum.ATTEND_FIRST_APPT_VS_INTERVENTION_STATUS, ["invalid_combination"])
def _validate_intervention_status(lf: pl.LazyFrame):
  return (
    lf.with_columns(
      pl.col("HADIR QUIT SERVICES", "STATUS INTERVENSI").fill_null("Null")
    )
    .with_columns(
      pl.concat_str(
        pl.col("HADIR QUIT SERVICES", "STATUS INTERVENSI"), separator=("|")
      ).alias("invalid_combination")
    )
    .filter(~pl.col("invalid_combination").is_in(intervention_status_whitelist))
  )


@store_data(RuleEnum.MEDIHIST_COMPLETENESS, ["MEDHIST", "MED HIST SPECIFY"])
def _validate_medihist(lf: pl.LazyFrame):
  """
  Rule: If `MEDHIST` is True, `MED HIST SPECIFY` should be filled, and vice versa.
  """
  return lf.with_columns(
    pl.when(pl.col("MED HIST SPECIFY").is_not_null())
    .then(True)
    .otherwise(False)
    .alias("medihist_specify_filled")
  ).filter(pl.col("MEDHIST") != pl.col("medihist_specify_filled"))


@store_data(
  RuleEnum.FAMIHISTCANCER_COMPLETENESS, ["FAMILY", "specify_filled", "relation_filled"]
)
def _validate_famihistcancer(lf: pl.LazyFrame):
  """
  Rule: If `FAMILY` is True, `FAMILYHSIT SPECIFY` and `RELATION` should be filled, and vice versa.
  """
  return (
    lf.with_columns(
      pl.when(pl.col("FAMILYHSIT SPECIFY").is_not_null())
      .then(True)
      .otherwise(False)
      .alias("specify_filled"),
      pl.when(pl.col("RELATION").is_not_null())
      .then(True)
      .otherwise(False)
      .alias("relation_filled"),
    )
    .with_columns(
      pl.concat_str("FAMILY", "specify_filled", "relation_filled", separator="|").alias(
        "invalid_combination"
      )
    )
    .filter(~pl.col("invalid_combination").is_in(famihistcancer_whitelist))
  )


class ValidationGeneral:
  """Validation object

  `run_all` invokes relevant store generation class using a ValidationStore context manager
  """

  list_all_func = [
    _validate_ic,
    _validate_ic_datebirth,
    _validate_inclusion_lesion_or_habit,
    _validate_lesion_telephone,
    _validate_r1,
    _validate_r2,
    _validate_date_r4,
    _validate_date_r5,
    _validate_date_r6,
    _validate_habit_vs_habit_cols,
    _validate_tobacco,
    _validate_betel,
    _validate_alcohol,
    _validate_referral_quit_vs_ready_quit,
    _validate_referral_quit_vs_date_referral_quit,
    _validate_attend_first_appt_null_check,
    _validate_intervention_status,
    _validate_medihist,
    _validate_famihistcancer,
  ]

  validation_df_store = "general"

  validation_df_schema = {
    "DISTRICT": pl.Utf8,
    "LOCATION OF SCREENING": pl.Utf8,
    "DATESCREEN": pl.Date,
    "ICNUMBER": pl.Utf8,
    "fail": pl.Struct(
      {"rule_number": pl.Int32, "rule": pl.Utf8, "data": pl.List(pl.Utf8)}
    ),
  }

  def __init__(self, lf: pl.LazyFrame, file_name: str) -> None:
    self.lf = lf.pipe(_pipe_validate_ic)
    self.file_name = file_name

  def run_all(self):
    with ValidationStore(
      self.validation_df_store, self.validation_df_schema, self.file_name
    ) as store_handler:
      for func in self.list_all_func:
        store_handler.extend_df(func(self.lf))

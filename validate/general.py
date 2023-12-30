import math
from datetime import date

import polars as pl

from constants import RuleEnum

from .store import store_data, ValidationStore
from .decorator import subset_full_ic

# constants
this_year = date.today().year
this_year_p1 = math.floor(this_year / 100)  # first two digits
this_year_p2 = this_year % 100  # last two digits


@subset_full_ic
@store_data(
  RuleEnum.IC_VS_DATEBIRTH,
  ["DATEBIRTH", "R3_date_from_ic"],
)
def _validate_date_r3(lf: pl.LazyFrame):
  """
  Rule: 3 `DATEBIRTH` vs `ICNUMBER`
  """
  # year_p1 is first two digits of birth year
  # year_p2 is second two digits of birth year
  return (
    lf.with_columns(  # slice first two digits as year_p2
      "DATEBIRTH",
      pl.col("ICNUMBER").str.slice(0, 2).cast(pl.Int16).alias("R3_year_p2"),
    )
    .with_columns(  # calculate first two digits of birth year from IC
      pl.when(pl.col("R3_year_p2") > this_year_p2)
      .then(this_year_p1 - 1)
      .otherwise(this_year_p1)
      .cast(pl.Utf8)
      .alias("R3_year_p1")
    )
    .with_columns(  # concat the first two digit with IC number to form full date string
      (pl.col("R3_year_p1") + pl.col("ICNUMBER").str.slice(0, 6)).alias("R3_datestr"),
    )
    .with_columns(  # slice and concat the string, then cast into date
      pl.concat_str(
        [
          pl.col("R3_datestr").str.slice(0, 4),
          pl.col("R3_datestr").str.slice(4, 2),
          pl.col("R3_datestr").str.slice(6, 2),
        ],
        separator="-",
      )
      .str.to_date()
      .alias("R3_date_from_ic")
    )
    .filter(pl.col("DATEBIRTH") != pl.col("R3_date_from_ic"))
  )


@store_data(RuleEnum.DATESCREEN_VS_DATEREFER, ["DATESCREEN", "DATE REFERRED QUIT SER"])
def _validate_date_r4(lf: pl.LazyFrame):
  """
  Rule: 4 `DATESCREEN` vs `DATE REFERRED` vs `DATE REFERRED QUIT SER`
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
  Rule: 5 `DATE REFERRED` vs `DATE SEEN BY SPECIALIST`
  """
  return lf.filter((pl.col("DATE SEEN BY SPECIALIST") < pl.col("DATE REFERRED")))


@store_data(
  RuleEnum.DATEREFER_QUIT_VS_QUIT_APPT,
  ["DATE REFERRED QUIT SER", "TARIKH TEMUJANJI QUIT SERVICE"],
)
def _validate_date_r6(lf: pl.LazyFrame):
  """
  Rule: 6 `DATE REFERRED QUIT SER` vs `TARIKH TEMUJANJI QUIT SERVICE`
  """
  return lf.filter(
    (pl.col("TARIKH TEMUJANJI QUIT SERVICE") < pl.col("DATE REFERRED QUIT SER"))
  )


@subset_full_ic
@store_data(RuleEnum.IC_VS_GENDER, ["ICNUMBER", "GENDER CODE"])
def _validate_r1(lf: pl.LazyFrame):
  """
  Rule: 1 `ICNUMBER` vs `GENDER`
  """
  return lf.with_columns(
    (pl.col("GENDER CODE").cast(pl.Int16) % 2).alias("R1_GENDER_mod"),
    (pl.col("ICNUMBER").str.slice(-1).cast(pl.Int16) % 2).alias("R1_IC_mod"),
  ).filter(pl.col("R1_GENDER_mod") != pl.col("R1_IC_mod"))


@store_data(RuleEnum.LESION_VS_REFER_SPECIALIST, ["LESION", "REFERAL TO SPECIALIST"])
def _validate_r2(lf: pl.LazyFrame):
  """
  Rule: 2 `LESION` vs `REFERAL TO SPECIALIST`
  """
  return lf.filter(pl.col("LESION") != pl.col("REFERAL TO SPECIALIST"))


class ValidationGeneral:
  """Validation object

  `run_all` invokes relevant store generation class using context manager
  """

  list_all_func = [
    _validate_r1,
    _validate_r2,
    _validate_date_r3,
    _validate_date_r4,
    _validate_date_r5,
    _validate_date_r6,
  ]

  validation_df_store = "general"

  validation_df_schema = {
    "DISTRICT": pl.Utf8,
    "LOCATION OF SCREENING": pl.Utf8,
    "DATESCREEN": pl.Date,
    "ICNUMBER": pl.Utf8,
    "fail": pl.Struct({"rule": pl.Utf8, "data": pl.List(pl.Utf8)}),
  }

  def __init__(self, lf: pl.LazyFrame, file_name: str) -> None:
    self.lf = lf
    self.file_name = file_name

  def run_all(self):
    with ValidationStore(
      self.validation_df_store, self.validation_df_schema, self.file_name
    ) as store_handler:
      for func in self.list_all_func:
        store_handler.extend_df(func(self.lf))

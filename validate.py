import math
from datetime import date
from pathlib import Path

import polars as pl

import utils
from includes import RuleEnum

PATH_INPUT = "input"
PATH_STORE = "store"

# define identifier columns
expr_id_cols = pl.col("DISTRICT", "LOCATION OF SCREENING", "DATESCREEN", "ICNUMBER")

# constants
this_year = date.today().year
this_year_p1 = math.floor(this_year / 100)  # first two digits
this_year_p2 = this_year % 100  # last two digits


def _get_validate_expr(rule_number: RuleEnum, col_list: list) -> pl.Expr:
  return [
    expr_id_cols,
    pl.struct(
      pl.lit(rule_number.name).alias("rule"),
      pl.concat_list([pl.lit(i) + pl.lit(": ") + pl.col(i) for i in col_list]).alias(
        "data"
      ),
    ).alias("fail"),
  ]


def _get_df_validate(df: pl.DataFrame | None):
  if df is None:
    return pl.DataFrame(
      schema={
        "DISTRICT": pl.Utf8,
        "LOCATION OF SCREENING": pl.Utf8,
        "DATESCREEN": pl.Date,
        "ICNUMBER": pl.Utf8,
        "fail": pl.Struct({"rule": pl.Utf8, "data": list}),
      },
    )
  else:
    df.clear()
    return df


def _extend_df_validate(df_validate: pl.DataFrame, new_data: pl.DataFrame):
  if new_data.is_empty():
    return df_validate
  elif df_validate.is_empty():
    # unable to vstack null dataframes
    # seems to be a bug
    # https://github.com/pola-rs/polars/issues/11824
    return pl.concat([df_validate, new_data], how="vertical_relaxed")
  else:
    return pl.concat([df_validate, new_data], how="vertical")
  # else:
  #   return pl.concat([df_validate, new_data], how="vertical_relaxed")


def _output_df_validate(df_validate: pl.DataFrame):
  df_validate.rechunk()
  with pl.Config(set_fmt_str_lengths=1000):
    print("output_df_validate:")
    print(df_validate.unnest("fail"))
  df_validate.clear()


def _validate_date_r3(
  df_validate: pl.DataFrame, df_full_ic: pl.DataFrame
) -> pl.DataFrame:
  """
  Rule: 3 `DATEBIRTH` vs `ICNUMBER`
  """
  df = (
    df_full_ic.select(
      expr_id_cols,
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
    .select(
      _get_validate_expr(RuleEnum.IC_VS_DATEBIRTH, ["DATEBIRTH", "R3_date_from_ic"]),
    )
  )

  return _extend_df_validate(df_validate, df)


def _validate_date_r4(df_validate: pl.DataFrame, df_all: pl.DataFrame):
  """
  Rule: 4 `DATESCREEN` vs `DATE REFERRED` vs `DATE REFERRED QUIT SER`
  """
  df = (
    df_all.select(expr_id_cols, "DATE REFERRED", "DATE REFERRED QUIT SER")
    .filter(
      (pl.col("DATE REFERRED") < pl.col("DATESCREEN"))
      | (pl.col("DATE REFERRED QUIT SER") < pl.col("DATESCREEN"))
    )
    .select(
      _get_validate_expr(
        RuleEnum.DATESCREEN_VS_DATEREFER, ["DATESCREEN", "DATE REFERRED QUIT SER"]
      )
    )
  )
  return _extend_df_validate(df_validate, df)


def _validate_date_r5(df_validate: pl.DataFrame, df_all: pl.DataFrame):
  """
  Rule: 5 `DATE REFERRED` vs `DATE SEEN BY SPECIALIST`
  """
  df = df_all.select(expr_id_cols, "DATE REFERRED", "DATE SEEN BY SPECIALIST").filter(
    (pl.col("DATE SEEN BY SPECIALIST") < pl.col("DATE REFERRED"))
  )
  return _extend_df_validate(df_validate, df)


def _validate_date_r6(df_validate: pl.DataFrame, df_all: pl.DataFrame):
  """
  Rule: 6 `DATE REFERRED QUIT SER` vs `TARIKH TEMUJANJI QUIT SERVICE`
  """
  df = (
    df_all.select(
      expr_id_cols, "DATE REFERRED QUIT SER", "TARIKH TEMUJANJI QUIT SERVICE"
    )
    .filter(
      (pl.col("TARIKH TEMUJANJI QUIT SERVICE") < pl.col("DATE REFERRED QUIT SER"))
    )
    .select(
      _get_validate_expr(
        RuleEnum.DATEREFER_QUIT_VS_QUIT_APPT,
        ["DATE REFERRED QUIT SER", "TARIKH TEMUJANJI QUIT SERVICE"],
      )
    )
  )
  return _extend_df_validate(df_validate, df)


def _validate_r1(df_validate: pl.DataFrame, df_full_ic: pl.DataFrame):
  """
  Rule: 1 `ICNUMBER` vs `GENDER`
  """
  df = (
    df_full_ic.select(expr_id_cols, "GENDER CODE")
    .with_columns(
      (pl.col("GENDER CODE").cast(pl.Int16) % 2).alias("R1_GENDER_mod"),
      (pl.col("ICNUMBER").str.slice(-1).cast(pl.Int16) % 2).alias("R1_IC_mod"),
    )
    .filter(pl.col("R1_GENDER_mod") != pl.col("R1_IC_mod"))
    .select(
      _get_validate_expr(RuleEnum.IC_VS_GENDER, ["ICNUMBER", "GENDER CODE"]),
    )
  )

  return _extend_df_validate(df_validate, df)


def _validate_r2(df_validate: pl.DataFrame, df_all: pl.DataFrame):
  """
  Rule: 2 `LESION` vs `REFERAL TO SPECIALIST`
  """
  df = (
    df_all.select(expr_id_cols, "LESION", "REFERAL TO SPECIALIST")
    .filter(pl.col("LESION") != pl.col("REFERAL TO SPECIALIST"))
    .select(
      _get_validate_expr(
        RuleEnum.LESION_VS_REFER_SPECIALIST, ["LESION", "REFERAL TO SPECIALIST"]
      )
    )
  )
  return _extend_df_validate(df_validate, df)


def _validate_dates(
  df_validate: pl.DataFrame, df_all: pl.DataFrame, df_full_ic: pl.DataFrame
):
  return (
    df_validate.pipe(_validate_date_r3, df_full_ic)
    .pipe(_validate_date_r4, df_all)
    .pipe(_validate_date_r5, df_all)
    .pipe(_validate_date_r6, df_all)
  )


def _validate_others(
  df_validate: pl.DataFrame, df_all: pl.DataFrame, df_full_ic: pl.DataFrame
):
  return df_validate.pipe(_validate_r1, df_full_ic).pipe(_validate_r2, df_all)


def main():
  df_validate = None
  for path in Path(PATH_INPUT).glob("*.accdb"):
    print(f"Validating '{path.stem}'")
    df_all = utils.get_df(path)
    df_full_ic = df_all.filter(pl.col("ICNUMBER").str.contains(r"^\d{12}$"))

    (
      _get_df_validate(df_validate)
      .pipe(_validate_dates, df_all, df_full_ic)
      .pipe(_validate_others, df_all, df_full_ic)
      .pipe(_output_df_validate)
    )


if __name__ == "__main__":
  main()

import os
from functools import wraps
from pathlib import Path

import polars as pl

from constants import RuleEnum, list_id_cols

PATH_STORE = os.getenv("PATH_STORE")


def store_data(rule_enum: RuleEnum, cols_as_data: list[str]):
  """Decorator for validation functions to wrap store operations.

  Parameters
  ----------
  rule_enum
      RuleEnum that will be included into the store dataframe.
  cols_as_data
      List of column names that will be included into the
      store dataframe's `data` column.
  """

  def decorator(func):
    @wraps(func)
    def wrapper(lf: pl.LazyFrame):
      result: pl.LazyFrame = func(lf)

      # Select columns (id_cols) and add fail <struct {data, fail}> for store
      return result.select(
        pl.col(list_id_cols),
        pl.struct(
          pl.lit(rule_enum.name).alias("rule"),
          pl.concat_list(
            [pl.lit(i) + pl.lit(": ") + pl.col(i) for i in cols_as_data]
          ).alias("data"),
        ).alias("fail"),
      )

    return wrapper

  return decorator


class GeneralValidationStore:
  """Context manager for general validation parquet store.

  This should be activated ??? [WIP]

  After exiting the context, `self.df` will be flushed as parquet into the store.
  """

  df_schema = {
    "DISTRICT": pl.Utf8,
    "LOCATION OF SCREENING": pl.Utf8,
    "DATESCREEN": pl.Date,
    "ICNUMBER": pl.Utf8,
    "fail": pl.Struct({"rule": pl.Utf8, "data": pl.List(pl.Utf8)}),
  }

  def __init__(self, file_name: str):
    self.file_name = file_name
    self.df = pl.DataFrame(schema=self.df_schema)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, exc_traceback):
    if self.df.is_empty():
      return

    self.df.rechunk().select(
      pl.lit(self.file_name).alias("file"), pl.all()
    ).write_parquet(
      Path(PATH_STORE).joinpath("general/" + self.file_name + ".parquet"),
      compression="lz4",
    )

  def extend_df(self, new_output: pl.DataFrame):
    if not new_output.is_empty():
      self.df = pl.concat([self.df, new_output], how="vertical")

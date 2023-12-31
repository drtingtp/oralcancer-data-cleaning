import os
from functools import wraps
from pathlib import Path

import polars as pl

from constants import RuleEnum

PATH_STORE = os.getenv("PATH_STORE")


def store_data(rule_enum: RuleEnum, cols_as_data: list[str] = []):
  """Decorator for validation functions to wrap store data.

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
      result = func(lf)

      assert (
        type(result) == pl.LazyFrame
      ), f"{func.__name__}() did not return a LazyFrame"

      return result.with_columns(
        pl.struct(
          pl.lit(rule_enum.value).alias("rule_number"),
          pl.lit(rule_enum.name).alias("rule"),
          pl.concat_list(
            [
              pl.concat_str(pl.lit(i + ": "), pl.col(i).cast(pl.Utf8))
              for i in cols_as_data
            ]
          ).alias("data"),
        ).alias("fail"),
      )

    return wrapper

  return decorator


class ValidationStore:
  """Context manager for validation parquet store.

  This should be activated by a Validation class in `runall()`.

  Upon entering, a dataframe (self.df) is created to log validation results.

  Upon exiting, `self.df` will be flushed as parquet into the store.
  """

  def __init__(self, store: str, validation_df_schema: dict, file_name: str):
    self.store = store
    self.file_name = file_name
    self.df = pl.DataFrame(schema=validation_df_schema)
    self.cols = [col for col in validation_df_schema.keys()]

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, exc_traceback):
    if self.df.is_empty():
      return

    self.df.rechunk().select(  # wrap file name as first column
      pl.lit(self.file_name).alias("file"),
      pl.all(),
    ).write_parquet(  # flush self.df into parquet file
      Path(PATH_STORE).joinpath(f"{self.store}/{self.file_name}.parquet"),
      compression="lz4",
    )

  def extend_df(self, lf: pl.LazyFrame):
    try:
      # wrap the lf with select columns required by validation_df_schema
      # collect
      new_output = lf.select(self.cols).collect()
      # concat
      self.df = pl.concat([self.df, new_output], how="vertical")

      if new_output.is_empty():
        return
    except Exception as e:
      msg = (
        f"Unhandled exception in validate.store.extend_df(), filename: {self.file_name}"
      )
      raise Exception(msg) from e

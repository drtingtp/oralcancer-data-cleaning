import polars as pl

from constants import RuleEnum, list_id_cols

from .lesion_colmap import col_map, chunks
from .store import ValidationStore, store_data

REPLACE_NA = False


def convert_into_lesion_lf(lf: pl.LazyFrame):
  """
  Used in ValidationLesion handler on init.

  Converts `lf` from general into `lesion_lf` - long-form table where unit of analysis is lesion.
  """

  # principle:
  # 1. pack 6 lesion descriptors into a struct
  # 2. pack 4 lesions into a list
  # final outcome is to get an explodable list (that will elongate the list), with unnestable columns (describing each lesion)
  #
  # 2023-12-30: simplified struct nesting (compared to workbench version) because struct cannot be filtered / joined / compared directly (as of now)
  def _get_lesion_expr():
    def _get_member_struct_expr(i: int):
      return pl.struct(
        lesion_id=pl.lit(col_map[i].id),
        type=col_map[i].type,
        size=col_map[i].size,
        site_A=col_map[i].site[0],
        site_B=col_map[i].site[1],
        site_C=col_map[i].site[2],
        site_D=col_map[i].site[3],
      ).alias(col_map[i].id)

    return [
      pl.col("LESION"),
      pl.concat_list([_get_member_struct_expr(i) for i in range(chunks)]).alias(
        "lesion_list"
      ),
    ]

  return (
    lf.select(pl.col(list_id_cols), *_get_lesion_expr())
    # explode and unnest
    .explode("lesion_list")
    .unnest("lesion_list")
  )


def convert_NA_into_nulls(lf: pl.LazyFrame):
  """Conditionally replace N/A data points with nulls.

  Used as a pl.DataFrame.pipe() parameter.

  Notes:

  `0 - not applicable` in `type` and `size`

  `00 = not applicable` in sites
  """
  if REPLACE_NA is False:
    return lf
  else:
    return lf.with_columns(
      pl.col(["type", "size"]).replace("0 - not applicable", None),
      pl.col(["site_A", "site_B", "site_C", "site_D"]).replace(
        "00 = not applicable", None
      ),
    )


def compute_lesion_filled(lf: pl.LazyFrame):
  """
  Computer the `lesion_filled` column based on the 6 lesion descriptors.

  Value is `1` if any is filled, `0` if all 6 columns are `null`.

  Used as a pl.DataFrame.pipe() parameter.
  """
  return lf.with_columns(
    pl.when(
      pl.any_horizontal(
        pl.col(["type", "size", "site_A", "site_B", "site_C", "site_D"]).is_not_null()
      )
    )
    .then(1)
    .otherwise(0)
    .alias("lesion_filled")
  )


@store_data(
  RuleEnum.LESION_VS_LESION_COLS,
  [
    "LESION",
    "lesion_count",
    "type",
    "size",
    "site_A",
    "site_B",
    "site_C",
    "site_D",
  ],
)
def _validate_r7(lf: pl.LazyFrame):
  """
  Rule: If `LESION` is False, `lesion_count` should be `0`; If `LESION` is True, `lesion_count` should be more than `0`.
  """
  return lf.with_columns(
    pl.col("lesion_filled").sum().over(list_id_cols).alias("lesion_count")
  ).filter(
    ((pl.col("LESION") == True) & (pl.col("lesion_count") == 0))
    | ((pl.col("LESION") == False) & (pl.col("lesion_count") > 0))
  )


@store_data(
  RuleEnum.LESION_COLS_COMPLETENESS,
  ["is_complete", "type_filled", "size_filled", "site_filled"],
)
def _validate_r8(lf: pl.LazyFrame):
  """
  Rule: Completeness check for lesion type, size and site - all must be filled if any one is filled.
  """
  # Because NA has no special meaning, it should be treated as null
  # Force the data to go through convert_NA_into_nulls pipe if the REPLACE_NA setting is False
  if REPLACE_NA is False:
    lf = lf.pipe(convert_NA_into_nulls).pipe(compute_lesion_filled)
  return (
    lf.with_columns(
      pl.when(pl.col("type").is_not_null())
      .then(True)
      .otherwise(False)
      .alias("type_filled"),
      pl.when(pl.col("size").is_not_null())
      .then(True)
      .otherwise(False)
      .alias("size_filled"),
      pl.when(
        pl.any_horizontal(pl.col("site_A", "site_B", "site_C", "site_D").is_not_null())
      )
      .then(True)
      .otherwise(False)
      .alias("site_filled"),
    )
    .with_columns(
      pl.all_horizontal("type_filled", "size_filled", "site_filled").alias(
        "is_complete"
      )
    )
    .filter((pl.col("lesion_filled") > 0) & (pl.col("is_complete") == False))
  )


class ValidationLesion:
  """Validation object

  `run_all` invokes relevant store generation class using context manager
  """

  list_all_func = [_validate_r7, _validate_r8]

  validation_df_store = "lesion"

  validation_df_schema = {
    "DISTRICT": pl.Utf8,
    "LOCATION OF SCREENING": pl.Utf8,
    "DATESCREEN": pl.Date,
    "ICNUMBER": pl.Utf8,
    "lesion_id": pl.Utf8,
    "fail": pl.Struct(
      {"rule_number": pl.Int32, "rule": pl.Utf8, "data": pl.List(pl.Utf8)}
    ),
  }

  def __init__(self, lf_general: pl.LazyFrame, file_name: str) -> None:
    # convert lf into long form
    self.lf = (
      lf_general.pipe(convert_into_lesion_lf)
      .pipe(convert_NA_into_nulls)
      .pipe(compute_lesion_filled)
    )
    self.file_name = file_name

  def run_all(self):
    with ValidationStore(
      self.validation_df_store, self.validation_df_schema, self.file_name
    ) as store_handler:
      for func in self.list_all_func:
        store_handler.extend_df(func(self.lf))

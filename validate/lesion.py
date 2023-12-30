import polars as pl

from collections import namedtuple
from constants import RuleEnum, list_id_cols

from .store import store_data, ValidationStore

REPLACE_NA = False

list_lesion_cols = [
  "TYPE1",
  "SIZE1",
  "SITE1A",
  "SITE1B",
  "SITE1C",
  "SITE1D",
  "TYPE2",
  "SIZE2",
  "SITE2A",
  "SITE2B",
  "SITE2C",
  "SITE2D",
  "TYPE3",
  "SIZE3",
  "SITE3A",
  "SITE3B",
  "SITE3C",
  "SITE3D",
  "OTHER PATHOLOGY",
  "SIZE OTHER",
  "SITE OTHER A",
  "SITE OTHER B",
  "SITE OTHER C",
  "SITE OTHER D",
]

# define namedtuple to generate col_map collection
ColMap = namedtuple("ColMap", ["id", "type", "size", "site"])


def convert_into_lesion_lf(lf: pl.LazyFrame):
  # calculate chunks
  # used in generating col_map
  chunk_size = 6  # 6 descriptors for each lesion
  col_list_len = len(list_lesion_cols)

  assert (
    col_list_len % chunk_size
  ) == 0, f"len(col_list): {col_list_len} not divisible by chunk_size: {chunk_size}"

  chunks = int(col_list_len / chunk_size)

  # save column names as col_map
  col_map: list[ColMap] = []

  for n in range(chunks):
    i = n * chunk_size  # i -- starting index
    id = None
    if n <= 2:
      id = n + 1
    else:
      id = "other"

    col_map.append(
      ColMap(
        str(id),  # id: 1, 2, other
        list_lesion_cols[i],  # type
        list_lesion_cols[i + 1],  # size
        [  # site: extract 3rd to 6th element in a chunk: i+2 (inclusive) to i+6 (exclusive)
          list_lesion_cols[j]
          for j in range(
            i + 2,
            i + 6,
          )
        ],
      )
    )

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
  Rule: 7 LESION vs all lesion-related columns
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
  Rule: 8 Lesion-related columns completeless
  """
  # Forcing the data to go through pipes if the REPLACE_NA setting is False
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
    "fail": pl.Struct({"rule": pl.Utf8, "data": pl.List(pl.Utf8)}),
  }

  def __init__(self, lf_general: pl.LazyFrame, file_name: str) -> None:
    # convert lf into long form``
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

import polars as pl
import pointblank as pb
import odyssey.core as od

from typing import Callable

# Set up config
from config import HOME, RAW_DATA, PROCESSED_DATA

type Metadata = dict[str, str|int|dict[int|float, str]]
type MetadataDict = dict[str, Metadata]

cols = [
    "ID",
    "G220_IPAQ_MOD_D", "G220_IPAQ_MOD_HPD", "G220_IPAQ_MOD_MPD", "G220_IPAQ_MOD_W", 
    "G220_IPAQ_VIG_D", "G220_IPAQ_VIG_HPD", "G220_IPAQ_VIG_MPD", "G220_IPAQ_VIG_W", 
    "G220_IPAQ_WALK_D", "G220_IPAQ_WALK_HPD", "G220_IPAQ_WALK_MPD", "G220_IPAQ_WALK_W", 
    "G220_IPAQ_SIT_WD_HPD", "G220_IPAQ_SIT_WD_MPD", 
    "G220_SIT_WD_TRUNC", "G220_IPAQ_SIT_COM",
    "G220_VIG_MET", "G220_VIG_MINS", 
    "G220_MOD_MET", "G220_MOD_MINS", 
    "G220_WALK_MET", "G220_WALK_MINS",
    "G220_IPAQ_CAT", "G220_TOT_MET", 
]

def check_total_mins(
    hpd_column: str,
    mpd_column: str
    ) -> Callable:
    """
    Returns a preprocessing function to verify the minutes for a given category have been correctly calculated.
    Cap the total at 180 minutes, and preserve null values.
    """
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            (pl.col(hpd_column).fill_null(0) * 60 + pl.col(mpd_column).fill_null(0))
            .pipe(lambda expr: pl.when(expr > 180).then(180).otherwise(expr))
            .alias("check")
        )
    return preprocessor

def check_met(
    mins_column: str, 
    n_days_column: str, 
    met_column: str,
    factor: int|float # the corresponding factor for the activity (Vig: 8, Mod: 4, Walk: 3.3)
    ) -> Callable:
    """Returns a preprocessing function to verify the calculated MET value for a given category."""
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            (pl.col(mins_column).fill_null(0) * pl.col(n_days_column).fill_null(0) * factor).alias("check"),
            pl.col(met_column).fill_null(0)
        )
    return preprocessor

def check_tot_met(
    met_columns: list[str],
    tot_met_column: str
    ) -> Callable:
    """Returns a preprocessing function to verify the calculated total MET value."""
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        expr = sum(pl.col(col).fill_null(0) for col in met_columns)
        
        return df.with_columns(
            expr.alias("check"),
            pl.col(tot_met_column).fill_null(0) # Fill nulls with 0; otherwise the validation skips if one value in a comparison is null
        )
    return preprocessor

def main():
    print(HOME)
    prefix = "G220"
    g220 = od.Dataset("G220_Q.sav", RAW_DATA)
    lf, _meta = g220.load_data()
    df = lf.select(cols).collect()

    validation = (
        pb.Validate(
            data=df,
        )
        .col_vals_eq(
            columns=f"{prefix}_VIG_MINS",
            value=pb.col("check"),
            pre=check_total_mins(f"{prefix}_IPAQ_VIG_HPD", f"{prefix}_IPAQ_VIG_MPD"),
            brief="Check total mins/day equals `HPD*60 + MPD`",
            na_pass=True,
        )
        .col_vals_eq(
            columns=f"{prefix}_MOD_MINS",
            value=pb.col("check"),
            pre=check_total_mins(f"{prefix}_IPAQ_MOD_HPD", f"{prefix}_IPAQ_MOD_MPD"),
            brief="Check total mins/day equals `HPD*60 + MPD`",
            na_pass=True,
        )
        .col_vals_eq(
            columns=f"{prefix}_WALK_MINS",
            value=pb.col("check"),
            pre=check_total_mins(f"{prefix}_IPAQ_WALK_HPD", f"{prefix}_IPAQ_WALK_MPD"),
            brief="Check total mins/day equals `HPD*60 + MPD`",
            na_pass=True,
        )
        .col_vals_eq(
            columns=f"{prefix}_VIG_MET",
            value=pb.col("check"),
            pre=check_met(f"{prefix}_VIG_MINS", f"{prefix}_IPAQ_VIG_D", f"{prefix}_VIG_MET", factor=8),
        )
        .col_vals_eq(
            columns=f"{prefix}_MOD_MET",
            value=pb.col("check"),
            pre=check_met(f"{prefix}_MOD_MINS", f"{prefix}_IPAQ_MOD_D", f"{prefix}_MOD_MET", factor=4),
        )
        .col_vals_eq(
            columns=f"{prefix}_WALK_MET",
            value=pb.col("check"),
            pre=check_met(f"{prefix}_WALK_MINS", f"{prefix}_IPAQ_WALK_D", f"{prefix}_WALK_MET", factor=3.3),
        )
        .col_vals_eq(
            columns=f"{prefix}_TOT_MET",
            value=pb.col("check"),
            pre=check_tot_met([f"{prefix}_VIG_MET", f"{prefix}_MOD_MET", f"{prefix}_WALK_MET"], f"{prefix}_TOT_MET"),
            brief="Check `TOT_MET` equals the sum of `VIG_MET`, `MOD_MET`, and `WALK_MET`"
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_VIG_D",
            left=1,
            right=7,
            segments=(f"{prefix}_IPAQ_VIG_W", 1),
            )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_VIG_HPD", 
            left=0,
            right=18, # unrealistic to do more than 18 hours of exercise per day (even that is a stretch!!)
            segments=(f"{prefix}_IPAQ_VIG_W", 1),
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_VIG_MPD", 
            left=0,
            right=59,
            segments=(f"{prefix}_IPAQ_VIG_W", 1),
        )
        .col_vals_between(
            columns=f"{prefix}_VIG_MINS", 
            left=0,
            right=180, # total mins per category is capped at 180 mins
            segments=(f"{prefix}_IPAQ_VIG_W", 1),
        )
        .col_vals_eq(
            columns=[f"{prefix}_IPAQ_VIG_D", f"{prefix}_IPAQ_VIG_HPD", f"{prefix}_IPAQ_VIG_MPD", f"{prefix}_VIG_MINS", f"{prefix}_VIG_MET"], 
            value=0,
            segments=(f"{prefix}_IPAQ_VIG_W", 0)
        )
        .col_vals_null(
            columns=[f"{prefix}_IPAQ_VIG_D", f"{prefix}_IPAQ_VIG_HPD", f"{prefix}_IPAQ_VIG_MPD", f"{prefix}_VIG_MINS", f"{prefix}_VIG_MET"],
            pre=lambda df: df.with_columns(pl.col(f"{prefix}_IPAQ_VIG_W").fill_null(-1)), # Pointblank doesn't seem to like segmenting values with null, so transform null to -1 and segment y that
            segments=(f"{prefix}_IPAQ_VIG_W", -1)
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_MOD_D",
            left=1,
            right=7,
            segments=(f"{prefix}_IPAQ_MOD_W", 1),
            )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_MOD_HPD", 
            left=0,
            right=18, # unrealistic to do more than 18 hours of exercise per day (even that is a stretch!!)
            segments=(f"{prefix}_IPAQ_MOD_W", 1),
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_MOD_MPD", 
            left=0,
            right=59,
            segments=(f"{prefix}_IPAQ_MOD_W", 1),
        )
        .col_vals_between(
            columns=f"{prefix}_MOD_MINS", 
            left=0,
            right=180, # total mins per category is capped at 180 mins
            segments=(f"{prefix}_IPAQ_MOD_W", 1),
        )
        .col_vals_eq(
            columns=[f"{prefix}_IPAQ_MOD_D", f"{prefix}_IPAQ_MOD_HPD", f"{prefix}_IPAQ_MOD_MPD", f"{prefix}_MOD_MINS", f"{prefix}_MOD_MET"], 
            value=0,
            segments=(f"{prefix}_IPAQ_MOD_W", 0)
        )
        .col_vals_null(
            columns=[f"{prefix}_IPAQ_MOD_D", f"{prefix}_IPAQ_MOD_HPD", f"{prefix}_IPAQ_MOD_MPD", f"{prefix}_MOD_MINS", f"{prefix}_MOD_MET"],
            pre=lambda df: df.with_columns(pl.col(f"{prefix}_IPAQ_MOD_W").fill_null(-1)), # Pointblank doesn't seem to like segmenting values with null, so transform null to -1 and segment y that
            segments=(f"{prefix}_IPAQ_MOD_W", -1)
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_WALK_D",
            left=1,
            right=7,
            segments=(f"{prefix}_IPAQ_WALK_W", 1),
            )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_WALK_HPD", 
            left=0,
            right=18, # unrealistic to do more than 18 hours of exercise per day (even that is a stretch!!)
            segments=(f"{prefix}_IPAQ_WALK_W", 1),
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_WALK_MPD", 
            left=0,
            right=59,
            segments=(f"{prefix}_IPAQ_WALK_W", 1),
        )
        .col_vals_between(
            columns=f"{prefix}_WALK_MINS", 
            left=0,
            right=180, # total mins per category is capped at 180 mins
            segments=(f"{prefix}_IPAQ_WALK_W", 1),
        )
        .col_vals_eq(
            columns=[f"{prefix}_IPAQ_WALK_D", f"{prefix}_IPAQ_WALK_HPD", f"{prefix}_IPAQ_WALK_MPD", f"{prefix}_WALK_MINS", f"{prefix}_WALK_MET"], 
            value=0,
            segments=(f"{prefix}_IPAQ_WALK_W", 0)
        )
        .col_vals_null(
            columns=[f"{prefix}_IPAQ_WALK_D", f"{prefix}_IPAQ_WALK_HPD", f"{prefix}_IPAQ_WALK_MPD", f"{prefix}_WALK_MINS", f"{prefix}_WALK_MET"],
            pre=lambda df: df.with_columns(pl.col(f"{prefix}_IPAQ_WALK_W").fill_null(-1)), # Pointblank doesn't seem to like segmenting values with null, so transform null to -1 and segment y that
            segments=(f"{prefix}_IPAQ_WALK_W", -1)
        )
    )

    print(validation.interrogate())

if __name__ == "__main__":
    main()
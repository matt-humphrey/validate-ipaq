from typing import Callable
import polars as pl
import pointblank as pb

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

def check_ipaq_cat(
    vig_d: str, # days of vigorous exercise per week
    mod_d: str,
    walk_d: str,
    vig_m: str, # mins of vigorous exercise per day
    mod_m: str,
    walk_m: str,
    tot_met: str, # total MET minutes per week
    cat: str # IPAQ category (low: 0, moderate: 1, high: 2)
    ) -> Callable:
    """
    Returns a preprocessing function to verify the IPAQ category.

    HIGH: 2
    Vigorous exercise on 3+ days for 20+ mins AND >= 1500 MET mins per week
    OR combination of any exercise on 7+ days AND >= 3000 MET mins per week

    MODERATE: 1
    Vig exercise 3+ days for 20+ mins
    OR mod exercise AND/OR walking 5+ days for 30 mins
    OR any exercise on 5+ days AND >= 600 MET mins per week

    LOW: 0
    None of the above criteria

    Assuming that by 'combination of any exercise on x+ days', that means
    two types of exercise on the same day technically counts as 2 days.
    Otherwise it's impossible to know, based on the data, across which days the participant exercised.
    (For instance, 3 x VIG, 3 x MOD, 3 x WALK could be across as few as 3, or as many as 7 days).
    """
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            (pl.when(
                (pl.col(vig_d).ge(3) & pl.col(vig_m).ge(20) & pl.col(tot_met).ge(1500)) | 
                (sum(pl.col(col).fill_null(0) for col in [vig_d, mod_d, walk_d]).ge(7) & pl.col(tot_met).ge(3000))
            ).then(2)
            .when(
                (pl.col(vig_d).ge(3) & pl.col(vig_m).ge(20)) |
                (sum(pl.col(col).fill_null(0) for col in [mod_d, walk_d]).ge(5) &
                 sum(pl.col(col).fill_null(0) for col in [mod_m, walk_m]).ge(30)) |
                (sum(pl.col(col).fill_null(0) for col in [vig_d, mod_d, walk_d]).ge(5) & pl.col(tot_met).ge(600))
            ).then(1)
            .when(pl.col(tot_met).is_null()).then(None) # Don't impute 0 if data is missing
            .otherwise(0)
            ).alias("check"),
            pl.col(cat).fill_null(0) # Fill nulls with 0; otherwise the validation skips if one value in a comparison is null
        )
    return preprocessor

# TODO: simplify/DRY - use partial functions or other method to reduce the duplication
# TODO: add flag like `check_null: bool = False` to optionally include the `col_vals_null` methods
def validate_ipaq(
    prefix: str, # prefix for the dataset
    df: pl.DataFrame
    ) -> pb.Validate:

    validation = (
        pb.Validate(
            data=df,
        )
        .col_vals_eq(
            columns=f"{prefix}_IPAQ_VIG_MINS",
            value=pb.col("check"),
            pre=check_total_mins(f"{prefix}_IPAQ_VIG_HPD", f"{prefix}_IPAQ_VIG_MPD"),
            brief="Check total mins/day equals `HPD*60 + MPD`",
            na_pass=True,
        )
        .col_vals_eq(
            columns=f"{prefix}_IPAQ_MOD_MINS",
            value=pb.col("check"),
            pre=check_total_mins(f"{prefix}_IPAQ_MOD_HPD", f"{prefix}_IPAQ_MOD_MPD"),
            brief="Check total mins/day equals `HPD*60 + MPD`",
            na_pass=True,
        )
        .col_vals_eq(
            columns=f"{prefix}_IPAQ_WALK_MINS",
            value=pb.col("check"),
            pre=check_total_mins(f"{prefix}_IPAQ_WALK_HPD", f"{prefix}_IPAQ_WALK_MPD"),
            brief="Check total mins/day equals `HPD*60 + MPD`",
            na_pass=True,
        )
        .col_vals_eq(
            columns=f"{prefix}_IPAQ_VIG_MET",
            value=pb.col("check"),
            pre=check_met(f"{prefix}_IPAQ_VIG_MINS", f"{prefix}_IPAQ_VIG_D", f"{prefix}_IPAQ_VIG_MET", factor=8),
        )
        .col_vals_eq(
            columns=f"{prefix}_IPAQ_MOD_MET",
            value=pb.col("check"),
            pre=check_met(f"{prefix}_IPAQ_MOD_MINS", f"{prefix}_IPAQ_MOD_D", f"{prefix}_IPAQ_MOD_MET", factor=4),
        )
        .col_vals_eq(
            columns=f"{prefix}_IPAQ_WALK_MET",
            value=pb.col("check"),
            pre=check_met(f"{prefix}_IPAQ_WALK_MINS", f"{prefix}_IPAQ_WALK_D", f"{prefix}_IPAQ_WALK_MET", factor=3.3),
        )
        .col_vals_eq(
            columns=f"{prefix}_IPAQ_TOT_MET",
            value=pb.col("check"),
            pre=check_tot_met([f"{prefix}_IPAQ_VIG_MET", f"{prefix}_IPAQ_MOD_MET", f"{prefix}_IPAQ_WALK_MET"], f"{prefix}_IPAQ_TOT_MET"),
            brief="Check `TOT_MET` equals the sum of `VIG_MET`, `MOD_MET`, and `WALK_MET`"
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_VIG_D",
            left=1,
            right=7,
            segments=(f"{prefix}_IPAQ_VIG_W", 1),
            na_pass=True
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_VIG_HPD", 
            left=0,
            right=18, # unrealistic to do more than 18 hours of exercise per day (even that is a stretch!!)
            segments=(f"{prefix}_IPAQ_VIG_W", 1),
            na_pass=True
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_VIG_MPD", 
            left=0,
            right=59,
            segments=(f"{prefix}_IPAQ_VIG_W", 1),
            na_pass=True
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_VIG_MINS", 
            left=0,
            right=180, # total mins per category is capped at 180 mins
            segments=(f"{prefix}_IPAQ_VIG_W", 1),
            na_pass=True
        )
        .col_vals_null(
            columns=[f"{prefix}_IPAQ_VIG_D", f"{prefix}_IPAQ_VIG_HPD", f"{prefix}_IPAQ_VIG_MPD"], 
            segments=(f"{prefix}_IPAQ_VIG_W", 0)
        )
        .col_vals_eq(
            columns=[f"{prefix}_IPAQ_VIG_MINS", f"{prefix}_IPAQ_VIG_MET"], 
            value=0,
            segments=(f"{prefix}_IPAQ_VIG_W", 0)
        )
        .col_vals_null(
            columns=[f"{prefix}_IPAQ_VIG_D", f"{prefix}_IPAQ_VIG_HPD", f"{prefix}_IPAQ_VIG_MPD", f"{prefix}_IPAQ_VIG_MINS", f"{prefix}_IPAQ_VIG_MET"],
            pre=lambda df: df.with_columns(pl.col(f"{prefix}_IPAQ_VIG_W").fill_null(-1)), # Pointblank doesn't seem to like segmenting values with null, so transform null to -1 and segment y that
            segments=(f"{prefix}_IPAQ_VIG_W", -1)
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_MOD_D",
            left=1,
            right=7,
            segments=(f"{prefix}_IPAQ_MOD_W", 1),
            na_pass=True
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_MOD_HPD", 
            left=0,
            right=18, # unrealistic to do more than 18 hours of exercise per day (even that is a stretch!!)
            segments=(f"{prefix}_IPAQ_MOD_W", 1),
            na_pass=True
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_MOD_MPD", 
            left=0,
            right=59,
            segments=(f"{prefix}_IPAQ_MOD_W", 1),
            na_pass=True
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_MOD_MINS", 
            left=0,
            right=180, # total mins per category is capped at 180 mins
            segments=(f"{prefix}_IPAQ_MOD_W", 1),
            na_pass=True
        )
        .col_vals_null(
            columns=[f"{prefix}_IPAQ_MOD_D", f"{prefix}_IPAQ_MOD_HPD", f"{prefix}_IPAQ_MOD_MPD"], 
            segments=(f"{prefix}_IPAQ_MOD_W", 0)
        )
        .col_vals_eq(
            columns=[f"{prefix}_IPAQ_MOD_MINS", f"{prefix}_IPAQ_MOD_MET"], 
            value=0,
            segments=(f"{prefix}_IPAQ_MOD_W", 0)
        )
        .col_vals_null(
            columns=[f"{prefix}_IPAQ_MOD_D", f"{prefix}_IPAQ_MOD_HPD", f"{prefix}_IPAQ_MOD_MPD", f"{prefix}_IPAQ_MOD_MINS", f"{prefix}_IPAQ_MOD_MET"],
            pre=lambda df: df.with_columns(pl.col(f"{prefix}_IPAQ_MOD_W").fill_null(-1)), # Pointblank doesn't seem to like segmenting values with null, so transform null to -1 and segment y that
            segments=(f"{prefix}_IPAQ_MOD_W", -1)
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_WALK_D",
            left=1,
            right=7,
            segments=(f"{prefix}_IPAQ_WALK_W", 1),
            na_pass=True
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_WALK_HPD", 
            left=0,
            right=18, # unrealistic to do more than 18 hours of exercise per day (even that is a stretch!!)
            segments=(f"{prefix}_IPAQ_WALK_W", 1),
            na_pass=True
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_WALK_MPD", 
            left=0,
            right=59,
            segments=(f"{prefix}_IPAQ_WALK_W", 1),
            na_pass=True
        )
        .col_vals_between(
            columns=f"{prefix}_IPAQ_WALK_MINS", 
            left=0,
            right=180, # total mins per category is capped at 180 mins
            segments=(f"{prefix}_IPAQ_WALK_W", 1),
            na_pass=True
        )
        .col_vals_null(
            columns=[f"{prefix}_IPAQ_WALK_D", f"{prefix}_IPAQ_WALK_HPD", f"{prefix}_IPAQ_WALK_MPD"], 
            segments=(f"{prefix}_IPAQ_WALK_W", 0)
        )
        .col_vals_eq(
            columns=[f"{prefix}_IPAQ_WALK_MINS", f"{prefix}_IPAQ_WALK_MET"], 
            value=0,
            segments=(f"{prefix}_IPAQ_WALK_W", 0)
        )
        .col_vals_null(
            columns=[f"{prefix}_IPAQ_WALK_D", f"{prefix}_IPAQ_WALK_HPD", f"{prefix}_IPAQ_WALK_MPD", f"{prefix}_IPAQ_WALK_MINS", f"{prefix}_IPAQ_WALK_MET"],
            pre=lambda df: df.with_columns(pl.col(f"{prefix}_IPAQ_WALK_W").fill_null(-1)), # Pointblank doesn't seem to like segmenting values with null, so transform null to -1 and segment y that
            segments=(f"{prefix}_IPAQ_WALK_W", -1)
        )
        .col_vals_eq(
            columns=f"{prefix}_IPAQ_CAT",
            value=pb.col("check"),
            pre=check_ipaq_cat(
                f"{prefix}_IPAQ_VIG_D", f"{prefix}_IPAQ_MOD_D", f"{prefix}_IPAQ_WALK_D",
                f"{prefix}_IPAQ_VIG_MINS", f"{prefix}_IPAQ_MOD_MINS", f"{prefix}_IPAQ_WALK_MINS",
                f"{prefix}_IPAQ_TOT_MET", f"{prefix}_IPAQ_CAT"
            ),
            na_pass=True,
            brief="Check `IPAQ_CAT` is correctly calculated."
        )
    ).interrogate()

    return validation
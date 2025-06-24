import polars as pl
from odyssey.core import read_sav, write_sav

from config import INTERIM_DATA, PROCESSED_DATA, DATASETS

# Config
categories = ["VIG", "MOD", "WALK"]
categories_with_factors = {"VIG": 8, "MOD": 4, "WALK": 3.3}

def clean_weekly_activity(prefix: str) -> list[pl.expr]:
    """
    Clean the W column to transform values other than 0 or 1 to None/Null (such as values of 999).
    """
    expressions = []

    for cat in categories:
        weekly_activity = f"{prefix}_IPAQ_{cat}_W"

        exp = (
            pl.when(pl.col(weekly_activity).is_in([0, 1]))
            .then(pl.col(weekly_activity))
            .otherwise(None)
            .alias(weekly_activity)
        )

        expressions.append(exp)
        
    return expressions

def create_ipaq_activity_dummy_variable(prefix: str) -> list[pl.expr]:
    """
    Create dummy variable `IPAQ_ACTIVITY` which is 1 if there is any valid data for VIG, MOD or WALK exercise, and is otherwise 0.
    Can use variable for downstream logic; ie. to set values to Null if there is no activity.
    """
    vig_w = f"{prefix}_IPAQ_VIG_W"
    mod_w = f"{prefix}_IPAQ_MOD_W"
    walk_w = f"{prefix}_IPAQ_WALK_W"

    return (
        pl.when(
            pl.col(vig_w).is_null() &
            pl.col(mod_w).is_null() &
            pl.col(walk_w).is_null()
        ).then(0)
        .otherwise(1)
        .alias("IPAQ_ACTIVITY")
    )

def clean_when_no_activity(prefix: str) -> list[pl.expr]:
    """
    Set all IPAQ activity columns to None when there was no recorded activity for any category.
    """
    expressions = []

    cols_to_null = [f"{prefix}_IPAQ_{cat}_{col}" for cat in categories for col in ["D", "HPD", "MPD", "MINS", "MET"]]
        
    for col in cols_to_null:
        expressions.append(pl.when(pl.col("IPAQ_ACTIVITY") == 0).then(None).otherwise(pl.col(col)).alias(col))
    
    return expressions

def clean_days(prefix: str) -> list[pl.expr]:
    """
    Clean the number of days of exercise per week.

    If W = 0 -> D = None
    If W = 1 -> D should equal a number between 1-7, or set to None
    """
    expressions = []

    for cat in categories:
        weekly_activity = f"{prefix}_IPAQ_{cat}_W"
        days = f"{prefix}_IPAQ_{cat}_D"

        exp = (
            pl.when(pl.col(weekly_activity).eq(1))
            .then(
                pl.when(pl.col(days).is_between(1, 7))
                .then(pl.col(days))
                .otherwise(None)
            )
            .otherwise(None)
            .alias(days)
        )

        expressions.append(exp)
        
    return expressions

def recalculate_mins(prefix: str) -> list[pl.expr]:
    """
    Calculate MINS column if there's a non-Null value for either or both of `HPD` and `MPD`.
    Cap total minutes at 180 mins (in line with IPAQ protocol).
    """
    expressions = []

    for cat in categories:
        weekly_activity = f"{prefix}_IPAQ_{cat}_W"
        hpd = f"{prefix}_IPAQ_{cat}_HPD"
        mpd = f"{prefix}_IPAQ_{cat}_MPD"
        mins = f"{prefix}_IPAQ_{cat}_MINS"

        can_calculate_mins = (
            pl.col(weekly_activity).eq(1) & 
            ~(pl.col(hpd).is_null() & pl.col(mpd).is_null())
        )

        exp = (
            pl.when(can_calculate_mins)
            .then(pl.min_horizontal(180, pl.col(hpd).fill_null(0)*60 + pl.col(mpd).fill_null(0)))
            .alias(mins)
        )
        expressions.append(exp)
        
    return expressions

def recalculate_met(prefix: str) -> list[pl.expr]:
    """
    Calculate MET column if there are valid values for both of `D` and `MINS`
    """
    expressions = []

    for cat, f in categories_with_factors.items():
        weekly_activity = f"{prefix}_IPAQ_{cat}_W"
        days = f"{prefix}_IPAQ_{cat}_D"
        mins = f"{prefix}_IPAQ_{cat}_MINS"
        met = f"{prefix}_IPAQ_{cat}_MET"
        
        can_calculate_met = (
            pl.col(weekly_activity).eq(1) & 
            pl.col(days).is_between(1, 7) & 
            pl.col(mins).is_between(0, 180)
        )

        exp = (
            pl.when(can_calculate_met)
            .then(pl.col(days) * pl.col(mins) * f)
            .otherwise(None)
            .alias(met)
        )
        expressions.append(exp)

    return expressions

def recalculate_tot_met(prefix: str) -> list[pl.expr]:
    """
    Calculate TOT_MET column.
    
    Will return None if no IPAQ activity.
    Otherwise, will return the same of the MET values for VIG, MOD and WALK, where None is converted to 0.
    """
    return (
        pl.when(pl.col("IPAQ_ACTIVITY").eq(0))
        .then(None)
        .otherwise(sum(pl.col(f"{prefix}_IPAQ_{cat}_MET").fill_null(0) for cat in categories))
        .alias(f"{prefix}_IPAQ_TOT_MET")
    )

def recalculate_ipaq_cat(prefix: str) -> list[pl.expr]:
    """
    Calculate the IPAQ category based on the criteria from TODO: insert document.

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
    vig_days = f"{prefix}_IPAQ_VIG_D"
    mod_days = f"{prefix}_IPAQ_MOD_D"
    walk_days = f"{prefix}_IPAQ_WALK_D"
    vig_mins = f"{prefix}_IPAQ_VIG_MINS"
    mod_mins = f"{prefix}_IPAQ_MOD_MINS"
    walk_mins = f"{prefix}_IPAQ_WALK_MINS"
    tot_met = f"{prefix}_IPAQ_TOT_MET"

    return (
        pl.when(pl.col("IPAQ_ACTIVITY").eq(0) | pl.col(tot_met).is_null())
        .then(None)
        .when(
            (pl.col(vig_days).ge(3) & pl.col(vig_mins).ge(20) & pl.col(tot_met).ge(1500)) | 
            (sum(pl.col(col).fill_null(0) for col in [vig_days, mod_days, walk_days]).ge(7) & pl.col(tot_met).ge(3000))
        ).then(2)
        .when(
            (pl.col(vig_days).ge(3) & pl.col(vig_mins).ge(20)) |
            (sum(pl.col(col).fill_null(0) for col in [mod_days, walk_days]).ge(5) &
                sum(pl.col(col).fill_null(0) for col in [mod_mins, walk_mins]).ge(30)) |
            (sum(pl.col(col).fill_null(0) for col in [vig_days, mod_days, walk_days]).ge(5) & pl.col(tot_met).ge(600))
        ).then(1)
        .otherwise(0)
        .alias(f"{prefix}_IPAQ_CAT")
    )

def clean_when_weekly_activity_is_0(prefix: str) -> list[pl.expr]:
    """
    Return expressions to update column values when weekly activity for a given category is 0.

    => D, HPD, and MPD = None
    => MINS, MET = 0
    """
    expressions = []

    for cat in categories:
        weekly_activity = f"{prefix}_IPAQ_{cat}_W"
        cols_to_null = [f"{prefix}_IPAQ_{cat}_{col}" for col in ["D", "HPD", "MPD"]]
        cols_to_zero = [f"{prefix}_IPAQ_{cat}_{col}" for col in ["MINS", "MET"]]
        
        for col in cols_to_null:
            expressions.append(pl.when(pl.col(weekly_activity) == 0).then(None).otherwise(pl.col(col)).alias(col))
        
        for col in cols_to_zero:
            expressions.append(pl.when(pl.col(weekly_activity) == 0).then(0).otherwise(pl.col(col)).alias(col))
    
    return expressions

def harmonise_ipaq(
    prefix: str,
    df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Apply harmonisation functions to the given dataset.
    """
    harmonised_df = (
        df
        .with_columns(clean_weekly_activity(prefix))
        .with_columns(create_ipaq_activity_dummy_variable(prefix))
        .with_columns(clean_when_no_activity(prefix))
        .with_columns(clean_days(prefix))
        .with_columns(recalculate_mins(prefix))
        .with_columns(recalculate_met(prefix))
        .with_columns(recalculate_tot_met(prefix))
        .with_columns(clean_when_weekly_activity_is_0(prefix))
        .with_columns(recalculate_ipaq_cat(prefix))
    )

    return harmonised_df
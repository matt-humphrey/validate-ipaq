import polars as pl

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

def clean_hpd(prefix: str) -> list[pl.expr]:
    """
    Clean the hours per day of exercise.

    Replace values of 999 with None.

    In cases where hours is greater than 16, check if it's likely a manual input error
    and should be converted into minutes.
    """
    expressions = []

    for cat in categories:
        hpd = f"{prefix}_IPAQ_{cat}_HPD"
        mpd = f"{prefix}_IPAQ_{cat}_MPD"

        exp1 = (
            pl.when(pl.col(hpd).le(16)) # This will automatically capture values of 999 and convert to None
            .then(pl.col(hpd))
            .when(
                (pl.col(hpd) % 5 == 0) & 
                (pl.col(mpd).is_null() | (pl.col(mpd).eq(0)))
            )
            .then(0)
            .otherwise(None)
            .alias(hpd)
        )
        
        exp2 = (
            pl.when(
                pl.col(hpd).is_between(20, 60) & 
                (pl.col(hpd) % 5 == 0) & 
                (pl.col(mpd).is_null() | (pl.col(mpd).eq(0)))
            )
            .then(pl.col(hpd))
            .otherwise(pl.col(mpd))
            .alias(mpd)
        )

        expressions.extend([exp1, exp2])
        
    return expressions

def clean_mpd(prefix: str) -> list[pl.expr]:
    """
    Clean the minutes per day of exercise.

    Replace values of 999 with None.

    In cases where hours is greater than 16, check if it's likely a manual input error
    and should be converted into minutes.
    """
    expressions = []

    for cat in categories:
        hpd = f"{prefix}_IPAQ_{cat}_HPD"
        mpd = f"{prefix}_IPAQ_{cat}_MPD"

        exp1 = (
            pl.when(pl.col(mpd).is_between(10, 60, closed='left')) # This will automatically capture values of 999 and convert to None
            .then(pl.col(mpd))
            .when(
                (pl.col(mpd) % 5 == 0) & 
                (pl.col(mpd).ge(10)) &      # trim values less than 10 mins
                (pl.col(hpd).is_null() | (pl.col(hpd).eq(0)))
            )
            .then(pl.col(mpd) % 60)   # ie. 90 mins -> 90 % 60 = 30 (mins)
            .otherwise(None)
            .alias(mpd)
        )
        
        exp2 = (
            pl.when(
                pl.col(mpd).ge(60) & 
                (pl.col(mpd) % 5 == 0) & 
                (pl.col(hpd).is_null() | (pl.col(hpd).eq(0)))
            )
            .then(pl.col(mpd) // 60)   # ie. 90 mins -> 90 // 60 = 1 (hour)
            .otherwise(pl.col(hpd))
            .alias(hpd)
        )

        expressions.extend([exp1, exp2])

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

def recalculate_tot_met(prefix: str) -> pl.expr:
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

def recalculate_ipaq_cat(prefix: str) -> pl.expr:
    """
    Calculate the IPAQ category based on the criteria from TODO: insert document.

    HIGH: 2
    Vigorous exercise on 3+ days AND >= 1500 MET mins per week (doesn't explicitly state that vig exercise must be 20+ mins)
    OR combination of any exercise on 7+ days AND >= 3000 MET mins per week

    MODERATE: 1
    Vig exercise 3+ days for 20+ mins
    OR mod exercise AND/OR walking 5+ days for 30 mins
    OR any exercise on 5+ days AND >= 600 MET mins per week

    LOW: 0
    None of the above criteria

    A combination of exercise on multiple days means that multiple types of exercise 
    on a single day are counted separately (ie. 30 mins of moderate exercise and 30 mins
    of walking would be considered two days).
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
            (pl.col(vig_days).ge(3) & pl.col(vig_mins).ge(10) & pl.col(tot_met).ge(1500)) |
            (sum(pl.col(col).fill_null(0) for col in [vig_days, mod_days, walk_days]).ge(7) & pl.col(tot_met).ge(3000))
        ).then(2)
        .when(
            (pl.col(vig_days).ge(3) & pl.col(vig_mins).ge(20)) |
            (sum(pl.col(col).fill_null(0) for col in [vig_days, mod_days, walk_days]).ge(5) & pl.col(tot_met).ge(600)) |
            ((pl.col(mod_days).ge(5) & pl.col(mod_mins).ge(30)) |
             (pl.col(walk_days).ge(5) & pl.col(walk_mins).ge(30)) |
             (sum(pl.col(col).fill_null(0) for col in [mod_days, walk_days]).ge(5) &
              pl.col(mod_mins).ge(30) & pl.col(walk_mins).ge(30))
            )
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
            expressions.append(pl.when(pl.col(weekly_activity).eq(0)).then(None).otherwise(pl.col(col)).alias(col))
        
        for col in cols_to_zero:
            expressions.append(pl.when(pl.col(weekly_activity).eq(0)).then(0).otherwise(pl.col(col)).alias(col))
    
    return expressions

def clean_sit_variables(prefix: str) -> list[pl.expr]:
    """
    Replace values of 999 with None (for G222).
    """
    expressions = []

    cols_to_clean = [f"{prefix}_IPAQ_SIT_{time}_{var}" for time in ["WD", "WE"] for var in ["HPD", "MPD"]]

    for col in cols_to_clean:
        expressions.append(pl.when(pl.col(col).eq(999)).then(None).otherwise(pl.col(col)).alias(col))

    return expressions

def recalculate_sit_trunc(prefix: str) -> list[pl.expr]:
    """
    Recalculate the SIT_TRUNC values (for G222 and G126).
    """
    expressions = []

    for time_of_week in ["WD", "WE"]:
        hpd = f"{prefix}_IPAQ_SIT_{time_of_week}_HPD"
        mpd = f"{prefix}_IPAQ_SIT_{time_of_week}_MPD"
        trunc = f"{prefix}_IPAQ_SIT_{time_of_week}_TRUNC"

        exp = (
            pl.when(pl.col(hpd).is_null() & pl.col(mpd).is_null())
            .then(None)
            .otherwise(pl.min_horizontal(960, pl.col(hpd).fill_null(0) * 60 + pl.col(mpd).fill_null(0)))
            .alias(trunc)
        )
        expressions.append(exp)

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
        .with_columns(clean_hpd(prefix))
        .with_columns(clean_mpd(prefix))
        .with_columns(recalculate_mins(prefix))
        .with_columns(recalculate_met(prefix))
        .with_columns(recalculate_tot_met(prefix))
        .with_columns(clean_when_weekly_activity_is_0(prefix))
        .with_columns(recalculate_ipaq_cat(prefix))
        .drop("IPAQ_ACTIVITY")
    )

    return harmonised_df
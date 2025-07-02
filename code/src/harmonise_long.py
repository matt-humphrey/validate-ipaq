import polars as pl

# Config
categories = [
    "JOB_VIG", 
    "JOB_MOD", 
    "JOB_WALK",
    "TRANS_MV",
    "TRANS_BIKE",
    "TRANS_WALK",
    "HOME_OUT_VIG",
    "HOME_OUT_MOD",
    "HOME_IN_MOD",
    "LSR_VIG", 
    "LSR_MOD", 
    "LSR_WALK",
    "SIT_WD",
    "SIT_WE",
    "STAND_WD",
    "STAND_WE",
    "LYING_WD",
    "LYING_WE",
]

categories_with_factors = {
    "JOB_VIG": 8,
    "JOB_MOD": 4,
    "JOB_WALK": 3.3,
    "TRANS_WALK": 3.3,
    "TRANS_BIKE": 6,
    "HOME_OUT_VIG": 5.5,
    "HOME_OUT_MOD": 4,
    "HOME_IN_MOD": 3,
    "LSR_VIG": 8,
    "LSR_MOD": 4,
    "LSR_WALK": 3.3
}

met_categories = {
    "TOT_WORK_MET": ["JOB_VIG", "JOB_MOD", "JOB_WALK"],
    "TOT_TRANS_MET": ["TRANS_WALK", "TRANS_BIKE"],
    "TOT_HOME_MET": ["HOME_OUT_VIG", "HOME_OUT_MOD", "HOME_IN_MOD"],
    "TOT_LSR_MET": ["LSR_VIG", "LSR_MOD", "LSR_WALK"],
    "VIG_MET": ["JOB_VIG", "LSR_VIG"],
    "MOD_MET": ["JOB_MOD", "TRANS_BIKE", "HOME_OUT_VIG", "HOME_OUT_MOD", "HOME_IN_MOD", "LSR_MOD"],
    "WALK_MET": ["JOB_WALK", "TRANS_WALK", "LSR_WALK"],
}

total_met = {"TOT_MET": ["VIG", "MOD", "WALK"]}

def clean_days(prefix: str) -> list[pl.expr]:
    """
    Clean the number of days of exercise per week.

    If W = 0 -> D = 0
    If W = 1 -> D should equal a number between 1-7, or set to None
    """
    expressions = []

    # The following categories are irrelevant (no 'day' column)
    categories_to_exclude = ["SIT", "STAND", "LYING"]
    filtered_categories = [cat for cat in categories if not any(c in cat for c in categories_to_exclude)]

    for cat in filtered_categories:
        weekly_activity = f"{prefix}_IPAQ_{cat}"
        days = f"{prefix}_IPAQ_{cat}_D"

        exp = (
            pl.when(pl.col(weekly_activity).eq(0))
            .then(0)
            .when(
                (pl.col(weekly_activity).eq(1)) & 
                (pl.col(days).is_between(1, 7))
            )
            .then(pl.col(days))
            .otherwise(None)
            .alias(days)
        )

        expressions.append(exp)
        
    return expressions

def clean_hpd(prefix: str) -> list[pl.expr]:
    """
    Clean the hours per day of exercise.

    In cases where hours is greater than 16, check if it's likely a manual input error
    and should be converted into minutes.
    """
    expressions = []

    for cat in categories:
        hpd = f"{prefix}_IPAQ_{cat}_HPD"
        mpd = f"{prefix}_IPAQ_{cat}_MPD"

        exp1 = (
            pl.when(
                (pl.col(hpd).le(16)) & 
                (pl.col(hpd) % 1 == 0) # HPD is a whole number
            )
            .then(pl.col(hpd))
            .when(
                (pl.col(hpd).lt(60)) & 
                (pl.col(hpd) % 5 == 0) & # If HPD is > 16 and divisible by 5, it's likely an error, intended for minutes
                (pl.col(mpd).is_null() | (pl.col(mpd).eq(0))) # Ensure no existing minutes column, or convert hours to None
            )
            .then(0)
            .when(
                (pl.col(hpd) % 1 != 0) & # If HPD is a float (ie. 1.5 hours), truncate to 1 hour
                (pl.col(mpd).is_null() | (pl.col(mpd).eq(0)))
            ) 
            .then(pl.col(hpd) // 1) # ie. 1.5 // 1 = 1 hour
            .otherwise(None)
            .alias(hpd)
        )
        
        exp2 = (
            pl.when(
                (pl.col(hpd).is_between(20, 60, closed='left')) & # 20 <= HPD < 60
                (pl.col(hpd) % 5 == 0) & 
                (pl.col(mpd).is_null() | (pl.col(mpd).eq(0)))
            )
            .then(pl.col(hpd))
            .when(
                (pl.col(hpd) % 1 != 0) & # If HPD is a float (ie. 1.5 hours), convert the decimal to minutes
                (pl.col(mpd).is_null() | (pl.col(mpd).eq(0)))
            ) 
            .then(pl.col(hpd) % 1 * 60) # ie. 1.5 % 1 = 0.5 -> 0.5 * 60 = 30 minutes
            .otherwise(pl.col(mpd))
            .alias(mpd)
        )

        expressions.extend([exp1, exp2])
        
    return expressions

def clean_mpd(prefix: str) -> list[pl.expr]:
    """
    Clean the minutes per day of exercise.

    In cases where hours is greater than 16, check if it's likely a manual input error
    and should be converted into minutes.
    """
    expressions = []

    for cat in categories:
        hpd = f"{prefix}_IPAQ_{cat}_HPD"
        mpd = f"{prefix}_IPAQ_{cat}_MPD"

        exp1 = (
            pl.when(pl.col(mpd).is_between(0, 10, closed='left'))
            .then(0)
            .when(pl.col(mpd).is_between(10, 60, closed='left'))
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

def recalculate_sit_trunc(prefix: str) -> list[pl.expr]:
    """
    Recalculate the SIT_TRUNC values
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

def create_dummy_met_variables(prefix: str) -> list[pl.expr]:
    """
    Create dummy variables for the MET of each category of exercise.
    """
    expressions = []

    for cat, f in categories_with_factors.items():
        weekly_activity = f"{prefix}_IPAQ_{cat}"
        days = f"{prefix}_IPAQ_{cat}_D"
        hpd = f"{prefix}_IPAQ_{cat}_HPD"
        mpd = f"{prefix}_IPAQ_{cat}_MPD"
        met = f"{prefix}_IPAQ_{cat}_MET"
        
        can_calculate_met = (
            (pl.col(weekly_activity).eq(1)) & 
            (pl.col(days).is_between(1, 7)) & 
            (pl.col(hpd).ge(1) | pl.col(mpd).ge(10))
        )

        exp = (
            pl.when(pl.col(weekly_activity).eq(0))
            .then(0)
            .when(can_calculate_met)
            .then((f * pl.col(days) * pl.min_horizontal(180, pl.col(hpd).fill_null(0)*60 + pl.col(mpd).fill_null(0))).round(2))
            .otherwise(None)
            .alias(met)
        )
    
        expressions.append(exp)

    return expressions

def recalculate_met(
    prefix: str, 
    categories: dict[str, list[str]]
) -> list[pl.expr]:
    """
    Recalculate MET columns
    """
    expressions = []

    for met, cols in categories.items():
        expressions.append(pl.sum_horizontal([f"{prefix}_IPAQ_{col}_MET" for col in cols]).alias(f"{prefix}_IPAQ_{met}"))

    return expressions

def recalculate_ipaq_cat(prefix: str) -> pl.expr:
    """
    Calculate the IPAQ category based on the criteria from TODO: insert document.

    HIGH: 2
    Vigorous exercise on 3+ days AND >= 1500 MET mins per week
    OR combination of any exercise on 7+ days AND >= 3000 MET mins per week

    MODERATE: 1
    Vig exercise 3+ days for 20+ mins
    OR mod exercise AND/OR walking 5+ days for 30 mins
    OR any exercise on 5+ days AND >= 600 MET mins per week

    LOW: 0
    None of the above criteria
    """
    job_vig_days = f"{prefix}_IPAQ_JOB_VIG_D"
    job_mod_days = f"{prefix}_IPAQ_JOB_MOD_D"
    job_walk_days = f"{prefix}_IPAQ_JOB_WALK_D"
    trans_bike_days = f"{prefix}_IPAQ_TRANS_BIKE_D"
    trans_walk_days = f"{prefix}_IPAQ_TRANS_WALK_D"
    home_out_vig_days = f"{prefix}_IPAQ_HOME_OUT_VIG_D"
    home_out_mod_days = f"{prefix}_IPAQ_HOME_OUT_MOD_D"
    home_in_mod_days = f"{prefix}_IPAQ_HOME_IN_MOD_D"
    lsr_vig_days = f"{prefix}_IPAQ_LSR_VIG_D" 
    lsr_mod_days = f"{prefix}_IPAQ_LSR_MOD_D" 
    lsr_walk_days = f"{prefix}_IPAQ_LSR_WALK_D" 
    job_vig_hpd = f"{prefix}_IPAQ_JOB_VIG_HPD"
    job_vig_mpd = f"{prefix}_IPAQ_JOB_VIG_MPD"
    lsr_vig_hpd = f"{prefix}_IPAQ_LSR_VIG_HPD"
    lsr_vig_mpd = f"{prefix}_IPAQ_LSR_VIG_MPD"
    tot_met = f"{prefix}_IPAQ_TOT_MET"

    vig_days = pl.sum_horizontal(pl.col(job_vig_days, lsr_vig_days))
    total_days = pl.sum_horizontal(pl.col(
        job_vig_days, job_mod_days, job_walk_days, trans_bike_days, 
        trans_walk_days, home_out_vig_days, home_out_mod_days, 
        home_in_mod_days, lsr_vig_days, lsr_mod_days, lsr_walk_days
    ))

    job_vig_mins = pl.col(job_vig_hpd) * 60 + pl.col(job_vig_mpd)
    lsr_vig_mins = pl.col(lsr_vig_hpd) * 60 + pl.col(lsr_vig_mpd)
    vig_mins = pl.sum_horizontal(job_vig_mins, lsr_vig_mins)

    return (
        pl.when(pl.col(tot_met).is_null())
        .then(None)
        .when(
            (vig_days.ge(3) & vig_mins.ge(10) & pl.col(tot_met).ge(1500)) |
            (total_days.ge(7) & pl.col(tot_met).ge(3000))
        ).then(2)
        .when(
            (vig_days.ge(3) & vig_mins.ge(20)) |
            (total_days.ge(5) & pl.col(tot_met).ge(600))
        ).then(1)
        .otherwise(0)
        .alias(f"{prefix}_IPAQ_CAT")
    )

def harmonise_ipaq(
    prefix: str,
    df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Apply harmonisation functions to the given dataset.
    """
    harmonised_df = (
        df
        .with_columns(clean_days(prefix))
        .with_columns(clean_hpd(prefix))
        .with_columns(clean_mpd(prefix))
        .with_columns(recalculate_sit_trunc(prefix))
        .with_columns(create_dummy_met_variables(prefix))
        .with_columns(recalculate_met(prefix, met_categories))
        .with_columns(recalculate_met(prefix, total_met))
        .with_columns(recalculate_ipaq_cat(prefix))
    )

    return harmonised_df
from typing import Callable
import polars as pl
import pointblank as pb
from odyssey.core import Dataset
from pathlib import Path

type Metadata = dict[str, str|int|dict[int|float, str]]
type MetadataDict = dict[str, Metadata]

def validate_jobs(df: pl.DataFrame) -> pb.Validate:

    validation = (
        pb.Validate(data=df)
        .col_vals_in_set(
            columns="G217_IPAQ_JOB",
            set=[0, 1, None],
        )
        .col_vals_null(
            columns=pb.starts_with("G217_IPAQ_JOB_"),
            segments=("G217_IPAQ_JOB", 0)
        )
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_JOB_VIG_"),
            value=0,
            segments=("G217_IPAQ_JOB_VIG", 0)
        )
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_JOB_MOD_"),
            value=0,
            segments=("G217_IPAQ_JOB_MOD", 0)
        )
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_JOB_WALK_"),
            value=0,
            segments=("G217_IPAQ_JOB_WALK", 0)
        )
        .col_vals_between(
            columns="G217_IPAQ_JOB_VIG_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_JOB_VIG", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_JOB_MOD_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_JOB_MOD", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_JOB_WALK_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_JOB_WALK", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_JOB_VIG_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_JOB_VIG", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_JOB_VIG_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_JOB_VIG_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_JOB_VIG_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_JOB_VIG", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_JOB_VIG_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_JOB_VIG", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_JOB_MOD_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_JOB_MOD", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_JOB_MOD_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_JOB_MOD_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_JOB_MOD_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_JOB_MOD", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_JOB_MOD_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_JOB_MOD", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_JOB_WALK_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_JOB_WALK", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_JOB_WALK_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_JOB_WALK_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_JOB_WALK_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_JOB_WALK", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_JOB_WALK_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_JOB_WALK", 1),
            na_pass=True
        )
        .col_vals_eq(
            columns="G217_IPAQ_TOT_WORK_MET",
            value=pb.col("check"),
            pre=check_total_work_met(
                "G217_IPAQ_JOB_VIG_D", "G217_IPAQ_JOB_VIG_HPD", "G217_IPAQ_JOB_VIG_MPD",
                "G217_IPAQ_JOB_MOD_D", "G217_IPAQ_JOB_MOD_HPD", "G217_IPAQ_JOB_MOD_MPD",
                "G217_IPAQ_JOB_WALK_D", "G217_IPAQ_JOB_WALK_HPD", "G217_IPAQ_JOB_WALK_MPD",
            ),
            na_pass=True
        )
    ).interrogate()

    return validation

def check_total_work_met(
    vig_d: str,
    vig_hpd: str,
    vig_mpd: str,
    mod_d: str,
    mod_hpd: str,
    mod_mpd: str,
    walk_d: str,
    walk_hpd: str,
    walk_mpd: str,
    ) -> Callable:
    """
    Check Total Work MET
    """
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        vig_met = 8 * pl.col(vig_d).fill_null(0) * pl.min_horizontal(180, pl.col(vig_hpd).fill_null(0) * 60 + pl.col(vig_mpd).fill_null(0))
        mod_met = 4 * pl.col(mod_d).fill_null(0) * pl.min_horizontal(180, pl.col(mod_hpd).fill_null(0) * 60 + pl.col(mod_mpd).fill_null(0))
        walk_met = 3.3 * pl.col(walk_d).fill_null(0) * pl.min_horizontal(180, pl.col(walk_hpd).fill_null(0) * 60 + pl.col(walk_mpd).fill_null(0))
        total_work_met = pl.sum_horizontal(col.round(2) for col in [vig_met, mod_met, walk_met])
        return df.with_columns(total_work_met.alias("check"))

    return preprocessor


def validate_transport(df: pl.DataFrame) -> pb.Validate:

    validation = (
        pb.Validate(data=df)
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_TRANS_MV_"),
            value=0,
            segments=("G217_IPAQ_TRANS_MV", 0)
        )
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_TRANS_BIKE_"),
            value=0,
            segments=("G217_IPAQ_TRANS_BIKE", 0)
        )
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_TRANS_WALK_"),
            value=0,
            segments=("G217_IPAQ_TRANS_WALK", 0)
        )
        .col_vals_between(
            columns="G217_IPAQ_TRANS_MV_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_TRANS_MV", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_TRANS_BIKE_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_TRANS_BIKE", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_TRANS_WALK_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_TRANS_WALK", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_TRANS_MV_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_TRANS_MV", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_TRANS_MV_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_TRANS_MV_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_TRANS_MV_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_TRANS_MV", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_TRANS_MV_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_TRANS_MV", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_TRANS_BIKE_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_TRANS_BIKE", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_TRANS_BIKE_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_TRANS_BIKE_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_TRANS_BIKE_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_TRANS_BIKE", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_TRANS_BIKE_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_TRANS_BIKE", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_TRANS_WALK_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_TRANS_WALK", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_TRANS_WALK_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_TRANS_WALK_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_TRANS_WALK_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_TRANS_WALK", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_TRANS_WALK_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_TRANS_WALK", 1),
            na_pass=True
        )
        .col_vals_eq(
            columns="G217_IPAQ_TOT_TRANS_MET",
            value=pb.col("check"),
            pre=check_total_transport_met(
                "G217_IPAQ_TRANS_BIKE_D", "G217_IPAQ_TRANS_BIKE_HPD", "G217_IPAQ_TRANS_BIKE_MPD",
                "G217_IPAQ_TRANS_WALK_D", "G217_IPAQ_TRANS_WALK_HPD", "G217_IPAQ_TRANS_WALK_MPD",
            ),
            na_pass=True
        )
    ).interrogate()

    return validation

def check_total_transport_met(
    bike_d: str,
    bike_hpd: str,
    bike_mpd: str,
    walk_d: str,
    walk_hpd: str,
    walk_mpd: str,
    ) -> Callable:
    """
    Check Total Transport MET
    """
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        bike_met = 6 * pl.col(bike_d).fill_null(0) * pl.min_horizontal(180, pl.col(bike_hpd).fill_null(0) * 60 + pl.col(bike_mpd).fill_null(0))
        walk_met = 3.3 * pl.col(walk_d).fill_null(0) * pl.min_horizontal(180, pl.col(walk_hpd).fill_null(0) * 60 + pl.col(walk_mpd).fill_null(0))
        total_work_met = pl.sum_horizontal(col.round(2) for col in [bike_met, walk_met])
        return df.with_columns(total_work_met.alias("check"))

    return preprocessor


def validate_home(df: pl.DataFrame) -> pb.Validate:

    validation = (
        pb.Validate(data=df)
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_HOME_OUT_VIG_"),
            value=0,
            segments=("G217_IPAQ_HOME_OUT_VIG", 0)
        )
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_HOME_OUT_MOD_"),
            value=0,
            segments=("G217_IPAQ_HOME_OUT_MOD", 0)
        )
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_HOME_IN_MOD_"),
            value=0,
            segments=("G217_IPAQ_HOME_IN_MOD", 0)
        )
        .col_vals_between(
            columns="G217_IPAQ_HOME_OUT_VIG_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_HOME_OUT_VIG", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_HOME_OUT_MOD_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_HOME_OUT_MOD", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_HOME_IN_MOD_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_HOME_IN_MOD", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_HOME_OUT_VIG_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_HOME_OUT_VIG", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_HOME_OUT_VIG_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_HOME_OUT_VIG_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_HOME_OUT_VIG_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_HOME_OUT_VIG", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_HOME_OUT_VIG_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_HOME_OUT_VIG", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_HOME_OUT_MOD_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_HOME_OUT_MOD", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_HOME_OUT_MOD_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_HOME_OUT_MOD_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_HOME_OUT_MOD_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_HOME_OUT_MOD", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_HOME_OUT_MOD_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_HOME_OUT_MOD", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_HOME_IN_MOD_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_HOME_IN_MOD", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_HOME_IN_MOD_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_HOME_IN_MOD_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_HOME_IN_MOD_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_HOME_IN_MOD", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_HOME_IN_MOD_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_HOME_IN_MOD", 1),
            na_pass=True
        )
        .col_vals_eq(
            columns="G217_IPAQ_TOT_HOME_MET",
            value=pb.col("check"),
            pre=check_total_home_met(
                "G217_IPAQ_HOME_OUT_VIG_D", "G217_IPAQ_HOME_OUT_VIG_HPD", "G217_IPAQ_HOME_OUT_VIG_MPD",
                "G217_IPAQ_HOME_OUT_MOD_D", "G217_IPAQ_HOME_OUT_MOD_HPD", "G217_IPAQ_HOME_OUT_MOD_MPD",
                "G217_IPAQ_HOME_IN_MOD_D", "G217_IPAQ_HOME_IN_MOD_HPD", "G217_IPAQ_HOME_IN_MOD_MPD",
            ),
            na_pass=True
        )
    ).interrogate()

    return validation

def check_total_home_met(
    out_vig_d: str,
    out_vig_hpd: str,
    out_vig_mpd: str,
    out_mod_d: str,
    out_mod_hpd: str,
    out_mod_mpd: str,
    in_mod_d: str,
    in_mod_hpd: str,
    in_mod_mpd: str,
    ) -> Callable:
    """
    Check Total Home MET
    """
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        out_vig_met = 5.5 * pl.col(out_vig_d).fill_null(0) * pl.min_horizontal(180, pl.col(out_vig_hpd).fill_null(0) * 60 + pl.col(out_vig_mpd).fill_null(0))
        out_mod_met = 4 * pl.col(out_mod_d).fill_null(0) * pl.min_horizontal(180, pl.col(out_mod_hpd).fill_null(0) * 60 + pl.col(out_mod_mpd).fill_null(0))
        in_mod_met = 3 * pl.col(in_mod_d).fill_null(0) * pl.min_horizontal(180, pl.col(in_mod_hpd).fill_null(0) * 60 + pl.col(in_mod_mpd).fill_null(0))
        total_home_met = pl.sum_horizontal(col.round(2) for col in [out_vig_met, out_mod_met, in_mod_met])
        return df.with_columns(total_home_met.alias("check"))

    return preprocessor


def validate_leisure(df: pl.DataFrame) -> pb.Validate:

    validation = (
        pb.Validate(data=df)
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_LSR_VIG_"),
            value=0,
            segments=("G217_IPAQ_LSR_VIG", 0)
        )
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_LSR_MOD_"),
            value=0,
            segments=("G217_IPAQ_LSR_MOD", 0)
        )
        .col_vals_eq(
            columns=pb.starts_with("G217_IPAQ_LSR_WALK_"),
            value=0,
            segments=("G217_IPAQ_LSR_WALK", 0)
        )
        .col_vals_between(
            columns="G217_IPAQ_LSR_VIG_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_LSR_VIG", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_LSR_MOD_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_LSR_MOD", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_LSR_WALK_D",
            left=1,
            right=7,
            segments=("G217_IPAQ_LSR_WALK", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_LSR_VIG_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_LSR_VIG", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_LSR_VIG_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_LSR_VIG_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_LSR_VIG_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_LSR_VIG", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_LSR_VIG_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_LSR_VIG", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_LSR_MOD_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_LSR_MOD", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_LSR_MOD_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_LSR_MOD_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_LSR_MOD_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_LSR_MOD", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_LSR_MOD_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_LSR_MOD", 1),
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_LSR_WALK_HPD", 
            left=0,
            right=16,
            segments=("G217_IPAQ_LSR_WALK", 1),
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_LSR_WALK_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_LSR_WALK_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_LSR_WALK_MPD", 
            left=0,
            right=59,
            segments=("G217_IPAQ_LSR_WALK", 1),
            na_pass=True
        )
        .col_vals_outside(
            columns="G217_IPAQ_LSR_WALK_MPD", 
            left=1,
            right=9,
            segments=("G217_IPAQ_LSR_WALK", 1),
            na_pass=True
        )
        .col_vals_eq(
            columns="G217_IPAQ_TOT_LSR_MET",
            value=pb.col("check"),
            pre=check_total_leisure_met(
                "G217_IPAQ_LSR_VIG_D", "G217_IPAQ_LSR_VIG_HPD", "G217_IPAQ_LSR_VIG_MPD",
                "G217_IPAQ_LSR_MOD_D", "G217_IPAQ_LSR_MOD_HPD", "G217_IPAQ_LSR_MOD_MPD",
                "G217_IPAQ_LSR_WALK_D", "G217_IPAQ_LSR_WALK_HPD", "G217_IPAQ_LSR_WALK_MPD",
            ),
            na_pass=True
        )
    ).interrogate()

    return validation

def check_total_leisure_met(
    vig_d: str,
    vig_hpd: str,
    vig_mpd: str,
    mod_d: str,
    mod_hpd: str,
    mod_mpd: str,
    walk_d: str,
    walk_hpd: str,
    walk_mpd: str,
    ) -> Callable:
    """
    Check Total Leisure MET
    """
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        vig_met = 8 * pl.col(vig_d).fill_null(0) * pl.min_horizontal(180, pl.col(vig_hpd).fill_null(0) * 60 + pl.col(vig_mpd).fill_null(0))
        mod_met = 4 * pl.col(mod_d).fill_null(0) * pl.min_horizontal(180, pl.col(mod_hpd).fill_null(0) * 60 + pl.col(mod_mpd).fill_null(0))
        walk_met = 3.3 * pl.col(walk_d).fill_null(0) * pl.min_horizontal(180, pl.col(walk_hpd).fill_null(0) * 60 + pl.col(walk_mpd).fill_null(0))
        total_work_met = pl.sum_horizontal(col.round(2) for col in [vig_met, mod_met, walk_met])
        return df.with_columns(total_work_met.alias("check"))

    return preprocessor


def validate_totals(df: pl.DataFrame) -> pb.Validate:

    validation = (
        pb.Validate(data=df)
        .col_vals_eq(
            columns="G217_IPAQ_WALK_MET",
            value=pb.col("check"),
            pre=check_walk_met(
                "G217_IPAQ_JOB_WALK_D", "G217_IPAQ_JOB_WALK_HPD", "G217_IPAQ_JOB_WALK_MPD",
                "G217_IPAQ_TRANS_WALK_D", "G217_IPAQ_TRANS_WALK_HPD", "G217_IPAQ_TRANS_WALK_MPD",
                "G217_IPAQ_LSR_WALK_D", "G217_IPAQ_LSR_WALK_HPD", "G217_IPAQ_LSR_WALK_MPD",
            ),
            na_pass=True
        )
        .col_vals_eq(
            columns="G217_IPAQ_MOD_MET",
            value=pb.col("check"),
            pre=check_mod_met(
                "G217_IPAQ_JOB_MOD_D", "G217_IPAQ_JOB_MOD_HPD", "G217_IPAQ_JOB_MOD_MPD",
                "G217_IPAQ_TRANS_BIKE_D", "G217_IPAQ_TRANS_BIKE_HPD", "G217_IPAQ_TRANS_BIKE_MPD",
                "G217_IPAQ_HOME_OUT_VIG_D", "G217_IPAQ_HOME_OUT_VIG_HPD", "G217_IPAQ_HOME_OUT_VIG_MPD",
                "G217_IPAQ_HOME_OUT_MOD_D", "G217_IPAQ_HOME_OUT_MOD_HPD", "G217_IPAQ_HOME_OUT_MOD_MPD",
                "G217_IPAQ_HOME_IN_MOD_D", "G217_IPAQ_HOME_IN_MOD_HPD", "G217_IPAQ_HOME_IN_MOD_MPD",
                "G217_IPAQ_LSR_MOD_D", "G217_IPAQ_LSR_MOD_HPD", "G217_IPAQ_LSR_MOD_MPD",
            ),
            na_pass=True
        )
        .col_vals_eq(
            columns="G217_IPAQ_VIG_MET",
            value=pb.col("check"),
            pre=check_vig_met(
                "G217_IPAQ_JOB_VIG_D", "G217_IPAQ_JOB_VIG_HPD", "G217_IPAQ_JOB_VIG_MPD",
                "G217_IPAQ_LSR_VIG_D", "G217_IPAQ_LSR_VIG_HPD", "G217_IPAQ_LSR_VIG_MPD",
            ),
            na_pass=True
        )
        .col_vals_eq(
            columns="G217_IPAQ_TOT_MET",
            value=pb.col("check"),
            pre=check_tot_met("G217_IPAQ_VIG_MET", "G217_IPAQ_MOD_MET", "G217_IPAQ_WALK_MET", "G217_IPAQ_TOT_MET"),
            brief="Check `TOT_MET` equals the sum of `VIG_MET`, `MOD_MET`, and `WALK_MET`",
            na_pass=True
        )
        .col_vals_eq(
            columns="G217_IPAQ_CAT",
            value=pb.col("check"),
            pre=check_ipaq_cat(
                "G217_IPAQ_JOB_VIG_D", "G217_IPAQ_JOB_MOD_D", "G217_IPAQ_JOB_WALK_D",
                "G217_IPAQ_TRANS_BIKE_D", "G217_IPAQ_TRANS_WALK_D",
                "G217_IPAQ_HOME_OUT_VIG_D", "G217_IPAQ_HOME_OUT_MOD_D", "G217_IPAQ_HOME_IN_MOD_D", 
                "G217_IPAQ_LSR_VIG_D", "G217_IPAQ_LSR_MOD_D", "G217_IPAQ_LSR_WALK_D",
                "G217_IPAQ_JOB_VIG_HPD", "G217_IPAQ_JOB_VIG_MPD", 
                "G217_IPAQ_LSR_VIG_HPD", "G217_IPAQ_LSR_VIG_MPD", 
                "G217_IPAQ_TOT_MET", "G217_IPAQ_CAT"
            ),
            na_pass=True,
            brief="Check `IPAQ_CAT` is correctly calculated."
        )
    ).interrogate()

    return validation

def check_walk_met(
    job_walk_d: str,
    job_walk_hpd: str,
    job_walk_mpd: str,
    trans_walk_d: str,
    trans_walk_hpd: str,
    trans_walk_mpd: str,
    lsr_walk_d: str,
    lsr_walk_hpd: str,
    lsr_walk_mpd: str,
) -> Callable:
    
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        job_walk_met = 3.3 * pl.col(job_walk_d).fill_null(0) * pl.min_horizontal(180, pl.col(job_walk_hpd).fill_null(0) * 60 + pl.col(job_walk_mpd).fill_null(0))
        trans_walk_met = 3.3 * pl.col(trans_walk_d).fill_null(0) * pl.min_horizontal(180, pl.col(trans_walk_hpd).fill_null(0) * 60 + pl.col(trans_walk_mpd).fill_null(0))
        lsr_walk_met = 3.3 * pl.col(lsr_walk_d).fill_null(0) * pl.min_horizontal(180, pl.col(lsr_walk_hpd).fill_null(0) * 60 + pl.col(lsr_walk_mpd).fill_null(0))
        total_walk_met = pl.sum_horizontal(col.round(2) for col in [job_walk_met, trans_walk_met, lsr_walk_met])
        return df.with_columns(total_walk_met.alias("check"))

    return preprocessor

def check_mod_met(
    job_mod_d: str,
    job_mod_hpd: str,
    job_mod_mpd: str,
    trans_bike_d: str,
    trans_bike_hpd: str,
    trans_bike_mpd: str,
    home_out_vig_d: str,
    home_out_vig_hpd: str,
    home_out_vig_mpd: str,
    home_out_mod_d: str,
    home_out_mod_hpd: str,
    home_out_mod_mpd: str,
    home_in_mod_d: str,
    home_in_mod_hpd: str,
    home_in_mod_mpd: str,
    lsr_mod_d: str,
    lsr_mod_hpd: str,
    lsr_mod_mpd: str,
) -> Callable:
    
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        job_mod_met = 4 * pl.col(job_mod_d).fill_null(0) * pl.min_horizontal(180, pl.col(job_mod_hpd).fill_null(0) * 60 + pl.col(job_mod_mpd).fill_null(0))
        trans_bike_met = 6 * pl.col(trans_bike_d).fill_null(0) * pl.min_horizontal(180, pl.col(trans_bike_hpd).fill_null(0) * 60 + pl.col(trans_bike_mpd).fill_null(0))
        home_out_vig_met = 5.5 * pl.col(home_out_vig_d).fill_null(0) * pl.min_horizontal(180, pl.col(home_out_vig_hpd).fill_null(0) * 60 + pl.col(home_out_vig_mpd).fill_null(0))
        home_out_mod_met = 4 * pl.col(home_out_mod_d).fill_null(0) * pl.min_horizontal(180, pl.col(home_out_mod_hpd).fill_null(0) * 60 + pl.col(home_out_mod_mpd).fill_null(0))
        home_in_mod_met = 3 * pl.col(home_in_mod_d).fill_null(0) * pl.min_horizontal(180, pl.col(home_in_mod_hpd).fill_null(0) * 60 + pl.col(home_in_mod_mpd).fill_null(0))
        lsr_mod_met = 4 * pl.col(lsr_mod_d).fill_null(0) * pl.min_horizontal(180, pl.col(lsr_mod_hpd).fill_null(0) * 60 + pl.col(lsr_mod_mpd).fill_null(0))
        total_mod_met = pl.sum_horizontal([job_mod_met, trans_bike_met, home_out_vig_met, home_out_mod_met, home_in_mod_met, lsr_mod_met])
        return df.with_columns(total_mod_met.alias("check"))

    return preprocessor

def check_vig_met(
    vig_job_d: str,
    vig_job_hpd: str,
    vig_job_mpd: str,
    vig_lsr_d: str,
    vig_lsr_hpd: str,
    vig_lsr_mpd: str,
) -> Callable:
    
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        vig_job_met = 8 * pl.col(vig_job_d).fill_null(0) * pl.min_horizontal(180, pl.col(vig_job_hpd).fill_null(0) * 60 + pl.col(vig_job_mpd).fill_null(0))
        vig_lsr_met = 8 * pl.col(vig_lsr_d).fill_null(0) * pl.min_horizontal(180, pl.col(vig_lsr_hpd).fill_null(0) * 60 + pl.col(vig_lsr_mpd).fill_null(0))
        total_vig_met = pl.sum_horizontal([vig_job_met, vig_lsr_met])
        return df.with_columns(total_vig_met.alias("check"))

    return preprocessor

def check_tot_met(
    vig_met: str,
    mod_met: str,
    walk_met: str,
    tot_met: str
) -> Callable:
    """Returns a preprocessing function to verify the calculated total MET value."""
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        expr = (
            pl.when(pl.col(vig_met).is_null() | pl.col(mod_met).is_null() | pl.col(walk_met).is_null())
            .then(None)
            .otherwise(pl.sum_horizontal(pl.col(vig_met, mod_met, walk_met)))
        )
        
        return df.with_columns(
            expr.alias("check"),
            pl.col(tot_met)
        )
    return preprocessor

def check_ipaq_cat(
    job_vig_days: str,
    job_mod_days: str,
    job_walk_days: str,
    trans_bike_days: str,
    trans_walk_days: str,
    home_out_vig_days: str,
    home_out_mod_days: str,
    home_in_mod_days: str,
    lsr_vig_days: str, 
    lsr_mod_days: str, 
    lsr_walk_days: str, 
    job_vig_hpd: str,
    job_vig_mpd: str,
    lsr_vig_hpd: str,
    lsr_vig_mpd: str,
    tot_met: str, 
    cat: str # IPAQ category (low: 0, moderate: 1, high: 2)
    ) -> Callable:
    """
    Returns a preprocessing function to verify the IPAQ category.

    HIGH: 2
    Vigorous exercise on 3+ days AND >= 1500 MET mins per week
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
        all_days = [
            job_vig_days, job_mod_days, job_walk_days, trans_bike_days, trans_walk_days, home_out_vig_days, 
            home_out_mod_days, home_in_mod_days, lsr_vig_days, lsr_mod_days, lsr_walk_days
        ]
        return df.with_columns(
            (pl.when(pl.col(tot_met).is_null()).then(None)
            .when(
                (pl.sum_horizontal(job_vig_days, lsr_vig_days).ge(3) & pl.col(tot_met).ge(1500)) | 
                (pl.sum_horizontal(all_days).ge(7) & pl.col(tot_met).ge(3000))
            ).then(2)
            .when(
                (pl.sum_horizontal(job_vig_days, lsr_vig_days).ge(3) & 
                    pl.sum_horizontal(
                        pl.col(job_vig_hpd).fill_null(0)*60, pl.col(job_vig_mpd).fill_null(0),
                        pl.col(lsr_vig_hpd).fill_null(0)*60, pl.col(lsr_vig_mpd).fill_null(0)
                    ).ge(20)) |
                (pl.sum_horizontal(all_days).ge(5) & pl.col(tot_met).ge(600))
            ).then(1)
            .otherwise(0)
            ).alias("check"),
            pl.col(cat).fill_null(0) # Fill nulls with 0; otherwise the validation skips if one value in a comparison is null
        )
    return preprocessor




def validate_sit_stand_and_lying(df: pl.DataFrame) -> pb.Validate:

    validation = (
        pb.Validate(data=df)
        .col_vals_eq(
            columns="G217_IPAQ_SIT_WD_TRUNC",
            value=pb.col("check"),
            pre=check_sit_trunc("G217_IPAQ_SIT_WD_HPD", "G217_IPAQ_SIT_WD_MPD"),
            brief="Check total mins/day equals `HPD*60 + MPD`",
            na_pass=True,
        )
        .col_vals_eq(
            columns="G217_IPAQ_SIT_WE_TRUNC",
            value=pb.col("check"),
            pre=check_sit_trunc("G217_IPAQ_SIT_WE_HPD", "G217_IPAQ_SIT_WE_MPD"),
            brief="Check total mins/day equals `HPD*60 + MPD`",
            na_pass=True,
        )
        .col_vals_between(
            columns="G217_IPAQ_STAND_WD_HPD", 
            left=0,
            right=16,
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_STAND_WD_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_STAND_WD_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_STAND_WD_MPD", 
            left=0,
            right=59,
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_STAND_WE_HPD", 
            left=0,
            right=16,
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_STAND_WE_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_STAND_WE_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_STAND_WE_MPD", 
            left=0,
            right=59,
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_LYING_WD_HPD", 
            left=0,
            right=24,
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_LYING_WD_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_LYING_WD_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_LYING_WD_MPD", 
            left=0,
            right=59,
            na_pass=True
        )
        .col_vals_between(
            columns="G217_IPAQ_LYING_WE_HPD", 
            left=0,
            right=24,
            na_pass=True
        )
        .col_vals_expr(
            expr=pl.col("G217_IPAQ_LYING_WE_HPD") % 1 == 0,
            brief="Check HPD is a whole number.",
            pre=lambda df: df.with_columns(pl.col("G217_IPAQ_LYING_WE_HPD").fill_null(0))
        )
        .col_vals_between(
            columns="G217_IPAQ_LYING_WE_MPD", 
            left=0,
            right=59,
            na_pass=True
        )
    )

    return validation.interrogate()

def check_sit_trunc(
    hpd: str,
    mpd: str,
    # sit_trunc: str
    ) -> Callable:
    """
    Returns a preprocessing function to verify the calculated total SIT time.
    
    SIT_TRUNC should equal HPD * 60 + MPD, with a maximum value of 960 mins.
    If both HPD and MPD are Null, SIT_TRUNC should be Null.
    """
    def preprocessor(df: pl.DataFrame) -> pl.DataFrame:
        expr = (
            pl.when(pl.col(hpd).is_null() & pl.col(mpd).is_null())
            .then(None)
            .otherwise(pl.min_horizontal(960, pl.col(hpd).fill_null(0) * 60 + pl.col(mpd).fill_null(0)))
        )
        
        return df.with_columns(
            expr.alias("check"),
            # pl.col(sit_trunc).fill_null(0) # Fill nulls with 0; otherwise the validation skips if one value in a comparison is null
        )
    return preprocessor
---
author: Matt Humphrey
date_created: 2025-05-14
date_completed: 2025-07-02
tags: 
  - validation
  - harmonisation
---

# Validating and Harmonising IPAQ Data

Although IPAQ data has been cleaned and harmonised by multiple past Data Officers, Alex noted (based on feedback from a researcher) that it was odd that we had values of 88 and 99 for N/A and Missing data in G220 and G222.
This was confusing for researchers, because it messed up analysis, and they would prefer Null/None values where there was no data.
This prompted an initial investigation to validate these two datasets, and clean up those values.
Upon investigation, there were several other issues discovered, both with the cleaning of the raw data, and the subsequent creation of derived variables. These issues extended across all IPAQ datasets.

In short, the way missing values were handled across datasets was inconsistent, and there were unique issues across ALL datasets which had not been correctly cleaned/harmonised, including incorrectly calculated total MET scores, and incorrect categorisation.

## Navigating this folder

All validation and scoring rules were taken from the official documentation, stored in `docs/IPAQ Scoring Protocol.pdf`.

The rules for validation were written in `code/src/utils.py` and `code/src/validate_long.py`.

The initial validation and data exploration for each file was done in Jupyter Notebooks, under `code/notebooks/interim`.
You can see the exhaustive results of every data issue discovered in those notebooks.

The functions to clean and harmonise the datasets were captured in `code/src/harmonise.py` and `code/src/harmonise_long.py`.
The script to run these functions on all datasets simultaneously is in `code/src/main.py`.
Prior to running the main script, I ran `code/src/make_interim.py` to run the initial changes to rename and drop variables.

The final testing to ensure all changes were correctly captured was done under `code/notebooks/processed`.

All the configuration, for which variables to rename, which to drop, and the new metadata (variable labels and field values) were defined in `code/src/config`.
See `variables.py` for a record of which variables were renamed and/or dropped.
See `metadata.py` for a record of how metadata was updated.

## Harmonisation issues

The following were working notes used to capture the different issues for each of the variables across the relevant datasets.

### IPAQ Short Form

The following list is not exhaustive, but was simply used to initially capture most of the issues when validating the short-form IPAQ datasets (G220, G222, G126, G227 and G228):

- [x] `W`
    - [x] One row (or two for WALK) where `VIG_W` is 1, and no following data -> Leave as is (missing data should be None)
    - [x] G227, values of 999 -> None
- [x] `D`
    - [x] Instances of None where `VIG_W` is 1 (it appears to be missing) -> leave `VIG_D` as None
    - [x] G227 and G228, 999 -> None
    - [x] G126, `WALK_D` is 0 when `WALK_W` is 1 -> None
    - [x] G126, `WALK_D` is 20 for one case (error?) -> None
- [x] `HPD`
    - [x] Instances of None where `VIG_W` is 1 and `VIG_D` is between 1-7 -> Same as `VIG_D`
    - [x] G227 and G228, several values >18 (32, 45, 60, 70) -> 
    - [x] G228, values of 0.5, 1.5, etc. ->
    - [x] G126, 999
    - [x] G126, value of 30
    - [x] G126, 888 when `WALK_W` is 0
- [x] `MPD`
    - [x] Instances of None where `VIG_W` is 1 and `VIG_D` is between 1-7 -> Same as `VIG_D`
    - [x] G228, several values of 60 or 90 (generally HPD = 0) -> 
    - [x] G126, 999
    - [x] G126, 888 when `WALK_W` is 0
- [x] `MINS`
    - [x] One instance of None where all prior `VIG` values were None, bar `VIG_W` -> Update test to allow this?
    - [x] G227, there are values >180 -> cap at 180
    - [x] G227, values of 9999 where `VIG_W` is 999
    - [x] G228, issues when HPD is large (>10); sometimes MINS = HPD -> ?? (cut at 18, or change to MPD when 30, 45, etc. - representing MPDs?)
    - [x] G126, `MINS` = 180 when HPD = 2? -> Recalculate MINS
    - [x] G126, `WALK_MINS` = 61 for two cases (where HPD = 1, MPD = 0)
- [x] `MET`
    - [x] G222, an instance of None where there's a valid `VIG_MINS` but `VIG_D` was None (missing) -> keep as None (update test to allow)
    - [x] G227, values of 999999
- [x] `TOT_MET`
    - [x] Equal to sum of MET values, where Null should be filled with 0 if any valid data
    - [x] G227, values of 99999
    - [x] G227, values of 99999 even when three legitimate values for MET? -> recalculate TOT_MET

### IPAQ Long Form

G217_TeenQ was the only dataset that captured the long version of IPAQ.

- [x] G217
    - [x] Work
        - [x] MPD - a few instances for VIG, MOD and WALK between 1 and 9 (could be mistaken for HPD) -> None
        - [x] TOT_WORK_MET - 374 cases where does not check out
    - [x] Transport
        - [x] HPD - one case with MV and one case with WALK where HPD >= 20 hours -> None
        - [x] MPD - A handful of cases with MPD = 60 mins -> If HPD is 0 then HPD = 1 and MPD = 0 otherwise None
        - [x] MPD - A number of cases with MPD between 1 and 9 mins -> None
        - [x] TOT_TRANS_MET - X cases where does not check out
    - [x] Home
        - [x] OUT_VIG_D - 99 cases where OUT_VIG is 1 and OUT_VIG_D is 0 (values for HPD and MPD) -> None
        - [x] OUT_VIG_HPD AND OUT_VIG_MPD - clearly the values are in the wrong columns -> swap col names
        - [x] HPD - one case where IN_MOD_HPD = 30 -> None
        - [x] MPD - number of cases where OUT/IN_MOD_HPD < 10 -> None
        - [x] TOT_HOME_MET - 346 cases where does not check out
    - [x] Leisure
        - [x] HPD - one case for MOD and one case for WALK where HPD > 16 -> 
        - [x] MPD - six cases (3 for vig, 2 for mod, 1 for walk) where MPD = 60 and HPD = 0 ->
        - [x] MPD - number of cases where MPD between 1 and 9 mins -> None
        - [x] TOT_LSR_MET - 410 cases where does not check out
    - [x] Sitting, Standing and Lying
         - [x] STAND_WE_HPD - two cases where HPD is > 16 (looks to be minutes - 20 and 30)
         - [x] LYING_WD_HPD - one case where HPD is > 16 (looks to be minutes - 45)

Harmonising:
- [x] Swap column names for `G217_IPAQ_OUT_VIG_HPD` and `G217_IPAQ_OUT_VIG_MPD` (done in interim dataset)
- [x] For D = 0: if _ (parent variable) is 1 -> None
- [x] For HPD > 16: if MPD = 0 and HPD % 5 = 0 and 20 <= HPD < 60 then MPD = HPD and HPD = 0 otherwise None
- [x] For MPD < 10: None
- [x] For MPD = 60: if HPD = 0 then HPD = 1 and MPD = 0 otherwise None
- [x] MET -> recalculate
- [x] CAT -> recalculate


# Issues

## Data

- [x] `W`
    - [x] One row (or two for WALK) where `VIG_W` is 1, and no following data -> Leave as is (missing data should be None)
    - [x] G227, values of 999
- [ ] `D`
    - [x] Instances of None where `VIG_W` is 1 (it appears to be missing) -> leave `VIG_D` as None
    - [x] G227 and G228, 999 -> None?
    - [x] G126, `WALK_D` is 0 when `WALK_W` is 1 -> None
    - [ ] G126, `WALK_D` is 20 for one case (error?) -> None
- [ ] `HPD`
    - [x] Instances of None where `VIG_W` is 1 and `VIG_D` is between 1-7 -> Same as `VIG_D`
    - [ ] G227 and G228, several values >18 (32, 45, 60, 70) -> 
    - [ ] G228, values of 0.5, 1.5, etc. ->
    - [x] G126, 999
    - [ ] G126, value of 30
    - [x] G126, 888 when `WALK_W` is 0
- [ ] `MPD`
    - [x] Instances of None where `VIG_W` is 1 and `VIG_D` is between 1-7 -> Same as `VIG_D`
    - [ ] G228, several values of 60 or 90 (generally HPD = 0) -> 
    - [x] G126, 999
    - [x] G126, 888 when `WALK_W` is 0
- [ ] `MINS`
    - [ ] One instance of None where all prior `VIG` values were None, bar `VIG_W` -> Update test to allow this?
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

### IPAQ CATEGORY AMBIGUITY

G228, ID: 10840
MOD x 2 (15 mins) and WALK x 3 (45 mins) for TOT_MET 565.5 -> 0 or 1?

## Script

G227, G228 and G126 don't have null values for VIG_W -> the check_null fn fails (no tests run).
=> make `validate_ipaq` more flexible by choosing which functions to "mix-in"?

Create a temporary binary dummy variable IPAQ_DONE, where it's 1 if there's valid data for any of VIG, MOD or WALK cols, and otherwise 0
=> If IPAQ_DONE is 1, all MINS and MET values should be >= 0 (no None values)
=> Also, when IPAQ_DONE is 1, where there's missing data for VIG_D, VIG_W, etc. they should be None
=> OR should MINS/MET values also be None where their prior data is invalid, but TOT_MET should be calculated based on None = 0?
=> If IPAQ_DONE is 0, ALL exercise-related variables should be None

- [x] G220
- [ ] G222
    - [ ] One instance of `G222_IPAQ_WALK_HPD` = 20
- [ ] G227
    - [ ] Several instances for VIG, MOD and WALK - HPD and MPD (HPD > 18, MPD >= 60)
    - [ ] One instance of WALK_HPD = 1 when WALK_W = None
- [ ] G228
    - [ ] Several instances for VIG, MOD and WALK - HPD and MPD (HPD > 18, MPD >= 60)
    - [ ] One instance of VIG_MPD = 1 when VIG_W = None
- [ ] G126
    - [ ] Several instances for VIG, MOD and WALK - HPD and MPD (HPD > 18, MPD >= 60)

- [ ] G217
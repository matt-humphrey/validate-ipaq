from odyssey.core import Metadata

# Define parameters which are commonly used across metadata to avoid repetition
weekly_activity = {
    "field_type": "Numeric",
    "field_values": {0: "No", 1: "Yes"},
    "field_width": 1,
    "decimals": 0,
    "variable_type": "nominal",
}

days = {
    "field_type": "Numeric",
    "field_values": {},
    "field_width": 1,
    "decimals": 0,
    "variable_type": "ordinal",
}

hpd_and_mpd = {
    "field_type": "Numeric",
    "field_values": {},
    "field_width": 2,
    "decimals": 0,
    "variable_type": "scale",
}

mins = {
    "field_type": "Numeric",
    "field_values": {},
    "field_width": 3,
    "decimals": 0,
    "variable_type": "scale",
}

met = {
    "field_type": "Numeric",
    "field_values": {},
    "field_width": 5,
    "decimals": 2,
    "variable_type": "scale",
}

IPAQ_VIG_W = Metadata(
    variable_basename = r"IPAQ_VIG_W",
    label = "IPAQ - Vigorous activity in the last 7 days",
    **weekly_activity
)

IPAQ_VIG_D = Metadata(
    variable_basename = r"IPAQ_VIG_D",
    label = "IPAQ - Vigorous activity number of days in last week",
    **days
)

IPAQ_VIG_HPD = Metadata(
    variable_basename = r"IPAQ_VIG_HPD",
    label = "IPAQ - Vigorous activity number of hours per day",
    **hpd_and_mpd
)

IPAQ_VIG_MPD = Metadata(
    variable_basename = r"IPAQ_VIG_MPD",
    label = "IPAQ - Vigorous activity number of minutes per day",
    **hpd_and_mpd
)

IPAQ_MOD_W = Metadata(
    variable_basename = r"IPAQ_MOD_W",
    label = "IPAQ - Moderate activity in the last 7 days - Y/N",
    **weekly_activity
)

IPAQ_MOD_D = Metadata(
    variable_basename = r"IPAQ_MOD_D",
    label = "IPAQ - Moderate activity number of days in last week",
    **days
)

IPAQ_MOD_HPD = Metadata(
    variable_basename = r"IPAQ_MOD_HPD",
    label = "IPAQ - Moderate activity number of hours per day",
    **hpd_and_mpd
)

IPAQ_MOD_MPD = Metadata(
    variable_basename = r"IPAQ_MOD_MPD",
    label = "IPAQ - Moderate activity number of minutes per day",
    **hpd_and_mpd
)

IPAQ_WALK_W = Metadata(
    variable_basename = r"IPAQ_WALK_W",
    label = "IPAQ - Walking activity in the last 7 days - Y/N",
    **weekly_activity
)

IPAQ_WALK_D = Metadata(
    variable_basename = r"IPAQ_WALK_D",
    label = "IPAQ - Walking number of days in last week",
    **days
)

IPAQ_WALK_HPD = Metadata(
    variable_basename = r"IPAQ_WALK_HPD",
    label = "IPAQ - Walking number of hours per day",
    **hpd_and_mpd
)

IPAQ_WALK_MPD = Metadata(
    variable_basename = r"IPAQ_WALK_MPD",
    label = "IPAQ - Walking number of minutes per day",
    **hpd_and_mpd
)

IPAQ_SIT_WD_HPD = Metadata(
    variable_basename = r"IPAQ_SIT_WD_HPD",
    label = "IPAQ - Sitting hours per day on a weekday",
    **hpd_and_mpd
)

IPAQ_SIT_WD_MPD = Metadata(
    variable_basename = r"IPAQ_SIT_WD_MPD",
    label = "IPAQ - Sitting minutes per day on a weekday",
    **hpd_and_mpd
)

IPAQ_SIT_WD_TRUNC = Metadata(
    variable_basename = r"IPAQ_SIT_WD_TRUNC",
    label = "IPAQ - Sitting total minutes per weekday - truncated <= 960mins",
    field_values = {},
    field_type = "Numeric",
    field_width = 3,
    decimals = 0,
    variable_type = "scale",
)

IPAQ_SIT_WE_HPD = Metadata(
    variable_basename = r"IPAQ_SIT_WE_HPD",
    label = "IPAQ - Sitting hours per day on a weekend day",
    **hpd_and_mpd
)

IPAQ_SIT_WE_MPD = Metadata(
    variable_basename = r"IPAQ_SIT_WE_MPD",
    label = "IPAQ - Sitting minutes per day on a weekend day",
    **hpd_and_mpd
)

IPAQ_SIT_WE_TRUNC = Metadata(
    variable_basename = r"IPAQ_SIT_WE_TRUNC",
    label = "IPAQ - Sitting total minutes per weekend day - truncated <= 960mins",
    field_values = {},
    field_type = "Numeric",
    field_width = 3,
    decimals = 0,
    variable_type = "scale",
)

IPAQ_VIG_MINS = Metadata(
    variable_basename = r"IPAQ_VIG_MINS",
    label = "IPAQ - Vigorous activity total minutes per day",
    **mins
)

IPAQ_MOD_MINS = Metadata(
    variable_basename = r"IPAQ_MOD_MINS",
    label = "IPAQ - Moderate activity total minutes per day",
    **mins
)

IPAQ_WALK_MINS = Metadata(
    variable_basename = r"IPAQ_WALK_MINS",
    label = "IPAQ - Walking total minutes per day",
    **mins
)

IPAQ_VIG_MET = Metadata(
    variable_basename = r"IPAQ_VIG_MET",
    label = "IPAQ - Vigorous activity MET minutes per week",
    **met
)

IPAQ_MOD_MET = Metadata(
    variable_basename = r"IPAQ_MOD_MET",
    label = "IPAQ - Moderate activity MET minutes per week",
    **met
)

IPAQ_WALK_MET = Metadata(
    variable_basename = r"IPAQ_WALK_MET",
    label = "IPAQ - Walking MET minutes per week",
    **met
)

IPAQ_TOT_MET = Metadata(
    variable_basename = r"IPAQ_TOT_MET",
    label = "IPAQ - Total MET minutes per week",
    **met
)

IPAQ_CAT = Metadata(
    variable_basename = r"IPAQ_CAT",
    label = "IPAQ - Physical activity category",
    field_values = {0: "Low", 1: "Moderate", 2: "High"},
    field_type = "Numeric",
    field_width = 1,
    decimals = 0,
    variable_type = "nominal",
)

### Additional metadata for IPAQ long format

IPAQ_JOB = Metadata(
    variable_basename = r"IPAQ_JOB$",
    label = "IPAQ - Have a job or do unpaid work outside home",
    **weekly_activity
)

IPAQ_JOB_VIG = Metadata(
    variable_basename = r"IPAQ_JOB_VIG$",
    label = "IPAQ - Vigorous job-related physical activity in the last 7 days",
    **weekly_activity
)

IPAQ_JOB_VIG_D = Metadata(
    variable_basename = r"IPAQ_JOB_VIG_D",
    label = "IPAQ - Vigorous job-related physical activity number of days",
    **days
)

IPAQ_JOB_VIG_HPD = Metadata(
    variable_basename = r"IPAQ_JOB_VIG_HPD",
    label = "IPAQ - Vigorous job-related physical activity number of hours per day",
    **hpd_and_mpd
)

IPAQ_JOB_VIG_MPD = Metadata(
    variable_basename = r"IPAQ_JOB_VIG_MPD",
    label = "IPAQ - Vigorous job-related physical activity number of minutes per day",
    **hpd_and_mpd
)

IPAQ_JOB_MOD = Metadata(
    variable_basename = r"IPAQ_JOB_MOD$",
    label = "IPAQ - Moderate job-related physical activity in the last 7 days",
    **weekly_activity
)

IPAQ_JOB_MOD_D = Metadata(
    variable_basename = r"IPAQ_JOB_MOD_D",
    label = "IPAQ - Moderate job-related physical activity number of days",
    **days
)

IPAQ_JOB_MOD_HPD = Metadata(
    variable_basename = r"IPAQ_JOB_MOD_HPD",
    label = "IPAQ - Moderate job-related physical activity number of hours per day",
    **hpd_and_mpd
)

IPAQ_JOB_MOD_MPD = Metadata(
    variable_basename = r"IPAQ_JOB_MOD_MPD",
    label = "IPAQ - Moderate job-related physical activity number of minutes per day",
    **hpd_and_mpd
)

IPAQ_JOB_WALK = Metadata(
    variable_basename = r"IPAQ_JOB_WALK$",
    label = "IPAQ - Walking job-related in the last 7 days",
    **weekly_activity
)

IPAQ_JOB_WALK_D = Metadata(
    variable_basename = r"IPAQ_JOB_WALK_D",
    label = "IPAQ - Walking job-related number of days",
    **days
)

IPAQ_JOB_WALK_HPD = Metadata(
    variable_basename = r"IPAQ_JOB_WALK_HPD",
    label = "IPAQ - Walking job-related number of hours per day",
    **hpd_and_mpd
)

IPAQ_JOB_WALK_MPD = Metadata(
    variable_basename = r"IPAQ_JOB_WALK_MPD",
    label = "IPAQ - Walking job-related number of minutes per day",
    **hpd_and_mpd
)

IPAQ_TRANS_MV = Metadata(
    variable_basename = r"IPAQ_TRANS_MV$",
    label = "IPAQ - Travelled in a motor vehicle in the last 7 days",
    **weekly_activity
)

IPAQ_TRANS_MV_D = Metadata(
    variable_basename = r"IPAQ_TRANS_MV_D",
    label = "IPAQ - Travelled in a motor vehicle number of days",
    **days
)

IPAQ_TRANS_MV_HPD = Metadata(
    variable_basename = r"IPAQ_TRANS_MV_HPD",
    label = "IPAQ - Travelled in a motor vehicle number of hours per day",
    **hpd_and_mpd
)

IPAQ_TRANS_MV_MPD = Metadata(
    variable_basename = r"IPAQ_TRANS_MV_MPD",
    label = "IPAQ - Travelled in a motor vehicle number of minutes per day",
    **hpd_and_mpd
)

IPAQ_TRANS_BIKE = Metadata(
    variable_basename = r"IPAQ_TRANS_BIKE$",
    label = "IPAQ - Travelled by bicycle in the last 7 days",
    **weekly_activity
)

IPAQ_TRANS_BIKE_D = Metadata(
    variable_basename = r"IPAQ_TRANS_BIKE_D",
    label = "IPAQ - Travelled by bicycle number of days",
    **days
)

IPAQ_TRANS_BIKE_HPD = Metadata(
    variable_basename = r"IPAQ_TRANS_BIKE_HPD",
    label = "IPAQ - Travelled by bicycle number of hours per day",
    **hpd_and_mpd
)

IPAQ_TRANS_BIKE_MPD = Metadata(
    variable_basename = r"IPAQ_TRANS_BIKE_MPD",
    label = "IPAQ - Travelled by bicycle number of minutes per day",
    **hpd_and_mpd
)

IPAQ_TRANS_WALK = Metadata(
    variable_basename = r"IPAQ_TRANS_WALK$",
    label = "IPAQ - Travelled by walking in the last 7 days",
    **weekly_activity
)

IPAQ_TRANS_WALK_D = Metadata(
    variable_basename = r"IPAQ_TRANS_WALK_D",
    label = "IPAQ - Travelled by walking number of days",
    **days
)

IPAQ_TRANS_WALK_HPD = Metadata(
    variable_basename = r"IPAQ_TRANS_WALK_HPD",
    label = "IPAQ - Travelled by walking number of hours per day",
    **hpd_and_mpd
)

IPAQ_TRANS_WALK_MPD = Metadata(
    variable_basename = r"IPAQ_TRANS_WALK_MPD",
    label = "IPAQ - Travelled by walking number of minutes per day",
    **hpd_and_mpd
)

IPAQ_HOME_OUT_VIG = Metadata(
    variable_basename = r"IPAQ_HOME_OUT_VIG$",
    label = "IPAQ - Vigorous physical activity in the garden or yard in the last 7 days",
    **weekly_activity
)

IPAQ_HOME_OUT_VIG_D = Metadata(
    variable_basename = r"IPAQ_HOME_OUT_VIG_D",
    label = "IPAQ - Vigorous physical activity in the garden or yard number of days",
    **days
)

IPAQ_HOME_OUT_VIG_HPD = Metadata(
    variable_basename = r"IPAQ_HOME_OUT_VIG_HPD",
    label = "IPAQ - Vigorous physical activity in the garden or yard number of hours per day",
    **hpd_and_mpd
)

IPAQ_HOME_OUT_VIG_MPD = Metadata(
    variable_basename = r"IPAQ_HOME_OUT_VIG_MPD",
    label = "IPAQ - Vigorous physical activity in the garden or yard number of minutes per day",
    **hpd_and_mpd
)

IPAQ_HOME_OUT_MOD = Metadata(
    variable_basename = r"IPAQ_HOME_OUT_MOD$",
    label = "IPAQ - Moderate physical activity in the garden or yard in the last 7 days",
    **weekly_activity
)

IPAQ_HOME_OUT_MOD_D = Metadata(
    variable_basename = r"IPAQ_HOME_OUT_MOD_D",
    label = "IPAQ - Moderate physical activity in the garden or yard number of days",
    **days
)

IPAQ_HOME_OUT_MOD_HPD = Metadata(
    variable_basename = r"IPAQ_HOME_OUT_MOD_HPD",
    label = "IPAQ - Moderate physical activity in the garden or yard number of hours per day",
    **hpd_and_mpd
)

IPAQ_HOME_OUT_MOD_MPD = Metadata(
    variable_basename = r"IPAQ_HOME_OUT_MOD_MPD",
    label = "IPAQ - Moderate physical activity in the garden or yard number of minutes per day",
    **hpd_and_mpd
)

IPAQ_HOME_IN_MOD = Metadata(
    variable_basename = r"IPAQ_HOME_IN_MOD$",
    label = "IPAQ - Moderate physical activity inside your home in the last 7 days",
    **weekly_activity
)

IPAQ_HOME_IN_MOD_D = Metadata(
    variable_basename = r"IPAQ_HOME_IN_MOD_D",
    label = "IPAQ - Moderate physical activity inside your home number of days",
    **days
)

IPAQ_HOME_IN_MOD_HPD = Metadata(
    variable_basename = r"IPAQ_HOME_IN_MOD_HPD",
    label = "IPAQ - Moderate physical activity inside your home number of hours per day",
    **hpd_and_mpd
)

IPAQ_HOME_IN_MOD_MPD = Metadata(
    variable_basename = r"IPAQ_HOME_IN_MOD_MPD",
    label = "IPAQ - Moderate physical activity inside your home number of minutes per day",
    **hpd_and_mpd
)

IPAQ_LSR_VIG = Metadata(
    variable_basename = r"IPAQ_LSR_VIG$",
    label = "IPAQ - Vigorous physical activity for leisure in the last 7 days",
    **weekly_activity
)

IPAQ_LSR_VIG_D = Metadata(
    variable_basename = r"IPAQ_LSR_VIG_D",
    label = "IPAQ - Vigorous physical activity for leisure number of days",
    **days
)

IPAQ_LSR_VIG_HPD = Metadata(
    variable_basename = r"IPAQ_LSR_VIG_HPD",
    label = "IPAQ - Vigorous physical activity for leisure number of hours per day",
    **hpd_and_mpd
)

IPAQ_LSR_VIG_MPD = Metadata(
    variable_basename = r"IPAQ_LSR_VIG_MPD",
    label = "IPAQ - Vigorous physical activity for leisure number of minutes per day",
    **hpd_and_mpd
)

IPAQ_LSR_MOD = Metadata(
    variable_basename = r"IPAQ_LSR_MOD$",
    label = "IPAQ - Moderate physical activity for leisure in the last 7 days",
    **weekly_activity
)

IPAQ_LSR_MOD_D = Metadata(
    variable_basename = r"IPAQ_LSR_MOD_D",
    label = "IPAQ - Moderate physical activity for leisure number of days",
    **days
)

IPAQ_LSR_MOD_HPD = Metadata(
    variable_basename = r"IPAQ_LSR_MOD_HPD",
    label = "IPAQ - Moderate physical activity for leisure number of hours per day",
    **hpd_and_mpd
)

IPAQ_LSR_MOD_MPD = Metadata(
    variable_basename = r"IPAQ_LSR_MOD_MPD",
    label = "IPAQ - Moderate physical activity for leisure number of minutes per day",
    **hpd_and_mpd
)

IPAQ_LSR_WALK = Metadata(
    variable_basename = r"IPAQ_LSR_WALK$",
    label = "IPAQ - Walking for leisure in the last 7 days",
    **weekly_activity
)

IPAQ_LSR_WALK_D = Metadata(
    variable_basename = r"IPAQ_LSR_WALK_D",
    label = "IPAQ - Walking for leisure number of days",
    **days
)

IPAQ_LSR_WALK_HPD = Metadata(
    variable_basename = r"IPAQ_LSR_WALK_HPD",
    label = "IPAQ - Walking for leisure number of hours per day",
    **hpd_and_mpd
)

IPAQ_LSR_WALK_MPD = Metadata(
    variable_basename = r"IPAQ_LSR_WALK_MPD",
    label = "IPAQ - Walking for leisure number of minutes per day",
    **hpd_and_mpd
)

IPAQ_STAND_WD_HPD = Metadata(
    variable_basename = r"IPAQ_STAND_WD_HPD",
    label = "IPAQ - Standing hours per day on a weekday",
    **hpd_and_mpd
)

IPAQ_STAND_WD_MPD = Metadata(
    variable_basename = r"IPAQ_STAND_WD_MPD",
    label = "IPAQ - Standing minutes per day on a weekday",
    **hpd_and_mpd
)

IPAQ_STAND_WE_HPD = Metadata(
    variable_basename = r"IPAQ_STAND_WE_HPD",
    label = "IPAQ - Standing hours per day on a weekday",
    **hpd_and_mpd
)

IPAQ_STAND_WE_MPD = Metadata(
    variable_basename = r"IPAQ_STAND_WE_MPD",
    label = "IPAQ - Standing minutes per day on a weekday",
    **hpd_and_mpd
)

IPAQ_LYING_WD_HPD = Metadata(
    variable_basename = r"IPAQ_LYING_WD_HPD",
    label = "IPAQ - Lying down hours per day on a weekday",
    **hpd_and_mpd
)

IPAQ_LYING_WD_MPD = Metadata(
    variable_basename = r"IPAQ_LYING_WD_MPD",
    label = "IPAQ - Lying down minutes per day on a weekday",
    **hpd_and_mpd
)

IPAQ_LYING_WE_HPD = Metadata(
    variable_basename = r"IPAQ_LYING_WE_HPD",
    label = "IPAQ - Lying down hours per day on a weekday",
    **hpd_and_mpd
)

IPAQ_LYING_WE_MPD = Metadata(
    variable_basename = r"IPAQ_LYING_WE_MPD",
    label = "IPAQ - Lying down minutes per day on a weekday",
    **hpd_and_mpd
)

IPAQ_JOB_VIG_MET = Metadata(
    variable_basename = r"IPAQ_JOB_VIG_MET",
    label = "IPAQ Work - Vigorous MET minutes per week",
    **met
)

IPAQ_JOB_MOD_MET = Metadata(
    variable_basename = r"IPAQ_JOB_MOD_MET",
    label = "IPAQ Work - Moderate MET minutes per week",
    **met
)

IPAQ_JOB_WALK_MET = Metadata(
    variable_basename = r"IPAQ_JOB_WALK_MET",
    label = "IPAQ Work - Walking MET minutes per week",
    **met
)

IPAQ_TRANS_BIKE_MET = Metadata(
    variable_basename = r"IPAQ_TRANS_BIKE_MET",
    label = "IPAQ Transport - Bike MET minutes per week",
    **met
)

IPAQ_TRANS_WALK_MET = Metadata(
    variable_basename = r"IPAQ_TRANS_WALK_MET",
    label = "IPAQ Transport - Walking MET minutes per week",
    **met
)

IPAQ_HOME_OUT_VIG_MET = Metadata(
    variable_basename = r"IPAQ_HOME_OUT_VIG_MET",
    label = "IPAQ Home - Vigorous outside MET minutes per week",
    **met
)

IPAQ_HOME_OUT_MOD_MET = Metadata(
    variable_basename = r"IPAQ_HOME_OUT_MOD_MET",
    label = "IPAQ Home - Moderate outside MET minutes per week",
    **met
)

IPAQ_HOME_IN_MOD_MET = Metadata(
    variable_basename = r"IPAQ_HOME_IN_MOD_MET",
    label = "IPAQ Home - Moderate inside MET minutes per week",
    **met
)

IPAQ_LSR_VIG_MET = Metadata(
    variable_basename = r"IPAQ_LSR_VIG_MET",
    label = "IPAQ Leisure - Vigorous MET minutes per week",
    **met
)

IPAQ_LSR_MOD_MET = Metadata(
    variable_basename = r"IPAQ_LSR_MOD_MET",
    label = "IPAQ Leisure - Moderate MET minutes per week",
    **met
)

IPAQ_LSR_WALK_MET = Metadata(
    variable_basename = r"IPAQ_LSR_WALK_MET",
    label = "IPAQ Leisure - Walking MET minutes per week",
    **met
)

IPAQ_TOT_WORK_MET = Metadata(
    variable_basename = r"IPAQ_TOT_WORK_MET",
    label = "IPAQ Work - Total MET minutes per week",
    **met
)

IPAQ_TOT_TRANS_MET = Metadata(
    variable_basename = r"IPAQ_TOT_TRANS_MET",
    label = "IPAQ Transport - Total MET minutes per week",
    **met
)

IPAQ_TOT_HOME_MET = Metadata(
    variable_basename = r"IPAQ_TOT_HOME_MET",
    label = "IPAQ Home - Total MET minutes per week",
    **met
)

IPAQ_TOT_LSR_MET = Metadata(
    variable_basename = r"IPAQ_TOT_LSR_MET",
    label = "IPAQ Leisure - Total MET minutes per week",
    **met
)

###

METADATA = [
    IPAQ_VIG_W, IPAQ_VIG_D, IPAQ_VIG_HPD, IPAQ_VIG_MPD, 
    IPAQ_MOD_W, IPAQ_MOD_D, IPAQ_MOD_HPD, IPAQ_MOD_MPD, 
    IPAQ_WALK_W, IPAQ_WALK_D, IPAQ_WALK_HPD, IPAQ_WALK_MPD, 
    IPAQ_SIT_WD_HPD, IPAQ_SIT_WD_MPD, IPAQ_SIT_WD_TRUNC, 
    IPAQ_SIT_WE_HPD, IPAQ_SIT_WE_MPD, IPAQ_SIT_WE_TRUNC, 
    IPAQ_VIG_MINS, IPAQ_MOD_MINS, IPAQ_WALK_MINS, 
    IPAQ_VIG_MET, IPAQ_MOD_MET, IPAQ_WALK_MET, 
    IPAQ_TOT_MET, IPAQ_CAT
]

LONG_METADATA = [
    IPAQ_JOB, IPAQ_JOB_VIG, IPAQ_JOB_VIG_D, IPAQ_JOB_VIG_HPD, IPAQ_JOB_VIG_MPD, 
    IPAQ_JOB_MOD, IPAQ_JOB_MOD_D, IPAQ_JOB_MOD_HPD, IPAQ_JOB_MOD_MPD, 
    IPAQ_JOB_WALK, IPAQ_JOB_WALK_D, IPAQ_JOB_WALK_HPD, IPAQ_JOB_WALK_MPD,
    IPAQ_TRANS_MV, IPAQ_TRANS_MV_D, IPAQ_TRANS_MV_HPD, IPAQ_TRANS_MV_MPD, 
    IPAQ_TRANS_BIKE, IPAQ_TRANS_BIKE_D, IPAQ_TRANS_BIKE_HPD, IPAQ_TRANS_BIKE_MPD, 
    IPAQ_TRANS_WALK, IPAQ_TRANS_WALK_D, IPAQ_TRANS_WALK_HPD, IPAQ_TRANS_WALK_MPD, 
    IPAQ_HOME_OUT_VIG, IPAQ_HOME_OUT_VIG_D, IPAQ_HOME_OUT_VIG_HPD, IPAQ_HOME_OUT_VIG_MPD, 
    IPAQ_HOME_OUT_MOD, IPAQ_HOME_OUT_MOD_D, IPAQ_HOME_OUT_MOD_HPD, IPAQ_HOME_OUT_MOD_MPD, 
    IPAQ_HOME_IN_MOD, IPAQ_HOME_IN_MOD_D, IPAQ_HOME_IN_MOD_HPD, IPAQ_HOME_IN_MOD_MPD, 
    IPAQ_LSR_VIG, IPAQ_LSR_VIG_D, IPAQ_LSR_VIG_HPD, IPAQ_LSR_VIG_MPD, 
    IPAQ_LSR_MOD, IPAQ_LSR_MOD_D, IPAQ_LSR_MOD_HPD, IPAQ_LSR_MOD_MPD, 
    IPAQ_LSR_WALK, IPAQ_LSR_WALK_D, IPAQ_LSR_WALK_HPD, IPAQ_LSR_WALK_MPD, 
    IPAQ_SIT_WD_HPD, IPAQ_SIT_WD_MPD, IPAQ_SIT_WD_TRUNC, 
    IPAQ_SIT_WE_HPD, IPAQ_SIT_WE_MPD, IPAQ_SIT_WE_TRUNC,
    IPAQ_STAND_WD_HPD, IPAQ_STAND_WD_MPD, IPAQ_STAND_WE_HPD, IPAQ_STAND_WE_MPD, 
    IPAQ_LYING_WD_HPD, IPAQ_LYING_WD_MPD, IPAQ_LYING_WE_HPD, IPAQ_LYING_WE_MPD, 
    IPAQ_JOB_VIG_MET, IPAQ_JOB_MOD_MET, IPAQ_JOB_WALK_MET, 
    IPAQ_TRANS_BIKE_MET, IPAQ_TRANS_WALK_MET, 
    IPAQ_HOME_OUT_VIG_MET, IPAQ_HOME_OUT_MOD_MET, IPAQ_HOME_IN_MOD_MET, 
    IPAQ_LSR_VIG_MET, IPAQ_LSR_MOD_MET, IPAQ_LSR_WALK_MET, IPAQ_TOT_WORK_MET, 
    IPAQ_TOT_TRANS_MET, IPAQ_TOT_HOME_MET, IPAQ_TOT_LSR_MET, 
    IPAQ_VIG_MET, IPAQ_MOD_MET, IPAQ_WALK_MET, 
    IPAQ_TOT_MET, IPAQ_CAT
]
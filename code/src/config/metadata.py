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
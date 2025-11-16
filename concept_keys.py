"""
Shared concept key constants for IPEDS crosswalks.

This module centralizes the canonical concept_key strings so they are
consistent across HD/IC, Enrollment, Admissions, and other surveys.
"""

# -------------------------------------------------------------------
# Stable institutional attributes (HD/IC spine)
# -------------------------------------------------------------------

STABLE_INSTITUTION_NAME = "STABLE_INSTITUTION_NAME"
STABLE_CONTROL = "STABLE_CONTROL"
STABLE_SECTOR = "STABLE_SECTOR"
STABLE_STFIPS = "STABLE_STFIPS"
STABLE_HBCU = "STABLE_HBCU"
STABLE_TRIBAL = "STABLE_TRIBAL"

# Future stable attributes can be added here, e.g.:
# STABLE_LOCALE = "STABLE_LOCALE"
# STABLE_DEGREE_GRANTING_STATUS = "STABLE_DEGREE_GRANTING_STATUS"

# -------------------------------------------------------------------
# Carnegie classification versions (HD spine)
# -------------------------------------------------------------------

CARNEGIE_2005 = "CARNEGIE_2005"
CARNEGIE_2010 = "CARNEGIE_2010"
CARNEGIE_2015 = "CARNEGIE_2015"
CARNEGIE_2018 = "CARNEGIE_2018"
CARNEGIE_2021 = "CARNEGIE_2021"

# -------------------------------------------------------------------
# Enrollment concepts (E12, EF, etc.) already used in
# CrossWalk Scripts/autofill_enrollment_crosswalk_core.py
# -------------------------------------------------------------------

E12_HEAD_ALL_TOT_ALL = "E12_HEAD_ALL_TOT_ALL"

EF_HEAD_ALL_TOT_ALL = "EF_HEAD_ALL_TOT_ALL"
EF_HEAD_FTFT_UG_DEGSEEK_TOT = "EF_HEAD_FTFT_UG_DEGSEEK_TOT"
EF_HEAD_FT_ALL_TOT_ALL = "EF_HEAD_FT_ALL_TOT_ALL"
EF_HEAD_FT_UG_TOT_ALL = "EF_HEAD_FT_UG_TOT_ALL"
EF_HEAD_FT_GR_TOT_ALL = "EF_HEAD_FT_GR_TOT_ALL"
EF_HEAD_FTFT_UG_RES_INSTATE = "EF_HEAD_FTFT_UG_RES_INSTATE"
EF_HEAD_FTFT_UG_RES_OUTSTATE = "EF_HEAD_FTFT_UG_RES_OUTSTATE"
EF_HEAD_FTFT_UG_RES_FOREIGN = "EF_HEAD_FTFT_UG_RES_FOREIGN"
EF_HEAD_FTFT_UG_RES_UNKNOWN = "EF_HEAD_FTFT_UG_RES_UNKNOWN"

# If you later add more E12/EF concepts, define them here first
# and then import them into the relevant autofill scripts.

# -------------------------------------------------------------------
# Admissions concepts (for HD/IC and any other admissions survey)
# NOTE: These are defined now for consistency; you can wire them into
#       your HD autofill when you're ready.
# -------------------------------------------------------------------

ADM_APP_TOT_ALL = "ADM_APP_TOT_ALL"       # Total applicants (all)
ADM_ADMIT_TOT_ALL = "ADM_ADMIT_TOT_ALL"   # Total admitted (all)
ADM_ENRL_TOT_ALL = "ADM_ENRL_TOT_ALL"     # Total enrolled (from admits)
ADM_RATE_TOT_ALL = "ADM_RATE_TOT_ALL"     # Admit rate
ADM_YIELD_TOT_ALL = "ADM_YIELD_TOT_ALL"   # Yield rate

# You can extend these for sex, FTFT, etc., as needed, but do NOT
# rename existing constants without updating all downstream users.

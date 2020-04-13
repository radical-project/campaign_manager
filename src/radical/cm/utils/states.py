"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""


# ------------------------------------------------------------------------------
#  States

NEW = 0  # New campaign is submitted
PLANNING = 1  # Planning the exeuction of the campaign
EXECUTING = 2  # At least one workflow is executing
DONE = 3  # Campaign has finished successfully
FAILED = 4  # Campaign execution has failed
CANCELED = 5  # Campaign got canceled by the user.
CFINAL = [DONE, FAILED, CANCELED]  # Final states for a campaign.


state_dict = {0: 'NEW',
              1: 'PLANNING',
              2: 'EXECUTING',
              3: 'DONE',
              4: 'FAILED',
              5: 'CANCELED'
             }
# ------------------------------------------------------------------------------

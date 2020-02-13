#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

verbose  = os.environ.get('RADICAL_PILOT_VERBOSE', 'REPORT')
os.environ['RADICAL_PILOT_VERBOSE'] = verbose

import radical.pilot as rp
import radical.utils as ru

# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------


#------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # read the config used for resource details
        report.header('submit pilots')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pd_init = {
                'resource'      : 'local.localhost_anaconda',
                'runtime'       : 120,  # pilot runtime (min)
                'exit_on_error' : True,
                'cores'         : 6,
                }
        pdesc = rp.ComputePilotDescription(pd_init)

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)


        report.header('submit units')

        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        report.info('creating unit description(s)\n\t')

        cuds = list()
        # create a new CU description, and fill it.
        # Here we don't use dict initialization.
        cud = rp.ComputeUnitDescription()
        cud.pre_exec   = ['conda activate campaign']
        cud.executable = 'python'
        cud.arguments  = ['static_resources_exp.py']
        
        # this is a shortcut for:
        cud.input_staging  = {'source': 'client:///static_resources_exp.py', 
                              'target': 'unit:///static_resources_exp.py',
                              'action': rp.TRANSFER}
        cuds.append(cud)
        cud = rp.ComputeUnitDescription()
        cud.pre_exec   = ['conda activate campaign']
        cud.executable = 'python'
        cud.arguments  = ['static_resources_exp2.py']
        
        # this is a shortcut for:
        cud.input_staging  = {'source': 'client:///static_resources_exp2.py', 
                              'target': 'unit:///static_resources_exp2.py',
                              'action': rp.TRANSFER}
        cuds.append(cud)
        cud = rp.ComputeUnitDescription()
        cud.pre_exec   = ['conda activate campaign']
        cud.executable = 'python'
        cud.arguments  = ['static_resources_exp3.py']
        
        # this is a shortcut for:
        cud.input_staging  = {'source': 'client:///static_resources_exp3.py', 
                              'target': 'unit:///static_resources_exp3.py',
                              'action': rp.TRANSFER}
        cuds.append(cud)
        cud = rp.ComputeUnitDescription()
        cud.pre_exec   = ['conda activate campaign']
        cud.executable = 'python'
        cud.arguments  = ['static_resources_exp4.py']
        
        # this is a shortcut for:
        cud.input_staging  = {'source': 'client:///static_resources_exp4.py', 
                              'target': 'unit:///static_resources_exp4.py',
                              'action': rp.TRANSFER}
        cuds.append(cud)
        cud = rp.ComputeUnitDescription()
        cud.pre_exec   = ['conda activate campaign']
        cud.executable = 'python'
        cud.arguments  = ['static_resources_exp5.py']
        
        # this is a shortcut for:
        cud.input_staging  = {'source': 'client:///static_resources_exp5.py', 
                              'target': 'unit:///static_resources_exp5.py',
                              'action': rp.TRANSFER}
        cuds.append(cud)
        cud = rp.ComputeUnitDescription()
        cud.pre_exec   = ['conda activate campaign']
        cud.executable = 'python'
        cud.arguments  = ['static_resources_exp6.py']
        
        # this is a shortcut for:
        cud.input_staging  = {'source': 'client:///static_resources_exp6.py', 
                              'target': 'unit:///static_resources_exp6.py',
                              'action': rp.TRANSFER}
        cuds.append(cud)
        report.progress()
        report.ok('>>ok\n')

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')
        umgr.wait_units()
    
    except Exception as e:
        # Something unexpected happened in the pilot code above
        report.error('caught Exception: %s\n' % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        report.warn('exit requested\n')

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        session.close()

    report.header()


#-------------------------------------------------------------------------------

import FW.Initialize.initialize_global_variables as iniVar
import FW.FW_logger as logger
from concurrent.futures import ThreadPoolExecutor
import importlib, time, multiprocessing, threading, datetime
import inspect
#from FW.FW_tags import tags, tests


def run_individual_script(test_main_fn,test_reporting_fn, current_test_name, dry_run=False, verbose_debug = False):
    """Trigger function for individual script execution.

    Parameters
    ----------
    test_main_fn : str
        name of test_main function from script
    test_reporting_fn : str
        name of test_reporting function from script
    current_test_name : str
        current test name
    dry_run : bool, default False
        If True, no data entered into automation execution database
    verbose_debug : bool, default False
        True if executing individual script in debug mode will give verbose error message if any

    returns
    --------
    None
    """
    print(f"Script execution started at = {datetime.datetime.now()}")
    vError = None

    print(f"=================Execution of Test {current_test_name} started =====================\n")


    if verbose_debug==False:
        try:
            # checking if our test_main_fn contains any arguments or not.
            if len(inspect.getfullargspec(test_main_fn).args)==0:
                test_main_fn()
            else:
                test_main_fn(current_test_name)
        except Exception as e:
            print("ERROR - " + str(e))
            iniVar.th_local.dict['logger'].append(str(threading.get_ident()) + " - ERROR - " + str(e))
            vError = "ERROR - " + str(e)
    
    if verbose_debug==True:
        if len(inspect.getfullargspec(test_main_fn).args) == 0:
            test_main_fn()
        else:
            test_main_fn(current_test_name)
            
    iniVar.dry_run = dry_run
    test_reporting_fn(testName=current_test_name, Error=vError)

    if iniVar.dry_run == False:  # Send entries to qTest for individual execution
        import FW.QTest_Integration.FW_pulse_integration as pulseInt
        report_file_path=iniVar.get_from_reporting_dict('report_path')
        pulseInt.send_result_data_to_qtest(report_file_path)

    print(f"Script execution time : {logger.get_from_reporting_dict('running_time')} sec")
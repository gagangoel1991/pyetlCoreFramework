import FW.Initialize.initialize_global_variables as iniVar
from concurrent.futures import ThreadPoolExecutor
import importlib, time, multiprocessing, threading, datetime,os
import FW.FW_logger as logger

gRrptCnt = 0
totaltestsCount = 0
lock = None

def runner(tests_and_path_list, exeType):
    """Runs multiple automated scripts in a batch, sequentially or parallelly. This is being called in Batch runner.

    Parameters
    -----------
    tests_and_path_list : touple of 2 lists
        List of test scripts names and list of their complete paths to be executed
    exeType : 'multithread 'or 'sequential'

    returns
    None
    """
    
    a= time.time()
    print(f"Script execution started at = {datetime.datetime.now()}")
    global gRrptCnt, totaltestsCount, lock

    tests_list = tests_and_path_list[0]  # name of tests to execute
    path_list = tests_and_path_list[1]  # full path of tests to be executed

    if exeType == "multithread":
        gRrptCnt=0
        totaltestsCount = len(tests_list)
        threadCnt = totaltestsCount
        lock = multiprocessing.Manager().Lock()
        with ThreadPoolExecutor(max_workers=threadCnt) as executor:
            futures = [executor.submit(_run_test, testName, path_name, lock) for testName, path_name in zip(tests_list, path_list)]
            for future in futures:
                future.result()
        print(f'Total time taken with multi-threaded executions : {time.time()-a}')
    
    
    
    if exeType == "sequential":
        
        b= time.time()
        gRrptCnt = 0
        totaltestsCount = len(tests_list)
        for testName,tstPath in zip(tests_list,path_list):
            a = time.time()
            #test_module = importlib.import_module(testName[:-3]) # access the module object from relative path
            #==========================
            # Access the module object from full path
            spec = importlib.util.spec_from_file_location(testName[:-3],tstPath)
            test_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(test_module)
            # ==========================
            print(f"=================  {gRrptCnt+1}/{totaltestsCount}.  Execution of Test {testName} started =====================\n")
            vError = None
            try:
                getattr(test_module, "test_main")(testName)
            except Exception as e:
                vError = str(e)
                iniVar.th_local.dict['logger'].append(str(threading.get_ident()) + " - ERROR - " + str(e))
                print(str(e))
            gRrptCnt =gRrptCnt+1
            
            getattr(test_module, "test_reporting")(gRrptCnt, testName, vError, totaltestsCount)
            
            #print(f'Total time taken: {time.time()-a} \n')
            print(f"Script execution time : {logger.get_from_reporting_dict('running_time')} sec \n")
        
        print(f'Sequential execution - total time taken for all tests: {time.time()-b}')

    # Send entries to qTest for batch or jenkins execution
    if iniVar.dry_run == False:
        import FW.QTest_Integration.FW_pulse_integration as pulseInt
        report_file_path = iniVar.get_from_reporting_dict('report_path')
        pulseInt.send_result_data_to_qtest(report_file_path)
    # after batch execution, print the email text in jenkins console and notification to teams if using Jenkins
    import FW.Jenkins_Integration.FW_jenkins_integration as jnk
    jnk._copy_report_to_shared_path_from_jenkins()
    jnk._print_summary_mail_in_console()
    jnk._print_overall_exec_status_in_console()
    jnk._teams_integration_from_jenkins()
            
        
def _run_test(testName, tstPath, lock):
    """Run test scripts, used internally only
    This function is used when the execution type is multithreaded.
    """
    global gRrptCnt, totaltestsCount
    print(f"================= Multi-threaded Execution of Test {testName} started =====================\n")
    #test_module = importlib.import_module(testName[:-3])  # access the module object from relative path
    # Below Access the module object from full path
    spec = importlib.util.spec_from_file_location(testName[:-3], tstPath)
    test_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(test_module)
    vError = None
    try:
        getattr(test_module, "test_main")(testName)
    except Exception as e:
        vError = str(e)
        iniVar.th_local.dict['logger'].append(str(threading.get_ident()) + " - ERROR - " + str(e))
        print(str(e))
    with lock:
        gRrptCnt =gRrptCnt+1
        getattr(test_module, "test_reporting")(gRrptCnt,testName, vError, totaltestsCount)
    
 
    
    
    
    
    
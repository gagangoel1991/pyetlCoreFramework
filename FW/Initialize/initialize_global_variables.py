current_project_path,current_project_root_path,current_project_test_path, dry_run = [None]*4   #reporting_dict
global_dict={}

from datetime import datetime
import threading, multiprocessing
import configparser, os, sys, pytz
from threading import RLock, local
from FW.FW_logger import add_in_reporting_dict, check_key_in_reporting_dict, get_from_reporting_dict

th_local = local()
lock = RLock()


def setupGlobalVariable(curr_project_path,curr_project_root_path,curr_project_test_path,initial_dict, var_TestName=''):
    """Initializes global variables and project variables

    Parameters
    ----------
    curr_project_path : string
        Current project path
    curr_project_root_path : string
        Current project root path
    curr_project_test_path : string
        Path of Test folder in the current project
    initial_dict : dict
        Blank dictionary object to initialize the reporting dictionary in the framework
    var_TestName : string, default is blank
        Name of the current executing test script

    returns
    ---------
    None
    """
    global current_project_path, current_project_root_path, current_project_test_path, th_local, dry_run    #reporting_dict
    dry_run = False  # if dry_run = True, execution record wouuld not be written in automation db.
    current_project_path = curr_project_path
    current_project_root_path = curr_project_root_path
    current_project_test_path = curr_project_test_path

    th_local.dict = initial_dict
    th_local.dict['logger'] = []
    th_local.dict['detail_tabs'] = dict()

    _add_in_reporting_dict_during_setup('testName', var_TestName.replace(".py",""), th_local.dict)
    read_project_config(th_local.dict)
    from FW.FW_exec_db_update import _check_version_compilance
    _check_version_compilance()

def read_project_config(reporting_dict):
    """Reads project config .ini file and stores the key-value pairs in reporting dictionary"""

    path_config = os.path.join(current_project_path, r"Configrations\project_config.ini")
    config = configparser.ConfigParser()
    config.read(path_config)

    ReleaseName = config['Project_setup']['ReleaseName']
    Environment = config['Project_setup']['Environment']
    Cycle = config['Project_setup']['Cycle']
    TeamName = config['Project_setup']['TeamName']
    LOB = config['Project_setup']['LOB']
    TestType = config['Project_setup']['TestType']

    # check qtest compliance mandetory check.
    _qtest_mandatory_check(config)

    # qTest projet id and test cycle if given, then result will be submitted there.
    if config.has_option('Project_setup','qTest_Project_ID') and config.has_option('Project_setup','qTest_Test_Cycle_ID'):
        qTest_Project_ID=config['Project_setup']['qTest_Project_ID']
        qTest_Test_Cycle_ID=config['Project_setup']['qTest_Test_Cycle_ID']
        # if qtest waived, set to None
        qTest_Project_ID = qTest_Project_ID.strip() if qTest_Project_ID.lower() != 'waived' else 'None'
        qTest_Test_Cycle_ID = qTest_Test_Cycle_ID.strip() if qTest_Test_Cycle_ID.lower() != 'waived' else 'None'
        _add_in_reporting_dict_during_setup('qTest_Project_ID', qTest_Project_ID, reporting_dict)
        _add_in_reporting_dict_during_setup('qTest_Test_Cycle_ID', qTest_Test_Cycle_ID, reporting_dict)

    _add_in_reporting_dict_during_setup('TestType', TestType, reporting_dict)
    _add_in_reporting_dict_during_setup('ReleaseName', ReleaseName, reporting_dict)
    _add_in_reporting_dict_during_setup('Environment', Environment, reporting_dict)
    _add_in_reporting_dict_during_setup('Cycle', Cycle, reporting_dict)
    _add_in_reporting_dict_during_setup('TeamName', TeamName, reporting_dict)
    _add_in_reporting_dict_during_setup('LOB', LOB, reporting_dict)

    _add_in_reporting_dict_during_setup('test_progress_status', 'Running', reporting_dict)
    _add_in_reporting_dict_during_setup('start_time', str(datetime.now(pytz.timezone('America/Toronto')))[:23], reporting_dict)


    # #lock1 = RLock()
    # with lock:
    #     for key in reporting_dict:
    #         print("{}: {}".format(key, reporting_dict[key]))

def _add_in_reporting_dict_during_setup(dict_key, dict_val, reporting_dict):
    """Adds variables in reporting dict
    Parameters
    ----------
    dict_key : Key of the reporting dictionary.
    dict_val : Value of reporting dictionary.
    reporting_dict : The actual dictionary which wll be appended with the key and value pair.

    returns
    ---------
    None
    """
    thId = str(threading.get_ident())
    # checking if the execution type is sequential or multithreaded as if it is multithreaded then we need to handle threading.
    if threading.current_thread().name == 'MainThread':
        reporting_dict[thId + '-' + dict_key] = dict_val
    else:
        #lock = multiprocessing.Manager().Lock()
        with lock:
            reporting_dict[thId + '-' + dict_key] = dict_val

def getTestData(datafilefullname):
    """Reads all the data variables from the file

    Parameters
    ----------
    datafilefullname : str
        path and file name with extension in TestData folder

    returns
    A valid module object
    """

    #extracting the path of testdata folder
    datapath = os.path.join(current_project_path, "TestData", datafilefullname)
    fileName = os.path.splitext(os.path.basename(datapath))[0]
    import importlib.machinery, importlib.util
    loader = importlib.machinery.SourceFileLoader(fileName, datapath)
    spec = importlib.util.spec_from_loader(loader.name, loader)
    my_module = importlib.util.module_from_spec(spec)
    loader.exec_module(my_module)
    return my_module

def _set_test_info_in_global_dict(rptCnt):
    """ This function to be called from 'prepare_report' function only to
    Add the test info key value pair to global dict"""
    global global_dict
    current_test_info_dic={
        'tc_name':get_from_reporting_dict('testName'),
        'tc_status':get_from_reporting_dict('overall_status'),
        'tc_run_time':get_from_reporting_dict('running_time')
    }

    common_test_info_dic={
        'lob':get_from_reporting_dict('LOB'),
        'test_type':get_from_reporting_dict('TestType'),
        'run_date':str(datetime.now(pytz.timezone('America/Toronto')))[:10],
        'release_name':get_from_reporting_dict('ReleaseName'),
        'env':get_from_reporting_dict('Environment'),
        'cycle':get_from_reporting_dict('Cycle'),
        'team_name':get_from_reporting_dict('TeamName'),
        'overall_execution_status':None # to be set later
    }

    global_dict[f"tc_{rptCnt}"]=current_test_info_dic
    if "common_test_info" not in global_dict:
        global_dict["common_test_info"] = common_test_info_dic

def _qtest_mandatory_check(config):
    "check if qTest project id and cycle id given in project config and not blank or none. It also store values in reporting dict"
    if (config.has_option('Project_setup','qTest_Project_ID')==True) \
            and (config['Project_setup']['qTest_Project_ID'].strip().isnumeric()==True or
                 config['Project_setup']['qTest_Project_ID'].strip().lower()=='waived'):
        # if value is either numeric or waived, store value
        pass
    else:
        sys.exit("WARNING: qTest_Project_ID parameter in project.config.ini file not valid. Pleaes provide valid value before proceed.\nFor detail, read: https://bitbucket.sunlifecorp.com/projects/TCOEICAS/repos/tcoe-pyetl-automation-docs/browse/pyETL-Data-Framework-docs/files/qTest-pyETL-integration/pyETL_QTEST_Integration.md")

    if (config.has_option('Project_setup','qTest_Test_Cycle_ID')==True) \
            and (config['Project_setup']['qTest_Test_Cycle_ID'].strip().isnumeric()==True or
                 config['Project_setup']['qTest_Test_Cycle_ID'].strip().lower()=='waived'):
        # if value is either numeric or waived, store value
        pass
    else:
        sys.exit("WARNING: qTest_Test_Cycle_ID parameter in project.config.ini file not valid. Pleaes provide valid value before proceed.\nFor detail, read: https://bitbucket.sunlifecorp.com/projects/TCOEICAS/repos/tcoe-pyetl-automation-docs/browse/pyETL-Data-Framework-docs/files/qTest-pyETL-integration/pyETL_QTEST_Integration.md")



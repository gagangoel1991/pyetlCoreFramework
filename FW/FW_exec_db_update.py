import pyodbc, getpass, os, base64, sys, subprocess, ctypes, pytz, datetime
from FW.FW_logger import add_in_reporting_dict, check_key_in_reporting_dict, get_from_reporting_dict, loggerFail, loggerPass, loggerInfo
import FW.Initialize.initialize_global_variables as iniVar
import FW.Jenkins_Integration.FW_jenkins_integration as jnk
import FW.QTest_Integration.FW_pulse_integration as qtest

currentVersion = '3.10'
def update_exec_db():
    """Update the automation execution metrics database."""
    sql, values = _prep_sql_to_insert()
    if values[7]==None:  # if script name is blank, do not update db
        print("*** Some issue happened, execution DB is not updated ***")
        return
    try:
        SERVER = r"SQC6R12P.sunlifecorp.com\V4P612"
        ##SERVER = "SQC6R12D.sunlifecorp.com\V4D612"
        Db = "TCoE_MetricsHistory"
        SQL_User = "dENPRQ=="
        SQL_Password = "VGUkVENvRTE="
        
        sql_driver = 'ODBC Driver 17 for SQL Server' if jnk._running_from_jenkins()==True else 'SQL Server'

        conn = pyodbc.connect('Driver={' + sql_driver + '};Server=' + SERVER + ';Database=' + Db + ';uid=' + base64.b64decode(SQL_User).decode('utf-8') + ';pwd=' + base64.b64decode(SQL_Password).decode('utf-8'))

        #sql, values = _prep_sql_to_insert()
        cursor = conn.cursor()

        cursor.execute(sql, values)

        conn.commit()
        conn.close()
        loggerPass("*** Auto Execution DB updated with script details *** ")
    except Exception as e:
        print(e)
        loggerFail("*** Some issue happened, execution DB is not updated ***")
        raise Exception(e)

def _prep_sql_to_insert():
    """Preapres the correct SQL query to insert execution details in automation db. This function is internally used by update_exec_db() function."""

    LoginID = getpass.getuser()
    ALMID = None
    TriggerBy = jnk._get_who_triggered_execution()
    ALMDomain=qtest._get_qtest_project_id()
    ALMProject=qtest._get_qtest_project_name()

    LOB = get_from_reporting_dict('LOB')
    SourceMachineName = os.environ['COMPUTERNAME']
    ExecutionMachineName =None
    ScriptName = qtest._remove_special_char_from_script_name(get_from_reporting_dict('testName'))
    Status = get_from_reporting_dict('test_progress_status')
    Iterations = 1
    DTStarTime = get_from_reporting_dict('start_time')
    DTEndTime = get_from_reporting_dict('end_time')
    TestStatus=get_from_reporting_dict('overall_status')
    IterationStatus=get_from_reporting_dict('test_progress_status')
    TestType =get_from_reporting_dict('TestType')
    DATE = str(datetime.datetime.now(pytz.timezone('America/Toronto')))[:10]
    ReleaseName= get_from_reporting_dict('ReleaseName')
    Environment = get_from_reporting_dict('Environment')
    TestSetPath =None
    DeviceBrowser =None
    DeviceType =None
    OS =None
    OSversion =None
    Complexity =None
    IssueType =None
    SubType =None
    IssueLogs =None
    TestCaseType =None
    Technology = 'Python'
    FrameworkType ='PYETL'
    RunTime = get_from_reporting_dict('running_time')
    Cycle =get_from_reporting_dict('Cycle')
    TeamName =get_from_reporting_dict('TeamName')
    buildVersion =currentVersion #pyETL version
    reportLink =None

    query = """SET ANSI_WARNINGS  OFF; insert into [TCoE_MetricsHistory].[dbo].[ASTestExecutionData] 
                     (LOB,LoginID,ALMID,ALMDomain,ALMProject,SourceMachineName,ExecutionMachineName,ScriptName,Status,Iterations,DTStarTime,DTEndTime,TestStatus,IterationStatus,
                      TestType,DATE,ReleaseName,Environment,TestSetPath,DeviceBrowser,DeviceType,OS,OSversion,Complexity,IssueType,SubType,IssueLogs, TestCaseType,Technology,
                      FrameworkType,RunTime,Cycle,TeamName,buildVersion,reportLink,TriggeredBy) 
                      values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    values = ( LOB, LoginID, ALMID, ALMDomain, ALMProject, SourceMachineName, ExecutionMachineName, ScriptName, Status, Iterations, DTStarTime, DTEndTime, TestStatus, IterationStatus, TestType,
    DATE, ReleaseName, Environment, TestSetPath, DeviceBrowser, DeviceType, OS, OSversion, Complexity, IssueType, SubType, IssueLogs, TestCaseType, Technology, FrameworkType,
    RunTime, Cycle, TeamName, buildVersion, reportLink, TriggerBy)

    return query, values

def _check_version_compilance():
    """Check current version of pyETL framework on the machine. If new version is available, pops message to upgrade to new framework with one-button-click."""

    # If running from Jenkins, no need to check for version for upgrade
    if jnk._running_from_jenkins()==True:
        return

    # if not running from Jenkins, check version for upgrade
    try:
        SERVER = r"SQC6R12P.sunlifecorp.com\V4P612"
        ##SERVER = "SQC6R12D.sunlifecorp.com\V4D612"
        Db = "TCoE_MetricsHistory"
        SQL_User = "dENPRQ=="
        SQL_Password = "VGUkVENvRTE="
        sql_driver = 'SQL Server'

        conn = pyodbc.connect('Driver={' + sql_driver + '};Server=' + SERVER + ';Database=' + Db + ';uid=' + base64.b64decode(SQL_User).decode('utf-8') + ';pwd=' + base64.b64decode(SQL_Password).decode('utf-8'))
        sql = "select max(cast(buildVersion as float)) from dbo.ASTestExecutionData where FrameworkType = 'PYETL' "

        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        conn.close()
        for row in rows:
            mVer = row[0]
            break

        #global currentVersion
        #currentVersion = '3.01'
        
        if mVer > float(currentVersion):
            MessageBox = ctypes.windll.user32.MessageBoxW
            ret = MessageBox(None, f"Newer version {mVer} of framework is available, click OK to upgrade the framework to latest version and rerun the script.", 'Data framework version update', 1)
            if ret == 1:
                import FW.Initialize.initialize_global_variables as iniVar
                file_path = iniVar.current_project_path + r"\Setup\FW_Setup.bat"
                p = subprocess.Popen(file_path, creationflags=subprocess.CREATE_NEW_CONSOLE)
                p.communicate()
                sys.exit('Framework upgraded, rerun the script')
            else:
                msg ="New version of data test automation framework available, upgrade the framework."
                raise Exception(msg)

    except Exception as e:
        print(e)
        raise Exception(e)

    _check_and_run_project_template_update()

def _check_and_run_project_template_update():
    if jnk._running_from_jenkins() == True:
        return

    import pathlib
    run_update = False
    new_template_ver = 1.4
    file_path = iniVar.current_project_path + r"\Setup\project_template_version.txt"
    if os.path.exists(file_path):
        with open(file_path) as f:
            template_ver=f.readline()
        if float(template_ver) < float(new_template_ver):
            run_update = True
    else:
        run_update=True

    if run_update ==True:
        src = r"c:\pyetl\ProjectUpdates\Setup"
        dst = iniVar.current_project_path + r"\Setup"
        os.system(rf'xcopy "{src}" "{dst}" /K /Y /E /H /S >%temp%/temp.txt')

        src = r"c:\pyetl\ProjectUpdates\0_JenkinsRunner.py"
        dst = iniVar.current_project_path + r"\Tests"
        os.system(rf'xcopy "{src}" "{dst}" /K /Y >%temp%/temp.txt')
        print("Project template has been updated successfully")






if  __name__ == "__main__":
    update_exec_db()

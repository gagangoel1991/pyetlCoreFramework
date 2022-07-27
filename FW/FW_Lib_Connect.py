import FW.Initialize.initialize_global_variables as iniVar
import configparser, os, io, time, base64
import pandas as pd
import psycopg2, tempfile, shutil
from psycopg2.extras import execute_batch
from FW.FW_logger import loggerPass, loggerFail, loggerInfo, get_from_reporting_dict, loggerDisplay
import cx_Oracle, pyodbc
import FW.Compare_Report.compare_report as cp

def read_salesforce_db_to_df(configfile, sql, flag_reduce_df_size=True, save_csv=False,  save_csv_suffix = "data_dump"):
    """Executes Salesforce Object Query Language (SOQL) query on Salesforce server db and returns resulted in tabular data frame for the query

    Parameters
    ----------
    configfile : string
        Location of .ini file containing database connection details.
    sql : string
        'Select' soql query to execute.
    flag_reduce_df_size: boolean, default True
        If false, then dataframe size will not be reduced. This will take more RAM in client machine but will be faster. By default it is on if row count in dataset > 50000

    returns
    ---------
    DataFrame : Sql query tabular data in the form of pandas dataframe

    """
    from simple_salesforce import Salesforce

    # Read from config file
    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)

    domain = config['SALESFORCE_DB']['Domain']
    username = config['SALESFORCE_DB']['User']
    security_token = config['SALESFORCE_DB']['Token']
    password = config['SALESFORCE_DB']['Password']
    password = base64.b64decode(password).decode('utf-8')    # Connection string

    sf = Salesforce(
    username=username,
    password=password,  # encode it
    security_token=security_token,
    domain=domain)

    print("SQL Query execution in progress...")
    loggerInfo(f"SQL Query: '{sql}'")

    sf_data = sf.query_all(sql) # to fetch all record

    df = pd.DataFrame(sf_data['records']).drop(columns='attributes')

    loggerPass('Salesforce SQL Query Executed successfully')
    df.replace([None], '(null)', inplace=True)

    # reduce size of dataframe
    df = cp._changeDataToCatagory(df, flag_reduce_df_size)

    if save_csv == True:
        tN = get_from_reporting_dict('testName')
        src_csv_path = os.path.join(iniVar.current_project_path, "Reports",  tN + "_" +  save_csv_suffix + ".csv")
        df.to_csv(src_csv_path, index=False)
        loggerPass(f"Source query result written to '{src_csv_path}' successfully")

    return df

def read_salesforce_db_schema(configfile, table_name, schema_columns_list=['name','type','precision','scale']):
    """Gets the schema of the salesforce table

    Parameters
    ----------
    configfile : string
        Location of .ini file containing database connection details.
    table_name : string
        name of table for which schmea to be read
    returns
    ---------
    DataFrame : Sql query tabular data in the form of pandas dataframe

    """
    from simple_salesforce import Salesforce

    loggerInfo("Creating Salesforce connection")
    # Read from config file
    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)

    domain = config['SALESFORCE_DB']['Domain']
    username = config['SALESFORCE_DB']['User']
    security_token = config['SALESFORCE_DB']['Token']
    password = config['SALESFORCE_DB']['Password']
    password = base64.b64decode(password).decode('utf-8')    # Connection string
    sf = Salesforce(
    username=username,
    password=password,  # encode it
    security_token=security_token,
    domain=domain)
    loggerInfo(f"Started Schema Read for {table_name}")
    table_details = getattr(sf,table_name)
    orderedDictList = table_details.describe()
    df_schema = pd.DataFrame(orderedDictList['fields'])
    df_schema = df_schema[schema_columns_list]
    df_schema.replace('(null)',0, inplace=True)
    loggerPass(f'Schema Read Query Executed successfully for {table_name}')
    return df_schema


def read_Hive_db_to_df(configfile, sql, flag_reduce_df_size=True, save_csv=False, save_csv_suffix="data_dump"):
    """Executes SQL query on AWS hive db and returs resulted tabular data frame for the query

    Parameters
    ----------
    configfile : string
        Location of .ini file containing database connection details.
    sql : string
        'Select' sql query to execute.
    flag_reduce_df_size: boolean, default True
        If false, then dataframe size will not be reduced. This will take more RAM in client machine but will be faster. By default it is on if row count in dataset > 50000
    save_csv : bool, default False
        Flag to indicate if resultant data frame to be stored in the form of .csv file in report folder.
    save_csv_suffix : string, default ""
        If save_csv argument = True, then save_csv_suffix will append to the name of csv file saved for query result
    returns
    ---------
    DataFrame : Sql query tabular data in the form of pandas dataframe

    Additional info:
    format of sample connection config file:
    [HiveDB]
    DSN = AWS_DATA_KNIGHTS
    autocommit = True
    """
    # Read from config file
    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)
    DSN = config['HiveDB']['DSN']
    autocommit = config['HiveDB']['autocommit']

    loggerInfo(f"Executing below SQL query on db :'Hive Db'")
    loggerInfo(f"SQL Query: '{sql}'")
    print("SQL Query execution in progress...")

    # execute query against postgre db
    # pyodbc.autocommit = False
    cnxn = pyodbc.connect(f"DSN={DSN}", autocommit=autocommit)
    cursor = cnxn.cursor()

    chunk_list = []
    dfs = pd.read_sql(sql, cnxn, coerce_float=False, chunksize=8000)
    for df_ch in dfs:
        chunk_list.append(df_ch.convert_dtypes(infer_objects=False).T.T)
        # df = pd.concat(chunk_list)
    # ==========================
    try:
        df = pd.concat(chunk_list)  # give error if sql not return anything
    except:
        df = pd.read_sql(sql, cnxn, coerce_float=False)  # will execute if df is empty.
    # ==========================

    cnxn.close()
    loggerPass('SQL Query Executed successfully')
    df.replace([None], '(null)', inplace=True)

    # reduce size of dataframe
    df = cp._changeDataToCatagory(df, flag_reduce_df_size)

    if save_csv == True:
        tN = get_from_reporting_dict('testName')
        src_csv_path = os.path.join(iniVar.current_project_path, "Reports", tN + "_" + save_csv_suffix + ".csv")
        df.to_csv(src_csv_path, index=False)
        loggerPass(f"SQL query result written to '{src_csv_path}' successfully")

    return df

def read_DB2_to_df(configfile, sql, save_csv=False, save_csv_suffix="data_dump", flag_reduce_df_size=True):
    """Executes SQL query on DB2 database and returs resulted tabular data frame for the query

        Parameters
        ----------
        configfile : string
            Location of .ini file containing database connection details.
        sql : string
            'Select' sql query to execute.
        flag_reduce_df_size: boolean, default True
            If false, then dataframe size will not be reduced. This will take more RAM in client machine but will be faster. By default it is on if row count in dataset > 50000
        save_csv : bool, default False
            Flag to indicate if resultant data frame to be stored in the form of .csv file in report folder.
        save_csv_suffix : string, default ""
            If save_csv argument = True, then save_csv_suffix will append to the name of csv file saved for query result
        returns
        ---------
        DataFrame : Sql query tabular data in the form of pandas dataframe

        Additional info:
        format of sample connection config file:
        [DB2_DB]
        User= x208
        Password = SIjmFudXMjE=
        DSN = D2XU
        """

    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)
    user = config['DB2_DB']['User']
    password = config['DB2_DB']['Password']
    password = base64.b64decode(password).decode('utf-8')  # 'decrypted'
    DSN = config['DB2_DB']['DSN']

    loggerInfo(f"Executing below Target SQL query on db :'DB2' using id: '{user}'")
    loggerInfo(f"SQL Query: '{sql}'")

    conn = pyodbc.connect(DSN=DSN, UID=user, PWD=password)

    loggerInfo('DB2 SQL Query Execution is in progress. It will take some time....')

    chunk_list = []

    dfs = pd.read_sql(sql, conn, coerce_float=False, chunksize=15000)
    for df_ch in dfs:
        chunk_list.append(df_ch.convert_dtypes(infer_objects=False).T.T)
    # df = pd.concat(chunk_list)
    # ==========================
    try:
        df = pd.concat(chunk_list)  # give error if sql not return anything
    except:
        df = pd.read_sql(sql, conn, coerce_float=False)  # will execute if df is empty.
    # ==========================
    conn.close()

    df.replace([None], '(null)', inplace=True)

    # reduce size of dataframe
    df = cp._changeDataToCatagory(df, flag_reduce_df_size)

    loggerPass('Source SQL Query Executed successfully')

    if save_csv == True:
        tN = get_from_reporting_dict('testName')
        src_csv_path = os.path.join(iniVar.current_project_path, "Reports", tN + "_" + save_csv_suffix + ".csv")
        df.to_csv(src_csv_path, index=False)
        loggerPass(f"Source query result written to '{src_csv_path}' successfully")

    return df

def read_PostgreSQL_to_df(configfile, sql, save_csv=False, save_csv_suffix="data_dump", flag_reduce_df_size=True):
    """Executes SQL query on PostgreDB and returs resulted tabular data frame for SQL query.

    Parameters
    ----------
    configfile : string
        Location of .ini file containing database connection details.
    sql : string
        'Select' sql query to execute.
    save_csv : bool, default False
        Flag to indicate if resultant data frame to be stored in the form of .csv file in report folder.
    save_csv_suffix : string, default "data_dump"
        If save_csv argument = True, then save_csv_suffix will append to the name of csv file saved for query result
    flag_reduce_df_size: boolean, default True
            If false, then dataframe size will not be reduced. This will take more RAM in client machine but will be faster. By default it is on if row count in dataset > 50000

    returns
    -------
    DataFrame : Sql query tabular data in the form of pandas dataframe
    """
    # Read from config file
    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)

    user = config['PostgreDB']['User']
    password = config['PostgreDB']['Password']  # 'encrypted'
    password = base64.b64decode(password).decode('utf-8')  # 'decrypted'
    host = config['PostgreDB']['Host']
    port = config['PostgreDB']['Port']
    database = config['PostgreDB']['Database']

    conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)

    loggerInfo(f"Executing below SQL query on db :'{database}' using id: '{user}'")
    loggerInfo(f"SQL Query: '{sql}'")
    print("SQL Query execution in progress...")

    # capture pid
    pid = conn.get_backend_pid()
    print(f"db connection pid = {pid}")
    with open(r"c:\pyetl\pid.txt", "w") as f:
        f.write(f"{pid}|{path_config}")

    with conn.cursor() as cur:
        sTime = time.time()
        cur.execute(sql)
        d = cur.fetchall()
        if len(d) == 0:
            df = pd.DataFrame(columns=[desc[0] for desc in cur.description])
        else:
            df = pd.DataFrame(d).convert_dtypes(infer_objects=False).T.T  # .astype(str)
            df.columns = [desc[0] for desc in cur.description]
        df.replace([None], '(null)', inplace=True)
        df.replace('<NA>', '(null)', inplace=True)

        loggerPass(f'SQL Query Executed successfully in {round(time.time() - sTime, 4)} sec')
        df = cp._changeDataToCatagory(df, flag_reduce_df_size)

    if save_csv == True:
        tN = get_from_reporting_dict('testName')
        src_csv_path = os.path.join(iniVar.current_project_path, "Reports", tN + "_" + save_csv_suffix + ".csv")
        df.to_csv(src_csv_path, index=False)
        loggerPass(f"SQL query result written to '{src_csv_path}' successfully")

    return df

def read_Redshift_to_df(configfile, sql, save_csv=False, save_csv_suffix="data_dump", flag_reduce_df_size=True):
    """Executes SQL query on RedshiftDB and returns resulted tabular data frame for source query.

    Parameters
    ----------
    configfile : string
        Location of .ini file containing database connection details.
    sql : string
        'Select' sql query to execute.
    save_csv : bool, default False
        Flag to indicate if resultant data frame to be stored in the form of .csv file in report folder.
    save_csv_suffix : string, default ""
        If save_csv argument = True, then save_csv_suffix will append to the name of csv file saved for query result
    flag_reduce_df_size: boolean, default True
        If false, then dataframe size will not be reduced. This will take more RAM in client machine but will be faster. By default it is on if row count in dataset > 50000

    returns
    -------
    DataFrame : Sql query tabular data in the form of pandas dataframe
    """
    # Read from config file
    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)

    user = config['RedshiftDB']['User']
    password = config['RedshiftDB']['Password']
    password = base64.b64decode(password).decode('utf-8')  # 'decrypted'
    host = config['RedshiftDB']['Host']
    port = config['RedshiftDB']['Port']
    database = config['RedshiftDB']['Database']

    loggerInfo(f"Executing below Source SQL query on db :'{database}' using id: '{user}'")
    loggerInfo(f"SQL Query: '{sql}'")
    print("SQL Query execution in progress...")
    # Create connection
    conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)

    chunk_list = []
    dfs = pd.read_sql(sql, conn, coerce_float=False, chunksize=8000)
    for df_ch in dfs:
        chunk_list.append(df_ch.convert_dtypes(infer_objects=False).T.T)
    #df = pd.concat(chunk_list)
    # ==========================
    try:
        df = pd.concat(chunk_list)  # give error if sql not return anything
    except:
        df = pd.read_sql(sql, conn, coerce_float=False)  # will execute if df is empty.
    # ==========================
    conn.close()
    loggerPass('Target SQL Query Executed successfully')
    df.replace([None], '(null)', inplace=True)
    # reduce size of dataframe
    df = cp._changeDataToCatagory(df, flag_reduce_df_size)

    if save_csv == True:
        tN = get_from_reporting_dict('testName')
        trg_csv_path = os.path.join(iniVar.current_project_path, "Reports", tN + "_" + save_csv_suffix + ".csv")
        df.to_csv(trg_csv_path, index=False)
        loggerPass(f"Target query result written to '{trg_csv_path}' successfully")

    return df

def read_PostgreSQL_to_df_Source(configfile,sql, save_csv = False, save_csv_suffix = "src", flag_reduce_df_size=True):
    """Warning - Instead of this function, start using read_PostgreSQL_to_df()

    Executes SQL query on PostgreDB and returs resulted tabular data frame for source query.

    Parameters
    ----------
    configfile : string
        Location of .ini file containing database connection details.
    sql : string
        'Select' sql query to execute.
    save_csv : bool, default False
        Flag to indicate if resultant data frame to be stored in the form of .csv file in report folder.
    save_csv_suffix : string, default "src"
        If save_csv argument = True, then save_csv_suffix will append to the name of csv file saved for query result
    flag_reduce_df_size: boolean, default True
            If false, then dataframe size will not be reduced. This will take more RAM in client machine but will be faster. By default it is on if row count in dataset > 50000

    returns
    -------
    DataFrame : Sql query tabular data in the form of pandas dataframe
    """
    #Read from config file
    path_config  = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)  
    
    user = config['PostgreDB']['User']
    password = config['PostgreDB']['Password']  #'encrypted'
    password = base64.b64decode(password).decode('utf-8')  #'decrypted'
    host = config['PostgreDB']['Host']
    port = config['PostgreDB']['Port']
    database = config['PostgreDB']['Database']

    conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)

    loggerInfo(f"Executing below Source SQL query on db :'{database}' using id: '{user}'")
    loggerInfo(f"SQL Query: '{sql}'")
    print("SQL Query execution in progress...")

    # capture pid
    pid = conn.get_backend_pid()
    print(f"db connection pid = {pid}")
    with open(r"c:\pyetl\pid.txt", "w") as f:
        f.write(f"{pid}|{path_config}")

    with conn.cursor() as cur:
        sTime = time.time()
        cur.execute(sql)
        d = cur.fetchall()
        if len(d) == 0:
            df = pd.DataFrame(columns=[desc[0] for desc in cur.description])
        else:
            df = pd.DataFrame(d).convert_dtypes(infer_objects=False).T.T  # .astype(str)
            df.columns = [desc[0] for desc in cur.description]
        df.replace([None], '(null)', inplace=True)
        df.replace('<NA>', '(null)', inplace=True)

        loggerPass(f'Source SQL Query Executed successfully in {round(time.time() - sTime, 4)} sec')
        df = cp._changeDataToCatagory(df, flag_reduce_df_size)

    if save_csv ==True:
        tN = get_from_reporting_dict('testName')
        src_csv_path = os.path.join(iniVar.current_project_path, "Reports", tN + "_" +  save_csv_suffix + ".csv")
        df.to_csv(src_csv_path, index = False)
        loggerPass(f"Source query result written to '{src_csv_path}' successfully")

    return df

def read_PostgreSQL_to_df_Target(configfile,sql, save_csv = False, save_csv_suffix = "trg", flag_reduce_df_size = True):
    """Warning - Instead of this function, start using read_PostgreSQL_to_df()

        Executes SQL query on PostgreDB and returs resulted tabular data frame for target query

        Parameters
        ----------
        configfile : string
            Location of .ini file containing database connection details.
        sql : string
            'Select' sql query to execute.
        save_csv : bool, default False
            Flag to indicate if resultant data frame to be stored in the form of .csv file in report folder.
        save_csv_suffix : string, default "trg"
            If save_csv argument = True, then save_csv_suffix will append to the name of csv file saved for query result
        flag_reduce_df_size: boolean, default True
            If false, then dataframe size will not be reduced. This will take more RAM in client machine but will be faster. By default it is on if row count in dataset > 30000

        return
        ---------
         DataFrame
            Sql query tabular data in the form of pandas dataframe
        """
    #Read from config file
    path_config  = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)  
    user = config['PostgreDB']['User']
    password = config['PostgreDB']['Password']
    password = base64.b64decode(password).decode('utf-8')  # 'decrypted'
    host = config['PostgreDB']['Host']
    port = config['PostgreDB']['Port']
    database = config['PostgreDB']['Database']

    conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)

    loggerInfo(f"Executing below Target SQL query on db :'{database}' using id: '{user}'")
    loggerInfo(f"SQL Query: '{sql}'")
    print("SQL Query execution in progress...")

    #capture pid
    pid = conn.get_backend_pid()
    print(f"db connection pid = {pid}")
    with open(r"c:\pyetl\pid.txt", "w") as f:
        f.write(f"{pid}|{path_config}")

    with conn.cursor() as cur:
        sTime = time.time()
        cur.execute(sql)

        d = cur.fetchall()
        if len(d) == 0:
            df = pd.DataFrame(columns=[desc[0] for desc in cur.description])
        else:
            df = pd.DataFrame(d).convert_dtypes(infer_objects=False).T.T  # .astype(str)
            df.columns = [desc[0] for desc in cur.description]
        df.replace([None], '(null)', inplace=True)
        df.replace('<NA>', '(null)', inplace=True)

        loggerPass(f'Source SQL Query Executed successfully in {round(time.time() - sTime, 4)} sec')
        df = cp._changeDataToCatagory(df, flag_reduce_df_size)

    # # Decrypt the password
    # #decoded_data = base64.b64decode(password)
    # #execute query against postgre db
    # with tempfile.TemporaryFile() as tmpfile:
    #     copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}  NULL as '(null)'".format(query=sql, head="HEADER")
    #     #Create connection
    #     conn = psycopg2.connect( user =user, password = password, host = host, port = port, database = database)
    #
    #     # ============= capture pid
    #     pid = conn.get_backend_pid()
    #     print(f"db connection pid = {pid}")
    #     with open(r"c:\pyetl\pid.txt", "w") as f:
    #         f.write(f"{pid}|{path_config}")
    #
    #     with conn.cursor() as cur:
    #         # cur.copy_to(copy_sql, tmpfile, null='(none)')
    #         cur.copy_expert(copy_sql, tmpfile)
    #     conn.close()
    #     # ===========================
    #
    #     tmpfile.seek(0)
    #     #df = pd.read_csv(tmpfile, header=0,  sep=',', dtype=str, keep_default_na=False)
    #
    #     chunk_list = []
    #     dfs = pd.read_csv(tmpfile, chunksize = 15000, header=0,  sep=',', dtype=str, keep_default_na=False)
    #     for df_ch in dfs:
    #         chunk_list.append(df_ch)
    #     df = pd.concat(chunk_list)
    #
    #
    #     # reduce size of dataframe
    #     df = cp._changeDataToCatagory(df)

    if save_csv ==True:
        tN = get_from_reporting_dict('testName')
        trg_csv_path = os.path.join(iniVar.current_project_path, "Reports", tN + "_" +  save_csv_suffix + ".csv")
        df.to_csv(trg_csv_path, index = False)
        loggerPass(f"Target query result written to '{trg_csv_path}' successfully")

    return df  

def read_MSSQL_DB_to_df(configfile, sql, flag_reduce_df_size=True, save_csv=False,  save_csv_suffix = "data_dump"):
    """Executes SQL query on MS SQL server db and returs resulted tabular data frame for the query

    Parameters
    ----------
    configfile : string
        Location of .ini file containing database connection details.
    sql : string
        'Select' sql query to execute.
    flag_reduce_df_size: boolean, default True
        If false, then dataframe size will not be reduced. This will take more RAM in client machine but will be faster. By default it is on if row count in dataset > 50000

    returns
    ---------
    DataFrame : Sql query tabular data in the form of pandas dataframe

    """
    # Read from config file
    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)

    server = config['MSSQL_DB']['Server']
    database = config['MSSQL_DB']['Database']
    trusted_connection = config['MSSQL_DB']['Trusted_Connection']
    # Connection string
    if (config.has_option('MSSQL_DB', 'Trusted_Connection') == False) or (
            config['MSSQL_DB']['Trusted_Connection'] == 'No'):
        user = config['MSSQL_DB']['User']
        password = config['MSSQL_DB']['Password']
        password = base64.b64decode(password).decode('utf-8')  # 'decrypted'
        conn_str = 'Driver={SQL Server};Server=' + server + ';Database=' + database + ';uid=' + user + ';pwd=' + password
        loggerInfo(f"Loading the dataset to db :'{database}' using id: '{user}'")
    else:
        conn_str = f"""Driver={{SQL Server}};Server={server};Database={database};Trusted_Connection={trusted_connection};"""
        loggerInfo(f"Loading the dataset to db :'{database}'")

    loggerInfo(f"Executing below SQL query on db :'{database}' ")
    loggerInfo(f"SQL Query: '{sql}'")
    print("SQL Query execution in progress...")

    # execute query against postgre db
    #conn_str = f"""Driver={{SQL Server}};Server={server};Database={database};Trusted_Connection={trusted_connection};"""
    conn = pyodbc.connect(conn_str)

    chunk_list = []
    dfs = pd.read_sql(sql, conn, coerce_float=False, chunksize=8000)
    for df_ch in dfs:
        chunk_list.append(df_ch.convert_dtypes(infer_objects=False).T.T)
    #df = pd.concat(chunk_list)
    # ==========================
    try:
        df = pd.concat(chunk_list)  # give error if sql not return anything
    except:
        df = pd.read_sql(sql, conn, coerce_float=False)  # will execute if df is empty.
    # ==========================

    conn.close()
    loggerPass('Target SQL Query Executed successfully')
    df.replace([None], '(null)', inplace=True)

    # reduce size of dataframe
    df = cp._changeDataToCatagory(df, flag_reduce_df_size)

    if save_csv == True:
        tN = get_from_reporting_dict('testName')
        src_csv_path = os.path.join(iniVar.current_project_path, "Reports",  tN + "_" +  save_csv_suffix + ".csv")
        df.to_csv(src_csv_path, index=False)
        loggerPass(f"Source query result written to '{src_csv_path}' successfully")

    return df

def read_Oracle_to_df(configfile, sql, save_csv=False,  save_csv_suffix = "data_dump", flag_reduce_df_size=True, encoding=None):
    """Executes SQL query on PostgreDB and returs resulted tabular data frame for source query.

    Parameters
    ----------
    configfile : string
        Location of .ini file containing database connection details.
    sql : string
        'Select' sql query to execute.
    save_csv : bool, default False
        Flag to indicate if resultant data frame to be stored in the form of .csv file in report folder.
    save_csv_suffix : string, default ""
        If save_csv argument = True, then save_csv_suffix will append to the name of csv file saved for query result
    flag_reduce_df_size: boolean, default True
        If false, then dataframe size will not be reduced. This will take more RAM in client machine but will be faster. By default it is on if row count in dataset > 50000

    returns
    -------
    DataFrame : Sql query tabular data in the form of pandas dataframe
    """
    # Read from config file
    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)

    user = config['OracleDB']['User']
    password = config['OracleDB']['Password']
    password = base64.b64decode(password).decode('utf-8')  # 'decrypted'
    host = config['OracleDB']['Host']
    port = config['OracleDB']['Port']

    if config.has_option('OracleDB','Database'):
        db = config['OracleDB']['Database']
        dsn = cx_Oracle.makedsn(host, port, service_name=db)
        loggerInfo(f"Executing below Source SQL query on db :'{db}' using id: '{user}'")
    if config.has_option('OracleDB','SID'):
        sid = config['OracleDB']['SID']
        dsn = cx_Oracle.makedsn(host, port, sid=sid)
        loggerInfo(f"Executing below Source SQL query on system id:'{sid}' using id: '{user}'")

    loggerInfo(f"SQL Query: '{sql}'")
    print("SQL Query execution in progress...")

    # Decrypt the password
    if encoding==None:
        conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    else:
        conn = cx_Oracle.connect(user=user, password=password, dsn=dsn, encoding=encoding, nencoding=encoding)

    #This method is not optimum and would be updated in future
    loggerInfo('Oracle SQL Query Execution is in progress. It will take some time....')

    chunk_list = []

    dfs = pd.read_sql(sql, conn, coerce_float=False, chunksize=15000)
    for df_ch in dfs:
        chunk_list.append(df_ch.convert_dtypes(infer_objects=False).T.T)
    # df = pd.concat(chunk_list)
    #==========================
    try:
        df = pd.concat(chunk_list)   # give error if sql not return anything
    except:
        df = pd.read_sql(sql, conn, coerce_float=False) # will execute if df is empty.
    #==========================
    conn.close()
    # Replace nulls to standard null string
    df = df.fillna('(null)')
    df.replace([None], '(null)', inplace=True)

    # reduce size of dataframe
    df = cp._changeDataToCatagory(df, flag_reduce_df_size)

    loggerPass('Source SQL Query Executed successfully')

    if save_csv == True:
        tN = get_from_reporting_dict('testName')
        src_csv_path = os.path.join(iniVar.current_project_path, "Reports",  tN + "_" +  save_csv_suffix + ".csv")
        df.to_csv(src_csv_path, index=False)
        loggerPass(f"Source query result written to '{src_csv_path}' successfully")

    return df

def read_FWF_to_df(configfile=None, columns_not_to_trim = None, columns_left_trim_only = None, columns_right_trim_only = None, fileencoding =None,
                   file_location =None, fWf_col_spec = 'fWf_Col_Spec', col_names = 'col_Names', add_column_for_file_name = False, flag_reduce_df_size=True):
    """Reads fixed width (position) flat file in a dataframe. It is very useful when the file size is very very large.

    Parameters
    ----------
    configfile : string
        Location of .ini file containing details (file loaction, parsing positions, column names etc.) of fixed width file.
    columns_not_to_trim : list or all , default None
        By default, stores trimmed values of all the columns. Columns in this list will not be trimmed and may contains spaces around values in the columns.
        This argument will take precedence over values of 'columns_right_trim_only' & 'columns_left_trim_only' if overlapping.
    columns_left_trim_only: list or all , default None
        Columns in this list will be left trimmed only. Columns in 'columns_left_trim_only' and 'columns_right_trim_only' should not overlap
    columns_right_trim_only: list or all , default None
        Columns in this list will be right trimmed only. Columns in 'columns_left_trim_only' and 'columns_right_trim_only' should not overlap
    fileencoding : str, default - None
        By default, flat file having data doesn't have special encoding. However, if file have different encoding, then provide this parameter with encoding of file. E.g. if the file is saved as 'utf-8' encoding,then pass this argument as 'utf-8'
    file_location : str, default - None
        If filepath given, function will load this file and ignores 'source file' in data config file.
    fWf_col_spec: str or list like string or list, default - 'fWf_col_spec'
        It is name of variable in config ini file having column specification or string of list of column postions
    col_names: str or list like string or list, default - 'col_names'
        It is name of variable in config ini file having column names or string of list of column names
    add_column_for_file_name : bool, default False
        If True - add a column 'source_file_name' having values as name of file having flat data
    flag_reduce_df_size: boolean, default True
        If false, then dataframe size will not be reduced. This will take more RAM in client machine but will be faster. By default it is on if row count in dataset > 50000

    returns
    --------
    DataFrame : Parsed file tabular data
    """
    if configfile!=None:
        path_config  = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
        #read ini file
        config = configparser.ConfigParser()
        config.read(path_config)

        # Stringigy input argument
        fWf_col_spec = str(fWf_col_spec)
        col_names = str(col_names)

        if file_location==None:
            file_location = config['FixWidthFile']['sourceFile']
        if config.has_option('FixWidthFile', fWf_col_spec):
            fWf_col_spec = config['FixWidthFile'][fWf_col_spec]
        if config.has_option('FixWidthFile', col_names):
            col_names = config['FixWidthFile'][col_names]
        fWf_col_spec = fWf_col_spec.replace('[', '').replace(']', '')
    else:
        # Stringigy input argument
        fWf_col_spec = str(fWf_col_spec)
        col_names = str(col_names)
        fWf_col_spec = fWf_col_spec.replace('[', '').replace(']', '')

    loggerInfo(f"Fixed width file location:'{file_location}'")
    loggerInfo(f"Field positions: '{fWf_col_spec}'")
    loggerInfo(f"Name of columns to be created: '{col_names}'")
    
    #tuplize positions . This is done to handle the indexing which starts from 0 in python and 1 in FWF .
    bb =[x.replace('(','').replace(')','') for x in fWf_col_spec.split(',')]
    fWf_col_spec= [(int(y)-1,int(bb[x+1])) for x,y in enumerate(bb) if x < len(bb)-1 and x%2 == 0]
    
    #list of col names
    col_names = [x.strip() for x in col_names.replace("[","").replace("]","").replace("'","").split(",")]

    #==============================This change implemented to handle trimming requiremnet=============================
    df_list = []
    if (columns_not_to_trim == None and columns_left_trim_only == None and columns_right_trim_only == None): # when all None, default TRIM all both side
        # for uft-8 encoded file manually, pass encoding as utf-8
        dfs = pd.read_fwf(file_location, chunksize=5000, colspecs=fWf_col_spec, names=col_names, dtype=str, keep_default_na=False, encoding=fileencoding)
        for df in dfs:
            df_list.append(df)
    else: # something keep UN-Trimmed
        dfs = pd.read_fwf(file_location, colspecs=fWf_col_spec,chunksize = 5000, delimiter = "\t", names=col_names,
                          dtype=str, keep_default_na=False, encoding = fileencoding)   # All UNTrimmed
        for df in dfs:
            df = _trimming_in_df(df, columns_not_to_trim=columns_not_to_trim, columns_left_trim_only=columns_left_trim_only, columns_right_trim_only=columns_right_trim_only)
            df_list.append(df)

    #=============================Below code block commented due to trimming requiremnt implementation.==============================
    # if columns_not_to_trim=='all': # if no column to trim
    #     columns_not_to_trim = col_names
    #
    # if columns_not_to_trim != None:  # for avoiding trimming on selected columns
    #     dfs = pd.read_fwf(file_location, colspecs=fWf_col_spec,chunksize = 5000, delimiter = "\t", names=col_names,
    #                       dtype=str, keep_default_na=False, encoding = fileencoding)
    #     for df in dfs:
    #         cols = [x for x in df.columns if x not in columns_not_to_trim]
    #         df[cols] = df[cols].apply(lambda x: x.str.strip())
    #         df_list.append(df)
    # else: # trim all columns
    #     # for uft-8 encoded file manually, pass encoding as utf-8
    #     dfs = pd.read_fwf(file_location,chunksize = 5000, colspecs=fWf_col_spec, names=col_names,
    #                       dtype=str, keep_default_na=False , encoding = fileencoding)
    #     for df in dfs:
    #         df_list.append(df)
    # ===========================================================
    df_src = pd.concat(df_list, ignore_index=True)

    # Add file name if required in the end
    if add_column_for_file_name == True:
        file_name = os.path.basename(file_location)
        df_src.insert(len(df_src.columns), "source_file_name", [file_name] * len(df_src.index), True)

    # reduce size of dataframe
    df_src = cp._changeDataToCatagory(df_src, flag_reduce_df_size)

    loggerPass('File is parsed to dataset successfully')
    return df_src

def read_csv_to_df(csvfilepath, delimiter=",", lst_colNames = None, PickInitial_n_records = None, add_column_for_file_name = False, flag_reduce_df_size=True, encoding = None):
    """Reads csv file with delimiter in a dataframe.

        Parameters
        ----------
        configfile : string
            Location of .csv or txt file
        delimiter : string, default as comma ','
            Delimiter for the csv file
        lst_colNames : list
            If file not contains column headers, then pass as list of columns to use.
        PickInitial_n_records : int, default None
            If given as n, then will read only initial n rows from file. This is useful while reading bigger file for analysis.
        add_column_for_file_name : boolean- default False
            If true, then a column will added as last column with file name
        flag_reduce_df_size: boolean - default True
            If False, will not optimize the size of dataframe. It will be faster. If True, then it will be slower while optimizing the size.
        encoding : str, default - None
            By default, flat file having data doesn't have special encoding. However, if file have different encoding, then provide this parameter with encoding of file. E.g. if the file is saved as 'utf-8' encoding,then pass this argument as 'utf-8'
        returns
        --------
        DataFrame : Parsed file tabular data

        """
    chunk_list = []

    if lst_colNames==None: #data have column names as first row
        lst_colNames = list(pd.read_csv(csvfilepath, nrows=1, sep=delimiter,encoding=encoding).head().columns.values)
        lst_colNames = [x.strip() for x in lst_colNames]

        dfs = pd.read_csv(csvfilepath, chunksize=6000, header=0, names=lst_colNames,  usecols=lst_colNames, nrows=PickInitial_n_records,
                          sep=delimiter, dtype=str, keep_default_na=False,encoding=encoding)

    else: # data Don't have column names as first row, use the passed list of cols
        dfs = pd.read_csv(csvfilepath, chunksize=6000, names=lst_colNames, nrows=PickInitial_n_records, sep=delimiter,
                      dtype=str, keep_default_na=False, index_col=False,encoding=encoding)

    for df_ch in dfs: chunk_list.append(df_ch)
    df1 = pd.concat(chunk_list)

    # Add file name if required in the end
    if add_column_for_file_name == True:
        file_name = os.path.basename(csvfilepath)
        df1.insert(len(df1.columns), "source_file_name", [file_name] * len(df1.index), True)
    # reduce size of dataframe

    df1 = cp._changeDataToCatagory(df1, flag_reduce_df_size)

    return df1

def load_csv_to_postgre_table(csvfilepath, db_configfile, vTableName, delimiter=",", lst_colNames = None, PickInitial_n_records = None, add_column_for_file_name = False, flag_reduce_df_size=True, encoding = None,
                              append=False, create_col_des=None, load_method='fast', grant_all_privilege_to_public=False, internal_sep=None):
    """Reads csv file and writes that to postgre_SQL database.

            Parameters
            ----------
            csvfilepath : string
                Location of .csv or txt file
            delimiter : string, default as comma ','
                Delimiter for the csv file
            lst_colNames : list
                If file not contains column headers, then pass as list of columns to use.
            PickInitial_n_records : int, default None
                If given as n, then will read only initial n rows from file. This is useful while reading bigger file for analysis.
            add_column_for_file_name : boolean- default False
                If true, then a column will added as last column with file name
            flag_reduce_df_size: boolean - default True
                If False, will not optimize the size of dataframe. It will be faster. If True, then it will be slower while optimizing the size.
            encoding : str, default - None
                By default, flat file having data doesn't have special encoding. However, if file have different encoding, then provide this parameter with encoding of file. E.g. if the file is saved as 'utf-8' encoding,then pass this argument as 'utf-8'
            db_configfile : string
                Relative location from Configuration folder in project for .ini file containing database connection details
            vTableName : string
                Table name in database
            append : bool, default False
                If False, a new table is created and data is added to this. If True, data is appended to existing table.
            create_col_des : string, default None
                Column description for insert statement.
            load_method : 'fast' or 'slow', default 'fast'
                Decides how to load the data in database table
            grant_all_privilege_to_public : bool, default False
                If true, will provide all the privilleges to all (Public)
            internal_sep : Char, default None
                Function internally identifies a char that can be used as separator in file internally. It should be a char which is not present in whole file. If passed as this argument, this char would be taken as internal separator.

            returns
            --------
            None
            """
    chunk_list = []
    vrows = get_rows_count_of_file(csvfilepath)
    df_list, n, chunksize = [], 0, 20000
    file_name = os.path.basename(csvfilepath)

    if lst_colNames == None:  # data have column names as first row
        lst_colNames = list(pd.read_csv(csvfilepath, nrows=1, sep=delimiter, encoding=encoding).head().columns.values)
        lst_colNames = [x.strip() for x in lst_colNames]

        dfs = pd.read_csv(csvfilepath, chunksize=chunksize, header=0, names=lst_colNames, usecols=lst_colNames,
                          nrows=PickInitial_n_records,
                          sep=delimiter, dtype=str, keep_default_na=False, encoding=encoding)

    else:  # data Don't have column names as first row, use the passed list of cols
        dfs = pd.read_csv(csvfilepath, chunksize=chunksize, names=lst_colNames, nrows=PickInitial_n_records, sep=delimiter,
                          dtype=str, keep_default_na=False, index_col=False, encoding=encoding)

    for df_ch in dfs:
        # Add file name if required in the end
        if add_column_for_file_name == True:
            df_ch.insert(len(df_ch.columns), "source_file_name", [file_name] * len(df_ch.index), True)

        df_to_postgre_table(df_ch, db_configfile, vTableName, append=append, create_col_des=create_col_des,
                            load_method=load_method, grant_all_privilege_to_public=grant_all_privilege_to_public,
                            internal_sep=internal_sep)
        append = True
        n = (n + chunksize) if (n + chunksize) < vrows else vrows
        loggerDisplay(f"Loaded rows {n} / {vrows} in the table")

    loggerPass('File is loaded successfully')

def df_to_redshift_table(df_src, configfile, vTableName, append=False, create_col_des=None, load_method='fast',
                         grant_all_privilege_to_public=False, internal_sep=None):
    """Inserts/appends dataframe to table in database.

    Parameters
    ----------
    df_src : dataframe
        DataFrame to be inserted/appened to table
    configfile : string
        Relative location from Configuration folder in project for .ini file containing database connection details
    vTableName : string
        Table name in database
    append : bool, default False
        If False, a new table is created and data is added to this. If True, data is appended to existing table.
    create_col_des : string, default None
        Column description for insert statement.
    load_method : 'fast' or 'slow', default 'fast'
        Decides how to load the data in database table
    grant_all_privilege_to_public : bool, default False
        If true, will provide all the privilleges to all (Public)
    internal_sep : Char, default None
        Function internally identifies a char that can be used as separator in file internally. It should be a char which is not present in whole file. If passed as this argument, this char would be taken as internal separator.
    returns
    ---------
    None
    """

    # Read from config file
    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)

    user = config['RedshiftDB']['User']
    password = config['RedshiftDB']['Password']
    password = base64.b64decode(password).decode('utf-8')  # 'decrypted'
    host = config['RedshiftDB']['Host']
    port = config['RedshiftDB']['Port']
    database = config['RedshiftDB']['Database']

    # Decrypt the password
    # decoded_data = base64.b64decode(password)
    # Create connection
    conn = psycopg2.connect(user=user, password=password,
                            host=host,
                            port=port,
                            database=database)

    loggerInfo(f"Loading the dataset to db :'{database}' using id: '{user}'")

    df_columns = list(df_src)
    columns = ",".join(df_columns)
    # default table column description
    if create_col_des == None:
        create_col_des = ",".join([col + ' VARCHAR (100)' for col in df_columns])
    else:
        create_col_des = create_col_des

    # create table by default
    if append == False:
        create_sql = f"""CREATE TABLE {vTableName} ({create_col_des})"""
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

        loggerPass(f"Created Table:'{vTableName}' successfully")

    # insert/append the new data to existing table
    totRec = len(df_src)
    loggerInfo(f"Total records : {totRec}, starting loading data in table...")
    df_src = df_src.replace(r'\\', r'\\\\', regex=True)  # to handle backslash

    if load_method == 'fast':
        a = time.time()
        f = io.StringIO()
        internal_sep = _find_internal_sep(f) if internal_sep == None else internal_sep
        df_src.to_csv(f, index=False, header=False, sep=internal_sep)
        f.seek(0)
        with conn.cursor() as cur:
            cur.copy_from(f, vTableName, sep=internal_sep)
        conn.commit()
        loggerInfo(f"Time taken to load data: {time.time() - a}")

    if load_method == 'slow':
        a = time.time()
        values = "VALUES({})".format(",".join(["%s" for _ in list(df_src)]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(vTableName, columns, values)
        with conn.cursor() as cur:
            execute_batch(cur, insert_stmt, df_src.values)
        conn.commit()

        loggerInfo(f"Time taken to load data: {time.time() - a}")

    if grant_all_privilege_to_public == True:
        sql = f"grant ALL PRIVILEGES on {vTableName} to public"
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        loggerPass(f"All previleges are given to PUBLIC successfully")

    conn.close()
    loggerPass(f"Data loaded successfully in Table:{vTableName}")

def df_to_mssql_table(df_src, configfile, vTableName, create_col_des=None, if_Table="New"):
    """Inserts/appends/Replaces a table with dataframe in MSSQL database .

    Parameters
    ----------
    df_src : dataframe
        DataFrame to be inserted/appened to table
    configfile : string
        Relative location from Configuration folder in project for .ini file containing database connection details
    vTableName : string
        Table name in database, along with schema name
    if_Table : String
        If 'New', a new table is created and data is added to this.
        If 'Replace', existing table is dropped and new table is created and data is added to existing table.
        If 'Append', appending data to existing table
    create_col_des : string, default None
        Column description for insert statement.
    returns
    ---------
    None
    """

    # Read from config file
    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)

    server = config['MSSQL_DB']['Server']
    database = config['MSSQL_DB']['Database']
    trusted_connection = config['MSSQL_DB']['Trusted_Connection']
    # Connection string
    if (config.has_option('MSSQL_DB','Trusted_Connection')==False) or (config['MSSQL_DB']['Trusted_Connection']=='No'):
        user = config['MSSQL_DB']['User']
        password = config['MSSQL_DB']['Password']
        password = base64.b64decode(password).decode('utf-8')  # 'decrypted'
        conn_str = 'Driver={SQL Server};Server=' + server + ';Database=' + database + ';uid=' + user + ';pwd=' + password
        loggerInfo(f"Loading the dataset to db :'{database}' using id: '{user}'")
    else:
        conn_str = f"""Driver={{SQL Server}};Server={server};Database={database};Trusted_Connection={trusted_connection};"""
        loggerInfo(f"Loading the dataset to db :'{database}'")

    # default table column description
    df_columns = list(df_src)
    if create_col_des == None:
        create_col_des = ",".join([col + ' VARCHAR (100)' for col in df_columns])
    else:
        create_col_des = create_col_des

    # Create connectiona and cursor object
    conn = pyodbc.connect(conn_str)
    with conn.cursor() as cur:
        if if_Table == "Replace": # Droppping old table
            cur.execute(f"drop table {vTableName}")
            conn.commit()
            if_Table ="New"  # set to create new table

        # create table by default
        if if_Table == "New":
            create_sql = f"""CREATE TABLE {vTableName} ({create_col_des})"""
            cur.execute(create_sql)
            conn.commit()
            loggerPass(f"Created Table:'{vTableName}' successfully")

        if if_Table == "New" or if_Table == "Append":
            loggerInfo(f"Total records : {len(df_src)}, starting loading data in table...")
            df_src = df_src.replace(r'\\', r'\\\\', regex=True)  # to handle backslash

            # insert/append the new data to existing table
            df_columns = list(df_src)
            insert_sql = f"insert into {vTableName} values ({','.join(['?' for v in df_columns])})"
            insert_cols = df_src.values.tolist()
            cur.fast_executemany = True
            cur.executemany(insert_sql, insert_cols)
            conn.commit()
        else:
            raise Exception("Invalid Table Operation ")

    conn.close()
    loggerPass(f"Table successfully loaded")

def df_to_postgre_table(df_src, configfile, vTableName, append=False, create_col_des=None, load_method='fast',
                        grant_all_privilege_to_public=False, internal_sep=None):
    """Inserts/appends dataframe to table in database.

    Parameters
    ----------
    df_src : dataframe
        DataFrame to be inserted/appened to table
    configfile : string
        Relative location from Configuration folder in project for .ini file containing database connection details
    vTableName : string
        Table name in database
    append : bool, default False
        If False, a new table is created and data is added to this. If True, data is appended to existing table.
    create_col_des : string, default None
        Column description for insert statement.
    load_method : 'fast' or 'slow', default 'fast'
        Decides how to load the data in database table
    grant_all_privilege_to_public : bool, default False
        If true, will provide all the privilleges to all (Public)
    internal_sep : Char, default None
        Function internally identifies a char that can be used as separator in file internally. It should be a char which is not present in whole file. If passed as this argument, this char would be taken as internal separator.

    returns
    --------
    None
    """

    # Read from config file
    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)

    user = config['PostgreDB']['User']
    password = config['PostgreDB']['Password']
    password = base64.b64decode(password).decode('utf-8')  # 'decrypted'
    host = config['PostgreDB']['Host']
    port = config['PostgreDB']['Port']
    database = config['PostgreDB']['Database']

    # Decrypt the password
    #decoded_data = base64.b64decode(password)
    # Create connection
    conn = psycopg2.connect(user=user, password=password,
                            host=host,
                            port=port,
                            database=database)

    loggerInfo(f"Loading the dataset to db :'{database}' using id: '{user}'")

    df_columns = list(df_src)
    columns = ",".join(df_columns)
    # default table column description
    if create_col_des == None:
        create_col_des = ",".join([col + ' VARCHAR (100)' for col in df_columns])
    else:
        create_col_des = create_col_des

    # create table by default
    if append == False:
        create_sql = f"""CREATE TABLE {vTableName} ({create_col_des})"""
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

        loggerPass(f"Created Table:'{vTableName}' successfully")

    # insert/append the new data to existing table
    totRec = len(df_src)
    loggerInfo(f"Total records : {totRec}, starting loading data in table...")
    df_src = df_src.replace(r'\\', r'\\\\', regex=True)  # to handle backslash

    if load_method == 'fast':
        import csv
        a = time.time()
        f = io.StringIO()

        internal_sep = _char_not_in_df(df_src) if internal_sep == None else internal_sep

        df_src.to_csv(f, index=False, header=False, sep=internal_sep, quotechar=internal_sep ) #To handle double quote in values
        f.seek(0)
        with conn.cursor() as cur:
            cur.copy_from(f, vTableName, sep=internal_sep)

        conn.commit()
        loggerInfo(f"Time taken to load data: {time.time() - a}")

    if load_method == 'slow':
        a = time.time()
        values = "VALUES({})".format(",".join(["%s" for _ in list(df_src)]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(vTableName, columns, values)
        with conn.cursor() as cur:
            execute_batch(cur, insert_stmt, df_src.values)
        conn.commit()

        loggerInfo(f"Time taken to load data: {time.time() - a}")

    if grant_all_privilege_to_public == True:
        sql = f"grant ALL PRIVILEGES on {vTableName} to public"
        with conn.cursor() as cur:
            cur.execute(sql)

        conn.commit()
        loggerPass(f"All previleges are given to PUBLIC successfully")

    conn.close()
    loggerPass(f"Data loaded successfully in Table:{vTableName}")

def load_fwf_to_postgre_table(file_configfile, db_configfile, vTableName, columns_not_to_trim = None, fileencoding =None, file_location =None,
                                append = False, create_col_des = None, load_method ='fast', grant_all_privilege_to_public=False, internal_sep=None):
    """Parse large FWF file and loads into postgres table directly. It is very useful when file size is very very large.

    Parameters
    ----------
    file_configfile : string
            Location of .ini file containing details (file loaction, parsing positions, column names etc.) of fixed width file.
    db_configfile : string
        Relative location from Configuration folder in project for .ini file containing database connection details
    vTableName : string
        Table name in database
    append : bool, default False
        If False, a new table is created and data is added to this. If True, data is appended to existing table.
    columns_not_to_trim : list , all , default None
        By default, stores trimmed values of all the columns. Columns in this list will not be trimmed and may contains spaces around values in the columns.
    fileencoding : str, default - None
        By default, flat file having data doesn't have special encoding. However, if file have different encoding, then provide this parameter with encoding of file. E.g. if the file is saved as 'utf-8' encoding,then pass this argument as 'utf-8'
    file_location : str, default None
        Pass this value as location of FWF file, then this value overwrites the FWF location found in file_configfile parameter ini file
    create_col_des : string, default None
        Column description for insert statement.
    load_method : 'fast' or 'slow', default 'fast'
        Decides how to load the data in database table
    grant_all_privilege_to_public : bool, default False
        If true, will provide all the privilleges to all (Public)
    internal_sep : Char, default None
        Function internally identifies a char that can be used as separator in file internally. It should be a char which is not present in whole file. If passed as this argument, this char would be taken as internal separator.

    returns
    --------
    None
    """


    path_config = os.path.join(iniVar.current_project_path, "Configrations", file_configfile + ".ini")
    # read ini file
    config = configparser.ConfigParser()
    config.read(path_config)

    if file_location == None:
        file_location = config['FixWidthFile']['sourceFile']

    fWF_Col_Spec = config['FixWidthFile']['fWf_Col_Spec'].replace('[', '').replace(']', '')
    col_Names = config['FixWidthFile']['col_Names']

    loggerInfo(f"Fixed width file location:'{file_location}'")
    loggerInfo(f"Field positions: '{fWF_Col_Spec}'")
    loggerInfo(f"Name of columns to be created: '{col_Names}'")

    # tuplize positions
    bb = [x.replace('(', '').replace(')', '') for x in fWF_Col_Spec.split(',')]
    fWF_Col_Spec = [(int(y) - 1, int(bb[x + 1])) for x, y in enumerate(bb) if x < len(bb) - 1 and x % 2 == 0]

    # list of col names
    col_Names = [x.strip() for x in col_Names.replace("[", "").replace("]", "").replace("'", "").split(",")]

    if columns_not_to_trim=='all': # if no column to trim
        columns_not_to_trim = col_Names

    vrows =get_rows_count_of_file(file_location)
    df_list, n, chunksize = [], 0, 20000
    if columns_not_to_trim != None:  # for avoiding trimming on selected columns
        dfs = pd.read_fwf(file_location, colspecs=fWF_Col_Spec, chunksize=chunksize, delimiter="\t", names=col_Names,
                          dtype=str, keep_default_na=False, encoding=fileencoding)
        for df in dfs:
            cols = [x for x in df.columns if x not in columns_not_to_trim]
            df[cols] = df[cols].apply(lambda x: x.str.strip())
            df_to_postgre_table(df, db_configfile, vTableName, append=append, create_col_des=create_col_des,
                                load_method=load_method,grant_all_privilege_to_public=grant_all_privilege_to_public, internal_sep=internal_sep)
            append = True
            n = (n + chunksize) if (n + chunksize)<vrows else vrows
            loggerDisplay(f"Loaded rows {n} / {vrows} in the table")
    else:  # trim all columns
        # for uft-8 encoded file manually, pass encoding as utf-8
        dfs = pd.read_fwf(file_location, chunksize=chunksize, colspecs=fWF_Col_Spec, names=col_Names, dtype=str,
                          keep_default_na=False, encoding=fileencoding)
        for df in dfs:
            df_to_postgre_table(df, db_configfile, vTableName, append=append, create_col_des=create_col_des,
                                load_method=load_method,grant_all_privilege_to_public=grant_all_privilege_to_public, internal_sep=internal_sep)
            append=True
            n = (n + chunksize) if (n + chunksize)<vrows else vrows
            loggerDisplay(f"Loaded rows {n} / {vrows} in the table")

    loggerPass('File is loaded successfully')

def df_to_oracle_table(df_src, configfile, vTableName, append=False, create_col_des=None):
    """Inserts/appends dataframe to table in oracle database.

        Parameters
        ----------
        df_src : dataframe
            DataFrame to be inserted/appened to table
        configfile : string
            Relative location from Configuration folder in project for .ini file containing database connection details
        vTableName : string
            Table name in database
        append : bool, default False
            If False, a new table is created and data is added to this. If True, data is appended to existing table.
        create_col_des : string, default None
            Column description for insert statement.
        returns
        ---------
        None
        """
    # Read from config file
    path_config = os.path.join(iniVar.current_project_path, "Configrations", configfile + ".ini")
    config = configparser.ConfigParser()
    config.read(path_config)

    user = config['OracleDB']['User']
    password = config['OracleDB']['Password']
    password = base64.b64decode(password).decode('utf-8')  # 'decrypted'
    host = config['OracleDB']['Host']
    port = config['OracleDB']['Port']
    database = config['OracleDB']['Database']

    if config.has_option('OracleDB','Database'):
        db = config['OracleDB']['Database']
        dsn = cx_Oracle.makedsn(host, port, service_name=db)

    if config.has_option('OracleDB','SID'):
        sid = config['OracleDB']['SID']
        dsn = cx_Oracle.makedsn(host, port, sid=sid)

    #columns from dataframe
    df_columns = list(df_src)
    columns = ",".join(df_columns)

    if create_col_des == None:
        create_col_des = ",".join([col + ' VARCHAR (100)' for col in df_columns])
    else:
        create_col_des = create_col_des

    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)

    # create table by default
    if append == False:
        create_sql = f"""CREATE TABLE {vTableName} ({create_col_des})"""
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

        loggerPass(f"Created Table:'{vTableName}' successfully")

    totRec = len(df_src)
    loggerInfo(f"Total records : {totRec}, starting loading data in table...")
    #df_src = df_src.replace(r'\\', r'\\\\', regex=True)  # Commented, no need to handle backslash in Oracle

    a = time.time()
    values = "VALUES({})".format(",".join([f":{m}" for m in range(0, len(df_src.columns.values) )]))
    insert_stmt = "INSERT INTO {} {}".format(vTableName, values)

    with conn.cursor() as cur:
        cur.executemany(insert_stmt,df_src.values.tolist())
    conn.commit()

    loggerInfo(f"Time taken to load data: {time.time() - a}")

    conn.close()
    loggerPass(f"Data loaded successfully in Table:{vTableName}")

def load_csv_to_oracle_table(csvfilepath, db_configfile, vTableName, delimiter=",", lst_colNames = None, PickInitial_n_records = None, add_column_for_file_name = False, encoding = None,
                              append=False, create_col_des=None):
    """Reads csv file and writes that to oracle_SQL database.

        Parameters
        ----------
        csvfilepath : string
            Location of .csv or txt file
        delimiter : string, default as comma ','
            Delimiter for the csv file
        lst_colNames : list
            If file not contains column headers, then pass as list of columns to use.
        PickInitial_n_records : int, default None
            If given as n, then will read only initial n rows from file. This is useful while reading bigger file for analysis.
        add_column_for_file_name : boolean- default False
            If true, then a column will added as last column with file name
        encoding : str, default - None
            By default, flat file having data doesn't have special encoding. However, if file have different encoding, then provide this parameter with encoding of file. E.g. if the file is saved as 'utf-8' encoding,then pass this argument as 'utf-8'
        db_configfile : string
            Relative location from Configuration folder in project for .ini file containing database connection details
        vTableName : string
            Table name in database
        append : bool, default False
            If False, a new table is created and data is added to this. If True, data is appended to existing table.
        create_col_des : string, default None
            Column description for insert statement.

        returns
        --------
        None
    """

    vrows = get_rows_count_of_file(csvfilepath)
    df_list, n, chunksize = [], 0, 15000
    file_name = os.path.basename(csvfilepath)

    if lst_colNames == None:  # data have column names as first row
        lst_colNames = list(pd.read_csv(csvfilepath, nrows=1, sep=delimiter, encoding=encoding).head().columns.values)
        lst_colNames = [x.strip() for x in lst_colNames]

        dfs = pd.read_csv(csvfilepath, chunksize=chunksize, header=0, names=lst_colNames, usecols=lst_colNames,
                          nrows=PickInitial_n_records,
                          sep=delimiter, dtype=str, keep_default_na=False, encoding=encoding)

    else:  # data Don't have column names as first row, use the passed list of cols
        dfs = pd.read_csv(csvfilepath, chunksize=chunksize, names=lst_colNames, nrows=PickInitial_n_records,
                          sep=delimiter,
                          dtype=str, keep_default_na=False, index_col=False, encoding=encoding)

    for df_ch in dfs:
        # Add file name if required in the end
        if add_column_for_file_name == True:
            df_ch.insert(len(df_ch.columns), "source_file_name", [file_name] * len(df_ch.index), True)

        df_to_oracle_table(df_ch,db_configfile,vTableName,append=append,create_col_des=create_col_des)
        append = True
        n = (n + chunksize) if (n + chunksize) < vrows else vrows
        loggerDisplay(f"Loaded rows {n} / {vrows} in the table")

    loggerPass('File is loaded successfully')

def get_rows_count_of_file(filepath):
    "Returns number of rows in a flat file path"
    def _blocks(file1, size=65536):
        while True:
            b = file1.read(size)
            if not b: break
            yield b
    with open(filepath, "r",encoding="utf-8",errors='ignore') as f:
        return sum(bl.count("\n") for bl in _blocks(f))

def find_char_presence_in_file(fileobj, regex):
    """Checks if a char or string (regex) present in a given file

    Parameters
    ----------
    fileobj : file object
        this is file object in which to find the presence of character e.g. f = open("example.txt")
    regex : str - a valid regular expression
        regular expression to find in file

    return
    --------
    Boolean : True if regular expression finds in the file

    """

    import re
    my_regex = re.compile(f".*{regex}.*")
    found = False
    #for line in open(file):
    for line in fileobj:
        match = my_regex.match(line)
        if match:
            print("found" + line)
            found=True
            break
    return found

def _find_internal_sep(fileobj):
    "Returns a char which is not in the file from sepList list. This can be used as delimiter internally for file"
    sepList = r'^~#|?;'
    regexList = r'.^$*+?|'

    found = False
    for sep in sepList:
        regex = (f"\{sep}") if sep in regexList else sep
        fileobj.seek(0)
        if find_char_presence_in_file(fileobj, regex) ==False:
            found = True

            break

    if found==False:
        loggerFail(f"Internal separator not found, find a suitable internal separator and try again")
    return sep

def _char_not_in_df(df):
    "Returns a char which is not in the dataframe from sepList list. This can be used as delimiter internally"
    import numpy as np
    sepList = r'^~#|?;'
    found = False
    for sep in sepList:
        #print(sep)
        cnt = np.sum([df[col].astype(str).str.contains(sep, na=False, regex=False) for col in df])
        if cnt == 0:
            found = True
            #print(sep)
            break

    if found == False:
        loggerFail(f"Character not in dataframe - is not identified, find a suitable character and try again")
    return sep

def encrypt_password(data_string):
    """Encrypted password to base64"""
    encoded_data = base64.b64encode((data_string).encode('utf-8')).decode('utf-8')
    return encoded_data

def _trimming_in_df(df, columns_not_to_trim=None, columns_left_trim_only=None, columns_right_trim_only=None):
    """This function trims the column values based on requirements. 'columns_not_to_trim' argument takes precedence over other arguments
    Parameters
    ----------
    df : dataframe
        Dataframe containing columns where trimming needs to be handled.
    columns_not_to_trim : list or all , default None
        By default, stores trimmed values of all the columns. Columns in this list will not be trimmed and may contains spaces around values in the columns.
        This argument will take precedence over values of 'columns_right_trim_only' & 'columns_left_trim_only' if overlapping.
    columns_left_trim_only: list or all , default None
        Columns in this list will be left trimmed only. Columns in 'columns_left_trim_only' and 'columns_right_trim_only' should not overlap
    columns_right_trim_only: list or all , default None
        Columns in this list will be right trimmed only. Columns in 'columns_left_trim_only' and 'columns_right_trim_only' should not overlap

    returns
    --------
    DataFrame : tabular data in the form of pandas dataframe

    """
    # df initially coming as all UN-Trimmed
    # ensure left and right mutually exclusive
    columns_not_to_trim1 = columns_not_to_trim
    columns_left_trim_only1 = columns_left_trim_only
    columns_right_trim_only1 = columns_right_trim_only

    if columns_not_to_trim == None: columns_not_to_trim = []
    if columns_not_to_trim == 'all': columns_not_to_trim = list(df.columns)

    if columns_left_trim_only == None: columns_left_trim_only = []
    if columns_left_trim_only == 'all': columns_left_trim_only = list(df.columns)

    if columns_right_trim_only == None: columns_right_trim_only = []
    if columns_right_trim_only == 'all': columns_right_trim_only = list(df.columns)

    comm_l_r = set(columns_left_trim_only).intersection(columns_right_trim_only)
    if len(comm_l_r) > 0:
        raise Exception(f"present in both left and right:\n{comm_l_r}")

    # if columns_not_to_trim is all -> just do that, nothing else
    if columns_not_to_trim1 == 'all':
        return df

    # Now columns_not_to_trim is not ALL - it is either blank or list. Get the final left and right
    columns_left_trim_only = list(set(columns_left_trim_only) - set(columns_not_to_trim))
    columns_right_trim_only = list(set(columns_right_trim_only) - set(columns_not_to_trim))
    col_list_for_trim_both_side = list(set(df.columns) - set(columns_right_trim_only) - set(columns_left_trim_only) - set(columns_not_to_trim))
    # print(f"col_list_for_trim_both_side : {col_list_for_trim_both_side}")
    # print(f"columns_left_trim_only : {columns_left_trim_only}")
    # print(f"columns_right_trim_only : {columns_right_trim_only}")

    # first apply left and/or right and after that calculate remaining columns_not_to_trim
    df[columns_left_trim_only] = df[columns_left_trim_only].apply(lambda x: x.str.lstrip())
    df[columns_right_trim_only] = df[columns_right_trim_only].apply(lambda x: x.str.rstrip())
    df[col_list_for_trim_both_side] = df[col_list_for_trim_both_side].apply(lambda x: x.str.strip())

    return df





if __name__=='__main__':
    df1 = read_FWF_to_df(configfile, columns_not_to_trim=None, columns_left_trim_only=None, columns_right_trim_only=None,
                   fileencoding=None,
                   file_location=None, fWf_col_spec='fWf_Col_Spec', col_names='col_Names',
                   add_column_for_file_name=False)
    bb=1

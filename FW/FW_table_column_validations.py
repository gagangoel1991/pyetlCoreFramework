import re
from FW.FW_logger import loggerPass, loggerInfo, loggerFail, loggerDisplay


def verify_all_values_in_column(vdataframe, colName, colvalue, ref_col_list, ignoreblanks = True):
    """Validates if all values of a column in dataframe are same and as colvalue.

    Parameters
    ----------
    vdataframe : dataframe
        DataFrame having data
    colName : string
        Column name for which validation to be done.
    colvalue : string
        Expected value in column
    ref_col_list : List
        List of reference (index) column in dataframe
    ignoreblanks : bool, default True
        If True, will not validate the blank cells in columns and not raise any error

    returns
    --------
    None
    """

    dframe = vdataframe.loc[~(vdataframe[colName] == '')] if ignoreblanks == True else vdataframe

    if len(dframe.loc[dframe[colName] != colvalue]) == 0:
        loggerPass(f"'{colName}' column have all the values as '{colvalue}' as required")
    else:
        dfs = dframe.loc[dframe[colName] != colvalue]
        failed = len(dfs)
        total = len(vdataframe)
        loggerFail(f"'{colName}' column don't have all the values as '{colvalue}'.\n{failed}/{total} records failed, below are some of the failed records:")
        ref_col_list.append(colName)
        loggerDisplay(dfs[ref_col_list].head().to_string(index=False))


def verify_column_is_required_column(dframe, colName, ref_col_list):
    """Validates no blank cell in given column

    Parameters
    ----------
    dframe : dataframe
        DataFrame having data
    colName : string
        Column name for which validation to be done.
    ref_col_list : List
        List of reference (index) column in dataframe

    returns
    --------
    None
    """

    if len(dframe.loc[dframe[colName] == '']) == 0:
        loggerPass(f"'{colName}' column have No Blank value as required")
    else:
        dfs = dframe.loc[dframe[colName] == '']
        failed = len(dfs)
        total = len(dframe)
        loggerFail(f"'{colName}' column have Blank/Null values.\n{failed}/{total} records failed, below are some of the failed records:")
        ref_col_list.append(colName)
        loggerDisplay(dfs[ref_col_list].head().to_string(index=False))


def verify_all_values_in_column_as_numeric(vdataframe, colName, ref_col_list, ignoreblanks = True):
    """Validates if all values of a column in dataframe are numeric.

    Parameters
    ----------
    vdataframe : dataframe
        DataFrame having data
    colName : string
        Column name for which validation to be done.
    ref_col_list : List
        List of reference (index) column in dataframe
    ignoreblanks : bool, default True
        If True, will not validate the blank cells in columns and not raise any error

    returns
    --------
    None
    """

    dframe = vdataframe.loc[~(vdataframe[colName] == '')] if ignoreblanks == True else vdataframe

    if dframe[colName].str.isnumeric().all() == True:
        loggerPass(f"'{colName}' column have all numeric values as required")
    else:
        dfs = dframe[~dframe[colName].apply(lambda x: x.isnumeric())]
        failed = len(dfs)
        total = len(vdataframe)
        loggerFail(f"'{colName}' column does't have all numeric values.\n{failed}/{total} records failed, below are some of the failed records :")
        ref_col_list.append(colName)
        loggerDisplay(dfs[ref_col_list].head().to_string(index=False))


def verify_all_values_in_column_follow_regex(vdataframe, colName, regex, ref_col_list, patternDisp=None, ignoreblanks = True):
    """Validates if all values of a column in dataframe matches with regular expression.

    Parameters
    ----------
    vdataframe : dataframe
        DataFrame having data
    colName : string
        Column name for which validation to be done.
    regex : regular expression pattern
        regex pattern to match all values
    patternDisp : string
        Printable string for regular expression
    ignoreblanks : bool, default True
        If True, will not validate the blank cells in columns and not raise any error

    returns
    --------
    None
    """

    dframe = vdataframe.loc[~(vdataframe[colName] == '')] if ignoreblanks == True else vdataframe

    if dframe[colName].apply(lambda x: bool(re.search(regex, x))).all() == True:
        loggerPass(f"'{colName}' column have all values as pattern '{patternDisp}', as required")
    else:
        dfs = dframe[~dframe[colName].apply(lambda x: bool(re.search(regex, x)))]
        failed = len(dfs)
        total = len(vdataframe)
        loggerFail(f"'{colName}' column does't have all values as pattern '{patternDisp}.\n{failed}/{total} records failed, below are some of the failed records :")
        ref_col_list.append(colName)
        loggerDisplay(dfs[ref_col_list].head().to_string(index=False))
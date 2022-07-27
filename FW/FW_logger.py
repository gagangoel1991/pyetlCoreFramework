from FW.Initialize import initialize_global_variables as iniVar
import threading, multiprocessing
from threading import RLock
import pandas as pd

lock = RLock()


def loggerPass(msg):
    """Write msg string to console and generated report. This will prefix the string with 'PASS' in green colored background.

    Parameters
    ----------
    msg : string
        String to write to console or report

    returns
    --------
    None
    """
#    print(threading.current_thread().name)
#    print(threading.get_ident())
    _helper("PASS", msg)

def loggerFail(msg):
    """Write msg string to console and generated report. This will prefix the string with 'FAIL' in red colored background.

    Parameters
    ----------
    msg : string
        String to write to console or report

    returns
    --------
    None
    """
    _helper("FAIL", msg)
    
def loggerInfo(msg):
    """Write msg string to console and generated report. This will prefix the string with 'INFO' in gray colored background.

    Parameters
    ----------
    msg : string
        String to write to console or report

    returns
    --------
    None
    """
    _helper("INFO", msg)

def loggerDisplay(msg=''):
    """Write msg string to console and generated report without any prefix or color.

    Parameters
    ----------
    msg : string
        String to write to console or report
    returns
    --------
    None
    """
    if isinstance(msg, pd.DataFrame):
        pd.set_option('display.max_columns', None)
        pd.set_option('expand_frame_repr', False)

    msg = str(msg)
    # Checking if the execution type is sequential or multithread and accordingly update the reporting dictionary.
    if threading.current_thread().name == 'MainThread':
        print(msg)
        iniVar.th_local.dict['logger'].append(str(threading.get_ident()) + " - " +  msg)
    else:
        with lock:
            print( msg)
            iniVar.th_local.dict['logger'].append(str(threading.get_ident()) + " - "  + msg)
    if isinstance(msg, pd.DataFrame):
        pd.pd.reset_option("max_columns")
        pd.set_option('expand_frame_repr', True)


    
def _helper(type, msg):
    """colour coding message according to the type"""
    if type == 'PASS':
        vMsg = colors().bg.green + type + colors.reset + " - " + msg
    if type == 'FAIL':
        vMsg = colors().bg.red + type + colors.reset + " - " + msg
    if type == 'INFO':
        vMsg = colors().bg.cyan + type + colors.reset + " - " + msg

    if threading.current_thread().name == 'MainThread':
        #print(type + " - " + msg)
        print(vMsg)
        iniVar.th_local.dict['logger'].append(str(threading.get_ident()) + " - " + type + " - " + msg)
    else:
        #lock = multiprocessing.Manager().Lock()
        with lock:
            print(type + " - " + msg)
            iniVar.th_local.dict['logger'].append(str(threading.get_ident()) + " - " + type  + " - " + msg)


def add_in_reporting_dict(dict_key, dict_val):
    """Add the key value pair to reporting dict, internally used for reporting."""

    thId = str(threading.get_ident())
    if threading.current_thread().name == 'MainThread':
        iniVar.th_local.dict[thId + '-' + dict_key] =dict_val
    else:
        #iniVar.reporting_dict[thId + '-' + dict_key] = dict_val
        #lock = multiprocessing.Manager().Lock()
        with lock:
            iniVar.th_local.dict[thId + '-' + dict_key] =dict_val

def get_from_reporting_dict(dict_key):
    """Returns the value of key from the reporting dictionary"""

    thId = str(threading.get_ident())
    if threading.current_thread().name == 'MainThread':
        return iniVar.th_local.dict[thId + '-' + dict_key]
    else:
        #lock = multiprocessing.Manager().Lock()
        with lock:
            return iniVar.th_local.dict[thId + '-' + dict_key]

def check_key_in_reporting_dict(dict_key):
    """Checks if given key is available in reporting dictionary"""

    thId = str(threading.get_ident())
    if threading.current_thread().name == 'MainThread':
        return (thId + '-' + dict_key) in iniVar.th_local.dict
    else:
        #lock = multiprocessing.Manager().Lock()
        with lock:
            return (thId + '-' + dict_key) in iniVar.th_local.dict

def add_detail_tabs_info_in_reporting_dict(tab_key, lst_comp_df):
    """Adds comparison result data frames to reporting dictionary's detail tabs dictionary . This will add the data into the reporting dict when there are additional tabs present. """

    thId = str(threading.get_ident())
    if threading.current_thread().name == 'MainThread':
        iniVar.th_local.dict['detail_tabs'][thId + '-' + tab_key] = lst_comp_df
    else:
        with lock:
            iniVar.th_local.dict['detail_tabs'][thId + '-' + tab_key] = lst_comp_df

def check_detail_tabs_info_present_in_reporting_dict():
    """Check if custom tab details available for adding in report"""

    thId = str(threading.get_ident())
    if threading.current_thread().name == 'MainThread':
        return bool(len(iniVar.th_local.dict['detail_tabs']))
    else:
        with lock:
            return bool(len([key for key in iniVar.th_local.dict['detail_tabs'].keys() if key.startswith(thId + '-')]))

def get_detail_tabs_name_list_from_reporting_dict():
    """Get the names of custom detail tabs to be added in report"""
    thId = str(threading.get_ident())

    # checking if the execution type is sequential or multithreaded as if it is multithreaded then we need to handle threading.
    if threading.current_thread().name == 'MainThread':
        return [key for key in iniVar.th_local.dict['detail_tabs'].keys() if key.startswith(thId + '-')]
    else:
        with lock:
            return [key for key in iniVar.th_local.dict['detail_tabs'].keys() if key.startswith(thId + '-')]

def get_detail_tabs_info_values_list_from_reporting_dict(dict_key):
    """Get the names of custom detail tabs to be added in report.
       Details tabs are generated when we have multiple comparison in the same script which require multiple tabs for report preparation or if we explicitly define "report_tab_name" attribute.
    """
    #thId = str(threading.get_ident())
    # checking if the execution type is sequential or multithreaded as if it is multithreaded then we need to handle threading.
    if threading.current_thread().name == 'MainThread':
        return iniVar.th_local.dict['detail_tabs'][ dict_key]
    else:
        with lock:
            return iniVar.th_local.dict['detail_tabs'][dict_key]


import os
import sys


class colors:
    """Constant color and format values for colored console outputs"""

    def __init__(self):
        if sys.platform.lower() == "win32":
            os.system('color')

    reset = '\033[0m'
    bold = '\033[01m'
    blink = '\x1b[5m'
    underline = '\033[4m'
    disable = '\033[02m'
    underline = '\033[04m'
    reverse = '\033[07m'
    strikethrough = '\033[09m'
    invisible = '\033[08m'

    class fg:
        """Constant color and format values for forground-colors in console outputs"""

        black = '\033[30m'
        red = '\033[31m'
        green = '\033[32m'
        orange = '\033[33m'
        blue = '\033[34m'
        purple = '\033[35m'
        cyan = '\033[36m'
        lightgrey = '\033[37m'
        darkgrey = '\033[90m'
        lightred = '\033[91m'
        lightgreen = '\033[92m'
        yellow = '\033[93m'
        lightblue = '\033[94m'
        pink = '\033[95m'
        lightcyan = '\033[96m'

    class bg:
        """Constant color and format values for background-colors in console outputs"""
        black = '\033[40m'
        red = '\033[41m'
        green = '\033[42m'
        orange = '\033[43m'
        blue = '\033[44m'
        purple = '\033[45m'
        cyan = '\033[46m'
        lightgrey = '\033[47m'




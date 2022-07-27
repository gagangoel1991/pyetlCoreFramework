import FW.Initialize.initialize_global_variables as iniVar
import glob, pathlib, functools
import os,re
import pandas as pd

def _infoPrint(func):
    @functools.wraps(func)
    def wrapper(*args):
        print("tags/test filters : ", [args[x] for x in range(len(args))])
        #ret to have a list of testscripts that matches the pattern.
        ret = func(*args)
        print("tests : " , [x.replace(iniVar.current_project_test_path,'') for x in ret[1]])  # prints test name along with its folder names
        return ret
    return wrapper

class tags:
    """Class representing tags filtering for multiple script execution"""
    
    def __init__(self, *args, **kwargs):
        pass
    def __call__(self, *args, **kwargs):
        return args[0]
        
    def _get_list_of_all_scripts():
        """Fetches names of all the scripts and their paths in the project"""
        # filelst =  glob.glob(iniVar.current_project_test_path + r"\*.py")
        # testlst = [x for x in filelst if (('0_batchrunner.py' not in x.lower()) and ('__init__.py' not in x.lower()))]
        # return testlst

        testNamelst = []
        pathlst = []
        for path in pathlib.Path(iniVar.current_project_test_path).rglob('*.py'):
            if (('0_batchrunner.py' not in path.name.lower()) and ('__init__.py' not in path.name.lower())):
                testNamelst.append(path.name.lower())
                pathlst.append(os.path.join(path.parent, path.name))
        return testNamelst, pathlst



    def _get_part_lines(fp):
        """To be used internally
            It extracts all the lines from the script before it finds the string "def test_main()" in a line.
        """
        fp_part =[]
        for ln in fp:
            if not ln.lower().startswith("def test_main():"):
                fp_part.append(ln)
            else:
                break
        return fp_part
    
    
    def _find_tests_with_tags(pattern, args):
        """Finds tests with the given tag name. To be used internally"""

        testlist = []
        pathlst_final=[]
        #extracting the script names and tag names
        filelst, pathlst = tags._get_list_of_all_scripts()
        taglist = [args[x] for x in range(len(args))]
        for f, p in zip(filelst, pathlst):
            with open(p, "r", encoding='utf-8') as fp:
                fp_part =tags._get_part_lines(fp)
                tagline =';'.join([ln.replace("'","\"").lower().replace("@tags","") for ln in fp_part if ln.startswith('@tags(') == True])
                #print(tagline + "--------------------------------------->" + pathlib.Path(f).stem)
                for tag in taglist:
                    lower_patt = pattern.replace('xxxx', tag).lower()
                    if lower_patt in tagline:
                        testlist.append(pathlib.Path(f).stem + pathlib.Path(f).suffix) # list of test name for test cases filtered as per criteria
                        pathlst_final.append(p)   # list of paths for test cases filtered as per criteria
                        break
            continue
        return testlist, pathlst_final
    
    @_infoPrint
    def startswith(*args):
        """Returns all the test scripts whose at least one of the tags starts with given values"""

        return tags._find_tests_with_tags('"xxxx', args)
    
    @_infoPrint
    def endswith(*args):
        """Returns all the test scripts whose at least one of the tags ends with given values"""

        return tags._find_tests_with_tags('xxxx"', args)
        
    @_infoPrint
    def containing(*args):
        """Returns all the test scripts whose at least one of the tags contains given values"""

        return tags._find_tests_with_tags('xxxx', args)

    @_infoPrint
    def equals(*args):
        """Returns all the test scripts whose at least one of the tags equals to given values"""

        return tags._find_tests_with_tags('"xxxx"', args)
        

class tests:
    """Class representing tests filtering for multiple script execution"""
           
    def _get_list_of_all_script_names():
        """Returns all the test scripts names, their original (case sensitive names) and their paths in given project"""
        #==========================
        testNamelst=[]
        pathlst=[]
        testNamelst_orig=[]
        for path in pathlib.Path(iniVar.current_project_test_path).rglob('*.py'):
            if (('0_batchrunner.py' not in path.name.lower()) and ('__init__.py' not in path.name.lower())):
                testNamelst.append(path.name.lower())
                pathlst.append(os.path.join(path.parent,path.name))
                testNamelst_orig.append(path.name)
        # ==========================
        # filelst =  glob.glob(iniVar.current_project_test_path + r"\*.py")
        # testNamelst = [pathlib.Path(x).stem.lower()+ pathlib.Path(x).suffix.lower() for x in filelst if (('0_batchrunner.py' not in x.lower()) and ('__init__.py' not in x.lower()))]
        # testNamelst_orig = [pathlib.Path(x).stem + pathlib.Path(x).suffix.lower() for x in filelst if (('0_batchrunner.py' not in x.lower()) and ('__init__.py' not in x.lower()))]
        return testNamelst,testNamelst_orig, pathlst

    @_infoPrint
    def startswith(*args):
        """Returns all the test scripts and paths whose name starts with given values"""
        testlist = []
        pathlst_final=[]
        fileNamelst, testNamelst_orig, pathlst = tests._get_list_of_all_script_names()
        tlist = [args[x] for x in range(len(args))]
        for i, fn in enumerate(fileNamelst):
            for t in tlist:
                if fn.startswith(t.lower()) == True:
                    testlist.append(testNamelst_orig[i])
                    pathlst_final.append(pathlst[i])
                    break
            continue
        #print(testlist )
        return testlist, pathlst_final
    
    @_infoPrint
    def endswith(*args):
        """Returns all the test scripts and their paths whose name ends with given values"""

        testlist = []
        pathlst_final = []
        fileNamelst, testNamelst_orig, pathlst = tests._get_list_of_all_script_names()
        tlist = [args[x] for x in range(len(args))]
        for i, fn in enumerate(fileNamelst):
            for t in tlist:
                if fn.endswith(t.lower()) == True:
                    testlist.append(testNamelst_orig[i])
                    pathlst_final.append(pathlst[i])
                    break
            continue
        #print(testlist)
        return testlist,pathlst_final

    @_infoPrint
    def containing(*args):
        """Returns all the test scripts and their paths whose name contains given values"""

        testlist = []
        pathlst_final = []
        fileNamelst, testNamelst_orig, pathlst = tests._get_list_of_all_script_names()
        tlist = [args[x] for x in range(len(args))]
        for i, fn in enumerate(fileNamelst):
            for t in tlist:
                if t.lower() in fn:
                    testlist.append(testNamelst_orig[i])
                    pathlst_final.append(pathlst[i])
                    break
            continue
        #print(testlist)
        return testlist,pathlst_final
    
    @_infoPrint
    def equals(*args):
        """Returns all the test scripts and their paths whose naem equals ot value of list"""

        testlist = []
        pathlst_final = []
        fileNamelst, testNamelst_orig, pathlst = tests._get_list_of_all_script_names()
        tlist = [args[x] for x in range(len(args))]
        for i, fn in enumerate(fileNamelst):
            for t in tlist:
                if fn==t.lower():
                    testlist.append(testNamelst_orig[i])
                    pathlst_final.append(pathlst[i])
                    break
            continue
        #print(testlist)
        return testlist,pathlst_final


def taglist(tests_folder_path = None, save_file_type='excel', save_file_folder_path=None):
    """generate list of tags in all scripts under Tests folder or given path - at console and save as csv or excel file

    Parameters
    ----------
    tests_folder_path : string ,default None
        It is the path of our test folder.
    save_file_type : string , default Excel
        It can take either 'excel' or a 'csv' as an argument. It will write the dataframe to a file .
    save_file_folder_path : string , default None
        It is the path of our Reports folder.

    returns
    ---------
    Dataframe : It contains list of tags for all scripts.

    """

    save_file_folder_path = iniVar.current_project_path + r"\Reports" if save_file_folder_path==None else save_file_folder_path
    tests_folder_path = iniVar.current_project_path + r"\Tests" if tests_folder_path==None else tests_folder_path

    regex = r"^@tags.*?\)"
    tags_dict = {}
    fileNames = os.listdir(tests_folder_path)

    for fileName in fileNames:
        if fileName.endswith(".py") and ("BatchRunner" not in fileName):     #ignore batchrunner files
            with open(fileName, 'r') as fo:
                myData = fo.readlines()
                matches = re.findall(regex, ''.join(myData), re.MULTILINE)        #All valid matches only - not commented etc.
                str_all_tags = ','.join(matches).replace('@tags(', '').replace(')', '').replace('"', '').replace("'", '')
                str_unique_tags = ', '.join(list(sorted(set(str_all_tags.split(','))))) if len(str_all_tags) >0 else 'No Tags available'   #finding unique tags
                tags_dict[fileName]= str_unique_tags

    df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in tags_dict.items()])).T.fillna('').reset_index()
    df.columns = ['Script Name' ,'Tags']
    df = df.set_index('Script Name')

    # print on console
    print(df)
    if save_file_type=='csv':
        df.to_csv(save_file_folder_path + r"\taglist.csv")
    if save_file_type == 'excel':
        df.to_excel(save_file_folder_path + r"\taglist.xlsx")

    return df
    

    
    
    
    
    
    

from FW.FW_logger import add_in_reporting_dict
import types


def run_post_test_hook(post_test_hook_function_list, post_test_hook_function_parameter_list):
    """Executes a function post report generation to determine the functionality of the code.
    Parameters.
    ----------
    post_test_hook_function_list: list of function to be executed.
    post_test_hook_function_parameter_list: list of arguments to be passed into function to be executed.
     """

    for post_hook_func, post_hook_func_arguments in zip(post_test_hook_function_list,post_test_hook_function_parameter_list):
        if post_hook_func_arguments != None:
            post_hook_func(*post_hook_func_arguments)
        else:
            post_hook_func()


def run_post_test_hook_functions(*args, parameters=''):
    """ To create a list of functions and list of arguments that will be passed into the run post test hook function.

    Parameters.
    ----------
    args: provides a list which comprises of function list , argument list or both
    parameters: ''/ list . default: ''
                If '' : Does not expects the parameters to be provided separately in function call. It Can accept :
                        1) dictionary having key, value pair as function name and arguments passed and . To Provide the value from a dictionary, function_names should be provided within 'func_name' key
                         name and arguments should be provided inside 'parameters' key name
                        2) list of function names
                if List : Then those parameters inside the list will be passed to the functions.
    """

    if parameters == '':
        # To check if input in args is a function type or a list of dictionary type. If Function type then if block will be executed otherwise else block will be executed for no parameters provided.
        if isinstance(args[0], types.FunctionType):
            function_list = [arg for arg in args]
            parameter_list = [None for arg in args]
        else:
            # This will be executed if input to the run_post_test_hook_functions is a dictionary type.
            function_list = [arg['func_name'] for arg in args[0]]  # To extract the list of function names
            parameter_list = [arg.get('parameters', None) for arg in args[0]]  # To extract the list of argument types

    else:
        # If parameters are provided then below code will be executed.
        function_list = [arg for arg in args]
        parameter_list = parameters

    add_in_reporting_dict('post_test_hook_function_list', function_list)
    add_in_reporting_dict('post_test_hook_function_parameter_list', parameter_list)








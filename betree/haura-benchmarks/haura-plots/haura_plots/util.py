"""
Utility functions which may be used in multiple plotting types.
"""

import json

# Constants
BLOCK_SIZE=4096
EPOCH_MS=500
SEC_MS=1000

# For color reference of "Wong" color scheme see:
# https://davidmathlogic.com/colorblind/#%23000000-%23E69F00-%2356B4E9-%23009E73-%23F0E442-%230072B2-%23D55E00-%23CC79A7
WHITE='#FFFFFF'
GREEN='#009E73'
YELLOW='#F0E442'
BLUE='#0072B2'
LIGHT_BLUE='#56B4E9'
RED='#D55E00'
ORANGE='#E69F00'

def read_jsonl(file):
    """
    Read from a file descriptor line by line a json, parse it, and return a list
    of read objects.
    """
    data = []
    while True:
        # Get next line from file
        line = file.readline()
        # if line is empty
        # end of file is reached
        if not line:
            break
        json_object = json.loads(line)
        data.append(json_object)
    return data

def subtract_last_index(array):
    """
    From a list of numbers subtract the value of the previous entry from the
    next. Operates in-place.
    """
    last_val = 0
    for index, value in enumerate(array):
        array[index] = value - last_val
        last_val = value
    array[0] = 0

def subtract_first_index(array):
    """
    From a list of numbers subtract the first entry from all entries. Operates
    in-place.
    """
    first_val = array[0]
    for index, value in enumerate(array):
        array[index] = value -first_val

def num_to_name(tier):
    """Convert a number to the corresponding tier name in the storage
    hierarchy."""
    match tier:
        case 0:
            return 'Fastest'
        case 1:
            return 'Fast'
        case 2:
            return 'Slow'
        case 3:
            return 'Slowest'
        case _:
            return '???'

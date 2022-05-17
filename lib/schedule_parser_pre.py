# -*- coding: utf-8 -*-
import re
from typing import Tuple, List, Any, Union
import numpy as np
import pandas as pd

def read_schedule(path: str, mode: str, enc: str) -> str:
    """
    read the input .inc file forming a string of text
    @param path: path to the input .inc file
    @param mode: reading mode
    @param enc: encoding
    @return: string of input text
    """
    with open(path, mode, encoding=enc) as file:
        text = file.read()

    return text

def inspect_schedule(text: str) -> bool:
    """
    inspect schedule syntax
    @param text: input text from .inc file
    @return: inspected input text from .inc file
    """
    if re.search(r'^\s*$', text):
        return False
    else:
        return True

def clean_schedule(text: str) -> str:
    """
    clean '-- ' comments
    @param text: inspected input text from .inc file
    @return: cleaned input text from .inc file
    """

    text2 = re.sub(r'\s*--.*', '', text)
    text3 = re.sub(r'\n+\s*', '\n', text2)

    return text3




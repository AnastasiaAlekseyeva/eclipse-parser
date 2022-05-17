# -*- coding: utf-8 -*-
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import Tuple, List, Union


def results_to_csv(schedule_list: List[List[str]], csv_file: str, columns: Union[bool, Tuple[str]]) -> pd.DataFrame:
    """
    form PanDas dataframe with results and (optional) writes it into .csv file
    @param schedule_list: list of elements [[DATA1, WELL1, PARAM1, PARAM2, ...], [DATA2, ...], ...]
    @param csv_file: path to .csv file to save PanDas dataframe with results
    @param columns: list of columns in output .csv file
    @return: PanDas dataframe with results
    """
    output = pd.DataFrame(schedule_list)
    output.columns = columns
    output.to_csv(csv_file, sep=";", header=columns)

    return output
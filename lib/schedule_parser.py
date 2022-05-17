# -*- coding: utf-8 -*-
import re
import numpy as np
import pandas as pd
from typing import Tuple, List, Any, Union
from lib import schedule_parser_pre
from lib import schedule_parser_post

def transform_schedule(keywords: Tuple[str], parameters: Tuple[str], input_file: str, output_csv: str, clean_file) -> pd.DataFrame:
    """
    read the input .inc-file and transform it to .csv schedule
    your main function
    @param keywords: a tuple of keywords we are interested in (DATES, COMPDAT, COMPDATL, etc.)
    @param parameters: column names of output .csv file
    @param input_file: path to your source input .inc file
    @param output_file: path to your output .csv file
    @return:
    """
    schedule_text = schedule_parser_pre.read_schedule(input_file, "r", enc="utf-8")

    if schedule_parser_pre.inspect_schedule(schedule_text):
        text_clean = schedule_parser_pre.clean_schedule(schedule_text)
        with open(clean_file, "w", encoding="utf-8") as handled_file:
            handled_file.write(text_clean)

        schedule = parse_schedule(text_clean, keywords)
        data_frame = schedule_parser_post.results_to_csv(schedule, output_csv, columns=parameters)

    return data_frame


def parse_schedule(text: str, keywords_tuple: Tuple[str]) -> List[List[str]]:
    """
    return list of elements ready to be transformed to the resulting DataFrame
    @param text: cleaned input text from .inc file
    @param keywords_tuple: a tuple of keywords we are interested in (DATES, COMPDAT, COMPDATL, etc.)
    @return: list of elements [[DATA1, WELL1, PARAM1, PARAM2, ...], [DATA2, ...], ...] ready to be transformed
    to the resulting DataFrame
    """
    cur_date = np.nan
    schedule_list = []
    block_list = []

    keyword_blocks = extract_keyword_blocks(text, keywords_tuple)

    for keyword_block_i in keyword_blocks:
        keyword, keyword_lines = extract_lines_from_keyword_block(keyword_block_i)
        cur_date, schedule_list, block_list = parse_keyword_block(keyword, keyword_lines, cur_date, schedule_list, block_list)

    keyword = 'END'
    cur_date, schedule_list, block_list = parse_keyword_block(keyword, keyword_lines, cur_date, schedule_list, block_list)

    return schedule_list

def extract_keyword_blocks(text: str,keywords: Tuple[str]) -> List[Tuple[str]]:
    """
    return keywords text blocks ending with a newline "/"
    @param text: cleaned input text from .inc file
    @param keywords_tuple: a tuple of keywords we are interested in (DATES, COMPDAT, COMPDATL, etc.)
    @return: list keywords text blocks ending with a newline "/"
    """
    list_of_blocks = re.split('\n/\n', text)
    list_output = []
    for i in range(len(list_of_blocks)):
        for j in range(len(keywords)):
            if re.findall(r'\w+', list_of_blocks[i])[0] == keywords[j]:
                # list_output.append(tuple(list_of_blocks[i].splitlines()))
                list_output.append(tuple(list_of_blocks[i].split('\n')))
    return list_output


def extract_lines_from_keyword_block(block: Tuple[str]) -> Tuple[str, List[str]]:
    """
    extract the main keyword and corresponding lines from a certain block from the input file
    @param block: a block of the input text related to the some keyword (DATA, COMPDAT, etc.)
    @return:
        - keyword - DATA, COMPDAT, etc.
        - lines - lines of the input text related to the current keyword
    """
    return block[0], list(block[1::])


def parse_keyword_block(keyword: str, keyword_lines: List[str], cur_date,
                        schedule_list: List[List[str]], block_list: List[List[str]]) \
        -> Tuple[str, List[List[str]], List[List[str]]]:
    """
    parse a block of the input text related to the current keyword (DATA, COMPDAT, etc.)
    @param keyword: DATA, COMPDAT, etc.
    @param keyword_lines: lines of the input text related to the current keyword
    @param current_date: the last parsed DATE. The first DATE is NaN if not specified
    @param schedule_list: list of elements [[DATA1, WELL1, PARAM1, PARAM2, ...], [DATA2, ...], ...]
    @param block_list: schedule_list but for the current keyword
    @return:
        - current_date - current DATE value which might be changed if keyword DATES appears
        - schedule_list - updated schedule_list
        - block_list - updated block_list
    """
    def parse_data(keyword_lines, schedule_list):
        for keyword_line in keyword_lines[:-1]:
            temp_data = parse_keyword_DATE_line(keyword_line)
            schedule_list.append([temp_data, np.nan])
        cur_date = parse_keyword_DATE_line(keyword_lines[-1])
        block_list = [1]

        return schedule_list, cur_date, block_list

    if keyword == "DATES" or keyword == "END":
        if len(block_list) != 0:
            schedule_list.append([cur_date, np.nan])
        else:
            pass
        schedule_list, cur_date, block_list = parse_data(keyword_lines, schedule_list)

    elif keyword == "COMPDAT":
        for well_comp_line in keyword_lines:
            well_comp_data = parse_keyword_COMPDAT_line(well_comp_line)
            well_comp_data.insert(0, cur_date)
            schedule_list.append(well_comp_data)
            block_list = []

    elif keyword == "COMPDATL":
        for well_comp_line in keyword_lines:
            well_comp_data = parse_keyword_COMPDATL_line(well_comp_line)
            well_comp_data.insert(0, cur_date)
            schedule_list.append(well_comp_data)
            block_list = []

    return cur_date, schedule_list, block_list


def parse_keyword_DATE_line(current_date_line: str) -> str:
    """
    parse a line related to a current DATA keyword block
    @param current_date_line: line related to a current DATA keyword block
    @return: list of parameters in a DATE line
    """
    return re.findall(r'\d{2}\s+[A-Z]{3}\s+\d{4}', current_date_line)[0]

def parse_keyword_COMPDAT_line(well_comp_line: str) -> List[str]:
    """
    parse a line related to a current COMPDAT keyword block
    @param well_comp_line: line related to a current COMPDAT keyword block
    @return: list of parameters (+ NaN Loc. grid. parameter) in a COMPDAT line
    """
    well_comp_line = default_params_unpacking_in_line(well_comp_line)
    well_comp_line = re.sub(r'/|\'', ' ', well_comp_line)
    well_comp_line = re.sub(r'\s+', ' ', well_comp_line)
    parameters_list = well_comp_line.split()
    parameters_list.insert(1, np.nan)

    return parameters_list

def parse_keyword_COMPDATL_line(well_comp_line: str) -> List[str]:
    """
    parse a line related to a current COMPDATL keyword block
    @param well_comp_line: line related to a current COMPDATL keyword block
    @return: list of parameters in a COMPDATL line
    """
    well_comp_line = default_params_unpacking_in_line(well_comp_line)
    well_comp_line = re.sub(r'/|\'', ' ', well_comp_line)
    well_comp_line = re.sub(r'\s+', ' ', well_comp_line)
    parameters_list = well_comp_line.split()

    return parameters_list

# для парсинга параметров по-умолчанию
def default_params_unpacking_in_line(line: str) -> str:
    """
    unpack default parameters set by the 'n*' expression
    @param line: line related to a current COMPDAT/COMPDATL keyword block
    @return: the unpacked line related to a current COMPDAT/COMPDATL keyword block
    """
    default_parameters = re.findall(r'\w+\*', line)

    for default_parameter_i in default_parameters:
        line = re.sub(default_parameter_i[:-1] + '\*', ('DEFAULT') * int(default_parameter_i[:-1]), line)
        while re.search(r'DEFAULTDEFAULT', line):
            line = re.sub('DEFAULTDEFAULT', 'DEFAULT DEFAULT', line)

    return line


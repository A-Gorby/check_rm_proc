import pandas as pd
pd.options.display.max_columns=200

import numpy as np
import os, sys, glob
import humanize
import re
import regex
import xlrd

import json
import itertools
#from urllib.request import urlopen
#import requests, xmltodict
import time, datetime
import math
from pprint import pprint
import gc
from tqdm import tqdm
tqdm.pandas()
import pickle

import duckdb
# https://stackoverflow.com/questions/75352219/fix-unimplemented-casting-error-in-duckdb-insert
duckdb.default_connection.execute("SET GLOBAL pandas_analyze_sample=100000")
import pyarrow

import logging
import zipfile
import tarfile
import py7zr
import argparse

import warnings
warnings.filterwarnings("ignore")

from openpyxl import load_workbook
from openpyxl import Workbook
from openpyxl.comments import Comment
from openpyxl.styles import colors
from openpyxl.styles import Font, Color
from openpyxl.utils import units
from openpyxl.styles import Border, Side, PatternFill, GradientFill, Alignment
from openpyxl import drawing

import matplotlib.pyplot as plt
# import seaborn as sns
# %matplotlib inline
from matplotlib.colors import ListedColormap, BoundaryNorm

from utils_io import insert_pd_col_after_col, save_df_lst_to_excel_xlsxwriter

class Logger():
    def __init__(self, name = 'main',
                 strfmt = '[%(asctime)s] [%(levelname)s] > %(message)s', # strfmt = '[%(asctime)s] [%(name)s] [%(levelname)s] > %(message)s'
                 level = logging.INFO,
                 datefmt = '%H:%M:%S', # '%Y-%m-%d %H:%M:%S'
                #  datefmt = '%H:%M:%S %p %Z',

                 ):
        self.name = name
        self.strfmt = strfmt
        self.level = level
        self.datefmt = datefmt
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.level) #logging.INFO)
        self.offset = datetime.timezone(datetime.timedelta(hours=3))
        # create console handler and set level to debug
        self.ch = logging.StreamHandler()
        self.ch.setLevel(self.level)
        # create formatter
        self.strfmt = strfmt # '[%(asctime)s] [%(levelname)s] > %(message)s'
        self.datefmt = datefmt # '%H:%M:%S'
        # СЃРѕР·РґР°РµРј С„РѕСЂРјР°С‚С‚РµСЂ
        self.formatter = logging.Formatter(fmt=strfmt, datefmt=datefmt)
        self.formatter.converter = lambda *args: datetime.datetime.now(self.offset).timetuple()
        self.ch.setFormatter(self.formatter)
        # add ch to logger
        self.logger.addHandler(self.ch)
logger = Logger().logger
logger.propagate = False

if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    # del logger
    logger = Logger().logger
    logger.propagate = False

def read_data(
    data_source_dir,
    fn_features_source,
    sh_n_features_source,
    col_source,
    fn_features_proc,
    sh_n_features_proc,
    col_proc,
    ):

    if fn_features_source is None or sh_n_features_source is None or fn_features_proc is None or sh_n_features_proc is None:
        logger.info("Не опеределены все или один из входных фалов и листов Excel")
        logger.info(f"Файл Excel, переданный в обработку: '{fn_features_source}'")
        logger.info(f"Лист Excel, переданный в обработку: '{sh_n_features_source}'")
        logger.info(f"Файл Excel, полученный из обработки: '{fn_features_proc}'")
        logger.info(f"Лист Excel, полученный из обработки: '{sh_n_features_proc}'")
    if not os.path.exists(os.path.join(data_source_dir, fn_features_source)):
        logger.error(f"Файл Excel, переданный в обработку: '{fn_features_source}' не найден")
    if not os.path.exists(os.path.join(data_source_dir, fn_features_proc)):
        logger.error(f"Файл Excel, полученный из обработки: '{fn_features_proc}' не найден")

    try:
        df_rm_source_t = pd.read_excel(os.path.join(data_source_dir, fn_features_source), sheet_name=sh_n_features_source, nrows=5)
        source_cols = list(df_rm_source_t.columns)
        source_int_cols = ['id наименования характеристики','id значения характеристики', 'Код ОКЕИ', 'КПГЗ 01.02.11* только',]
        source_str_cols = list(set(source_cols)-set(source_int_cols))
        converters = dict(zip(source_int_cols, len(source_int_cols)*[int]))
        converters.update(
            dict(zip(source_str_cols, len(source_str_cols)*[str]))
        )
        # pprint(converters)
        df_rm_source = pd.read_excel(os.path.join(data_source_dir, fn_features_source), sheet_name=sh_n_features_source,
                                 converters=converters) 
        logger.info(f"Файл Excel, переданный в обработку: '{fn_features_source}':\n(строк, колонок): {str(df_rm_source.shape)}")
    except Exception as err:
       logger.error(str(err))
       logger.error("Работа программы прекращена")
       sys.exit()

    try:
        df_rm_proc = pd.read_excel(os.path.join(data_source_dir, fn_features_proc), sheet_name=sh_n_features_proc,
                                   converters=converters) #{'ИНП':str, 'Наименование СПГЗ':str})
        logger.info(f"Файл Excel, полученный из обработки: '{fn_features_proc}':\n(строк, колонок): {str(df_rm_proc.shape)}")
    except Exception as err:
       logger.error(str(err))
       logger.error("Работа программы прекращена")
       sys.exit()

    req_cols_rm_source = source_cols.copy()
    if col_proc in req_cols_rm_source:
        req_cols_rm_source.remove(col_proc)
    if not set(req_cols_rm_source).issubset(df_rm_source.columns):
        logger.error(f"Файл Excel, переданный в обработку, не содержит одну или несколько обязательных колонок: \n{str(list(set(req_cols_rm_source) - set(df_rm_source.columns)))}")

    req_cols_rm_proc = req_cols_rm_source + [col_proc]
    if not set(req_cols_rm_proc).issubset(df_rm_proc.columns):
        logger.error(f"Файл Excel, полученный из обработки, не содержит одну или несколько обязательных колонок: \n{str(list(set(req_cols_rm_proc) - set(df_rm_proc.columns)))}")

    return df_rm_source, df_rm_proc, req_cols_rm_source

def compare_key_fields(
    df_rm_source, 
    df_rm_proc, 
    req_cols_rm_source,
    debug=False):
    """
    пар "Наименование СПГЗ"/"Наименование СПГЗ clean"
    """
   
    name_spgz_col = "Наименование СПГЗ"
    name_spgz_clean_col = "Наименование СПГЗ clean"
    imp_col = 'ИНП'
    name_char_col = 'Наименование характеристики 02'

    # проверка "полных" ключей - все кроме 'Наимнеование харакетристики (экспертно)'
    df_diff_01 = df_rm_source[req_cols_rm_source].merge(df_rm_proc[req_cols_rm_source], indicator=True, how='outer')
    if df_diff_01[df_diff_01['_merge'] == 'left_only'].shape[0] > 0:
        logger.info('В файле, полученном из обработки, отсутствуют следующие строки с ключевыми полями:')
        display(df_diff_01[df_diff_01['_merge'] == 'left_only'])
    if df_diff_01[df_diff_01['_merge'] == 'right_only'].shape[0] > 0:
        logger.info('В файле, полученном из обработки, добавлены следующие строки с ключевыми полями, которых не было в исходном файле:')
        display(df_diff_01[df_diff_01['_merge'] == 'right_only'])
    if (
        (df_diff_01[df_diff_01['_merge'] == 'right_only'].shape[0]==0) or
        (df_diff_01[df_diff_01['_merge'] == 'left_only'].shape[0]==0)
        ):
        logger.info('В исходном файле и в файле, полученном из обработки, \nключевые поля (все кроме которое подлежит правке и то, которое исправлялось)\n- одинаковы')
    return df_diff_01

def compare_eq_key_fields(df_diff_01,
                          df_rm_source,
                          df_rm_proc,
                          source_col='Наименование характеристики 02',
                          expert_col='Наименование характеристики (экспертно)',
                          debug=False):

    df_diff_02 = df_diff_01[df_diff_01['_merge']=='both'].drop(columns=['_merge']).merge(
        df_rm_proc,
        how='left',
        # indicator=True
    )
    df_diff_02 = insert_pd_col_after_col(
                  df_diff_02, expert_col, source_col
                )
    # display(df_diff_02[(df_diff_02['_merge']=='left_only') | (df_diff_02['_merge']=='right_only') ])

    if debug:
        spgz_name = 'Средство гемостатическое синтетическое пептидное'
        display(df_diff_02[
            (df_diff_02['Наименование СПГЗ']==spgz_name) &
            (df_diff_02['Источник']=='Наименование РМ')
        ]
        )

    df_rm_source_cpy = df_diff_01.drop(columns=['_merge']).merge(
        df_rm_source,
        how='left')
    df_rm_source_cpy[expert_col] = df_rm_source_cpy[source_col]

    df_diff_03 = df_rm_source_cpy.merge(
        df_diff_02,
        how='outer',
        indicator=True
    )

    df_diff_03 = insert_pd_col_after_col(
                  df_diff_03, expert_col, source_col
                )
    if debug:
        logger.info("Корректные клюевые строки с выполненными изменениями:")
        display(df_diff_03[(df_diff_03['_merge']=='right_only') ])
        # содержит только изменения в колонке expert_Col (в строках с неизмененными ключевыми полями - всеми остльными) по сравнению с исходным
        # display(df_diff_03[(df_diff_03['_merge']=='left_only') ])
        # содержит в колонке expert_Col (в строках с неизмененными ключевыми полями - всеми остльными) значения до изменния
        # и строки, в откорых изменены ключевые поля (все кроме experrt_col) - т.е. ошибочные

    return df_diff_02, df_diff_03

import string
def define_process_type_in_row(source_value, expert_value, spgz_name,
                               sep = '|',
                               debug=False):
    proc_type_divide = 'Разбиение'
    proc_type_reformat = 'Изменение регистра букв'
    proc_type_merge = 'Слияние'
    proc_type_cutting = 'Отсечение'
    proc_type_reformulation = 'Переформулирование'
    proc_type_unknwn = 'unknwn'
    # proc_type_divide_merge = 'Разбиение/Слияние'
    # proc_type_divide_spgz = 'ПереРазбиение СПГЗ'

    proc_type_comm_permutation = 'С перестановкой слов'
    proc_type_comm_not_in_source = 'Нет ни одного слова из исходной колонки'
    proc_type_comm_not_in_spgz = 'Отдельных слов нет в наименовании СПГЗ'
    proc_type_comm_reformulation = 'Переформулирование'
    proc_type_comm_wo_punctuation = 'Без учета пунктуации'
    proc_type_comm_unknwn = 'unknwn'

    proc_type, proc_type_comm = None, None
    re_punct_tmplt = re.compile(f"[{re.escape(string.punctuation)}]")
    if debug:
        print("source_value:", source_value)
        print("expert_value:", expert_value)
        print("spgz_name:", spgz_name)

    if sep in expert_value:
        expert_value_sep_lower_lst = [s.lower().strip() for s in expert_value.split(sep)]
        expert_value_sep_lower_lst = [s for s in expert_value_sep_lower_lst if len(s)>0]
        expert_value_sep_lower = ' '.join(expert_value_sep_lower_lst)

        if source_value.lower() == expert_value_sep_lower:
            proc_type = proc_type_divide
        # elif set(source_value.lower().split()).issubset([sss for ss in [s.split() for s in expert_value_sep_lower_lst] for sss in ss] ):
        elif set(source_value.lower().split())==set([sss for ss in [s.split() for s in expert_value_sep_lower_lst] for sss in ss] ):
        # если слова собраны в другом порядке (не обязательно обратном)
        # доп split если в expert value col не только слова, но и словосочетания
            proc_type = proc_type_divide
            proc_type_comm = proc_type_comm_permutation
        elif(
            (len(set(re_punct_tmplt.sub('', source_value).strip().lower().split()).intersection((set(re_punct_tmplt.sub('', expert_value_sep_lower).strip().split())))) <
             len(set(re_punct_tmplt.sub('', source_value).strip().lower().split()))
        )
        ):
          proc_type = proc_type_divide
          proc_type_comm = proc_type_comm_reformulation
        else:
            proc_type = proc_type_divide
            proc_type_comm = proc_type_comm_unknwn

    elif (
            (source_value.lower() == expert_value.lower()) and
            (source_value != expert_value)
        ):
            proc_type = proc_type_reformat
    elif (
            (source_value.strip().lower() in expert_value.strip().lower()) and
            (len(expert_value.strip()) > len(source_value.strip()))
        ):
                proc_type = proc_type_merge
    elif (
            (re_punct_tmplt.sub('', source_value).strip().lower() in re_punct_tmplt.sub('', expert_value).strip().lower()) and
            (len(re_punct_tmplt.sub('', source_value).strip()) < len(re_punct_tmplt.sub('', expert_value).strip()))
        ):
                proc_type = proc_type_merge
                proc_type_comm = proc_type_comm_wo_punctuation
    elif (
        (expert_value.strip().lower() in source_value.strip().lower()) and
        (len(source_value.strip()) > len(expert_value.strip()))
    ):
        proc_type = proc_type_cutting
    elif(
        (len(set(re_punct_tmplt.sub('', source_value).strip().lower().split()).intersection((set(re_punct_tmplt.sub('', expert_value).strip().lower().split())))) <
             len(set(re_punct_tmplt.sub('', source_value).strip().lower().split())))
        # or
        # (len(set(re_punct_tmplt.sub('', source_value).strip().lower().split()).intersection((set(re_punct_tmplt.sub('', expert_value).strip().lower().split())))) <
        # len(set(re_punct_tmplt.sub('', expert_value).strip().lower().split())))
        ):
        if debug:
            # print(set(re_punct_tmplt.sub('', source_value).strip().lower().split()).disjoint(set(re_punct_tmplt.sub('', expert_value).strip().lower().split())))
            print(set(re_punct_tmplt.sub('', source_value).strip().lower().split()).intersection((set(re_punct_tmplt.sub('', expert_value).strip().lower().split()))))
        proc_type = proc_type_reformulation
    elif(
        (len(set(re_punct_tmplt.sub('', expert_value).strip().lower().split()).intersection((set(re_punct_tmplt.sub('', spgz_name).strip().lower().split())))) <
             len(set(re_punct_tmplt.sub('', spgz_name).strip().lower().split())))
    ):
        proc_type = proc_type_reformulation
        proc_type_comm = proc_type_comm_unknwn
    else:
        proc_type = proc_type_unknwn

    # elif set([sss for ss in [s.split() for s in expert_value_sep_lower_lst] for sss in ss]).issubset(
    #     [sss for ss in [s.split() for s in spgz_name.lower().split()] for sss in ss] ):
    #     # нет слов из исходной колонки, но все слова есть в наименовании СПГЗ
    #     proc_type = proc_type_divide_spgz
    #     proc_type_err = proc_type_err_not_in_source


    return proc_type, proc_type_comm

def define_process_type(
    df_diff_01,
    df_diff_02,
    df_diff_03,
    source_col='Наименование характеристики 02',
    expert_col='Наименование характеристики (экспертно)',
    spgz_name_col='Наименование СПГЗ clean',
    proc_type_col='Тип обработки',
    proc_type_comm_col='Комм к обработке', # proc_type_comm_col
    sep = '|',
    debug=False):
    """
    только для строк где 'Источник'=='Наименование РМ'
    """
    proc_type_divide = 'Разбиение'
    proc_type_reformat = 'Изменение регистра букв'
    proc_type_merge = 'Слияние'
    proc_type_cutting = 'Отсечение'
    proc_type_reformulation = 'Переформулирование'
    proc_type_unknwn = 'unknwn'
    # proc_type_divide_merge = 'Разбиение/Слияние'
    # proc_type_divide_spgz = 'ПереРазбиение СПГЗ'

    proc_type_comm_permutation = 'С перестановкой слов'
    proc_type_comm_not_in_source = 'Нет ни одного слова из исходной колонки'
    proc_type_comm_not_in_spgz = 'Отдельных слов нет в наименовании СПГЗ'
    proc_type_comm_reformulation = 'Переформулирование'
    proc_type_comm_wo_punctuation = 'Без учета пунктуации'
    proc_type_comm_unknwn = 'unknwn'


    df_diff_04 = df_diff_03[
        (df_diff_03['_merge']=='right_only') &
        (df_diff_03['Источник']=='Наименование РМ')
        ]
    df_diff_04[proc_type_col] = None
    df_diff_04[proc_type_comm_col] = None
    # "Корректные ключевые строки с выполненными изменениями"
    if not set([source_col, expert_col, spgz_name_col]).issubset(set(df_diff_04.columns)):
        loggwr.error(f"Во взодном dataframe - нет одной или всез необходимых колонок:{str([source_col, expert_col, spgz_name_col])}")
        sys.exit(2)
    for i_row, row in tqdm(df_diff_04.iterrows(), total= df_diff_04.shape[0]):
        source_value = row[source_col]
        expert_value = row[expert_col]
        spgz_name = row[spgz_name_col]
        try:
            proc_type, proc_type_comm = define_process_type_in_row(source_value, expert_value, spgz_name,
                                  sep,
                                  debug=False)
            df_diff_04.loc[i_row, proc_type_col] = proc_type
            df_diff_04.loc[i_row, proc_type_comm_col] = proc_type_comm
        except Exception as err:
            print()
            logger.error(str(err))
            logger.error("Работа процедуры 'define_process_type' прекращена: ")
            break
            sys.exit()

    df_diff_04 = insert_pd_col_after_col(df_diff_04, move_col=proc_type_col, trgt_col=expert_col)
    df_diff_04 = insert_pd_col_after_col(df_diff_04, move_col=proc_type_comm_col, trgt_col=proc_type_col)

    return df_diff_04

def make_total_proc_df(
    df_diff_01,
    df_diff_04,
    # source_col='Наименование характеристики 02',
    # expert_col='Наименование характеристики (экспертно)',
    # expert_proc_col='Наименование характеристики (экспертно) обработано',
    source_col = 'Значение характеристики split',
    expert_col = 'Значение характеристики split (экспертно)',
    expert_proc_col='Значение характеристики split (экспертно) обработано',

    spgz_name_col='Наименование СПГЗ clean',
    proc_type_col='Тип обработки',
    proc_type_comm_col='Ошибка обработки',
    sep = '|',
    debug=False
):
    """
    1. разбить expert_col df_diff_04[df_diff_04['Тип обработки']=='Разбиение']
    2. drop_duplicates() key_cols & expert_col df_diff_04[df_diff_04['Тип обработки']=='Слияние']
    3. pd.concat df_diff_01[df_diff_01['_merge'] == 'both'].drop(columns=['_merge'])
    """
    proc_type_divide = 'Разбиение'
    proc_type_merge = 'Слияние'
    proc_type_reformat = 'Изменение регистра букв'
    proc_type_cutting = 'Отсечение'
    proc_type_reformulation = 'Переформулирование'
    proc_type_unknwn = 'unknwn'
    # proc_type_divide_merge = 'Разбиение/Слияние'
    # proc_type_divide_spgz = 'ПереРазбиение СПГЗ'

    proc_type_merge_lst = [
        proc_type_merge,
        proc_type_reformat,
        proc_type_cutting,
        proc_type_reformulation,
        proc_type_unknwn,
    ]
    df_diff_04_columns = list(df_diff_04.columns)
    df_diff_04_columns.remove(source_col)

    # proc_type_divide = 'Разбиение'
    try:
        df_total_01 = split_column_to_rows(
            df_diff_04[df_diff_04[proc_type_col]==proc_type_divide].drop_duplicates(df_diff_04_columns),
            to_split_col = expert_col,
            splitted_col = expert_proc_col,
            sep=sep,
            rename_col = False,
            debug = False
            )
        print()
        print("Cтроки для разбиения: (строк, колонок):", df_diff_04[df_diff_04[proc_type_col]=='Разбиение'].drop_duplicates(df_diff_04_columns).shape)
        if debug:
            display(df_diff_04[df_diff_04[proc_type_col]=='Разбиение'].drop_duplicates(df_diff_04_columns))
        print("Cтроки после разбиения: (строк, колонок):", df_total_01.shape)
    except Exception as err:
        print()
        logger.error(str(err))
        logger.error("Работа программы прекращена: в файле отсутствует нужная колокна")
        sys.exit()

    # proc_type_merge = 'Слияние'
    df_total_02 = df_diff_04[df_diff_04[proc_type_col].isin(proc_type_merge_lst)].drop_duplicates(df_diff_04_columns, keep='first')
    df_total_02[expert_proc_col] = df_total_02[expert_col]
    print("Cтроки для слияния: (строк, колонок):", df_diff_04[df_diff_04[proc_type_col].isin(proc_type_merge_lst)].drop_duplicates(df_diff_04_columns).shape)
    if debug:
         display(df_diff_04[df_diff_04[proc_type_col]==proc_type_merge
          ]. drop_duplicates(df_diff_04_columns)
          )
    print("Cтроки после слияния: (строк, колонок):", df_total_02.shape)

    print("Cтроки без изменения: (строк, колонок):", df_diff_01[df_diff_01['_merge'] == 'both'].shape)
    df_total_03 = df_diff_01[df_diff_01['_merge'] == 'both']

    df_total_03[expert_col] = df_total_03[source_col]
    df_total_03[expert_proc_col] = df_total_03[expert_col]

    df_total = pd.concat([df_total_01, df_total_02, df_total_03], axis=0)
    df_total = insert_pd_col_after_col(df_total, move_col=proc_type_col, trgt_col=expert_col)
    df_total = insert_pd_col_after_col(df_total, move_col=proc_type_comm_col, trgt_col=proc_type_col)
    df_total = insert_pd_col_after_col(df_total, move_col=expert_proc_col, trgt_col=proc_type_comm_col)


    return df_total

def main (
    data_source_dir,
    fn_features_source,
    sh_n_features_source,
    col_source,
    fn_features_proc,
    sh_n_features_proc,
    col_proc,
):
    source_col = col_source
    proc_col = col_proc
    expert_col = col_proc

    df_rm_source, df_rm_proc, req_cols_rm_source = read_data(
        data_source_dir,
        fn_features_source,
        sh_n_features_source,
        col_source,
        fn_features_proc,
        sh_n_features_proc,
        col_proc,
        )

    df_diff_01 = compare_key_fields(df_rm_source, df_rm_proc, req_cols_rm_source, debug=False)

    df_diff_02, df_diff_03 = compare_eq_key_fields(df_diff_01, df_rm_source, df_rm_proc, source_col, expert_col, debug=False)

    df_diff_04 = define_process_type(
        df_diff_01, df_diff_02, df_diff_03,
        source_col, expert_col,
        spgz_name_col='Наименование СПГЗ clean',
        proc_type_col='Тип обработки',
        proc_type_comm_col='Комм к обработке', # proc_type_comm_col
        debug=False)
    
    df_total = make_total_proc_df(
        df_diff_01, df_diff_04,
        source_col, expert_col,
        expert_proc_col=expert_col + ' - обработано',
        spgz_name_col='Наименование СПГЗ clean',
        proc_type_col='Тип обработки',
        proc_type_comm_col='Комм к обработке',
        debug=False)
    
    width_lst = [20,20,15,20,15,30, # ... 'Значение характеристики'
    15,15,15,15, # ... 'Тип'
    15,15,15, # ...'КТРУ характеристика'
    20,30,30,20,20, #... 'Наименование категории'
    30,10,30,30, #... 'Наименование характеристики 02',
      30,             #... 'Наименование характеристики (экспертно)'
    30,15, 30, #...'Изделие'
    20, 20,30, 20,30,20,30,20,30, 20,20,20,20, # ...'Наименование КПГЗ 9-го уровня
    20,30, 20,30, 20,30, 20,30, 20,30, 20,30, # коды разделов
    20,20,20,20,20,20, # флаги
    20, # 'Источник'
    20, # 'Флаг Да/Нет/Неважно/Наличие и др (1 - соответствует флагу, 0 - не соответствует флагу)',

    ]
    width_lst_total = [20,20,15,20,15,30, # ... 'Значение характеристики'
    15,15,15,15, # ... 'Тип'
    15,15,15, # ...'КТРУ характеристика'
    20,30,30,20,20, #... 'Наименование категории'
    30,10,30,30, #... 'Наименование характеристики 02',
      30,             #... 'Наименование характеристики (экспертно)'
      20,20, # тип обработки ошибка обработки
      30,             #... 'Наименование характеристики (экспертно) обработано'
    30,15, 30, #...'Изделие'
    20, 20,30, 20,30,20,30,20,30, 20,20,20,20, # ...'Наименование КПГЗ 9-го уровня
    20,30, 20,30, 20,30, 20,30, 20,30, 20,30, # коды разделов
    20,20,20,20,20,20, # флаги
    20, # 'Источник'
    20, # 'Флаг Да/Нет/Неважно/Наличие и др (1 - соответствует флагу, 0 - не соответствует флагу)',
    ]
    widths_lsts_list = (4*[width_lst])
    # widths_lsts_list.extend([width_lst + [20,20]])
    widths_lsts_list.extend([width_lst_total])
    widths_lsts_list.extend([width_lst_total])
    # for lst  in widths_lsts_list:
    #   print(lst)
    fn_save = save_df_lst_to_excel_xlsxwriter(
        df_lst=[
            df_diff_01[df_diff_01['_merge'] == 'left_only'].drop(columns=['_merge']),
            # print('В файле, полученном из обработки, отсутствуют следующие строки с ключевыми полями:')
            df_diff_01[df_diff_01['_merge'] == 'right_only'].drop(columns=['_merge']),
            # print('В файле, полученном из обработки, добавлены следующие строки с ключевыми полями, которых не было в исходном файле:')
            df_diff_01[df_diff_01['_merge'] == 'both'].drop(columns=['_merge']),
            # "Корректные ключевые строки без имзенений"
            df_diff_03[(df_diff_03['_merge']=='right_only')].drop(columns=['_merge']),
            # "Корректные ключевые строки с выполненными изменениями"
            df_diff_04, #.drop(columns=['_merge']),
            # типы орбработок в "Корректные ключевые строки с выполненными изменениями"
            df_total
                ],
        data_processed_dir=data_processed_dir,
        fn_main = '20240723_Хар_ки_ЕИ_Изделия'+ '_protocol',
        sh_n_lst=['Записи-нет в исходном', 'Записи-ошибочно добавленные ', 'Записи-в исх и обраб-ном',
                  'Записи - обработанные',
                  'Тип обработки',
                  'Проверено и обработано',
                  ],
        widths_lsts_list = widths_lsts_list,
        # indexes = None,
        )
    return (
        df_rm_source, df_rm_proc, req_cols_rm_source,
        df_diff_01,
        df_diff_02, df_diff_03,
        df_diff_04,
        df_total,
        fn_save,
        )

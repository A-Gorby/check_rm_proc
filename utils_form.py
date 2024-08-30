import ipywidgets as widgets
import pandas as pd
import numpy as np
import os
import sys
from ipywidgets import Layout, Box, Label

# from utils_io import logger
from utils_io import Logger

logger = Logger().logger
logger.propagate = False

if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    # del logger
    # logger = Logger().logger
    # logger.propagate = False


class FormsInput:

    def __init__(self, data_source_dir):
        self.data_source_dir = data_source_dir
        self.form_01_to_null()
        self.fn_01 = None
        self.fn_02 = None

    def form_01_to_null(self):
        self.sheets_01 = []
        self.sheets_02 = []

        self.selected_sheet_01 = None
        self.selected_sheet_02 = None
        self.fn_check_file_01_drop_down = None
        self.fn_check_file_02_drop_down = None
        self.check_sheet_names_01_drop_down = None
        self.check_sheet_names_02_drop_down = None
        self.columns_01_drop_down = None
        self.columns_02_drop_down = None
        self.source_col_drop_down = None
        self.proc_col_drop_down = None
        self.source_column_default = 'Значение характеристики split'
        # self.proc_column_default = 'Значение характеристики split (экспертно)'
        self.proc_column_default = 'Значение характеристики split после проверки'
        self.form_01 = None

    def form_param_01(self, fn_list):

        form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')

        self.fn_check_file_01_drop_down = widgets.Dropdown( options=fn_list, value=None)
        check_box_file_01 = Box([Label(value="Выберите Excel-файл, переданный в обработку"), self.fn_check_file_01_drop_down], layout=form_item_layout)

        self.check_sheet_names_01_drop_down = widgets.Dropdown(value=None)
        check_box_sheet_names_01 = Box([Label(value="Выберите Лист Excel-файла, переданного в обработку"), self.check_sheet_names_01_drop_down], layout=form_item_layout)
        # cols_name_corr_drop_down = widgets.SelectMultiple( options=corr_cols_name, value= [corr_cols_name[0]], disabled=False)
        self.source_col_drop_down = widgets.Dropdown(value=None)
        source_col_drop_down = Box([Label(value="Выберите колонку с данными для обработки экспертами"), self.source_col_drop_down], layout=form_item_layout)

        self.fn_check_file_02_drop_down = widgets.Dropdown( options=fn_list, value=None)
        check_box_file_02 = Box([Label(value="Выберите Excel-файл, полученный из обработки"), self.fn_check_file_02_drop_down], layout=form_item_layout)

        self.check_sheet_names_02_drop_down = widgets.Dropdown(value=None)
        check_box_sheet_names_02 = Box([Label(value="Выберите Лист Excel-файла, полученного из обработки"), self.check_sheet_names_02_drop_down], layout=form_item_layout)
        self.proc_col_drop_down = widgets.Dropdown(value=None)
        proc_col_drop_down = Box([Label(value="Выберите колонку с данными, обработанными экспертами"), self.proc_col_drop_down], layout=form_item_layout)

        form_items = [check_box_file_01, check_box_sheet_names_01, source_col_drop_down,
                      check_box_file_02, check_box_sheet_names_02, proc_col_drop_down,
                      ]

        self.form_01 = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='75%')) #width='auto'))
        # return self.form_01, fn_check_file1_drop_douwn, fn_check_file2_drop_douwn, sections_drop_douwn

    def on_fn_check_file_01_drop_douwn_change(self, change):
        self.fn_01 = self.fn_check_file_01_drop_down.value

        xl_01 = pd.ExcelFile(os.path.join(self.data_source_dir, self.fn_01))
        self.sheets_01 = xl_01.sheet_names
        print(f"Листы Excel-файла, переданного в обработку: {str(self.sheets_01)}") # logger
        self.check_sheet_names_01_drop_down.options = self.sheets_01

    def on_fn_check_file_02_drop_douwn_change(self, change):
        self.fn_02 = self.fn_check_file_02_drop_down.value

        xl_02 = pd.ExcelFile(os.path.join(self.data_source_dir, self.fn_02))
        self.sheets_02 = xl_02.sheet_names
        print(f"Листы Excel-файла, полученного из обработки: {str(self.sheets_02)}") # logger
        self.check_sheet_names_02_drop_down.options = self.sheets_02

    def on_check_sheet_names_01_drop_down_change(self, change):
        if type(self.check_sheet_names_01_drop_down.value)==str:
            self.sheet_name_01 = self.check_sheet_names_01_drop_down.value
            df_01 = pd.read_excel(os.path.join(self.data_source_dir, self.fn_01), sheet_name = self.sheet_name_01, nrows=5)
            self.columns_01_drop_down = list(df_01.columns)
            self.source_col_drop_down.options = self.columns_01_drop_down
            print(f"Колонки Excel-файла, переданного в обработку, лист: '{self.sheet_name_01}': {str(self.columns_01_drop_down)}")
            if self.source_column_default in self.columns_01_drop_down:
                self.source_col_drop_down.value = self.source_column_default
    def on_check_sheet_names_02_drop_down_change(self, change):
        if type(self.check_sheet_names_02_drop_down.value)==str:
            self.sheet_name_02 = self.check_sheet_names_02_drop_down.value
            df_02 = pd.read_excel(os.path.join(self.data_source_dir, self.fn_02), sheet_name = self.sheet_name_02, nrows=5)
            self.columns_02_drop_down = list(df_02.columns)
            self.proc_col_drop_down.options = self.columns_02_drop_down
            print(f"Колонки Excel-файла, полученного из обработки, лист: '{self.sheet_name_02}': {str(self.columns_02_drop_down)}")
            if self.proc_column_default in self.columns_02_drop_down:
                self.proc_col_drop_down.value = self.proc_column_default

    def get_col_names_from_excel(path, fn, sheets):
        cols_file = []
        for sheet in sheets:
            try:
                df = pd.read_excel(os.path.join(path, fn), sheet_name=sheet, nrows=5, header=0)
                cols_file.append(list(df.columns))
                print(sheet, list(df.columns))
            except Exception as err:
                print(err)

        return cols_file


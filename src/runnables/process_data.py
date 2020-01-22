import pandas as pd
from src.data_processing import load_raw_data, clean_form_data

if __name__ == '__main__':
    pd.set_option('display.max_columns', 1000)
    pd.set_option('display.width', 1000)
    classroom_sessions, form_answers = load_raw_data()
    clean_data_dict = clean_form_data(form_answers)

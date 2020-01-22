import pandas as pd
import os


def save_parquet(df: pd.DataFrame, path: str, overwrite=True, save_excel=True):
    path = os.path.abspath(path)
    path_dir = os.path.split(path)[0]
    if not os.path.isdir(path_dir):
        os.makedirs(path_dir)
    if not os.path.isfile(path) or overwrite:
        for c in df.columns:
            df = df.assign(**{c: df[c].astype(str)})
        df.to_parquet(path)
        print('Saved file: ' + path)
        if save_excel:
            excel_path = path[0:path.find('.')] + '.xlsx'
            df.to_excel(excel_path)
            print('Saved file: ' + excel_path)

import pandas as pd
from src.definitions import DATA_DIR, CLASSROOM_SESSIONS_FILENAME, FORM_ANSWERS_FILENAME
from src.general_utilities import save_parquet


def load_raw_data() -> [pd.ExcelFile, pd.ExcelFile]:
    """
    Loads original raw data from excel files and returns them as pandas ExcelFiles with multiple sheets.
    :return: Raw data as ExcelFiles with multiple sheets.
    """
    classroom_sessions = pd.ExcelFile(DATA_DIR + '/raw/' + CLASSROOM_SESSIONS_FILENAME)
    form_answers = pd.ExcelFile(DATA_DIR + '/raw/' + FORM_ANSWERS_FILENAME)
    return classroom_sessions, form_answers


def clean_form_data(form_answers: pd.ExcelFile, save=True) -> dict:
    dct = dict()
    dct['questions'] = clean_form_questions(form_answers, save=save)
    dct['submissions'] = clean_form_submissions(form_answers, save=save)
    dct['code_book'] = clean_form_code_book(form_answers, save=save)

    sheet_names = ['2 General Information', '3 SCHOOL & EDUCATION', '4 YOUR HEALTH',
                   '5 HIV LITERACY', '6 ATTITUDES & OPINIONS', '7 SEXUAL RELATIONS',
                   '8 THE STAR FOR LIFE PROGRAMME']
    filenames = ['general', 'education', 'health', 'hiv', 'opinions', 'sexual', 'sfl']

    for i in range(0, len(filenames)):
        dct[filenames[i]] = clean_form_answer_sheet(form_answers, sheet_name=sheet_names[i], filename=filenames[i],
                                                    save=save)
    return dct


def clean_form_questions(form_answers: pd.ExcelFile, save=True) -> pd.DataFrame:
    questions = pd.read_excel(form_answers, 'Questions')
    questions = questions.rename(columns={'Question name': 'question_name',
                                          'Section': 'section',
                                          'Question Id': 'question_id',
                                          'Question text': 'question_text',
                                          'Question type': 'question_type'})
    questions = questions.drop(columns='#')
    if save:
        save_parquet(questions, DATA_DIR + '/cleaned/questions.parq')
    return questions


def clean_form_submissions(form_answers: pd.ExcelFile, save=True) -> pd.DataFrame:
    submissions = pd.read_excel(form_answers, 'Submissions')
    submissions = submissions.drop(columns=['Received', 'End', 'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15'])
    submissions = submissions.rename(columns={'Submission Id': 'submission_id',
                                              'Fieldworker Name': 'fieldworker_name',
                                              'Fieldworker Id': 'fieldworker_id',
                                              'Device': 'device',
                                              'Duration (seconds)': 'received',
                                              'Longitude': 'end',
                                              'Latitude': 'duration_seconds',
                                              'Language': 'longitude',
                                              'Survey Version': 'latitude',
                                              'Unnamed: 11': 'language',
                                              'Unnamed: 12': 'survey_version'})
    submissions = submissions.assign(longitude=submissions['longitude'].astype(str))
    submissions = submissions.assign(latitude=submissions['latitude'].astype(str))
    if save:
        save_parquet(submissions, DATA_DIR + '/cleaned/submissions.parq')
    return submissions


def clean_form_code_book(form_answers: pd.ExcelFile, save=True) -> pd.DataFrame:
    code_book = pd.read_excel(form_answers, 'Code Book')
    code_book = code_book[['Question Id', 'Question Name', 'Option Label', 'Option Value']]
    code_book = code_book.rename(columns={'Question Id': 'question_id',
                                          'Question Name': 'question_name',
                                          'Option Label': 'option_label',
                                          'Option Value': 'option_value'})
    if save:
        save_parquet(code_book, DATA_DIR + '/cleaned/code_book.parq')
    return code_book


def clean_form_answer_sheet(form_answers: pd.ExcelFile, sheet_name: str, filename: str, save=True) -> pd.DataFrame:
    df = pd.read_excel(form_answers, sheet_name)
    # Drop columns with no values
    df = df.drop(columns=['[Repeats On Question]', '[Repeat Question Value]', '[Repeating Index]'])
    # Rename columns to suitable format
    df = df.rename(columns={'[Submission Id]': 'submission_id',
                                               '[Fieldworker Name]': 'fieldworker_name',
                                               '[Fieldworker Id]': 'fieldworker_id',
                                               'Received Date': 'received_date'})
    if save:
        if filename.find('.') >= 0:
            filename = filename[0:filename.find('.')]
        save_parquet(df, DATA_DIR + '/cleaned/' + filename + '.parq')
    return df

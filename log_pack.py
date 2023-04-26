import os
import logging
from dotenv import load_dotenv

load_dotenv()

file_log = os.getenv('FILENAME_LOG')

logging.basicConfig(filename=file_log, encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(message)s')

def log_error(error):
    logging.error('Error: %s', error)

def log_success(text_message):
    logging.info(text_message)

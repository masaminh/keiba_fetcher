"""設定情報."""
import os
from os.path import dirname, join

from dotenv import load_dotenv

load_dotenv(join(dirname(__file__), '.env'))

QUEUE_NAME = os.environ.get('QUEUE_NAME')
BUCKET_NAME = os.environ.get('BUCKET_NAME')

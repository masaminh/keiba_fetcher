"""競馬情報のフェッチ."""

import logging

import dateutil.parser
from aws_xray_sdk.core import patch_all

import logic
import settings

patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Lambda handler.

    Arguments:
        event {dict} -- Event data
        context {object} -- Lambda Context runtime methods and attributes

    """
    del context

    logger.info('start: event.time=%s', event["time"])
    nowtime = dateutil.parser.parse(event['time'])
    logic.entry(settings.QUEUE_NAME, settings.BUCKET_NAME, nowtime)
    return "OK"

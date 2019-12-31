"""競馬コンテンツフェッチ処理のロジック部."""
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urljoin, urlparse

import boto3
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from more_itertools import chunked

logger = logging.getLogger()


def entry(queue_name, bucket_name, nowtime):
    """ロジックのエントリーポイント.

    Arguments:
        queue_name {str} -- キュー名
        bucket_name {str} -- バケット名
        nowtime {datetime} -- 開始時刻
    """
    main_loop(queue_name, bucket_name, nowtime)


def main_loop(queue_name, bucket_name, nowtime):
    """メインループ.

    Arguments:
        queue_name {str} -- キュー名
        bucket_name {str} -- バケット名
        nowtime {datetime} -- 開始時刻
    """
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    endtime = nowtime + timedelta(seconds=60 * 10)

    calendar_added = False

    while datetime.now(timezone.utc) < endtime:
        msg_list = queue.receive_messages(
            MaxNumberOfMessages=10, WaitTimeSeconds=1)
        if msg_list:
            for message in msg_list:
                try:
                    message_object = json.loads(message.body)
                    target = message_object['target']
                    referer = message_object['referer']
                    uris = fetch(target, referer, bucket_name, nowtime)
                    if uris is None:
                        continue
                    if uris:
                        uridicts = (
                            {'target': x, 'referer': target} for x in uris)
                        messages = (
                            {'Id': f'{i}', 'MessageBody': json.dumps(x)}
                            for (i, x) in enumerate(uridicts))
                        for chunk in chunked(messages, 10):
                            queue.send_messages(Entries=chunk)

                    message.delete()
                except Exception as e:
                    logger.error(f'Exception occured. {e}')
        elif not calendar_added:
            add_calandar_message(queue_name, nowtime)
            calendar_added = True
        else:
            break


def add_calandar_message(queue_name, nowtime):
    """カレンダー取得メッセージを登録する.

    Arguments:
        queue_name {str} -- キュー名
        nowtime {datetime} -- 開始時刻
    """
    delta = timedelta(days=7)
    daterange = [nowtime - delta, nowtime + delta]
    rangeyearmonth = {(x.year, x.month) for x in daterange}
    urlbase = 'https://www.jbis.or.jp/race/calendar/'
    uris = (
        urlbase + f'?year={x[0]:04}&month={x[1]:02}'
        for x in rangeyearmonth)

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    entries = [
        {
            'Id': f'{i}',
            'MessageBody': json.dumps({'target': x, 'referer': None})
        }
        for (i, x) in enumerate(uris)]
    queue.send_messages(Entries=entries)


def fetch(uri, referer, bucket, nowtime):
    """フェッチを行う.

    Arguments:
        uri {str} -- フェッチ先URI
        referer {str} -- 参照元URI
        bucket {str} -- バケット名
        nowtime {datetime} -- 現時刻

    Returns:
        list(str) -- 次に処理するURIリスト
    """
    logger.info(f'fetching: {uri}')
    fetcher = get_fetcher(uri, referer)
    uris = fetcher.fetch(bucket, nowtime)
    return uris


def get_fetcher(uri, referer):
    """フェッチ用クラスオブジェクトを取得する.

    Arguments:
        uri {str} -- 取得先URI
        referer {str} -- 参照元URI

    Returns:
        Fetcher -- フェッチ用クラスオブジェクト
    """
    fetcher = None

    parsed = urlparse(uri)
    netloc = parsed.netloc
    path = parsed.path

    if netloc == 'www.jbis.or.jp':
        if path == '/race/calendar/':
            fetcher = JbisCalendarFetcher(uri)
        elif re.fullmatch(r'/race/calendar/\d{8}/\d{3}/', path):
            fetcher = JbisRaceListFetcher(uri)
        elif re.fullmatch(r'/race/result/\d{8}/\d{3}/\d{2}/', path):
            fetcher = JbisRaceResultFetcher(uri)
        elif re.fullmatch(r'/race/\d{8}/\d{3}/\d{2}.html', path):
            fetcher = JbisRaceEntryFetcher(uri)
        elif re.fullmatch(r'/horse/\d{10}/record/all/', path):
            fetcher = JbisHorseRecordFetcher(uri, referer)
        elif re.fullmatch(r'/horse/\d{10}/', path):
            fetcher = JbisHorseRecordFetcher(uri + 'record/all/', referer)

    if fetcher is None:
        fetcher = DefaultFetcher(uri)

    return fetcher


def get_s3_object(bucket, key):
    """指定のS3オブジェクトの情報を取得する.

    Arguments:
        bucket {str} -- バケット名
        key {str} -- キー

    Returns:
        S3.ObjectSummary -- オブジェクトの情報。指定オブジェクトが無い場合はNone
    """
    s3 = boto3.resource('s3')
    itr = s3.Bucket(bucket).objects.filter(Prefix=key)
    summaries = [x for x in itr if x.key == key]
    result = summaries[0] if summaries else None
    return result


def fetch_to_s3(uri, bucket, key):
    """URI指定されたコンテンツをs3に取得する.

    Arguments:
        uri {str} -- 取得先URI
        bucket {str} -- 保存バケット名
        key {str} -- 保存キー名

    Returns:
        bytes -- 取得したコンテンツ
    """
    time.sleep(1)
    response = requests.get(uri, timeout=10)
    if response.status_code != 200:
        logger.error(f'http status code={response.status_code} : {uri}')
        return None

    content = response.content
    s3 = boto3.resource('s3')
    s3.Bucket(bucket).put_object(Key=key, Body=content)
    return content


def is_fetch_target_race_result(uri, now):
    """レース結果のuriがフェッチ対象ならTrueを返す.

    Arguments:
        uri {str} -- 判定対象のURI
        now {str} -- 処理開始時の時刻.

    Returns:
        bool -- レース結果のURIがフェッチ対象かどうか
    """
    result = False

    path = urlparse(uri).path
    m = re.fullmatch(r'/race/result/(\d{4})(\d{2})(\d{2})/\d{3}/\d{2}/', path)

    if m:
        uridate = datetime(
            int(m.group(1)), int(m.group(2)), int(m.group(3)),
            tzinfo=timezone.utc)
        delta = relativedelta(year=2)

        result = ((now - delta) <= uridate)

    return result


class JbisCalendarFetcher:
    """JBISのカレンダーフェッチ用クラス."""

    def __init__(self, uri):
        """コンストラクタ.

        Arguments:
            uri {str} -- フェッチ先URI
        """
        self._uri = uri

    def fetch(self, bucket, nowtime):
        """フェッチ処理.

        Arguments:
            bucket {str} -- 格納先バケット名
            nowtime {datetime} -- 現時刻

        Returns:
            list(str) -- 次に処理するURIリスト
        """
        uris = []
        key = self.get_s3_key()
        summary = get_s3_object(bucket, key)
        if summary is None:
            # S3未格納だった場合は古い日付を設定してフェッチを発火させる
            s3_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
        else:
            s3_time = summary.last_modified

        # 23時間経過を閾値にする
        if nowtime - s3_time > timedelta(seconds=23 * 60 * 60):
            content = fetch_to_s3(self._uri, bucket, key)
            uris = self.get_next_uris(content)

        return uris

    def get_s3_key(self):
        """URIからS3のキーを取得する."""
        parsed = urlparse(self._uri)
        params = parse_qs(parsed.query)
        key = f'jbis{parsed.path}{params["year"][0]}/{params["month"][0]}'
        return key

    def get_next_uris(self, content):
        """コンテンツ内から次のURIリストを取得する.

        Arguments:
            content {bytes} -- コンテンツ

        Returns:
            list(str) -- URIリスト
        """
        if content is None:
            return None

        soup = BeautifulSoup(content, 'lxml')
        anchors = soup.select('ul.list-icon-01 a')
        relatives = (x['href'] for x in anchors)
        uris = [urljoin(self._uri, x) for x in relatives]
        return uris


class JbisRaceListFetcher:
    """JBISのレース一覧のフェッチクラス."""

    def __init__(self, uri):
        """コンストラクタ.

        Arguments:
            uri {str} -- 取得先URI
        """
        self._uri = uri

    def fetch(self, bucket, nowtime):
        """フェッチ処理.

        Arguments:
            bucket {str} -- 格納先バケット名
            nowtime {datetime} -- 現時刻

        Returns:
            list(str) -- 次に処理するURIリスト
        """
        uris = []
        key = self.get_s3_key()
        summary = get_s3_object(bucket, key)

        if summary is None:
            # S3未格納だった場合は古い日付を設定してフェッチを発火させる
            s3_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
        else:
            body = summary.get()['Body'].read()
            soup = BeautifulSoup(body, 'lxml')
            h = list(soup.select('table.tbl-data-04 th'))

            if h:
                s3_time = summary.last_modified if h[3] == '芝ダ' else nowtime
            else:
                s3_time = datetime(2000, 1, 1, tzinfo=timezone.utc)

        # 23時間経過を閾値にする
        if nowtime - s3_time > timedelta(seconds=23 * 60 * 60):
            content = fetch_to_s3(self._uri, bucket, key)
            uris = self.get_next_uris(content)

        return uris

    def get_s3_key(self):
        """URIからS3のキーを取得する."""
        parsed = urlparse(self._uri)
        key = f'jbis{parsed.path}'[:-1]
        return key

    def get_next_uris(self, content):
        """コンテンツ内から次のURIリストを取得する.

        Arguments:
            content {bytes} -- コンテンツ

        Returns:
            list(str) -- URIリスト
        """
        if content is None:
            return None

        uris = []

        soup = BeautifulSoup(content, 'lxml')
        lines = soup.select('table.tbl-data-04 tbody tr')
        headers = list(soup.select('table.tbl-data-04 th'))

        if headers and headers[2].string != '芝ダ':
            link_index = 1 if headers[3].string == '芝ダ' else 0
            anchors = (list(x.select('td'))[link_index] for x in lines)
            relatives = ((x.a['href'] if x.a else None) for x in anchors)
            uris = [urljoin(self._uri, x) for x in relatives if x]

        return uris


class JbisRaceResultFetcher:
    """JBISのレース結果のフェッチクラス."""

    def __init__(self, uri):
        """コンストラクタ.

        Arguments:
            uri {str} -- 取得先URI
        """
        self._uri = uri

    def fetch(self, bucket, nowtime):
        """フェッチ処理.

        Arguments:
            bucket {str} -- 格納先バケット名
            nowtime {datetime} -- 現時刻
        """
        uris = []
        if not is_fetch_target_race_result(self._uri, nowtime):
            return uris

        key = self.get_s3_key()
        summary = get_s3_object(bucket, key)

        if summary is None:
            # S3未格納だった場合は取得する。S3格納済みならそれ以上処理しない
            content = fetch_to_s3(self._uri, bucket, key)
            uris = self.get_next_uris(content)

        return uris

    def get_s3_key(self):
        """URIからS3のキーを取得する."""
        parsed = urlparse(self._uri)
        key = f'jbis{parsed.path}'[:-1]
        return key

    def get_next_uris(self, content):
        """コンテンツ内から次のURIリストを取得する.

        Arguments:
            content {bytes} -- コンテンツ

        Returns:
            list(str) -- URIリスト
        """
        if content is None:
            return None

        soup = BeautifulSoup(content, 'lxml')
        anchors = soup.select('table.tbl-data-04 tr td:nth-child(4)>a')
        relatives = (x['href'] + 'record/all/' for x in anchors)
        uris = [urljoin(self._uri, x) for x in relatives]
        return uris


class JbisRaceEntryFetcher:
    """JBISのレース出走表のフェッチクラス."""

    def __init__(self, uri):
        """コンストラクタ.

        Arguments:
            uri {str} -- 取得先URI
        """
        self._uri = uri

    def fetch(self, bucket, nowtime):
        """フェッチ処理.

        Arguments:
            bucket {str} -- 格納先バケット名
            nowtime {datetime} -- 現時刻
        """
        uris = []
        key = self.get_s3_key()
        summary = get_s3_object(bucket, key)

        if summary is None:
            # S3未格納だった場合は古い日付を設定してフェッチを発火させる
            s3_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
        else:
            s3_time = summary.last_modified

        # 23時間経過を閾値にする
        if nowtime - s3_time > timedelta(seconds=23 * 60 * 60):
            content = fetch_to_s3(self._uri, bucket, key)
            uris = self.get_next_uris(content)

        return uris

    def get_s3_key(self):
        """URIからS3のキーを取得する."""
        parsed = urlparse(self._uri)
        key = f'jbis{parsed.path}'[:-1]
        return key

    def get_next_uris(self, content):
        """コンテンツ内から次のURIリストを取得する.

        Arguments:
            content {bytes} -- コンテンツ

        Returns:
            list(str) -- URIリスト
        """
        if content is None:
            return None

        soup = BeautifulSoup(content, 'lxml')
        anchors = soup.select('table.tbl-data-04 tr td:nth-child(3)>a')
        relatives = (x['href'] + 'record/all/' for x in anchors)
        uris = [urljoin(self._uri, x) for x in relatives]
        return uris


class JbisHorseRecordFetcher:
    """JBISの馬情報のフェッチクラス."""

    def __init__(self, uri, referer):
        """コンストラクタ.

        Arguments:
            uri {str} -- 取得先URI
        """
        self._uri = uri
        self._referer = referer

    def fetch(self, bucket, nowtime):
        """フェッチ処理.

        Arguments:
            bucket {str} -- 格納先バケット名
            nowtime {datetime} -- 現時刻
        """
        uris = []
        key = self.get_s3_key()
        summary = get_s3_object(bucket, key)

        if summary is None:
            # S3未格納だった場合は古い日付を設定してフェッチを発火させる
            s3_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
        else:
            body = summary.get()['Body'].read()
            soup = BeautifulSoup(body, 'lxml')
            dateelms = list(soup.select('table.tbl-data-04 tbody th.sort-02'))
            dates = {x.string for x in dateelms}
            path = urlparse(self._referer).path
            m = re.fullmatch(r'/race/(\d{8})/\d{3}/\d{2}.html', path)

            if not m:
                m = re.fullmatch(r'/race/result/(\d{8})/\d{3}/\d{2}/', path)

            if m:
                s = m.group(1)
                referer_date = f'{s[:4]}/{s[4:6]}/{s[6:]}'

                if referer_date in dates:
                    # refererのレースがすでに含まれていたら、取得不要
                    s3_time = nowtime
                else:
                    # refererのレースが含まれていない場合はs3保存時刻を使う
                    s3_time = summary.last_modified

            else:
                # refererが期待外だった場合はs3保存時刻を使う
                s3_time = summary.last_modified

        # 23時間経過を閾値にする
        if nowtime - s3_time > timedelta(seconds=23 * 60 * 60):
            content = fetch_to_s3(self._uri, bucket, key)
            uris = self.get_next_uris(content)

        filtered = [x for x in uris if is_fetch_target_race_result(x, nowtime)]
        return filtered

    def get_s3_key(self):
        """URIからS3のキーを取得する."""
        parsed = urlparse(self._uri)
        key = f'jbis{parsed.path}'[:-1]
        return key

    def get_next_uris(self, content):
        """コンテンツ内から次のURIリストを取得する.

        Arguments:
            content {bytes} -- コンテンツ

        Returns:
            list(str) -- URIリスト
        """
        if content is None:
            return None

        return []


class DefaultFetcher:
    """デフォルトのフェッチクラス."""

    def __init__(self, uri):
        """コンストラクタ.

        Arguments:
            uri {str} -- 取得先URI
        """
        self._uri = uri

    def fetch(self, bucket, nowtime):
        """フェッチ処理.

        Arguments:
            bucket {str} -- 格納先バケット名
            nowtime {datetime} -- 現時刻
        """
        logger.warning('unknown uri type: %s', self._uri)
        return None

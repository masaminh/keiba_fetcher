"""logicのテスト."""

from datetime import datetime, timezone
from unittest import mock

import src.logic as logic


def test_entry():
    """entry()のテスト."""
    with mock.patch('src.logic.main_loop') as n:
        logic.entry(
            'QUEUE',
            'BUCKET',
            datetime(
                2019,
                12,
                15,
                tzinfo=timezone.utc))
        n.assert_called_once_with(
            'QUEUE', 'BUCKET',
            datetime(2019, 12, 15, tzinfo=timezone.utc))


def test_fetch():
    """fetch()のテスト."""
    nowtime = datetime(2019, 12, 1, 12, 0, 0)

    with mock.patch('src.logic.get_fetcher') as m:
        logic.fetch(
            'https://www.yahoo.co.jp', 'http://referer', 'bucket', nowtime)
        m.assert_called_once_with('https://www.yahoo.co.jp', 'http://referer')
        m.return_value.fetch.assert_called_once_with('bucket', nowtime)


def test_get_s3_object():
    """get_s3_object()のテスト."""
    with mock.patch('boto3.resource') as m:
        n = mock.MagicMock(
            key='key', last_modified=datetime(
                2019, 12, 1, 10, 0, 0))

        m.return_value.Bucket.return_value.objects.filter.return_value = [n]
        s3object = logic.get_s3_object('bucket', 'key')
        assert s3object.last_modified == datetime(2019, 12, 1, 10, 0, 0)


def test_get_s3_object_none():
    """get_s3_object()のテスト."""
    with mock.patch('boto3.resource') as m:
        m.return_value.Bucket.return_value.objects.filter.return_value = []
        s3object = logic.get_s3_object('bucket', 'key')
        assert s3object is None


def test_fetch_to_s3():
    """fetch_to_s3()のテスト."""
    uri = 'http://host/path'
    bucket = 'bucket'
    key = 'key'

    with mock.patch('requests.get') as get:
        with mock.patch('boto3.resource') as resource:
            get.return_value.status_code = 200
            get.return_value.content = b'1'

            content = logic.fetch_to_s3(uri, bucket, key)
            assert content == b'1'
            get.assert_called_once_with(uri, timeout=10)
            resource.assert_called_once_with('s3')
            resource.return_value.Bucket.assert_called_once_with(bucket)
            resource.return_value.Bucket.return_value.put_object.\
                assert_called_once_with(Key=key, Body=b'1')


def test_fetch_to_s3_error():
    """fetch_to_s3()のテスト."""
    uri = 'http://host/path'
    bucket = 'bucket'
    key = 'key'

    with mock.patch('requests.get') as get:
        with mock.patch('boto3.resource') as resource:
            get.return_value.status_code = 500
            get.return_value.content = b'1'

            content = logic.fetch_to_s3(uri, bucket, key)
            assert content is None
            get.assert_called_once_with(uri, timeout=10)
            resource.assert_not_called()


def test_get_fetcher_jbis_calendar():
    """get_fetcher()のテスト."""
    fetcher = logic.get_fetcher(
        'https://www.jbis.or.jp/race/calendar/', 'http://referer')
    assert isinstance(fetcher, logic.JbisCalendarFetcher)


def test_get_fetcher_jbis_race_list():
    """get_fetcher()のテスト."""
    fetcher = logic.get_fetcher(
        'https://www.jbis.or.jp/race/calendar/20200322/231/', 'http://referer')
    assert isinstance(fetcher, logic.JbisRaceListFetcher)


def test_get_fetcher_unknown():
    """get_fetcher()のテスト."""
    fetcher = logic.get_fetcher('https://www.yahoo.co.jp', 'http://referer')
    assert isinstance(fetcher, logic.DefaultFetcher)


def test_get_jbis_calendar_fetcher_fetch():
    """JbisCalendarFetcher.fetch()のテスト."""
    uri = 'https://www.jbis.or.jp/race/calendar/?year=2019&month=02'
    bucket = 'bucket'
    key = 'jbis/race/calendar/2019/02'
    s3time = datetime(2019, 12, 1, 12, 0, 0)
    nowtime = datetime(2019, 12, 2, 12, 0, 0)
    content = (
        b'<html><body><ul class="list-icon-01"><a href="/a" /></ul>' +
        b'<ul class="list-icon-01"><a href="/b" /></ul></body></html>')

    fetcher = logic.JbisCalendarFetcher(uri)

    with mock.patch('src.logic.get_s3_object') as m:
        with mock.patch('src.logic.fetch_to_s3') as n:
            m.return_value.last_modified = s3time
            n.return_value = content
            uris = fetcher.fetch(bucket, nowtime)
            m.assert_called_once_with(bucket, key)
            n.assert_called_once_with(uri, bucket, key)
            assert uris == [
                'https://www.jbis.or.jp/a',
                'https://www.jbis.or.jp/b']


def test_get_jbis_calendar_fetcher_fetch_newobject():
    """JbisCalendarFetcher.fetch()のテスト."""
    uri = 'https://www.jbis.or.jp/race/calendar/?year=2019&month=02'
    bucket = 'bucket'
    key = 'jbis/race/calendar/2019/02'
    s3time = datetime(2019, 12, 1, 12, 0, 0)
    nowtime = datetime(2019, 12, 1, 13, 0, 0)

    fetcher = logic.JbisCalendarFetcher(uri)

    with mock.patch('src.logic.get_s3_object') as m:
        with mock.patch('src.logic.fetch_to_s3') as n:
            m.return_value.last_modified = s3time
            uris = fetcher.fetch(bucket, nowtime)
            m.assert_called_once_with(bucket, key)
            n.assert_not_called()
            assert uris == []


def test_get_jbis_calendar_fetcher_fetch_noobject():
    """JbisCalendarFetcher.fetch()のテスト."""
    uri = 'https://www.jbis.or.jp/race/calendar/?year=2019&month=02'
    bucket = 'bucket'
    key = 'jbis/race/calendar/2019/02'
    nowtime = datetime(2019, 12, 1, 13, 0, 0, tzinfo=timezone.utc)
    content = (
        b'<html><body><ul class="list-icon-01"><a href="/a" /></ul>' +
        b'<ul class="list-icon-01"><a href="/b" /></ul></body></html>')

    fetcher = logic.JbisCalendarFetcher(uri)

    with mock.patch('src.logic.get_s3_object', return_value=None) as m:
        with mock.patch('src.logic.fetch_to_s3') as n:
            n.return_value = content
            uris = fetcher.fetch(bucket, nowtime)
            m.assert_called_once_with(bucket, key)
            n.assert_called_once_with(uri, bucket, key)
            assert uris == [
                'https://www.jbis.or.jp/a',
                'https://www.jbis.or.jp/b']


def test_get_jbis_calendar_fetcher_get_s3_key():
    """JbisCalendarFetcher.get_s3_key()のテスト."""
    fetcher = logic.JbisCalendarFetcher(
        'https://www.jbis.or.jp/race/calendar/?year=2019&month=02')
    key = fetcher.get_s3_key()
    assert key == 'jbis/race/calendar/2019/02'


def test_get_jbis_calendar_fetcher_get_next_uris():
    """JbisCalendarFetcher.get_next_uris()のテスト."""
    content = (
        b'<html><body><ul class="list-icon-01"><a href="/a" /></ul>' +
        b'<ul class="list-icon-01"><a href="/b" /></ul></body></html>')
    fetcher = logic.JbisCalendarFetcher(
        'https://www.jbis.or.jp/race/calendar/?year=2019&month=02')
    uris = fetcher.get_next_uris(content)
    assert uris == ['https://www.jbis.or.jp/a', 'https://www.jbis.or.jp/b']


def test_get_jbis_racelist_fetcher_fetch_result():
    """JbisRaceListFetcher.fetch()のテスト."""
    uri = 'https://www.jbis.or.jp/race/calendar/20200317/220/'
    bucket = 'bucket'
    key = 'jbis/race/calendar/20200317/220'
    s3time = datetime(2020, 3, 18, 12, 0, 0)
    nowtime = datetime(2020, 3, 19, 12, 0, 0)
    contentstr = (
        '<html>' +
        '<meta http-equiv="Content-Type" content="text/html; ' +
        'charset=Shift_JIS">' +
        '<body><table class="tbl-data-04">' +
        '<thead><tr><th>R</th><th>レース名</th><th>距離</th>' +
        '<th></th><th></th><th></th><th></th><th></th></tr></thead>' +
        '<tbody><tr><th>1</th><td><a href="/a">レース1</a></td><td>ダ1200m</td>' +
        '<td></td><td></td><td></td><td></td><td></td></tr></tbody>' +
        '</table></body></html>')
    content = contentstr.encode('shift_jis')

    fetcher = logic.JbisRaceListFetcher(uri)

    with mock.patch('src.logic.get_s3_object') as m:
        m.return_value.last_modified = s3time
        body = mock.MagicMock(read=mock.MagicMock(return_value=content))
        m.return_value.get.return_value = {'Body': body}
        with mock.patch('src.logic.fetch_to_s3') as n:
            n.return_value = content
            uris = fetcher.fetch(bucket, nowtime)
            m.assert_called_once_with(bucket, key)
            n.assert_not_called()
            assert uris == [
                'https://www.jbis.or.jp/a']


def test_get_jbis_racelist_fetcher_fetch_entry():
    """JbisRaceListFetcher.fetch()のテスト."""
    uri = 'https://www.jbis.or.jp/race/calendar/20200319/220/'
    bucket = 'bucket'
    key = 'jbis/race/calendar/20200319/220'
    s3time = datetime(2020, 3, 18, 12, 0, 0)
    nowtime = datetime(2020, 3, 19, 12, 0, 0)
    contentstr = (
        '<html>' +
        '<meta http-equiv="Content-Type" content="text/html; ' +
        'charset=Shift_JIS">' +
        '<body><table class="tbl-data-04">' +
        '<thead><tr><th>R</th><th>発走時刻</th><th>レース名</th>' +
        '<th>芝ダ</th><th></th><th></th><th></th></tr></thead>' +
        '<tbody><tr><th>1</th><td></td><td><a href="/a">レース1</a></td>' +
        '<td>ダ</td>' +
        '<td></td><td></td><td></td></tr></tbody>' +
        '</table></body></html>')
    content = contentstr.encode('shift_jis')

    fetcher = logic.JbisRaceListFetcher(uri)

    with mock.patch('src.logic.get_s3_object') as m:
        m.return_value.last_modified = s3time
        body = mock.MagicMock(read=mock.MagicMock(return_value=content))
        m.return_value.get.return_value = {'Body': body}
        with mock.patch('src.logic.fetch_to_s3') as n:
            n.return_value = content
            uris = fetcher.fetch(bucket, nowtime)
            m.assert_called_once_with(bucket, key)
            n.assert_called_once()
            assert uris == [
                'https://www.jbis.or.jp/a']


def test_get_jbis_racelist_fetcher_fetch_stakes_entry():
    """JbisRaceListFetcher.fetch()のテスト."""
    uri = 'https://www.jbis.or.jp/race/calendar/20200322/231/'
    bucket = 'bucket'
    key = 'jbis/race/calendar/20200322/231'
    s3time = datetime(2020, 3, 18, 12, 0, 0)
    nowtime = datetime(2020, 3, 19, 12, 0, 0)
    contentstr = (
        '<html>' +
        '<meta http-equiv="Content-Type" content="text/html; ' +
        'charset=Shift_JIS">' +
        '<body><table class="tbl-data-04">' +
        '<thead><tr><th>R</th><th>レース名</th>' +
        '<th>芝ダ</th><th></th><th></th><th></th></tr></thead>' +
        '<tbody><tr><th>-</th><td>レース1</td>' +
        '<td>ダ</td>' +
        '<td></td><td></td><td></td></tr></tbody>' +
        '</table></body></html>')
    content = contentstr.encode('shift_jis')

    fetcher = logic.JbisRaceListFetcher(uri)

    with mock.patch('src.logic.get_s3_object') as m:
        m.return_value.last_modified = s3time
        body = mock.MagicMock(read=mock.MagicMock(return_value=content))
        m.return_value.get.return_value = {'Body': body}
        with mock.patch('src.logic.fetch_to_s3') as n:
            n.return_value = content
            uris = fetcher.fetch(bucket, nowtime)
            m.assert_called_once_with(bucket, key)
            n.assert_called_once()
            assert uris == []


def test_default_fetcher_fetch():
    """DefaultFetcher.fetch()のテスト."""
    with mock.patch('src.logic.logger') as m:
        logic.DefaultFetcher('abc').fetch(
            'bucket', datetime(2019, 12, 1, 12, 0, 0))
        m.warning.assert_called_once()

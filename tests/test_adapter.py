from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import unittest

import pytest
from mock import call, patch

from pygenie.adapter.genie_3 import Genie3Adapter, get_payload
from pygenie.adapter.genie_x import substitute
from pygenie.conf import GenieConf
from pygenie.exceptions import GenieHTTPError, GenieLogNotFoundError
from pygenie.jobs import PrestoJob

from .utils import fake_response


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestStringSubstitution(unittest.TestCase):
    """Test script parameter substitution."""

    def test_substitute(self):
        """Test script parameter substitution."""

        assert (
            substitute('hello $name, goodbye $last',
                       dict(name='test1', last='bye1')) ==
            'hello test1, goodbye bye1')

    def test_substitute_expansion(self):
        """Test script parameter substitution (expansion)."""

        assert (
            substitute('hello ${name}, goodbye ${last}',
                       dict(name='test2', last='bye2')) ==
            'hello test2, goodbye bye2')

    def test_substitute_missing(self):
        """Test script parameter substitution with missing parameters."""

        assert (
            substitute('hello $name, goodbye $last',
                       dict(name='tester3')) ==
            'hello tester3, goodbye $last')

    def test_substitute_missing_expansion(self):
        """Test script parameter substitution with missing parameters (expansion)."""

        assert (
            substitute('hello ${name}, goodbye ${last}',
                       dict(name='tester4')) ==
            'hello tester4, goodbye ${last}')


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestGenie3JobSubmission(unittest.TestCase):
    """Test Genie 3 job submission."""

    @patch('requests.sessions.Session.request')
    def test_job_submit(self, request):
        """Test Genie 3 adapter job submit."""

        request.return_value = fake_response(None, status_code=202)

        adapter = Genie3Adapter()

        adapter.submit_job(PrestoJob().script("select * from dual"))

        assert 1 == request.call_count

    @patch('requests.sessions.Session.request')
    def test_job_submit_409(self, request):
        """Test Genie 3 adapter job submit (409 response)."""

        request.side_effect = [
            fake_response(None, status_code=409),
            fake_response(None, status_code=202),
            fake_response(None, status_code=503),
        ]

        adapter = Genie3Adapter()

        with pytest.raises(GenieHTTPError):
            adapter.submit_job(PrestoJob().script("select * from dual"))

        assert 1 == request.call_count

    @patch('requests.sessions.Session.request')
    def test_job_submit_various_responses(self, request):
        """Test Genie 3 adapter job submit (various response codes then 202)."""

        request.side_effect = [
            fake_response(None, status_code=403),
            fake_response(None, status_code=404),
            fake_response(None, status_code=412),
            fake_response(None, status_code=503),
            fake_response(None, status_code=504),
            fake_response(None, status_code=202),
            fake_response(None, status_code=403),
            fake_response(None, status_code=404),
            fake_response(None, status_code=412),
            fake_response(None, status_code=503),
            fake_response(None, status_code=504),
        ]

        adapter = Genie3Adapter()

        adapter.submit_job(PrestoJob().script("select * from dual"),
                           attempts=15,
                           backoff=0)

        assert 6 == request.call_count


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestGenie3Adapter(unittest.TestCase):
    """Test Genie 3 adapter."""

    @patch('requests.sessions.Session.request')
    def test_stderr_log_not_found(self, request):
        """Test Genie 3 adapter getting stderr log which does not exist."""

        request.return_value = fake_response(None, status_code=404)

        adapter = Genie3Adapter()

        with pytest.raises(GenieLogNotFoundError):
            adapter.get_stderr('job_id_dne')

    def test_set_job_name_with_script_has_params(self):
        """Test Genie 3 adapter setting job name (if not set) with script containing parameters."""

        job = PrestoJob() \
            .script('select * from ${table}') \
            .parameter('table', 'foo.fizz').username('test')

        payload = get_payload(job)

        assert 'test.PrestoJob.Query' in payload['name']

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get')
    def test_get_info_for_rj_all(self, get):
        """Test Genie 3 adapter get info call for job (all)."""

        get.side_effect = [{'_links':{'self':{'href':'http://example.com'}}},{},[],{},{},{},{}]

        adapter = Genie3Adapter()
        adapter.get_info_for_rj('111-all')

        assert (
            [
                call('111-all', timeout=30),
                call('111-all', path='request', timeout=30),
                call('111-all', path='applications', timeout=30, if_not_found=[]),
                call('111-all', path='cluster', timeout=30, if_not_found={}),
                call('111-all', path='command', timeout=30, if_not_found={}),
                call('111-all', path='execution', timeout=30, if_not_found={}),
                call('111-all', path='output', timeout=30, headers={'Accept': 'application/json'}, if_not_found={})
            ] ==
            get.call_args_list)

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get')
    def test_get_info_for_rj_all_timeout(self, get):
        """Test Genie 3 adapter get info call for job (all) (with timeout)."""


        get.side_effect = [
            {'_links':{'self':{'href':'http://example.com'}}},
            {},
            [],
            {},
            {},
            {},
            {}
        ]
        adapter = Genie3Adapter()
        adapter.get_info_for_rj('111-all-timeout', timeout=1)

        assert (
            [
                call('111-all-timeout', timeout=1),
                call('111-all-timeout', path='request', timeout=1),
                call('111-all-timeout', path='applications', timeout=1, if_not_found=[]),
                call('111-all-timeout', path='cluster', timeout=1, if_not_found={}),
                call('111-all-timeout', path='command', timeout=1, if_not_found={}),
                call('111-all-timeout', path='execution', timeout=1, if_not_found={}),
                call('111-all-timeout', path='output', timeout=1, headers={'Accept': 'application/json'}, if_not_found={})
            ] ==
            get.call_args_list)

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get')
    def test_get_info_for_rj_job(self, get):
        """Test Genie 3 adapter get info call for job (job section)."""

        get.side_effect = [{'_links':{'self':{'href':'http://example.com'}}}]

        adapter = Genie3Adapter()
        adapter.get_info_for_rj('111-job', job=True)

        assert (
            [
                call('111-job', timeout=30),
            ] ==
            get.call_args_list)

    @patch('requests.sessions.Session.request')
    def test_get_404(self, request):
        """Test Genie 3 adapter get with if_not_found (404)."""

        request.side_effect = [
            fake_response(None, status_code=404),
            fake_response(None, status_code=111),
            fake_response(None, status_code=112),
            fake_response(None, status_code=113),
        ]

        adapter = Genie3Adapter()

        assert (
            'DEFAULT' ==
            Genie3Adapter() .get(job_id='job-get-404', path='output', if_not_found='DEFAULT'))

        assert (
            1 ==
            request.call_count)

    @patch('requests.sessions.Session.request')
    def test_get_500(self, request):
        """Test Genie 3 adapter get with if_not_found (500)."""

        request.side_effect = [
            fake_response(None, status_code=500),
            fake_response(None, status_code=500),
            fake_response(None, status_code=500),
            fake_response(None, status_code=500),
            fake_response(None, status_code=500),
            fake_response(None, status_code=500),
            fake_response(None, status_code=500),
        ]

        adapter = Genie3Adapter()

        assert (
            {} ==
            Genie3Adapter().get(job_id='job-get-500', path='output', if_not_found={}, attempts=3, backoff=0))

        assert (
            3 ==
            request.call_count)


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestGenie3AdapterDisableTimeout(unittest.TestCase):
    """Test Genie 3 adapter with disabled timeout."""

    def setUp(self):
        conf = GenieConf()
        conf.genie.set('disable_adapter_timeout', 'True')
        self.adapter = Genie3Adapter(conf=conf)

    @staticmethod
    def has_timeout_in_kwargs(kwargs):
        return 'timeout' in kwargs

    def test_disable_adapter_timeout_conf(self):
        """Test Genie 3 adapter disable_adapter_timeout values."""

        for value in {'True', 'true', 'TRUE', True, '1', 1}:
            conf = GenieConf()
            conf.genie.set('disable_adapter_timeout', value)
            adapter = Genie3Adapter(conf=conf)
            assert True == adapter.disable_timeout

    @patch('pygenie.adapter.genie_x.GenieBaseAdapter.call')
    def test_get_log_disabled_timeout(self, genie_call):
        """Test Genie 3 adapter get_log() (disabled timeout)."""

        self.adapter.get_log('job-1111', 'some.log', timeout=111)

        assert True == ('timeout' not in genie_call.call_args[1])

    @patch('pygenie.adapter.genie_x.GenieBaseAdapter.call')
    def test_get_disabled_timeout(self, genie_call):
        """Test Genie 3 adapter get() (disabled timeout)."""

        self.adapter.get('job-2222', timeout=222)

        assert True == ('timeout' not in genie_call.call_args[1])

    @patch('pygenie.adapter.genie_x.GenieBaseAdapter.call')
    def test_get_info_for_rj_disabled_timeout(self, genie_call):
        """Test Genie 3 adapter get_info_for_rj() (disabled timeout)."""

        self.adapter.get_info_for_rj('job-3333', execution=True, timeout=333)

        assert (
            False ==
            any([self.has_timeout_in_kwargs(kall[1]) for kall in genie_call.call_args_list]))

    @patch('pygenie.adapter.genie_x.GenieBaseAdapter.call')
    def test_get_status_disabled_timeout(self, genie_call):
        """Test Genie 3 adapter get_status() (disabled timeout)."""

        self.adapter.get_status('job-4444', timeout=1)

        assert True == ('timeout' not in genie_call.call_args[1])

    @patch('pygenie.adapter.genie_x.GenieBaseAdapter.call')
    def test_submit_job_disabled_timeout(self, genie_call):
        """Test Genie 3 adapter submit_job() (disabled timeout)."""

        job = PrestoJob().script('select * from table')

        self.adapter.submit_job(job)

        assert None == genie_call.call_args[1]['timeout']

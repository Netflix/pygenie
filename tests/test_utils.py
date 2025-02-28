from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import sys
import unittest
from socket import timeout

import pytest
from mock import patch, MagicMock
from requests.exceptions import ConnectionError, Timeout

from pygenie.exceptions import GenieHTTPError, GenieJobNotFoundError
from pygenie.jobs.utils import generate_job_id, is_file, reattach_job
from pygenie.utils import call, str_to_list

from .utils import FakeRunningJob, fake_response


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
@patch('requests.sessions.Session.request')
class TestCall(unittest.TestCase):
    """Test HTTP request calls to Genie server."""

    def test_202(self, request):
        """Test HTTP request via call() with 200 response."""

        request.return_value = fake_response({}, 202)

        resp = call('http://genie-202')

        assert 202 == resp.status_code
        assert 1 == request.call_count

    @patch('pygenie.utils.time.sleep')
    def test_various_status_code_retries(self, sleep, request):
        """Test HTTP request via call() with various non-200 status code responses."""

        request.side_effect = [
            fake_response({}, 403),
            fake_response({}, 403),
            fake_response({}, 404),
            fake_response({}, 404),
            fake_response({}, 409),
            fake_response({}, 409),
            fake_response({}, 412),
            fake_response({}, 503),
            fake_response({}, 503),
            fake_response({}, 504),
        ]

        with pytest.raises(GenieHTTPError):
            call('http://genie-non-200', attempts=10, backoff=0)

        assert 10 == request.call_count
        assert 9 == sleep.call_count

    @patch('pygenie.utils.time.sleep')
    def test_503_202(self, sleep, request):
        """Test HTTP request via call() with non-200 responses then 202 response."""

        request.side_effect = [
            fake_response({}, 403),
            fake_response({}, 404),
            fake_response({}, 409),
            fake_response({}, 412),
            fake_response({}, 503),
            fake_response({}, 504),
            fake_response({}, 202),
            fake_response({}, 403),
            fake_response({}, 404),
            fake_response({}, 409),
            fake_response({}, 412),
            fake_response({}, 503),
            fake_response({}, 504)
        ]

        resp = call('http://genie-non-200-202', attempts=10, backoff=0)

        assert 202 == resp.status_code
        assert 7 == request.call_count
        assert 6 == sleep.call_count

    @patch('pygenie.utils.time.sleep')
    def test_503_200(self, sleep, request):
        """Test HTTP request via call() with non-200 responses then 200 response."""

        request.side_effect = [
            fake_response({}, 403),
            fake_response({}, 404),
            fake_response({}, 409),
            fake_response({}, 412),
            fake_response({}, 503),
            fake_response({}, 504),
            fake_response({}, 200),
            fake_response({}, 403),
            fake_response({}, 404),
            fake_response({}, 409),
            fake_response({}, 412),
            fake_response({}, 503),
            fake_response({}, 504)
        ]

        resp = call('http://genie-non-200-200', attempts=10, backoff=0)

        assert 200 == resp.status_code
        assert 7 == request.call_count
        assert 6 == sleep.call_count

    @patch('pygenie.utils.time.sleep')
    def test_404_not_none(self, sleep, request):
        """Test HTTP request via call() with 404 response (raise error)."""

        request.return_value = fake_response({}, 404)

        with pytest.raises(GenieHTTPError):
            call('http://genie-404-raise', attempts=1, backoff=0)

        assert 1 == request.call_count
        assert 0 == sleep.call_count

    @patch('pygenie.utils.time.sleep')
    def test_404_none(self, sleep, request):
        """Test HTTP request via call() with 404 response (return None)."""

        request.return_value = fake_response({}, 404)

        resp = call('http://genie-404-none', none_on_404=True, attempts=1, backoff=0)

        assert None == resp
        assert 1 == request.call_count
        assert 0 == sleep.call_count

    @patch('pygenie.utils.time.sleep')
    def test_failure_code(self, sleep, request):
        """Test HTTP request via call() with a failure status code."""

        request.side_effect = [
            fake_response({}, 500),
            fake_response({}, 503),
            fake_response({}, 500),
            fake_response({}, 412),
            fake_response({}, 503),
            fake_response({}, 500),
            fake_response({}, 412),
            fake_response({}, 503),
            fake_response({}, 500)
        ]

        with pytest.raises(GenieHTTPError):
            call('http://genie-failure-code', failure_codes=412, attempts=10, backoff=0)

        assert 4 == request.call_count
        assert 3 == sleep.call_count

    @patch('pygenie.utils.time.sleep')
    def test_failure_codes(self, sleep, request):
        """Test HTTP request via call() with failure status codes."""

        request.side_effect = [
            fake_response({}, 500),
            fake_response({}, 503),
            fake_response({}, 409),
            fake_response({}, 500),
            fake_response({}, 412),
            fake_response({}, 503),
            fake_response({}, 500),
            fake_response({}, 412),
            fake_response({}, 503),
            fake_response({}, 500)
        ]

        with pytest.raises(GenieHTTPError):
            call('http://genie-failure-codes', failure_codes=[409, 412], attempts=10, backoff=0)

        assert 3 == request.call_count
        assert 2 == sleep.call_count

    @patch('pygenie.utils.time.sleep')
    def test_retry_timeout(self, sleep, request):
        """Test HTTP request via call() with timeouts."""

        request.side_effect = [
            Timeout,
            ConnectionError,
            timeout,
            Timeout,
            ConnectionError,
        ]

        with pytest.raises(Timeout):
            call('http://genie-timeout', attempts=4, backoff=0)

        assert 4 == request.call_count
        assert 3 == sleep.call_count

    @patch('pygenie.utils.time.sleep')
    def test_retry_timeout_202(self, sleep, request):
        """Test HTTP request via call() with timeouts (finishing with 202)."""

        request.side_effect = [
            Timeout,
            ConnectionError,
            fake_response({}, 202),
            timeout,
            Timeout
        ]

        call('http://genie-timeout-202', attempts=5, backoff=0)

        assert 3 == request.call_count
        assert 2 == sleep.call_count

    @patch('pygenie.utils.time.sleep')
    def test_retry_timeout_404(self, sleep, request):
        """Test HTTP request via call() with timeouts with failure code."""

        request.side_effect = [
            Timeout,
            ConnectionError,
            timeout,
            fake_response({}, 404),
            Timeout,
            ConnectionError,
            timeout,
            Timeout,
            ConnectionError,
            timeout
        ]

        with pytest.raises(GenieHTTPError):
            call('http://genie-timeout-404', failure_codes=404, attempts=7, backoff=0)

        assert 4 == request.call_count
        assert 3 == sleep.call_count

    @patch('pygenie.utils.time.sleep')
    def test_retry_timeout_404_return_none(self, sleep, request):
        """Test HTTP request via call() with timeouts (finishing with 404 but return None)."""

        request.side_effect = [
            Timeout,
            ConnectionError,
            timeout,
            fake_response({}, 404),
            Timeout,
            ConnectionError,
            timeout
        ]

        resp = call('http://genie-timeout-404',
                    attempts=7,
                    backoff=0,
                    failure_codes=404,
                    none_on_404=True)

        assert 4 == request.call_count
        assert 3 == sleep.call_count
        assert None == resp


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestStringToList(unittest.TestCase):
    """Test converting string to list."""

    def test_string_to_list_string(self):
        """Test string to list with default delimiter (comma)."""

        assert (
            str_to_list('a, b, c') ==
            ['a', 'b', 'c'])

    def test_string_to_list_string_delimiter(self):
        """Test string to list with specified delimiter."""

        assert (
            str_to_list(' a | b | c ', delimiter='|') ==
            ['a', 'b', 'c'])

    def test_string_to_list_none(self):
        """Test string to list with input arg None."""

        assert (
            str_to_list(None) ==
            None)


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestReattachJob(unittest.TestCase):
    """Test reattaching to a running job."""

    @patch('pygenie.jobs.running.RunningJob.update')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    def test_reattaching_job(self, get_status, update):
        """Test reattaching to a running job."""

        running_job = reattach_job('test-reattach-job')

        get_status.assert_called_once_with('test-reattach-job')

    @patch('pygenie.jobs.running.RunningJob.update')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    def test_reattaching_job_does_not_exist(self, get_status, update):
        """Test reattaching to a running job that does not exist."""

        get_status.side_effect = GenieJobNotFoundError

        with pytest.raises(GenieJobNotFoundError) as cm:
            running_job = reattach_job('test-reattach-job-dne')


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestGeneratingJobId(unittest.TestCase):
    """Test generating job ids."""

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_new(self, mock_reattach_job):
        """Test generating job id which is completely new."""

        job_id = 'brand-new'

        mock_reattach_job.side_effect = [
            GenieJobNotFoundError
        ]

        assert job_id == generate_job_id(job_id)

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_return_success_true_running(self, mock_reattach_job):
        """Test generating job id with returning successful job set to True with a job that is running."""

        job_id = 'running'

        mock_reattach_job.side_effect = [
            FakeRunningJob(job_id=job_id, status='FAILED'),
            FakeRunningJob(job_id=job_id+'-1', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-2', status='FAILED'),
            FakeRunningJob(job_id=job_id+'-3', status='RUNNING'),
            FakeRunningJob(job_id=job_id+'-4', status='SUCCEEDED'),
            GenieJobNotFoundError
        ]

        assert job_id+'-3' == generate_job_id(job_id)

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_return_success_true(self, mock_reattach_job):
        """Test generating job id with returning successful job set to True."""

        job_id = 'return-successful-true'

        mock_reattach_job.side_effect = [
            FakeRunningJob(job_id=job_id, status='FAILED'),
            FakeRunningJob(job_id=job_id+'-1', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-2', status='SUCCEEDED'),
            FakeRunningJob(job_id=job_id+'-3', status='SUCCEEDED'),
            FakeRunningJob(job_id=job_id+'-4', status='KILLED'),
            GenieJobNotFoundError
        ]

        assert job_id+'-2' == generate_job_id(job_id)

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_with_return_success_false(self, mock_reattach_job):
        """Test generating job id with returning successful job set to False."""

        job_id = 'return-successful-false'

        mock_reattach_job.side_effect = [
            FakeRunningJob(job_id=job_id, status='FAILED'),
            FakeRunningJob(job_id=job_id+'-1', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-2', status='SUCCEEDED'),
            FakeRunningJob(job_id=job_id+'-3', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-4', status='FAILED'),
            FakeRunningJob(job_id=job_id+'-5', status='SUCCEEDED'),
            GenieJobNotFoundError
        ]

        assert job_id+'-6' == generate_job_id(job_id, return_success=False)

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_with_return_success_false_with_running(self, mock_reattach_job):
        """Test generating job id with returning successful job set to False with a job that is running."""

        job_id = 'return-successful-false-running'

        mock_reattach_job.side_effect = [
            FakeRunningJob(job_id=job_id, status='FAILED'),
            FakeRunningJob(job_id=job_id+'-1', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-2', status='SUCCEEDED'),
            FakeRunningJob(job_id=job_id+'-3', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-4', status='RUNNING'),
            FakeRunningJob(job_id=job_id+'-5', status='SUCCEEDED'),
            GenieJobNotFoundError
        ]

        assert job_id+'-4' == generate_job_id(job_id, return_success=False)

    def test_gen_job_id_with_return_success_true_override_existing_true(self):
        """Test generating job id with both returning successful job and overriding existing job set to True."""

        job_id = 'return-successful-true-override-existing-true'

        with self.assertRaises(ValueError) as context:
            generate_job_id(job_id, return_success=True, override_existing=True)

        self.assertEqual(str(context.exception), "return_success and override_existing cannot both be True")

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_with_return_success_false_override_existing_true(self, mock_reattach_job):
        """Test generating job id with returning successful job set to False and
        overriding existing job set to True."""

        job_id = 'return-successful-false-override-existing-true'

        mock_reattach_job.side_effect = [
            FakeRunningJob(job_id=job_id, status='FAILED'),
            FakeRunningJob(job_id=job_id + '-1', status='KILLED'),
            FakeRunningJob(job_id=job_id + '-2', status='SUCCEEDED'),
            FakeRunningJob(job_id=job_id + '-3', status='KILLED'),
            FakeRunningJob(job_id=job_id + '-4', status='FAILED'),
            FakeRunningJob(job_id=job_id + '-5', status='SUCCEEDED'),
            GenieJobNotFoundError
        ]

        assert job_id + '-6' == generate_job_id(job_id, return_success=False, override_existing=True)

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_with_return_success_false_override_existing_true_with_running(self, mock_reattach_job):
        """Test generating job id with returning successful job set to False and
        overriding existing job set to True with a job that is running."""

        job_id = 'return-successful-false-override-existing-true-running'

        mock_running_job = MagicMock()
        mock_running_job.job_id = job_id + '-5'
        mock_running_job.status = 'RUNNING'
        mock_running_job.is_done = False
        mock_running_job.kill.return_value = fake_response({}, 200)

        mock_reattach_job.side_effect = [
            FakeRunningJob(job_id=job_id, status='FAILED'),
            FakeRunningJob(job_id=job_id + '-1', status='KILLED'),
            FakeRunningJob(job_id=job_id + '-2', status='SUCCEEDED'),
            FakeRunningJob(job_id=job_id + '-3', status='KILLED'),
            FakeRunningJob(job_id=job_id + '-4', status='FAILED'),
            mock_running_job,
            GenieJobNotFoundError
        ]

        assert job_id + '-6' == generate_job_id(job_id, return_success=False, override_existing=True)
        mock_running_job.kill.assert_called_once()

    def test_is_file_for_valid_s3path(self):
        path = 's3://root/myfile'
        assert is_file(path) == True

    @pytest.mark.skipif(sys.version_info != (3, 7), reason="Requires Python 3.7")
    def test_is_file_for_s3path_with_null_bytes(self):
        # simulate https://github.com/python/cpython/pull/7695
        path = 's3://root/myfile\x00'
        assert is_file(path) == False

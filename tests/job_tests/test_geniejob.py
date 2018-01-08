from __future__ import absolute_import, division, print_function, unicode_literals

import os
import unittest

from mock import patch
from nose.tools import assert_equals, assert_raises, assert_true

import pygenie

from ..utils import FakeRunningJob


assert_equals.__self__.maxDiff = None


@pygenie.adapter.genie_3.set_jobname
def set_jobname(job):
    return dict()


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingGenieJob(unittest.TestCase):
    """Test GenieJob."""

    def test_default_command_tag(self):
        """Test GenieJob default command tags."""

        job = pygenie.jobs.GenieJob()

        assert_equals(
            job.get('default_command_tags'),
            [u'type:genie']
        )

    def test_cmd_args_explicit(self):
        """Test GenieJob explicit cmd args."""

        job = pygenie.jobs.GenieJob() \
            .command_arguments('explicitly stating command args')

        assert_equals(
            job.cmd_args,
            u'explicitly stating command args'
        )

    def test_cmd_args_constructed(self):
        """Test GenieJob constructed cmd args."""

        with assert_raises(pygenie.exceptions.GenieJobError) as cm:
            pygenie.jobs.GenieJob().cmd_args


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingGenieJobRepr(unittest.TestCase):
    """Test GenieJob repr."""

    @patch('pygenie.jobs.core.is_file')
    def test_repr(self, is_file):
        """Test GenieJob repr."""

        is_file.return_value = True

        job = pygenie.jobs.GenieJob() \
            .applications('app1') \
            .applications('app2') \
            .archive(False) \
            .cluster_tags('cluster1') \
            .cluster_tags('cluster2') \
            .command_arguments('genie job repr args') \
            .command_tags('cmd1') \
            .command_tags('cmd2') \
            .dependencies('/dep1') \
            .dependencies('/dep2') \
            .description('description') \
            .disable_archive() \
            .genie_email('jsmith@email.com') \
            .genie_setup_file('/setup.sh') \
            .genie_timeout(999) \
            .genie_url('http://asdfasdf') \
            .genie_username('jsmith') \
            .group('group1') \
            .job_id('geniejob_repr') \
            .job_name('geniejob_repr') \
            .job_version('1.1.1') \
            .parameter('param1', 'pval1') \
            .parameter('param2', 'pval2') \
            .parameters(param3='pval3', param4='pval4') \
            .post_cmd_args('post1') \
            .post_cmd_args(['post2', 'post3']) \
            .tags('tag1') \
            .tags(['tag3', 'tag4']) \
            .tags('tag2')

        assert_equals(
            str(job),
            '.'.join([
                'GenieJob()',
                'applications("app1")',
                'applications("app2")',
                'archive(False)',
                'cluster_tags("cluster1")',
                'cluster_tags("cluster2")',
                'command_arguments("genie job repr args")',
                'command_tags("cmd1")',
                'command_tags("cmd2")',
                'dependencies("/dep1")',
                'dependencies("/dep2")',
                'description("description")',
                'genie_email("jsmith@email.com")',
                'genie_setup_file("/setup.sh")',
                'genie_timeout(999)',
                'genie_url("http://asdfasdf")',
                'genie_username("jsmith")',
                'group("group1")',
                'job_id("geniejob_repr")',
                'job_name("geniejob_repr")',
                'job_version("1.1.1")',
                'parameter("param1", "pval1")',
                'parameter("param2", "pval2")',
                'parameter("param3", "pval3")',
                'parameter("param4", "pval4")',
                'post_cmd_args("post1")',
                'post_cmd_args(["post2", "post3"])',
                'tags("tag1")',
                'tags("tag2")',
                'tags(["tag3", "tag4"])'
            ])
        )

    def test_genie_cpu(self):
        """Test GenieJob repr (genie_cpu)."""

        job = pygenie.jobs.GenieJob() \
            .job_id('123') \
            .genie_username('user') \
            .genie_cpu(12)

        assert_equals(
            '.'.join([
                'GenieJob()',
                'genie_cpu(12)',
                'genie_username("user")',
                'job_id("123")'
            ]),
            str(job)
        )

    def test_genie_memory(self):
        """Test GenieJob repr (genie_memory)."""

        job = pygenie.jobs.GenieJob() \
            .job_id('123') \
            .genie_username('user') \
            .genie_memory(7000)

        assert_equals(
            '.'.join([
                'GenieJob()',
                'genie_memory(7000)',
                'genie_username("user")',
                'job_id("123")'
            ]),
            str(job)
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingGenieJobAdapters(unittest.TestCase):
    """Test adapting GenieJob to different clients."""

    def setUp(self):
        self.dirname = os.path.dirname(os.path.realpath(__file__))

    def test_genie3_payload(self):
        """Test GenieJob payload for Genie 3."""

        with patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'}):
            genie3_conf = pygenie.conf.GenieConf() \
                .load_config_file(os.path.join(self.dirname, 'genie3.ini'))

        job = pygenie.jobs.GenieJob(genie3_conf) \
            .applications(['applicationid1']) \
            .cluster_tags('type:cluster1') \
            .command_arguments('command args for geniejob') \
            .command_tags('type:geniecmd') \
            .dependencies(['/file1', '/file2']) \
            .description('this job is to test geniejob adapter') \
            .archive(False) \
            .genie_cpu(3) \
            .genie_email('jdoe@email.com') \
            .genie_memory(999) \
            .genie_timeout(100) \
            .genie_url('http://fdsafdsa') \
            .genie_username('jdoe') \
            .group('geniegroup1') \
            .job_id('geniejob1') \
            .job_name('testing_adapting_geniejob') \
            .tags('tag1, tag2') \
            .job_version('0.0.1alpha')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                'applications': ['applicationid1'],
                'attachments': [],
                'clusterCriterias': [
                    {'tags': ['type:cluster1']},
                    {'tags': ['type:genie']},
                ],
                'commandArgs': 'command args for geniejob',
                'commandCriteria': ['type:geniecmd'],
                'cpu': 3,
                'dependencies': ['/file1', '/file2'],
                'description': 'this job is to test geniejob adapter',
                'disableLogArchival': True,
                'email': 'jdoe@email.com',
                'group': 'geniegroup1',
                'id': 'geniejob1',
                'memory': 999,
                'name': 'testing_adapting_geniejob',
                'setupFile': None,
                'tags': ['tag1', 'tag2'],
                'timeout': 100,
                'user': 'jdoe',
                'version': '0.0.1alpha'
            }
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingJobExecute(unittest.TestCase):
    """Test executing job."""

    @patch('pygenie.jobs.core.reattach_job')
    @patch('pygenie.jobs.core.generate_job_id')
    @patch('pygenie.jobs.core.execute_job')
    def test_job_execute(self, exec_job, gen_job_id, reattach_job):
        """Testing job execution."""

        job = pygenie.jobs.HiveJob() \
            .job_id('exec') \
            .genie_username('exectester') \
            .script('select * from db.table')

        job.execute()

        gen_job_id.assert_not_called()
        reattach_job.assert_not_called()
        exec_job.assert_called_once_with(job)

    @patch('pygenie.jobs.core.reattach_job')
    @patch('pygenie.jobs.core.generate_job_id')
    @patch('pygenie.jobs.core.execute_job')
    def test_job_execute_retry(self, exec_job, gen_job_id, reattach_job):
        """Testing job execution with retry."""

        job_id = 'exec-retry'
        new_job_id = '{}-5'.format(job_id)

        gen_job_id.return_value = new_job_id
        reattach_job.side_effect = pygenie.exceptions.GenieJobNotFoundError

        job = pygenie.jobs.HiveJob() \
            .job_id(job_id) \
            .genie_username('exectester') \
            .script('select * from db.table')

        job.execute(retry=True)

        gen_job_id.assert_called_once_with(job_id,
                                           return_success=True,
                                           conf=job._conf)
        reattach_job.assert_called_once_with(new_job_id, conf=job._conf)
        exec_job.assert_called_once_with(job)
        assert_equals(new_job_id, job._job_id)

    @patch('pygenie.jobs.core.reattach_job')
    @patch('pygenie.jobs.core.generate_job_id')
    @patch('pygenie.jobs.core.execute_job')
    def test_job_execute_retry_force(self, exec_job, gen_job_id, reattach_job):
        """Testing job execution with force retry."""

        job_id = 'exec-retry-force'
        new_job_id = '{}-8'.format(job_id)

        gen_job_id.return_value = new_job_id
        reattach_job.side_effect = pygenie.exceptions.GenieJobNotFoundError

        job = pygenie.jobs.HiveJob() \
            .job_id(job_id) \
            .genie_username('exectester') \
            .script('select * from db.table')

        job.execute(retry=True, force=True)

        gen_job_id.assert_called_once_with(job_id,
                                           return_success=False,
                                           conf=job._conf)
        reattach_job.assert_called_once_with(new_job_id, conf=job._conf)
        exec_job.assert_called_once_with(job)
        assert_equals(new_job_id, job._job_id)


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingSetJobId(unittest.TestCase):
    """Test setting job id."""

    def test_job_id_empty_string(self):
        """Test setting job id empty string ('')."""

        with assert_raises(AssertionError) as cm:
            job = pygenie.jobs.GenieJob().job_id('')

    def test_job_id_none(self):
        """Test setting job id None."""

        with assert_raises(AssertionError) as cm:
            job = pygenie.jobs.GenieJob().job_id(None)

    def test_job_id(self):
        """Test setting job id."""

        job = pygenie.jobs.GenieJob().job_id('job-id-1234')

        assert_equals(
            'job-id-1234',
            job._job_id
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingSetJobName(unittest.TestCase):
    """Test setting job name with script name or a query string."""

    def test_set_job_name_script(self):
        """Test setting job name from script file name."""

        assert_equals(
            {'name': 'user.HiveJob.Script.FILE_NAME'},
            set_jobname(pygenie.jobs.HiveJob() \
                .script('s3://path/to/file/file_name.hive').username('user'))
        )

    def test_set_job_name_query(self):
        """Test setting job name for query string set as script"""

        assert_true(
            'user.PrestoJob.Query' in
            set_jobname(pygenie.jobs.PrestoJob() \
                        .script('select * from db.table').username('user')).get(
                'name', ''
            )
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingSetGenieUrl(unittest.TestCase):
    """Test setting genie url."""

    def test_set_genie_url(self):
        """Test setting genie url."""

        url = 'http://www.test-genie-url.com:7001'

        job = pygenie.jobs.GenieJob() \
            .genie_url(url)

        assert_equals(
            url,
            job._conf.genie.url
        )

    def test_set_genie_url_trailing_slash(self):
        """Test setting genie url with trailing slash."""

        url = 'http://www.test-genie-url.com:7001/'
        url_clean = url.rstrip('/')

        job = pygenie.jobs.GenieJob() \
            .genie_url(url)

        assert_equals(
            url_clean,
            job._conf.genie.url
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingGrouping(unittest.TestCase):
    """Test job Genie grouping."""

    def test_grouping_repr(self):
        """Test job Genie grouping repr."""

        job = pygenie.jobs.GenieJob() \
            .job_id('1234') \
            .genie_username('group_repr') \
            .genie_grouping('test_group_repr_1') \
            .genie_grouping('test_group_repr_2')

        assert_equals(
           '.'.join([
                'GenieJob()',
                'genie_grouping("test_group_repr_2")',
                'genie_username("group_repr")',
                'job_id("1234")',
            ]),
            str(job)
        )

    def test_setting_grouping(self):
        """Test setting job Genie grouping."""

        job = pygenie.jobs.GenieJob() \
            .genie_grouping('test_group') \
            .genie_grouping('test_group_1')

        assert_equals(
            'test_group_1',
            job._genie_grouping
        )

    def test_grouping_payload_genie3(self):
        """Test job Genie grouping payload (Genie 3)."""

        job = pygenie.jobs.GenieJob() \
            .command_arguments('test') \
            .genie_grouping('test_grouping_payload') \

        assert_equals(
            'test_grouping_payload',
            pygenie.adapter.genie_3.get_payload(job)['grouping']
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingGroupingInstance(unittest.TestCase):
    """Test job Genie grouping instance."""

    def test_grouping_instance_repr(self):
        """Test job Genie grouping instance repr."""

        job = pygenie.jobs.GenieJob() \
            .job_id('1234') \
            .genie_username('group_instance_repr') \
            .genie_grouping_instance('test_group_instance_repr_1') \
            .genie_grouping_instance('test_group_instance_repr_2') \

        assert_equals(
            '.'.join([
                'GenieJob()',
                'genie_grouping_instance("test_group_instance_repr_2")',
                'genie_username("group_instance_repr")',
                'job_id("1234")',
            ]),
            str(job)
        )

    def test_setting_grouping_instance(self):
        """Test setting job Genie grouping instance."""

        job = pygenie.jobs.GenieJob() \
            .genie_grouping_instance('test_group_1234') \
            .genie_grouping_instance('test_group_777')

        assert_equals(
            'test_group_777',
            job._genie_grouping_instance
        )

    def test_grouping_instance_payload_genie3(self):
        """Test job Genie grouping instance payload (Genie 3)."""

        job = pygenie.jobs.GenieJob() \
            .command_arguments('test') \
            .genie_grouping_instance('test_grouping_instance_payload') \

        assert_equals(
            'test_grouping_instance_payload',
            pygenie.adapter.genie_3.get_payload(job)['groupingInstance']
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingMetadata(unittest.TestCase):
    """Test job metadata."""

    def test_metadata_repr(self):
        """Test job metadata repr."""

        job = pygenie.jobs.GenieJob() \
            .job_id('1234') \
            .genie_username('test_metadata_repr') \
            .metadata(foo='fizz') \
            .metadata(foo='foo') \
            .metadata(bar='buzz') \
            .metadata(hello='hi') \

        assert_equals(
            '.'.join([
                'GenieJob()',
                'genie_username("test_metadata_repr")',
                'job_id("1234")',
                'metadata(bar="buzz")',
                'metadata(foo="fizz")',
                'metadata(foo="foo")',
                'metadata(hello="hi")'
            ]),
            str(job)
        )

    def test_setting_metadata(self):
        """Test setting job metadata."""

        job = pygenie.jobs.GenieJob() \
            .metadata(key1='val1') \
            .metadata(key2='val2') \
            .metadata(key3='val3') \
            .metadata(key1='val1a') \
            .metadata(key4='val4', key5='val5')

        assert_equals(
            {
                'key1': 'val1a',
                'key2': 'val2',
                'key3': 'val3',
                'key4': 'val4',
                'key5': 'val5',
            },
            job._metadata
        )

    def test_metadata_payload_genie3(self):
        """Test job metadata payload (Genie 3)."""

        job = pygenie.jobs.GenieJob() \
            .command_arguments('test') \
            .metadata(k1=1, k2=2)

        assert_equals(
            {
                'k1': 1,
                'k2': 2,
            },
            pygenie.adapter.genie_3.get_payload(job)['metadata']
        )

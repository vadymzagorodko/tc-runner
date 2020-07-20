import gzip
import json
import logging
import os
import tempfile
import zipfile

import pytest
import retrying
import test_helpers
from dcos_test_utils.diagnostics import Diagnostics
from dcos_test_utils.helpers import check_json


__maintainer__ = 'mnaboka'
__contact__ = 'dcos-cluster-ops@mesosphere.io'

# Expected latency for all dcos-diagnostics units to refresh after postflight plus
# another minute to allow for check-time to settle. See: DCOS_OSS-988
LATENCY = 120


@pytest.mark.supportedwindows
@retrying.retry(wait_fixed=2000, stop_max_delay=LATENCY * 1000)
def test_dcos_diagnostics_health(dcos_api_session):
    """
    test health endpoint /system/health/v1
    """
    required_fields = ['units', 'hostname', 'ip', 'dcos_version', 'node_role', 'mesos_id', 'dcos_diagnostics_version']
    required_fields_unit = ['id', 'health', 'output', 'description', 'help', 'name']

    # Check all mains dcos-diagnostics instances on base port since this is extra-cluster request (outside localhost)
    for host in dcos_api_session.mains:
        response = check_json(dcos_api_session.health.get('/', node=host))
        assert len(response) == len(required_fields), 'response must have the following fields: {}'.format(
            ', '.join(required_fields)
        )

        # validate units
        assert 'units' in response, 'units field not found'
        assert isinstance(response['units'], list), 'units field must be a list'
        assert len(response['units']) > 0, 'units field cannot be empty'
        for unit in response['units']:
            assert len(unit) == len(required_fields_unit), 'unit must have the following fields: {}'.format(
                ', '.join(required_fields_unit)
            )
            for required_field_unit in required_fields_unit:
                assert required_field_unit in unit, '{} must be in a unit repsonse'

            # id, health and description cannot be empty
            assert unit['id'], 'id field cannot be empty'
            assert unit['health'] in [0, 1], 'health field must be 0 or 1'
            assert unit['description'], 'description field cannot be empty'

        # check all required fields but units
        for required_field in required_fields[1:]:
            assert required_field in response, '{} field not found'.format(required_field)
            assert response[required_field], '{} cannot be empty'.format(required_field)

    # Check all agents running dcos-diagnostics behind agent-adminrouter on 61001
    for host in dcos_api_session.subordinates:
        response = check_json(dcos_api_session.health.get('/', node=host))
        assert len(response) == len(required_fields), 'response must have the following fields: {}'.format(
            ', '.join(required_fields)
        )

        # validate units
        assert 'units' in response, 'units field not found'
        assert isinstance(response['units'], list), 'units field must be a list'
        assert len(response['units']) > 0, 'units field cannot be empty'
        for unit in response['units']:
            assert len(unit) == len(required_fields_unit), 'unit must have the following fields: {}'.format(
                ', '.join(required_fields_unit)
            )
            for required_field_unit in required_fields_unit:
                assert required_field_unit in unit, '{} must be in a unit repsonse'

            # id, health and description cannot be empty
            assert unit['id'], 'id field cannot be empty'
            assert unit['health'] in [0, 1], 'health field must be 0 or 1'
            assert unit['description'], 'description field cannot be empty'

        # check all required fields but units
        for required_field in required_fields[1:]:
            assert required_field in response, '{} field not found'.format(required_field)
            assert response[required_field], '{} cannot be empty'.format(required_field)


@pytest.mark.supportedwindows
@retrying.retry(wait_fixed=2000, stop_max_delay=LATENCY * 1000)
def test_dcos_diagnostics_nodes(dcos_api_session):
    """
    test a list of nodes with statuses endpoint /system/health/v1/nodes
    """
    for main in dcos_api_session.mains:
        response = check_json(dcos_api_session.health.get('/nodes', node=main))
        assert len(response) == 1, 'nodes response must have only one field: nodes'
        assert 'nodes' in response
        assert isinstance(response['nodes'], list)
        assert len(response['nodes']) == len(dcos_api_session.mains + dcos_api_session.all_subordinates), \
            ('a number of nodes in response must be {}'.
             format(len(dcos_api_session.mains + dcos_api_session.all_subordinates)))

        # test nodes
        validate_node(response['nodes'])


@pytest.mark.supportedwindows
def test_dcos_diagnostics_nodes_node(dcos_api_session):
    """
    test a specific node enpoint /system/health/v1/nodes/<node>
    """
    for main in dcos_api_session.mains:
        # get a list of nodes
        response = check_json(dcos_api_session.health.get('/nodes', node=main))
        nodes = list(map(lambda node: node['host_ip'], response['nodes']))

        for node in nodes:
            node_response = check_json(dcos_api_session.health.get('/nodes/{}'.format(node), node=main))
            validate_node([node_response])


@pytest.mark.supportedwindows
def test_dcos_diagnostics_nodes_node_units(dcos_api_session):
    """
    test a list of units from a specific node, endpoint /system/health/v1/nodes/<node>/units
    """
    for main in dcos_api_session.mains:
        # get a list of nodes
        response = check_json(dcos_api_session.health.get('/nodes', node=main))
        nodes = list(map(lambda node: node['host_ip'], response['nodes']))

        for node in nodes:
            units_response = check_json(dcos_api_session.health.get('/nodes/{}/units'.format(node), node=main))

            assert len(units_response) == 1, 'unit response should have only 1 field `units`'
            assert 'units' in units_response
            validate_units(units_response['units'])


@pytest.mark.supportedwindows
def test_dcos_diagnostics_nodes_node_units_unit(dcos_api_session):
    """
    test a specific unit for a specific node, endpoint /system/health/v1/nodes/<node>/units/<unit>
    """
    for main in dcos_api_session.mains:
        response = check_json(dcos_api_session.health.get('/nodes', node=main))
        nodes = list(map(lambda node: node['host_ip'], response['nodes']))
        for node in nodes:
            units_response = check_json(dcos_api_session.health.get('/nodes/{}/units'.format(node), node=main))
            unit_ids = list(map(lambda unit: unit['id'], units_response['units']))

            for unit_id in unit_ids:
                validate_unit(
                    check_json(dcos_api_session.health.get('/nodes/{}/units/{}'.format(node, unit_id), node=main)))


@pytest.mark.supportedwindows
@retrying.retry(wait_fixed=2000, stop_max_delay=LATENCY * 1000)
def test_dcos_diagnostics_units(dcos_api_session):
    """
    test a list of collected units, endpoint /system/health/v1/units
    """
    # get all unique unit names
    all_units = set()
    for node in dcos_api_session.mains:
        node_response = check_json(dcos_api_session.health.get('/', node=node))
        for unit in node_response['units']:
            all_units.add(unit['id'])

    for node in dcos_api_session.all_subordinates:
        node_response = check_json(dcos_api_session.health.get('/', node=node))
        for unit in node_response['units']:
            all_units.add(unit['id'])

    # test against mains
    for main in dcos_api_session.mains:
        units_response = check_json(dcos_api_session.health.get('/units', node=main))
        validate_units(units_response['units'])

        pulled_units = list(map(lambda unit: unit['id'], units_response['units']))
        logging.info('collected units: {}'.format(pulled_units))
        diff = set(pulled_units).symmetric_difference(all_units)
        assert set(pulled_units) == all_units, ('not all units have been collected by dcos-diagnostics '
                                                'puller, missing: {}'.format(diff))


@pytest.mark.supportedwindows
@retrying.retry(wait_fixed=2000, stop_max_delay=LATENCY * 1000)
def test_systemd_units_health(dcos_api_session):
    """
    test all units and make sure the units are healthy. This test will fail if any of systemd unit is unhealthy,
    meaning it focuses on making sure the dcos_api_session is healthy, rather then testing dcos-diagnostics itself.
    """
    unhealthy_output = []
    assert dcos_api_session.mains, "Must have at least 1 main node"
    report_response = check_json(dcos_api_session.health.get('/report', node=dcos_api_session.mains[0]))
    assert 'Units' in report_response, "Missing `Units` field in response"
    for unit_name, unit_props in report_response['Units'].items():
        assert 'Health' in unit_props, "Unit {} missing `Health` field".format(unit_name)
        if unit_props['Health'] != 0:
            assert 'Nodes' in unit_props, "Unit {} missing `Nodes` field".format(unit_name)
            assert isinstance(unit_props['Nodes'], list), 'Field `Node` must be a list'
            for node in unit_props['Nodes']:
                assert 'Health' in node, 'Field `Health` is expected to be in nodes properties, got {}'.format(node)
                if node['Health'] != 0:
                    assert 'Output' in node, 'Field `Output` is expected to be in nodes properties, got {}'.format(node)
                    assert isinstance(node['Output'], dict), 'Field `Output` must be a dict'
                    assert unit_name in node['Output'], 'unit {} must be in node Output, got {}'.format(unit_name,
                                                                                                        node['Output'])
                    assert 'IP' in node, 'Field `IP` is expected to be in nodes properties, got {}'.format(node)
                    unhealthy_output.append(
                        'Unhealthy unit {} has been found on node {}, health status {}. journalctl output {}'.format(
                            unit_name, node['IP'], unit_props['Health'], node['Output'][unit_name]))

    if unhealthy_output:
        raise AssertionError('\n'.join(unhealthy_output))


@pytest.mark.supportedwindows
def test_dcos_diagnostics_units_unit(dcos_api_session):
    """
    test a unit response in a right format, endpoint: /system/health/v1/units/<unit>
    """
    for main in dcos_api_session.mains:
        units_response = check_json(dcos_api_session.health.get('/units', node=main))
        pulled_units = list(map(lambda unit: unit['id'], units_response['units']))
        for unit in pulled_units:
            unit_response = check_json(dcos_api_session.health.get('/units/{}'.format(unit), node=main))
            validate_units([unit_response])


@retrying.retry(wait_fixed=2000, stop_max_delay=LATENCY * 1000)
def test_dcos_diagnostics_units_unit_nodes(dcos_api_session):
    """
    test a list of nodes for a specific unit, endpoint /system/health/v1/units/<unit>/nodes
    """

    def get_nodes_from_response(response):
        assert 'nodes' in response, 'response must have field `nodes`. Got {}'.format(response)
        nodes_ip_map = make_nodes_ip_map(dcos_api_session)
        nodes = []
        for node in response['nodes']:
            assert 'host_ip' in node, 'node response must have `host_ip` field. Got {}'.format(node)
            assert node['host_ip'] in nodes_ip_map, 'nodes_ip_map must have node {}.Got {}'.format(node['host_ip'],
                                                                                                   nodes_ip_map)
            nodes.append(nodes_ip_map.get(node['host_ip']))
        return nodes

    for main in dcos_api_session.mains:
        units_response = check_json(dcos_api_session.health.get('/units', node=main))
        pulled_units = list(map(lambda unit: unit['id'], units_response['units']))
        for unit in pulled_units:
            nodes_response = check_json(dcos_api_session.health.get('/units/{}/nodes'.format(unit), node=main))
            validate_node(nodes_response['nodes'])

        # make sure dcos-mesos-main.service has main nodes and dcos-mesos-subordinate.service has agent nodes
        main_nodes_response = check_json(
            dcos_api_session.health.get('/units/dcos-mesos-main.service/nodes', node=main))

        main_nodes = get_nodes_from_response(main_nodes_response)

        assert len(main_nodes) == len(dcos_api_session.mains), \
            '{} != {}'.format(main_nodes, dcos_api_session.mains)
        assert set(main_nodes) == set(dcos_api_session.mains), 'a list of difference: {}'.format(
            set(main_nodes).symmetric_difference(set(dcos_api_session.mains))
        )

        agent_nodes_response = check_json(
            dcos_api_session.health.get('/units/dcos-mesos-subordinate.service/nodes', node=main))

        agent_nodes = get_nodes_from_response(agent_nodes_response)

        assert len(agent_nodes) == len(dcos_api_session.subordinates), '{} != {}'.format(agent_nodes, dcos_api_session.subordinates)


@pytest.mark.supportedwindows
def test_dcos_diagnostics_units_unit_nodes_node(dcos_api_session):
    """
    test a specific node for a specific unit, endpoint /system/health/v1/units/<unit>/nodes/<node>
    """
    required_node_fields = ['host_ip', 'health', 'role', 'output', 'help']

    for main in dcos_api_session.mains:
        units_response = check_json(dcos_api_session.health.get('/units', node=main))
        pulled_units = list(map(lambda unit: unit['id'], units_response['units']))
        for unit in pulled_units:
            nodes_response = check_json(dcos_api_session.health.get('/units/{}/nodes'.format(unit), node=main))
            pulled_nodes = list(map(lambda node: node['host_ip'], nodes_response['nodes']))
            logging.info('pulled nodes: {}'.format(pulled_nodes))
            for node in pulled_nodes:
                node_response = check_json(
                    dcos_api_session.health.get('/units/{}/nodes/{}'.format(unit, node), node=main))
                assert len(node_response) == len(required_node_fields), 'required fields: {}'.format(
                    ', '.format(required_node_fields)
                )

                for required_node_field in required_node_fields:
                    assert required_node_field in node_response, 'field {} must be set'.format(required_node_field)

                # host_ip, health, role, help cannot be empty
                assert node_response['host_ip'], 'host_ip field cannot be empty'
                assert node_response['health'] in [0, 1], 'health must be 0 or 1'
                assert node_response['role'], 'role field cannot be empty'
                assert node_response['help'], 'help field cannot be empty'


@pytest.mark.supportedwindows
def test_dcos_diagnostics_report(dcos_api_session):
    """
    test dcos-diagnostics report endpoint /system/health/v1/report
    """
    for main in dcos_api_session.mains:
        report_response = check_json(dcos_api_session.health.get('/report', node=main))
        assert 'Units' in report_response
        assert len(report_response['Units']) > 0

        assert 'Nodes' in report_response
        assert len(report_response['Nodes']) > 0


@pytest.mark.parametrize('use_legacy_api', [False, True])
def test_dcos_diagnostics_bundle_create_download_delete(dcos_api_session, use_legacy_api):
    """
    test bundle create, read, delete workflow
    """

    health_url = dcos_api_session.default_url.copy(
        query='cache=0',
        path='system/health/v1',
    )

    diagnostics = Diagnostics(
        default_url=health_url,
        mains=dcos_api_session.mains,
        all_subordinates=dcos_api_session.all_subordinates,
        session=dcos_api_session.copy().session,
        use_legacy_api=use_legacy_api,
    )

    app, test_uuid = test_helpers.marathon_test_app()
    with dcos_api_session.marathon.deploy_and_cleanup(app):
        bundle = _create_bundle(diagnostics)
        _check_diagnostics_bundle_status(dcos_api_session)
        _download_and_extract_bundle(dcos_api_session, bundle, diagnostics)
        _download_and_extract_bundle_from_another_main(dcos_api_session, bundle, diagnostics)
        _delete_bundle(diagnostics, bundle)


def _check_diagnostics_bundle_status(dcos_api_session):
    # validate diagnostics job status response
    diagnostics_bundle_status = check_json(dcos_api_session.health.get('/report/diagnostics/status/all'))
    required_status_fields = ['is_running', 'status', 'last_bundle_dir', 'job_started',
                              'diagnostics_bundle_dir', 'diagnostics_job_timeout_min',
                              'journald_logs_since_hours', 'diagnostics_job_get_since_url_timeout_min',
                              'command_exec_timeout_sec', 'diagnostics_partition_disk_usage_percent',
                              'job_progress_percentage']

    for _, properties in diagnostics_bundle_status.items():
        for required_status_field in required_status_fields:
            assert required_status_field in properties, 'property {} not found'.format(required_status_field)


def _create_bundle(diagnostics: Diagnostics):
    last_datapoint = {
        'time': None,
        'value': 0
    }

    create_response = diagnostics.start_diagnostics_job().json()
    diagnostics.wait_for_diagnostics_job(last_datapoint=last_datapoint)
    diagnostics.wait_for_diagnostics_reports()
    bundles = diagnostics.get_diagnostics_reports()
    assert len(bundles) > 0, 'bundle file not found'

    bundle_name = create_response.get('id')
    if not bundle_name:
        bundle_name = create_response['extra']['bundle_name']
    assert bundle_name in bundles

    return bundle_name


def _delete_bundle(diagnostics: Diagnostics, bundle):
    bundles = diagnostics.get_diagnostics_reports()
    assert bundle in bundles, 'not found {} in {}'.format(bundle, bundles)

    diagnostics.delete_bundle(bundle)

    bundles = diagnostics.get_diagnostics_reports()
    assert bundle not in bundles, 'found {} in {}'.format(bundle, bundles)


def _download_and_extract_bundle(dcos_api_session, bundle, diagnostics):
    _download_bundle_from_main(dcos_api_session, 0, bundle, diagnostics)


def _download_and_extract_bundle_from_another_main(dcos_api_session, bundle, diagnostics,):
    if len(dcos_api_session.mains) > 1:
        _download_bundle_from_main(dcos_api_session, 1, bundle, diagnostics)


def _download_bundle_from_main(dcos_api_session, main_index, bundle, diagnostics):
    """ Download DC/OS diagnostics bundle from a main

    :param dcos_api_session: dcos_api_session fixture
    :param main_index: main index from dcos_api_session.mains array
    :param bundle: bundle name to download from main
    :param diagnostics: DCOS Diagnostics client
    """
    bundles = diagnostics.get_diagnostics_reports()
    assert bundle in bundles, 'not found {} in {}'.format(bundle, bundles)

    expected_common_files = ['dmesg_-T.output',
                             'ip_addr.output',
                             'ip_route.output',
                             'ps_aux_ww_Z.output',
                             'optmesospherebincurl_-s_-S_http:localhost:62080v1vips.output',
                             'optmesospherebincurl_-s_-S_http:localhost:62080v1records.output',
                             'optmesospherebincurl_-s_-S_http:localhost:62080v1metricsdefault.output',
                             'optmesospherebincurl_-s_-S_http:localhost:62080v1metricsdns.output',
                             'optmesospherebincurl_-s_-S_http:localhost:62080v1metricsmesos_listener.output',
                             'optmesospherebincurl_-s_-S_http:localhost:62080v1metricslashup.output',
                             'timedatectl.output',
                             'binsh_-c_cat etc*-release.output',
                             'systemctl_list-units_dcos*.output',
                             'sestatus.output',
                             'iptables-save.output',
                             'ip6tables-save.output',
                             'ipset_list.output',
                             'opt/mesosphere/active.buildinfo.full.json',
                             'opt/mesosphere/etc/dcos-version.json',
                             'opt/mesosphere/etc/expanded.config.json',
                             'opt/mesosphere/etc/user.config.yaml',
                             'dcos-diagnostics-health.json',
                             'var/lib/dcos/cluster-id',
                             'proc/cmdline',
                             'proc/cpuinfo',
                             'proc/meminfo',
                             'proc/self/mountinfo',
                             'optmesospherebindetect_ip.output',
                             'sysctl_-a.output',
                             ]

    # these files are expected to be in archive for a main host
    expected_main_files = [
        'binsh_-c_cat proc`systemctl show dcos-mesos-main.service -p MainPID| cut -d\'=\' -f2`environ.output',
        '5050-quota.json',
        '5050-overlay-main_state.json',
        'dcos-mesos-main.service',
        'var/lib/dcos/exhibitor/zookeeper/snapshot/myid',
        'var/lib/dcos/exhibitor/conf/zoo.cfg',
        'var/lib/dcos/mesos/log/mesos-main.log',
        'var/lib/dcos/mesos/log/mesos-main.log.1',
        'var/lib/dcos/mesos/log/mesos-main.log.2.gz',
        'var/lib/dcos/mesos/log/mesos-main.log.3.gz',
    ] + expected_common_files

    expected_agent_common_files = [
        '5051-containers.json',
        '5051-containerizer_debug.json',
        '5051-overlay-agent_overlay.json',
        'var/log/mesos/mesos-agent.log',
        'docker_--version.output',
        'docker_ps.output',
    ]

    # for agent host
    expected_agent_files = [
        'dcos-mesos-subordinate.service',
        'binsh_-c_cat proc`systemctl show dcos-mesos-subordinate.service -p MainPID| cut -d\'=\' -f2`environ.output'
    ] + expected_agent_common_files + expected_common_files

    # for public agent host
    expected_public_agent_files = [
        'dcos-mesos-subordinate-public.service',
        'binsh_-c_cat proc`systemctl show dcos-mesos-subordinate-public.service -p MainPID| cut -d\'=\' -f2`environ.output'
    ] + expected_agent_common_files + expected_common_files

    def _read_from_zip(z: zipfile.ZipFile, item: str, to_json=True):
        # raises KeyError if item is not in zipfile.
        item_content = z.read(item).decode()

        if to_json:
            # raises ValueError if cannot deserialize item_content.
            return json.loads(item_content)

        return item_content

    def _get_dcos_diagnostics_health(z: zipfile.ZipFile, item: str):
        # try to load dcos-diagnostics health report and validate the report is for this host
        try:
            _health_report = _read_from_zip(z, item)
        except KeyError:
            # we did not find a key in archive, let's take a look at items in archive and try to read
            # diagnostics logs.

            # namelist() gets a list of all items in a zip archive.
            logging.info(z.namelist())

            # summaryErrorsReport.txt is diagnostic job log files.
            log = 'summaryErrorsReport.txt'
            try:
                log_data = _read_from_zip(z, log, to_json=False)
                logging.info("{}:\n{}".format(log, log_data))
            except KeyError:
                logging.info("Could not read {}".format(log))
            raise

        except ValueError:
            logging.info("Could not deserialize dcos-diagnostics-health")
            raise

        return _health_report

    with tempfile.TemporaryDirectory() as tmp_dir:
        bundle_full_location = os.path.join(tmp_dir, bundle)
        diagnostics.download_diagnostics_reports([bundle], tmp_dir, dcos_api_session.mains[main_index])

        # validate bundle zip file.
        assert zipfile.is_zipfile(bundle_full_location)
        z = zipfile.ZipFile(bundle_full_location)

        # get a list of all files in a zip archive.
        archived_items = z.namelist()

        # validate error log is empty
        if 'summaryErrorsReport.txt' in archived_items:
            log_data = _read_from_zip(z, 'summaryErrorsReport.txt', to_json=False)
            raise AssertionError('summaryErrorsReport.txt must be empty. Got {}'.format(log_data))

        # make sure all required log files for main node are in place.
        for main_ip in dcos_api_session.mains:
            main_folder = main_ip + '_main/'

            # try to load dcos-diagnostics health report and validate the report is for this host
            health_report = _get_dcos_diagnostics_health(z, main_folder + 'dcos-diagnostics-health.json')
            assert 'ip' in health_report
            assert health_report['ip'] == main_ip

            # make sure systemd unit output is correct and does not contain error message
            unit_output = get_file_content(main_folder + 'dcos-mesos-main.service', z)
            verify_unit_response(unit_output, 100)

            verify_archived_items(main_folder, archived_items, expected_main_files)

            state_output = get_file_content(main_folder + '5050-main_state.json', z)
            validate_state(state_output)

        # make sure all required log files for agent node are in place.
        for subordinate_ip in dcos_api_session.subordinates:
            agent_folder = subordinate_ip + '_agent/'

            # try to load dcos-diagnostics health report and validate the report is for this host
            health_report = _get_dcos_diagnostics_health(z, agent_folder + 'dcos-diagnostics-health.json')
            assert 'ip' in health_report
            assert health_report['ip'] == subordinate_ip

            # make sure systemd unit output is correct and does not contain error message
            unit_output = get_file_content(agent_folder + 'dcos-mesos-subordinate.service', z)
            verify_unit_response(unit_output, 100)

            verify_archived_items(agent_folder, archived_items, expected_agent_files)

        # make sure all required log files for public agent node are in place.
        for public_subordinate_ip in dcos_api_session.public_subordinates:
            agent_public_folder = public_subordinate_ip + '_agent_public/'

            # try to load dcos-diagnostics health report and validate the report is for this host
            health_report = _get_dcos_diagnostics_health(z, agent_public_folder + 'dcos-diagnostics-health.json')
            assert 'ip' in health_report
            assert health_report['ip'] == public_subordinate_ip

            # make sure systemd unit output is correct and does not contain error message
            unit_output = get_file_content(agent_public_folder + 'dcos-mesos-subordinate-public.service', z)
            verify_unit_response(unit_output, 100)

            verify_archived_items(agent_public_folder, archived_items, expected_public_agent_files)


def get_file_content(unzipped_file, z):
    archived_items = z.namelist()
    if unzipped_file in archived_items:
        return z.open(unzipped_file).read()
    expected_gzipped_file = (unzipped_file + '.gz')
    if expected_gzipped_file in archived_items:
        gzipped_state_output = z.open(expected_gzipped_file)
        return gzip.decompress(gzipped_state_output.read())
    raise AssertionError("Not found {} nor {} in {}".format(unzipped_file, expected_gzipped_file, archived_items))


def make_nodes_ip_map(dcos_api_session):
    """
    a helper function to make a map detected_ip -> external_ip
    """
    node_private_public_ip_map = {}
    for node in dcos_api_session.mains:
        detected_ip = check_json(dcos_api_session.health.get('/', node=node))['ip']
        node_private_public_ip_map[detected_ip] = node

    for node in dcos_api_session.all_subordinates:
        detected_ip = check_json(dcos_api_session.health.get('/', node=node))['ip']
        node_private_public_ip_map[detected_ip] = node

    return node_private_public_ip_map


def validate_node(nodes):
    assert isinstance(nodes, list), 'input argument must be a list'
    assert len(nodes) > 0, 'input argument cannot be empty'
    required_fields = ['host_ip', 'health', 'role']

    for node in nodes:
        assert len(node) == len(required_fields), 'node should have the following fields: {}. Actual: {}'.format(
            ', '.join(required_fields), node)
        for required_field in required_fields:
            assert required_field in node, '{} must be in node. Actual: {}'.format(required_field, node)

        # host_ip, health, role fields cannot be empty
        assert node['health'] in [0, 1], 'health must be 0 or 1'
        assert node['host_ip'], 'host_ip cannot be empty'
        assert node['role'], 'role cannot be empty'


def validate_units(units):
    assert isinstance(units, list), 'input argument must be list'
    assert len(units) > 0, 'input argument cannot be empty'
    required_fields = ['id', 'name', 'health', 'description']

    for unit in units:
        assert len(unit) == len(required_fields), 'a unit must have the following fields: {}. Actual: {}'.format(
            ', '.join(required_fields), unit)
        for required_field in required_fields:
            assert required_field in unit, 'unit response must have field: {}. Actual: {}'.format(required_field, unit)

        # a unit must have all 3 fields not empty
        assert unit['id'], 'id field cannot be empty'
        assert unit['name'], 'name field cannot be empty'
        assert unit['health'] in [0, 1], 'health must be 0 or 1'
        assert unit['description'], 'description field cannot be empty'


def validate_unit(unit):
    assert isinstance(unit, dict), 'input argument must be a dict'

    required_fields = ['id', 'health', 'output', 'description', 'help', 'name']
    assert len(unit) == len(required_fields), 'unit must have the following fields: {}. Actual: {}'.format(
        ', '.join(required_fields), unit)
    for required_field in required_fields:
        assert required_field in unit, '{} must be in a unit. Actual: {}'.format(required_field, unit)

    # id, name, health, description, help should not be empty
    assert unit['id'], 'id field cannot be empty'
    assert unit['name'], 'name field cannot be empty'
    assert unit['health'] in [0, 1], 'health must be 0 or 1'
    assert unit['description'], 'description field cannot be empty'
    assert unit['help'], 'help field cannot be empty'


def validate_state(state_output):
    state = json.loads(state_output)
    assert len(state["frameworks"]) > 1, "bundle must contain information about frameworks"

    task_count = sum([len(f["tasks"]) for f in state["frameworks"]])
    assert task_count > 0, "bundle must contains information about tasks"


def verify_archived_items(folder, archived_items, expected_files):
    for expected_file in expected_files:
        expected_file = folder + expected_file

        # We don't know in advance whether the file will be gzipped or not,
        # because that depends on the size of the diagnostics file, which can
        # be influenced by multiple factors that are not under our control
        # here.
        # Since we only want to check whether the file _exists_ and don't care
        # about whether it's gzipped or not, we check for an optional `.gz`
        # file type in case it wasn't explicitly specified in the assertion.
        # For more context, see: https://jira.mesosphere.com/browse/DCOS_OSS-4531
        expected_gzipped_file = (expected_file + '.gz')
        unzipped_exists = expected_file in archived_items
        gzipped_exists = expected_gzipped_file in archived_items

        message = ('expecting {} or {} in {}'.format(expected_file, expected_gzipped_file, archived_items))
        assert (unzipped_exists or gzipped_exists), message


def verify_unit_response(unit_output, min_lines):
    assert len(unit_output.decode().split('\n')) >= min_lines, 'Expect at least {} lines. Full unit output {}'.format(
        min_lines, unit_output)

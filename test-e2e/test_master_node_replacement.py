"""
Tests for replacing main nodes.
"""

import textwrap
import uuid
from pathlib import Path
from typing import Iterator

import docker
import pytest
from _pytest.fixtures import SubRequest
from cluster_helpers import wait_for_dcos_oss
from dcos_e2e.backends import Docker
from dcos_e2e.cluster import Cluster
from dcos_e2e.node import Output, Role
from docker.models.networks import Network


@pytest.fixture()
def docker_network_three_available_addresses() -> Iterator[Network]:
    """
    Return a custom Docker network with 3 assignable IP addresses.
    """
    # We want to return a Docker network with only three assignable IP
    # addresses.
    # To do this, we create a network with 8 IP addresses, where 5 are
    # reserved.
    #
    # Why we have 8 IP addresses available:
    # * The IP range is "172.28.0.0/29"
    # * We get 2 ^ (32 - 29) = 8 IP addresses
    #
    # The 8 IP addresses in the IPAM Pool are:
    # * 172.28.0.0 (reserved because this is the subnet identifier)
    # * 172.28.0.1 (reserved because this is the gateway address)
    # * 172.28.0.2 (available)
    # * 172.28.0.3 (available)
    # * 172.28.0.4 (available)
    # * 172.28.0.5 (reserved because we reserve this with `aux_addresses`)
    # * 172.28.0.6 (reserved because we reserve this with `aux_addresses`)
    # * 172.28.0.7 (reserved because this is the broadcast address)
    client = docker.from_env(version='auto')
    aux_addresses = {
        'reserved_address_0': '172.28.0.5',
        'reserved_address_1': '172.28.0.6',
    }
    ipam_pool = docker.types.IPAMPool(
        subnet='172.28.0.0/29',
        iprange='172.28.0.0/29',
        gateway='172.28.0.1',
        aux_addresses=aux_addresses,
    )
    network = client.networks.create(
        name='dcos-e2e-network-{random}'.format(random=uuid.uuid4()),
        driver='bridge',
        ipam=docker.types.IPAMConfig(pool_configs=[ipam_pool]),
        attachable=False,
    )
    try:
        yield network
    finally:
        network.remove()


def test_replace_all_static(
    artifact_path: Path,
    docker_network_three_available_addresses: Network,
    tmp_path: Path,
    request: SubRequest,
    log_dir: Path,
) -> None:
    """
    In a cluster with an Exhibitor backend consisting of a static ZooKeeper
    ensemble, after removing one main, and then adding another main with
    the same IP address, the cluster will get to a healthy state. This is
    repeated until all mains in the original cluster have been replaced.
    The purpose of this test is to assert that the ``node-poststart``
    procedure correctly prevents a main node replacement from being performed
    too quickly. A new main node should only become part of the cluster if
    there are no more underreplicated ranges reported by CockroachDB.

    Permanent CockroachDB data loss and a potential breakage of DC/OS occurs
    when a second main node is taken down for replacement while CockroachDB
    is recovering and there are still underreplicated ranges due to a recent
    other main node replacement.
    """
    docker_backend = Docker(network=docker_network_three_available_addresses)

    with Cluster(
        cluster_backend=docker_backend,
        # Allocate all 3 available IP addresses in the subnet.
        mains=3,
        agents=0,
        public_agents=0,
    ) as original_cluster:
        main = next(iter(original_cluster.mains))
        result = main.run(
            args=[
                'ifconfig',
                '|', 'grep', '-B1', str(main.public_ip_address),
                '|', 'grep', '-o', '"^\w*"',
            ],
            output=Output.LOG_AND_CAPTURE,
            shell=True,
        )
        interface = result.stdout.strip().decode()
        ip_detect_contents = textwrap.dedent(
            """\
            #!/bin/bash -e
            if [ -f /sbin/ip ]; then
               IP_CMD=/sbin/ip
            else
               IP_CMD=/bin/ip
            fi

            $IP_CMD -4 -o addr show dev {interface} | awk '{{split($4,a,"/");print a[1]}}'
            """.format(interface=interface),
        )
        ip_detect_path = tmp_path / 'ip-detect'
        ip_detect_path.write_text(data=ip_detect_contents)
        static_config = {
            'main_discovery': 'static',
            'main_list': [str(main.private_ip_address)
                            for main in original_cluster.mains],
        }
        dcos_config = {
            **original_cluster.base_config,
            **static_config,
        }
        original_cluster.install_dcos_from_path(
            dcos_installer=artifact_path,
            dcos_config=dcos_config,
            ip_detect_path=ip_detect_path,
        )
        wait_for_dcos_oss(
            cluster=original_cluster,
            request=request,
            log_dir=log_dir,
        )
        current_cluster = original_cluster
        tmp_clusters = set()

        original_mains = original_cluster.mains

        try:
            for main_to_be_replaced in original_mains:
                # Destroy a main and free one IP address.
                original_cluster.destroy_node(node=main_to_be_replaced)

                temporary_cluster = Cluster(
                    cluster_backend=docker_backend,
                    # Allocate one container with the now free IP address.
                    mains=1,
                    agents=0,
                    public_agents=0,
                )
                tmp_clusters.add(temporary_cluster)

                # Install a new main on a new container with the same IP address.
                (new_main, ) = temporary_cluster.mains
                new_main.install_dcos_from_path(
                    dcos_installer=artifact_path,
                    dcos_config=dcos_config,
                    role=Role.MASTER,
                    ip_detect_path=ip_detect_path,
                )
                # Form a new cluster with the newly create main node.
                new_cluster = Cluster.from_nodes(
                    mains=current_cluster.mains.union({new_main}),
                    agents=current_cluster.agents,
                    public_agents=current_cluster.public_agents,
                )
                # The `wait_for_dcos_oss` function waits until the new main has
                # joined the cluster and all mains are healthy. Without the
                # cockroachdb check, this succeeds before all cockroachdb ranges
                # have finished replicating to the new main. That meant that the
                # next main would be replaced too quickly, while it had data that
                # was not present elsewhere in the cluster. This lead to
                # irrecoverable dataloss.  This function waits until the
                # main node is "healthy". This is a requirement for replacing the
                # next main node.
                #
                # We don't call the cockroachdb ranges check directly as the
                # purpose of this test is to ensure that when an operator follows
                # our documented procedure for replacing a main node multiple
                # times in a row (e.g. during a cluster upgrade) then the cluster
                # remains healthy throughout and afterwards.
                #
                # If we called the check directly here, we would be
                # sure the check is being called, but we would not be sure that
                # "wait_for_dcos_oss", i.e., the standard procedure for determining
                # whether a node is healthy, is sufficient to prevent the cluster
                # from breaking.
                #
                # We perform this check after every main is replaced, as that is
                # what we tell operators to do: "After installing the new main
                # node, wait until it becomes healthy before proceeding to the
                # next."
                #
                # The procedure for replacing multiple mains is documented here:
                # https://docs.mesosphere.com/1.12/installing/production/upgrading/#dcos-masters
                wait_for_dcos_oss(
                    cluster=new_cluster,
                    request=request,
                    log_dir=log_dir,
                )
                # Use the new cluster object in the next replacement iteration.
                current_cluster = new_cluster

        finally:
            for cluster in tmp_clusters:
                cluster.destroy()

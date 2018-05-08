from dtest import Tester, create_ks
from tools.jmxutils import JolokiaAgent, remove_perf_disable_shared_mem, make_mbean
from time import sleep
from cassandra.policies import ConstantReconnectionPolicy
import pytest

since = pytest.mark.since

def setup(obj):
    cluster = obj.cluster
    cluster.populate(2)

    node = cluster.nodelist()[0]
    remove_perf_disable_shared_mem(node)

    cluster.start(wait_for_binary_proto=True)
    session = obj.patient_cql_connection(
        node,
        retry_policy=ConstantReconnectionPolicy(0, max_attempts=None)
    )
    setupSchema(session)

    return (session, node)

def setupSchema(session):
    create_ks(session, 'ks', 1)
    result = session.execute("""
                             CREATE TABLE test (
                                 id int,
                                 val varchar,
                                 PRIMARY KEY (id)
                             );""")

def _get_mbean():
    return make_mbean('db', type='Connection')

def ban_host(host, node):
    mbean = _get_mbean()
    with JolokiaAgent(node) as jmx:
        jmx.execute_method(
            mbean,
            'banHostname',
            arguments=[host]
        )
        return jmx.read_attribute(mbean, 'BannedHostnames')

def permit_host(host, node):
    mbean = _get_mbean()
    with JolokiaAgent(node) as jmx:
        jmx.execute_method(
            mbean,
            'permitHostname',
            arguments=[host]
        )
        return jmx.read_attribute(mbean, 'BannedHostnames')


class TestClientConnection(Tester):

    @since('4.0')
    def test_client_connection(self):
        """
        @jira_ticket CASSANDRA-10789

        Test:
            1 - JMX 'banHostname' method adds a hostname to the blacklist and
            any existing connections to the hostname are closed.
            2 - JMX 'permitHostname' method removes hostname from blacklist
            and that client driver connections can reconnect to the cluster
            once removed.
        """

        session, node = setup(self)
        host = session.cluster.metadata.get_host(node.ip_addr)
        assert host.is_up

        banned_hosts = ban_host('localhost', node)

        # Force an exchange to mark the node as down.
        for i in range(0, 3):
            session.execute(
                "INSERT INTO ks.test (id, val) VALUES ({}, 'aaaa');".format(i)
            )

        assert not host.is_up
        assert len(banned_hosts) == 1 and node.ip_addr in banned_hosts[0]

        banned_hosts = permit_host('127.0.0.1', node)

        # Wait for client driver's reconnection policy to reconnect with the
        # node:
        sleep(1)

        assert host.is_up
        assert len(banned_hosts) == 0


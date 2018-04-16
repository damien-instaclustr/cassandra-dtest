from dtest import Tester, create_ks, create_cf
from tools.jmxutils import JolokiaAgent, remove_perf_disable_shared_mem, make_mbean


class ClientRequestMetricContainer():

    def __init__(self, node, scope):
        self.node = node
        self.scope = scope

        self.local_requests_mbean = make_mbean(
            'metrics',
            type='ClientRequest',
            scope=self.scope,
            name='LocalRequests'
        )

        self.remote_requests_mbean = make_mbean(
            'metrics',
            type='ClientRequest',
            scope=self.scope,
            name='RemoteRequests'
        )

        self.current_local_requests = self.getLocalRequests()
        self.current_remote_requests= self.getRemoteRequests()

    def getLocalRequests(self):
        with JolokiaAgent(self.node) as jmx:
            return jmx.read_attribute(self.local_requests_mbean, 'Count')

    def getRemoteRequests(self):
        with JolokiaAgent(self.node) as jmx:
            return jmx.read_attribute(self.remote_requests_mbean, 'Count')

    def compareLocalRequests(self):
        return self.getLocalRequests() - self.current_local_requests

    def compareRemoteRequests(self):
        return self.getRemoteRequests() - self.current_remote_requests


class TokenDistributor():
    """
    Assumes static tokens with num_tokens == 1
    """

    def __init__(self, nodes_list):
        self.nodes = nodes_list

    def get_node_from_token(self, token):
        first = None
        current = None
        for node in self.nodes:
            diff = node.initial_token - token
            if diff > 0:
                if current is None:
                    current = node
                else:
                    current_diff = current.inital_token - token
                    current = node if diff < current_diff else current

            if first is None:
                first = node
            else:
                first = node if node.initial_token < first.initial_token else first

        if current is None:
            return first
        else:
            return current

class TestLocalRemoteRequests(Tester):

    def test_read_and_write(self):
        cluster  = self.cluster
        cluster.populate(2)
        test_node = cluster.nodelist()[0]
        remove_perf_disable_shared_mem(test_node)
        cluster.start(wait_for_binary_proto=True)
        session = self.patient_exclusive_cql_connection(test_node)

        create_ks(session, 'ks', 1)
        session.execute("""
                        CREATE TABLE test (
                            id int,
                            ord int,
                            val varchar,
                            PRIMARY KEY (id, ord)
                        );""")

        token_dist = TokenDistributor(cluster.nodelist())

        # Get initial values:
        read_metrics = ClientRequestMetricContainer(test_node, 'Read')
        write_metrics = ClientRequestMetricContainer(test_node, 'Write')

        # Run test:
        num_read_writes = 1000
        for i in range(0, num_read_writes):
            session.execute("""
                            INSERT INTO ks.test (id, ord, val)
                            VALUES ({}, 1, 'aaaa');
                            """.format(i))
            session.execute("""
                            SELECT id, ord, val
                            FROM ks.test
                            WHERE id={};
                            """.format(i))

        # Collect results:
        metric_results = {
            'local_reads': read_metrics.compareLocalRequests(),
            'remote_reads': read_metrics.compareRemoteRequests(),
            'local_writes': write_metrics.compareLocalRequests(),
            'remote_writes':  write_metrics.compareRemoteRequests()
        }

        # Get expected results:
        local_count = 0
        remote_count = 0

        for i in range(0, num_read_writes):
            result = session.execute("""
                                     SELECT token(id) from ks.test
                                     WHERE id={};
                                     """.format(i))
            for r in result:
                node = token_dist.get_node_from_token(r.system_token_id)
                if node == test_node:
                    local_count = local_count + 1
                else:
                    remote_count = remote_count + 1

        # Compared expected against the collected results:
        assert local_count == metric_results['local_writes']
        assert remote_count == metric_results['remote_writes']
        assert local_count == metric_results['local_reads']
        assert remote_count == metric_results['remote_reads']


import pytest
from dtest import Tester, get_ip_from_node, create_ks
from tools.misc import new_node
from dtest_setup_overrides import DTestSetupOverrides
import time
import threading

since = pytest.mark.since

error_data = None
lock = threading.Lock()

def _log_error_handler(errordata):
    global error_data

    with lock:
        error_data = errordata

KEYSPACE_NAME = 'test_allocate_token_ks'

@since('3.0')
class TestAllocateTokensForKeyspace(Tester):
    """
    Test cases where a valid and invalid keyspace name is given to allocate_tokens_for_keyspace
    @jira_ticket CASSANDRA-12757
    """
    def test_valid_keyspace(self):
        cluster = self.cluster
        cluster.populate(2)
        cluster.start(wait_for_binary_proto=True)

        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        session.execute("CREATE KEYSPACE {} WITH replication = {{'class':'SimpleStrategy', 'replication_factor': 3 }}".format(KEYSPACE_NAME))

        node3 = new_node(cluster)
        node3.start(
            wait_for_binary_proto=True,
            jvm_args=["-Dcassandra.allocate_tokens_for_keyspace={}".format(KEYSPACE_NAME)]
        )

    def test_invalid_keyspace(self):

        self.fixture_dtest_setup.allow_log_errors = True

        cluster = self.cluster
        cluster.populate(2)
        cluster.start(wait_for_binary_proto=True)

        _log_watch_thread = cluster.actively_watch_logs_for_error(_log_error_handler, interval=0.25)

        node3 = new_node(cluster)
        node3.start(
            jvm_args=["-Dcassandra.allocate_tokens_for_keyspace={}".format(KEYSPACE_NAME)]
        )

        for i in range(0, 60):
            with lock:
                if error_data is not None:
                    assert(len(error_data) == 1)
                    node, messages = list(error_data.items())[0]
                    assert(node == 'node3')
                    for m in messages:
                        if 'org.apache.cassandra.exceptions.ConfigurationException: Problem opening token allocation keyspace {}'.format(KEYSPACE_NAME) in m:
                            break
                    else:
                        assert(False)
                    break
            time.sleep(1)
        else:
            assert(False)


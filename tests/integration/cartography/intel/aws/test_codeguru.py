import cartography.intel.aws.codeguru
import tests.data.aws.codeguru

TEST_ACCOUNT_ID = '000000000000'
TEST_REGION = 'eu-west-1'
TEST_UPDATE_TAG = 123456789


def test_load_repository_associations(neo4j_session):
    data = tests.data.aws.codeguru.GET_REPOSITORY_ASSOCIATIONS
    cartography.intel.aws.codeguru.load_repository_associations(
        neo4j_session,
        data,
        TEST_REGION,
        TEST_ACCOUNT_ID,
        TEST_UPDATE_TAG,
    )

    expected_nodes = {
        "test-001",
        "test-002",
    }

    nodes = neo4j_session.run(
        """
        MATCH (r:CodeguruAssociation) RETURN r.owner;
        """,
    )
    actual_nodes = {n['r.owner'] for n in nodes}

    assert actual_nodes == expected_nodes

def test_load_repository_associations_relationships(neo4j_session):
    # Create Test AWSAccount
    neo4j_session.run(
        """
        MERGE (aws:AWSAccount{id: {aws_account_id}})
        ON CREATE SET aws.firstseen = timestamp()
        SET aws.lastupdated = {aws_update_tag}
        """,
        aws_account_id=TEST_ACCOUNT_ID,
        aws_update_tag=TEST_UPDATE_TAG,
    )

    # Load Test API Gateway REST APIs
    data = tests.data.aws.codeguru.GET_REPOSITORY_ASSOCIATIONS
    cartography.intel.aws.codeguru.load_repository_associations(
        neo4j_session,
        data,
        TEST_REGION,
        TEST_ACCOUNT_ID,
        TEST_UPDATE_TAG,
    )
    expected = {
        (TEST_ACCOUNT_ID, 'test-001'),
        (TEST_ACCOUNT_ID, 'test-002'),
    }

    # Fetch relationships
    result = neo4j_session.run(
        """
        MATCH (n1:AWSAccount)-[:RESOURCE]->(n2:CodeguruAssociation) RETURN n1.id, n2.owner;
        """,
    )
    actual = {
        (r['n1.id'], r['n2.owner']) for r in result
    }

    assert actual == expected

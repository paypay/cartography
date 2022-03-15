import logging

import neo4j

from cartography.config import Config
from cartography.intel.crowdstrike.endpoints import sync_hosts
from cartography.intel.crowdstrike.spotlight import sync_vulnerabilities
from cartography.intel.crowdstrike.util import get_authorization
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
def start_crowdstrike_ingestion(
    neo4j_session: neo4j.Session, config: Config,
) -> None:
    """
    Perform ingestion of crowdstrike data.
    :param neo4j_session: Neo4J session for database interface
    :param config: A cartography.config object
    :return: None
    """
    common_job_parameters = {
        "UPDATE_TAG": config.update_tag,
    }
    if (
        not config.crowdstrike_client_id or
        not config.crowdstrike_client_secret
    ):
        logger.info("crowdstrike config not found")
        return
    crowdstrike_api_url = config.crowdstrike_api_url
    if crowdstrike_api_url is None:
        crowdstrike_api_url = "https://api.crowdstrike.com"

    authorization = get_authorization(
        config.crowdstrike_client_id,
        config.crowdstrike_client_secret,
        crowdstrike_api_url,
    )
    sync_hosts(
        neo4j_session,
        config.update_tag,
        authorization,
    )
    sync_vulnerabilities(
        neo4j_session,
        config.update_tag,
        authorization,
    )
    run_cleanup_job(
        "crowdstrike_import_cleanup.json",
        neo4j_session,
        common_job_parameters,
    )

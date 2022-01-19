import json
import logging
from typing import Any
from typing import Dict
from typing import List

import boto3
import neo4j
from botocore.exceptions import ClientError

from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
@aws_handle_regions
def get_repository_associations(boto3_session: boto3.session.Session, region: str) -> List[str]:
    client = boto3_session.client('codeguru-reviewer', region_name=region)
    paginator = client.get_paginator('list_repository_associations')
    associations: List[Any] = []
    for page in paginator.paginate():
        associations.extend(page.get('RepositoryAssociationSummaries', []))

    return associations

@timeit
def get_association_name(associations: List[str]) -> List[str]:
    name = []
    for assoc in associations:
        name.append(assoc['Name'])

    return name

@timeit
@aws_handle_regions
def get_code_reviews(boto3_session: boto3.session.Session,region: str,associations: List[str]) -> List[str]:
    client = boto3_session.client('codeguru-reviewer', region_name=region)
    response = client.list_code_reviews(States=['Completed'],Type='PullRequest',RepositoryNames=associations)

    return response

#def get_recommendations(boto3_session: boto3.session.Session,region: str,reviews: List[str]) -> List[str]:

@timeit
def sync(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str], current_aws_account_id: str,
    update_tag: int, common_job_parameters: Dict,
) -> None:
    for region in regions:
        if region == 'ap-northeast-1':
            logger.info("Syncing CodeGuru for region '%s' in account '%s'.", region, current_aws_account_id)
            associations = get_repository_associations(boto3_session, region)
            if len(associations) == 0:
                continue
            logger.info(associations)
            association_name = get_association_name(associations)
            code_reviews = get_code_reviews(boto3_session, region, association_name)
import boto3
from aws_lambda_typing.events import S3Event
from aws_lambda_typing.context import Context

s3_client = boto3.client('s3')

from catalyst_ngd_wrappers.ngd_api_wrappers import get_latest_collection_versions, \
    get_specific_latest_collections, items, items_limit, items_geom, items_col, \
    items_limit_geom, items_limit_col, items_geom_col, items_limit_geom_col

from schemas import LatestCollectionsSchema, CatalystBaseSchema, LimitSchema, GeomSchema, \
    ColSchema, LimitGeomSchema, LimitColSchema, GeomColSchema, LimitGeomColSchema
from utils import remove_query_params, handle_error, construct_response


class AWSSerialisedRequest:
    '''
    A class to represent an HTTP request with its parameters and headers.
    '''

    def __init__(self, event: dict) -> None:
        self.method = event.get('http').get('method')
        self.params = event.get('queryStringParameters', {})
        self.route_params = event.get('pathParameters')
        self.headers = event.get('headers', {})

def aws_serialise_response(data: dict) -> dict:

    code = data.get('code', 200)
    response = {
        "isBase64Encoded": False,
        "statusCode": code,
        "headers": data['headers'],
        "body": data
    }
    return response

def aws_process_request(event: dict, **kwargs) -> dict:
    try:
        data = AWSSerialisedRequest(event)
        response = construct_response(data = data, **kwargs)
        serialised_response = aws_serialise_response(response)
        return serialised_response
    except Exception as e:
        handle_error(error = e, code = 500)

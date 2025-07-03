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
        req_context = event.get('requestContext', {})
        self.url = req_context.get('domainName') + req_context.get('path')
        self.params = event.get('queryStringParameters', {})
        self.route_params = event.get('pathParameters')
        self.headers = event.get('headers', {})

def aws_serialise_response(data: dict) -> dict:

    code = data.pop('code', 200)
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
        bare_error = handle_error(error = e, code = 500)
        return aws_serialise_response(bare_error)


def http_latest_collections(event: dict, context: dict) -> dict:
    '''AWS Lambda function.
    Handles the processing of API requests to retrieve OS NGD collections, either all or a specific one.
    Handles parameter validation and telemetry tracking.'''
    response = aws_process_request(event = event)
    return response


def lambda_handler_base(event: dict, context: dict) -> dict:
    '''AWS Lambda function, OS NGD API - Features, No extensions applied.'''
    response = aws_process_request(
        event=event,
        schema_class=CatalystBaseSchema,
        ngd_api_func=items
    )
    return response


def lambda_handler_limit(event: dict, context: dict) -> dict:
    '''AWS Lambda function, OS NGD API - Features, Exensions applied:
        - Limit'''
    response = aws_process_request(
        event=event,
        schema_class=LimitSchema,
        ngd_api_func=items_limit
    )
    return response

def lambda_handler_geom(event: dict, context: dict) -> dict:
    '''AWS Lambda function, OS NGD API - Features, Exensions applied:
        - Geom'''
    response = aws_process_request(
        event=event,
        schema_class=GeomSchema,
        ngd_api_func=items_geom
    )
    return response

def lambda_handler_col(event: dict, context: dict) -> dict:
    '''AWS Lambda function, OS NGD API - Features, Exensions applied:
        - Col'''
    response = aws_process_request(
        event=event,
        schema_class=ColSchema,
        ngd_api_func=items_col
    )
    return response


def lambda_handler_limit_geom(event: dict, context: dict) -> dict:
    '''AWS Lambda function, OS NGD API - Features, Exensions applied:
        - Limit
        - Geom'''
    response = aws_process_request(
        event=event,
        schema_class=LimitGeomSchema,
        ngd_api_func=items_limit_geom
    )
    return response


def lambda_handler_limit_col(event: dict, context: dict) -> dict:
    '''AWS Lambda function, OS NGD API - Features, Exensions applied:
        - Limit
        - Col'''
    response = aws_process_request(
        event=event,
        schema_class=LimitColSchema,
        ngd_api_func=items_limit_col
    )
    return response


def lambda_handler_geom_col(event: dict, context: dict) -> dict:
    '''AWS Lambda function, OS NGD API - Features, Exensions applied:
        - Geom
        - Col'''
    response = aws_process_request(
        event=event,
        schema_class=GeomColSchema,
        ngd_api_func=items_geom_col
    )
    return response


def lambda_handler_limit_geom_col(event: dict, context: dict) -> dict:
    '''AWS Lambda function, OS NGD API - Features, Exensions applied:
        - Limit
        - Geom
        - Col'''
    response = aws_process_request(
        event=event,
        schema_class=LimitGeomColSchema,
        ngd_api_func=items_limit_geom_col
    )
    return response

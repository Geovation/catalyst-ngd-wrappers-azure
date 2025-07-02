import json

import boto3
from aws_lambda_typing.events import S3Event
from aws_lambda_typing.context import Context

s3_client = boto3.client('s3')

from marshmallow.exceptions import ValidationError

from catalyst_ngd_wrappers.ngd_api_wrappers import get_latest_collection_versions, \
    get_specific_latest_collections, items, items_limit, items_geom, items_col, \
    items_limit_geom, items_limit_col, items_geom_col, items_limit_geom_col

from schemas import LatestCollectionsSchema, CatalystBaseSchema, LimitSchema, GeomSchema, \
    ColSchema, LimitGeomSchema, LimitColSchema, GeomColSchema, LimitGeomColSchema
from utils import remove_query_params, handle_error


def construct_response(
    event: dict,
    schema_class: type,
    func_: callable
) -> HttpResponse:
    '''
    Translates the request headers and path and query parameters into a function call.
    Translates the function response into an HTTP response, handling errors and telemetry.
    '''

    try:
        # Handle incorrect HTTP methods
        if event.get('http').get('method') != 'GET':
            return handle_error(
                description = "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
                code = 405
            )

        # Load the schema and parse the request parameters
        schema = schema_class()
        multi_collection = isinstance(schema, ColSchema)

        params = event.get('queryStringParameters', {})
        if multi_collection:
            col = params.get('collection')
            if col:
                params['collection'] = col.split(',')

        try:
            parsed_params = schema.load(params)
        except ValidationError as e:
            return handle_error(e)

        custom_params = {
            k: parsed_params.pop(k)
            for k in schema.fields.keys()
            if k  in parsed_params
        }
        if not multi_collection:
            custom_params['collection'] = event.get('pathParameters').get('collection')

        headers = event.get('headers', {})
        data = func_(
            query_params=parsed_params,
            headers=headers,
            **custom_params
        )

        descr = data.get('description')
        if data.get('errorSource') and isinstance(descr, str):
            fields = [
                x.replace('_', '-')
                for x in schema.fields
                if x != 'limit'
            ]
            attributes = ', '.join(fields)
            data['description'] = descr.format(attr=attributes)

        #custom_dimensions = data.pop('telemetryData', None)
        #if custom_dimensions:
            #track_event('OS NGD API - Features', custom_dimensions=custom_dimensions)

        code = data.get('code', 200)
        response = {
            "isBase64Encoded": False,
            "statusCode": code,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": data
        }
        return response
    except Exception as e:
        handle_error(error = e, code = 500)
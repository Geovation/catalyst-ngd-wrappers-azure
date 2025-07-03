import json

import azure.functions as func

from azure.functions import HttpRequest, HttpResponse
from azure.monitor.events.extension import track_event
from azure.monitor.opentelemetry import configure_azure_monitor

from marshmallow.exceptions import ValidationError

from catalyst_ngd_wrappers.ngd_api_wrappers import get_latest_collection_versions, \
    get_specific_latest_collections, items, items_limit, items_geom, items_col, \
    items_limit_geom, items_limit_col, items_geom_col, items_limit_geom_col

from schemas import LatestCollectionsSchema, CatalystBaseSchema, LimitSchema, GeomSchema, \
    ColSchema, LimitGeomSchema, LimitColSchema, GeomColSchema, LimitGeomColSchema
from utils import remove_query_params, handle_error, construct_response

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

configure_azure_monitor()


@app.function_name('http_latest_single_col')
@app.route("catalyst/features/latest-collections/{collection?}")
def http_latest_collections(req: HttpRequest) -> HttpResponse:
    '''Handles the processing of API requests to retrieve OS NGD collections, either all or a specific one.
    Handles parameter validation and telemetry tracking.'''

    try:
        if req.method != 'GET':
            return handle_error(
                description = "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
                code = 405
            )

        schema = LatestCollectionsSchema()
        params = {**req.params}
        fail_condition1 = len(params) > 1
        fail_condition2 = len(params) == 1 and not params.get('recent-update-days')
        if fail_condition1 or fail_condition2:
            return handle_error(
                code = 400,
                description = "The only supported query parameter is 'recent-update-days'.",
            )

        collection = req.route_params.get('collection')
        try:
            parsed_params = schema.load(params)
        except ValidationError as e:
            return handle_error(e)

        custom_dimensions = {
            f'query_params.{str(k)}': str(v)
            for k, v in parsed_params.items()
        }
        custom_dimensions.pop('key', None)
        url = remove_query_params(req.url)
        custom_dimensions.update({
            'method': 'GET',
            'url.path': url,
        })
        if collection:
            data = get_specific_latest_collections([collection], **parsed_params)
            custom_dimensions['url.path_params.collection'] = collection
        else:
            data = get_latest_collection_versions(**parsed_params)

        json_data = json.dumps(data)

        track_event('HTTP_Request', custom_dimensions=custom_dimensions)

        return HttpResponse(
            body=json_data,
            mimetype="application/json"
        )
    except Exception as e:
        handle_error(error = e, code = 500)


class AzureSerialisedRequest:
    '''
    A class to represent an HTTP request with its parameters and headers.
    '''

    def __init__(self, req: HttpRequest) -> None:
        self.method = req.method
        self.params = {**req.params}
        self.route_params = req.route_params
        self.headers = req.headers.__dict__.get('__http_headers__', {})


def azure_serialise_response(data: dict) -> HttpResponse:

    code = data.get('code', 200)
    json_data = json.dumps(data)
    response = HttpResponse(
        body=json_data,
        mimetype="application/json",
        status_code=code
    )
    return response


def azure_process_request(req: HttpRequest, **kwargs) -> HttpResponse:
    try:
        data = AzureSerialisedRequest(req = req)
        response = construct_response(data = data, **kwargs)
        serialised_response = azure_serialise_response(response)
        return serialised_response
    except Exception as e:
        handle_error(error = e, code = 500)


@app.function_name('http_base')
@app.route("catalyst/features/{collection}/items")
def http_base(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, No extensions applied.'''
    response = azure_process_request(
        req=req,
        schema_class=CatalystBaseSchema,
        ngd_api_func=items
    )
    return response


@app.function_name('http_limit')
@app.route("catalyst/features/{collection}/items/limit")
def http_limit(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Limit'''
    response = azure_process_request(
        req=req,
        schema_class=LimitSchema,
        ngd_api_func=items_limit
    )
    return response


@app.function_name('http_geom')
@app.route("catalyst/features/{collection}/items/geom")
def http_geom(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Geom'''
    response = azure_process_request(
        req=req,
        schema_class=GeomSchema,
        ngd_api_func=items_geom
    )
    return response


@app.function_name('http_col')
@app.route("catalyst/features/multi-collection/items/col")
def http_col(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Col'''
    response = azure_process_request(
        req=req,
        schema_class=ColSchema,
        ngd_api_func=items_col
    )
    return response


@app.function_name('http_limit_geom')
@app.route("catalyst/features/{collection}/items/limit-geom")
def http_limit_geom(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Limit
        - Geom'''
    response = azure_process_request(
        req=req,
        schema_class=LimitGeomSchema,
        ngd_api_func=items_limit_geom
    )
    return response


@app.function_name('http_limit_col')
@app.route("catalyst/features/multi-collection/items/limit-col")
def http_limit_col(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Limit
        - Col'''
    response = azure_process_request(
        req=req,
        schema_class=LimitColSchema,
        ngd_api_func=items_limit_col
    )
    return response


@app.function_name('http_geom_col')
@app.route("catalyst/features/multi-collection/items/geom-col")
def http_geom_col(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Geom
        - Col'''
    response = azure_process_request(
        req=req,
        schema_class=GeomColSchema,
        ngd_api_func=items_geom_col
    )
    return response


@app.function_name('http_limit_geom_col')
@app.route("catalyst/features/multi-collection/items/limit-geom-col")
def http_limit_geom_col(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Limit
        - Geom
        - Col'''
    response = azure_process_request(
        req=req,
        schema_class=LimitGeomColSchema,
        ngd_api_func=items_limit_geom_col
    )
    return response

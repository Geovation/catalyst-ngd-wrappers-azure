import json

import azure.functions as func

from azure.functions import HttpRequest, HttpResponse
#from azure.monitor.events.extension import track_event
from azure.monitor.opentelemetry import configure_azure_monitor

from catalyst_ngd_wrappers.ngd_api_wrappers import items, items_limit, items_geom, \
    items_col, items_limit_geom, items_limit_col, items_geom_col, items_limit_geom_col

from schemas import FeaturesBaseSchema, LimitSchema, GeomSchema, ColSchema, \
    LimitGeomSchema, LimitColSchema, GeomColSchema, LimitGeomColSchema

from utils import BaseSerialisedRequest, handle_error, \
    construct_features_response, construct_collections_response

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

configure_azure_monitor()


class AzureSerialisedRequest(BaseSerialisedRequest):
    '''
    A class to represent an Azure HTTP request with its parameters and headers.
    '''

    def __init__(self, req: HttpRequest) -> None:
        method = req.method
        url = req.url
        params = {**req.params}
        route_params = req.route_params
        headers = req.headers.__dict__.get('__http_headers__', {})
        super().__init__(method, url, params, route_params, headers)


def azure_serialise_response(data: dict) -> HttpResponse:
    '''
    Serialises the response data into an HttpResponse object suitable for Azure Functions.
    '''

    code = data.get('code', 200)
    json_data = json.dumps(data)
    response = HttpResponse(
        body=json_data,
        mimetype="application/json",
        status_code=code
    )
    return response


def azure_process_request(
        req: HttpRequest,
        construct_response_func: callable = construct_features_response,
        **kwargs
    ) -> HttpResponse:
    '''Processes Azure HTTP requests, serialising the request and constructing a response.'''
    try:
        data = AzureSerialisedRequest(req = req)
        response = construct_response_func(data = data, **kwargs)
        serialised_response = azure_serialise_response(response)
        return serialised_response
    except Exception as e:
        bare_error = handle_error(error = e, code = 500)
        return azure_serialise_response(bare_error)


@app.function_name('http_latest_single_col')
@app.route("catalyst/features/latest-collections/{collection?}")
def http_latest_collections(req: HttpRequest) -> HttpResponse:
    '''Handles the processing of API requests to retrieve OS NGD collections, either all or a specific one.
    Handles parameter validation and telemetry tracking.'''
    response = azure_process_request(
        req = req,
        construct_response_func = construct_collections_response
    )
    return response


@app.function_name('http_base')
@app.route("catalyst/features/{collection}/items")
def http_base(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, No extensions applied.'''
    response = azure_process_request(
        req=req,
        schema_class=FeaturesBaseSchema,
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

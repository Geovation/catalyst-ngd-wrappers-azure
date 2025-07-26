from marshmallow.exceptions import ValidationError

from catalyst_ngd_wrappers.ngd_api_wrappers import \
    get_latest_collection_versions, get_specific_latest_collections

from schemas import CollectionsSchema, ColSchema

def remove_query_params(url: str) -> str:
    '''Removes query parameters from a URL.'''
    if '?' in url:
        return url.split('?')[0]
    return url

class BaseSerialisedRequest:
    '''
    A base class to represent an HTTP request with its parameters and headers.
    '''

    def __init__(
            self,
            method: str,
            url: str,
            params: dict,
            route_params: dict,
            headers: dict
        ) -> None:
        self.method = method
        self.url = url
        self.params = params
        self.route_params = route_params
        self.headers = headers

def handle_error(
    error: Exception = None,
    description: str = None,
    code: int = 400
) -> dict:
    '''Formats and configures errors, returning a JSON response.'''
    assert error or description, "Either error or description must be provided."
    if not description:
        description = str(error)
    error_body = {
        "code": code,
        "description": description,
        "errorSource": "Catalyst Wrapper"
    }
    return error_body


def construct_features_response(
    data: BaseSerialisedRequest,
    schema_class: type,
    ngd_api_func: callable
) -> dict:
    '''
    Translates the request headers and path and query parameters into a function call.
    Translates the function response into an HTTP response, handling errors and telemetry.
    '''
    # Handle incorrect HTTP methods
    if data.method != 'GET':
        return handle_error(
            description = "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
            code = 405
        )

    # Load the schema and parse the request parameters
    schema = schema_class()
    multi_collection = isinstance(schema, ColSchema)

    params = data.params
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
        custom_params['collection'] = data.route_params.get('collection')

    response_data = ngd_api_func(
        query_params=parsed_params,
        headers=data.headers,
        **custom_params
    )

    descr = response_data.get('description')
    if response_data.get('errorSource') and isinstance(descr, str):
        fields = [
            x.replace('_', '-')
            for x in schema.fields
            if x != 'limit'
        ]
        attributes = ', '.join(fields)
        response_data['description'] = descr.format(attr=attributes)

    #custom_dimensions = data.pop('telemetryData', None)
    #if custom_dimensions:
        #track_event('OS NGD API - Features', custom_dimensions=custom_dimensions)

    return response_data

def construct_collections_response(data: BaseSerialisedRequest) -> dict:
    ''' Handles the processing of API requests to retrieve OS NGD collections, either all or a specific one.
    Handles parameter validation and telemetry tracking.
    Input: data - BaseSerialisedRequest object containing request details. This includes:
        - method: HTTP method (e.g., 'GET')
        - url: URL of the request
        - params: Query parameters as a dictionary
        - route_params: Route parameters as a dictionary
        - headers: Request headers as a dictionary
    '''
    if data.method != 'GET':
        return handle_error(
            description = "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
            code = 405
        )

    schema = CollectionsSchema()
    params = data.params

    collection = data.route_params.get('collection')
    try:
        parsed_params = schema.load(params)
    except ValidationError as e:
        return handle_error(e)

    log_request_details = parsed_params.pop('log_request_details', True)
    if collection and parsed_params:
        return handle_error(
            code = 400,
            description = "The only supported query parameter for this endpoint is 'log-request-details'",
        )
    recent_update_days = parsed_params.pop('recent_update_days', None)
    if parsed_params:
        return handle_error(
            code = 400,
            description = "The only supported query parameters for this endpoint are: 'recent-update-days', 'log-request-details'",
        )

    if collection:
        response_data = get_specific_latest_collections(collection=[collection])
    else:
        response_data = get_latest_collection_versions(recent_update_days=recent_update_days)

    if log_request_details:
        custom_dimensions = {
            'method': 'GET',
            'url.path': data.url,
        }
        #track_event('HTTP_Request', custom_dimensions=custom_dimensions)

    return response_data

from marshmallow.exceptions import ValidationError
from schemas import LatestCollectionsSchema, ColSchema

from catalyst_ngd_wrappers.ngd_api_wrappers import \
    get_latest_collection_versions, get_specific_latest_collections

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
    """Formats and configures errors, returning a JSON response."""
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
    if data.method != 'GET':
        return handle_error(
            description = "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
            code = 405
        )

    schema = LatestCollectionsSchema()
    params = data.params
    fail_condition1 = len(params) > 1
    fail_condition2 = len(params) == 1 and not params.get('recent-update-days')
    if fail_condition1 or fail_condition2:
        return handle_error(
            code = 400,
            description = "The only supported query parameter is 'recent-update-days'.",
        )

    collection = data.route_params.get('collection')
    try:
        parsed_params = schema.load(params)
    except ValidationError as e:
        return handle_error(e)

    custom_dimensions = {
        f'query_params.{str(k)}': str(v)
        for k, v in parsed_params.items()
    }
    custom_dimensions.pop('key', None)
    custom_dimensions.update({
        'method': 'GET',
        'url.path': data.url,
    })
    if collection:
        data = get_specific_latest_collections([collection], **parsed_params)
        custom_dimensions['url.path_params.collection'] = collection
    else:
        data = get_latest_collection_versions(**parsed_params)

    #track_event('HTTP_Request', custom_dimensions=custom_dimensions)

    return data

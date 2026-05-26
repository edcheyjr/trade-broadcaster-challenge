from __main__ import app

from flask import request

from .data import symbols
from .helpers import api_route


@app.route(api_route('/symbols'), methods=['GET'])
def symbols_index():
    filtered_data = symbols
    name = request.args.get('name')
    if name is not None:
        filtered_data = list(filter(lambda symbol: symbol['name'] == name, symbols))

    return filtered_data


@app.route(api_route('/symbols/<id>'), methods=['GET'])
def symbols_get(id):
    symbol_items = list(filter(lambda symbol: symbol['id'] == id, symbols))
    if len(symbol_items) > 0:
        return symbol_items[0]
    else:
        return "", 404

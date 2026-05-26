from flask import Flask

app = Flask(__name__)

from src import symbols_routes

@app.errorhandler(500)
def internal_error(error):
    return {'message': error.name}, 500

port = 3000
app.run(host='127.0.0.1', port=port)

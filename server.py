from flask import Flask
from bigdata import*

from flask import jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route("/get/<int:_token>/<int:_begin>/<int:_end>/<string:_period>")
def getChartDataFor(_token, _begin, _end, _period):
    data = build_data(_token, _begin, _end, _period)
    return jsonify(data)

if __name__ == "__main__":
    app.run()

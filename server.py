from flask import Flask, jsonify, request, abort
from lib.data import SampleFactory

app = Flask(__name__)

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]
supported = ["test"]


@app.route('/', methods=['POST'])
def get_task():
    selected_plugin = ""
    if not request.json or not 'plugins' in request.json:
        abort(400)
    if len(request.json.get("plugins")) == 0:
        abort(400)
    for plugin in request.json.get("plugins"):
        if plugin in supported:
            selected_plugin = plugin
            break
    else:
        abort(400)
    return jsonify({'tasks': tasks})


@app.route('/persist', methods=['POST'])
def persist():
    f = SampleFactory()
    sample = f.from_json(request.get_json())
    print(sample)
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(debug=True)

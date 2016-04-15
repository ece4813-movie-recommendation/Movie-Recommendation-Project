from flask import Flask
from flask import request, render_template, jsonify, url_for

app = Flask(__name__)

@app.route("/")
def index():
    return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)

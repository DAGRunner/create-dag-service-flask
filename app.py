from flask import Flask
from flask import request

app = Flask(__name__)

@app.route('/')
def index():
    return 'HELLO WORLD'

if __name__ == "__main__":         
    app.run()
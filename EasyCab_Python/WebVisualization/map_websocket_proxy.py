from flask import Flask, render_template
import os
import sys
import kafka

KAFKA_BROKER = 'localhost:9094'

app = Flask(__name__)
@app.route("/")

def home():
   return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)
from flask import Flask, render_template
from pymongo import MongoClient

app = Flask(__name__)

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["twitter_db"]
collection = db["predictions"]

@app.route("/")
def index():
    # Fetch data from MongoDB
    predictions = collection.find()

    # Prepare data for rendering
    data = []
    for prediction in predictions:
        tweet = prediction["tweet"]
        predicted_class = prediction["predicted_class"]
        data.append({"tweet": tweet, "predicted_class": predicted_class})

    # Render HTML template with the data
    return render_template("index.html", data=data)

if __name__ == "__main__":
    app.run(debug=True)

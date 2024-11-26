from flask import Flask
from flask import request
from flask import jsonify, make_response, render_template
import numpy as np

from datetime import datetime, timezone
from db_table import db_table

app = Flask(__name__)

# Set up db connection
def get_db_conn():
    db_schema = {
        "Date": "date PRIMARY KEY",
        "Gold": "float",
        "Silver": "float"
    }
    return db_table("Prices", db_schema)

@app.route('/compare', methods=["GET"])
def compare_dates():
    try:
        db = get_db_conn()
        # Making sure we have all the arguments we need
        if not request.args.get("start_date"):
            return make_response(jsonify({"message": "Invalid start date"}), 400)
        if not request.args.get("end_date"):
            return make_response(jsonify({"message": "Invalid end date"}), 400)

        start = request.args.get("start_date")
        end = request.args.get("end_date")

        # Convert datetime to Unix timestamp to index into DB
        start = datetime.strptime(
            start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
        end = datetime.strptime(
            end, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()

        if start > end:
            return make_response(jsonify({"message": "Start date cannot be greater than end date"}), 400)

        results = db.select_prices_between_dates(start, end, "gold")
        gold_data = {}
        gold_numpy_array = []
        for result in results:
            date = str(datetime.fromtimestamp(result[0], timezone.utc))
            price = result[1] or -1
            if price == -1:
                continue
            gold_data[date[:-9]] = price
            gold_numpy_array.append(price)

        gold_mean = np.mean(gold_numpy_array).astype(float)
        gold_variance = np.var(gold_numpy_array).astype(float)

        results = db.select_prices_between_dates(start, end, "silver")
        silver_data = {}
        silver_numpy_array = []
        for result in results:
            date = str(datetime.fromtimestamp(result[0], timezone.utc))
            price = result[1] or -1
            if price == -1:
                continue
            silver_data[date[:-9]] = price
            silver_numpy_array.append(price)

        silver_mean = np.mean(silver_numpy_array).astype(float)
        silver_variance = np.var(silver_numpy_array).astype(float)

        return_data = {
            "start_date": start,
            "end_date": end,
            "gold_mean": round(gold_mean, 2),
            "gold_variance": round(gold_variance, 2),
            "silver_mean": round(silver_mean, 2),
            "silver_variance": round(silver_variance, 2)
        }

        return make_response(jsonify(return_data), 200)
    except Exception as error:
        return make_response(jsonify({"message": str(error)}), 400)

# route to render index.html file from templates folder
@app.route('/')
def index():
    return render_template('index.html')

app.run(port=8000)

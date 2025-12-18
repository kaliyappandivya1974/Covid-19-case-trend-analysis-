# app.py
from flask import Flask, render_template, jsonify, request
from spark_processor import CovidTrendProcessor

app = Flask(__name__)

# --- CRITICAL: Initialize Spark ONCE here ---
# This creates the session immediately when the app starts
processor = CovidTrendProcessor('data/covid_case_trends_sample.csv')

@app.route('/')
def index():
    # Dynamic Link: Ask Spark what port it is actually using
    active_spark_url = processor.get_spark_ui_url()
    print(f"DEBUG: Spark UI URL sent to template: {active_spark_url}")
    
    # Pass this URL to the HTML template
    return render_template('index.html', spark_ui_url=active_spark_url)

@app.route('/api/trends')
def get_trends():
    country = request.args.get('country', 'USA')
    metric = request.args.get('metric', 'confirmed')
    data = processor.get_trends(country, metric)
    return jsonify(data)

if __name__ == '__main__':
    # use_reloader=False prevents Spark from starting twice and crashing
    app.run(debug=True, port=5000, use_reloader=False)
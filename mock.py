from flask import Flask, jsonify
import random
import logging

# Disable Flask & Werkzeug logs
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)
app.logger.disabled = True
log.disabled = True

@app.route("/success", methods=["GET"])
def success():
    if random.choice([True, False]):
        return jsonify({
            "status": "success",
            "message": "Request processed successfully"
        }), 200
    else:
        return jsonify({
            "status": "error",
            "message": "Intermittent failure occurred"
        }), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3050, debug=False, use_reloader=False)

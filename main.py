from flask import Flask, jsonify, request
import requests
from config import YOUTUBE_API_KEY, CHANNEL_ID

app = Flask(__name__)

def get_youtube_subscribers(api_key, channel_id):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={channel_id}&key={api_key}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        if "items" in data and data["items"]:
            return int(data["items"][0]["statistics"]["subscriberCount"])
    except:
        pass
    return None

@app.route("/api/stats", methods=["GET"])
def get_stats():
    yt_id = request.args.get("yt", CHANNEL_ID)
    result = {
        "youtube": get_youtube_subscribers(YOUTUBE_API_KEY, yt_id)
    }
    return jsonify(result)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)

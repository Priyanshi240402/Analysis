from flask import Flask, request, jsonify
import mysql.connector
import urllib.request
from bs4 import BeautifulSoup
import requests
import json

app = Flask(__name__)

def send_data_to_external_api(channelId, videos_published, audience, views, whatch_time, total_likes, playlists, audience_retention, recent_videos, audience_by_countries, audience_by_demographics, traffic_source, external_source):
    url = "https://api.promulgateinnovations.com/api/v1/setYoutubeAnalytics"
    payload = {
        "channelId": channelId,
        "views": views,
        "whatch_time": whatch_time,
        "videos_published": videos_published,
        "total_likes": total_likes,
        "playlists": playlists,
        "audience_retention": audience_retention,
        "recent_videos": recent_videos,
        "audience_by_countries": audience_by_countries,
        "audience_by_demographics": audience_by_demographics,
        "traffic_source": traffic_source,
        "external_source": external_source,
        "audience": audience,  # Include the audience variable here
        "addedBy": "Priyanshi"
    }
    headers = {
        'Authorization': 'Basic cHJvbXVsZ2F0ZTpwcm9tdWxnYXRl',
        'Content-Type': 'application/json'
    }

    response = requests.post(url, headers=headers, json=payload)

    print(response.text)

def scrape_channel_data(page_soup):
    # Existing scrape_channel_data function
    # Extract data from the web scraping
    uploads = page_soup.findAll("span", {"id": "youtube-stats-header-uploads"})
    subs = page_soup.findAll("span", {"id": "youtube-stats-header-subs"})
    views = page_soup.findAll("span", {"id": "youtube-stats-header-views"})

    # Add additional data scraping logic for the new columns
    whatch_time = page_soup.findAll("span", {"id": "youtube-stats-header-whatch_time"})
    total_likes = page_soup.findAll("span", {"id": "youtube-stats-header-total_likes"})
    playlists = page_soup.findAll("span", {"id": "youtube-stats-header-playlists"})
    audience_retention = page_soup.findAll("span", {"id": "youtube-stats-header-audience_retention"})
    recent_videos = page_soup.findAll("span", {"id": "youtube-stats-header-recent_videos"})
    audience_by_countries = page_soup.findAll("span", {"id": "youtube-stats-header-audience_by_countries"})
    audience_by_demographics = page_soup.findAll("span", {"id": "youtube-stats-header-audience_by_demographics"})
    traffic_source = page_soup.findAll("span", {"id": "youtube-stats-header-traffic_source"})
    external_source = page_soup.findAll("span", {"id": "youtube-stats-header-external_source"})
    audience_value = page_soup.findAll("span", {"id": "youtube-stats-header-audience"})

    # Assuming you want to return the text values of uploads, subscribers, and country
    return [
        uploads[0].text if uploads else None,
        subs[0].text if subs else None,
        views[0].text if views else None,
        whatch_time[0].text if whatch_time else None,
        total_likes[0].text if total_likes else None,
        playlists[0].text if playlists else None,
        audience_retention[0].text if audience_retention else None,
        recent_videos[0].text if recent_videos else None,
        audience_by_countries[0].text if audience_by_countries else None,
        audience_by_demographics[0].text if audience_by_demographics else None,
        traffic_source[0].text if traffic_source else None,
        external_source[0].text if external_source else None,
        audience_value[0].text if audience_value else None
    ]

def fetch_and_store_youtube_data(channel_url, channelId):
    user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
    headers = {'User-Agent': user_agent}

    try:
        request = urllib.request.Request(channel_url, None, headers)
        uClient = urllib.request.urlopen(request)
        page_html = uClient.read()
        uClient.close()

        page_soup = BeautifulSoup(page_html, 'html.parser')

        mydb = mysql.connector.connect(
            host="localhost",
            database="youtube",
            user="root",
            password="Princy@123#"
        )

        mycursor = mydb.cursor()

        table_name = 'websap'

        scraped_data = scrape_channel_data(page_soup)

        sql = f"INSERT INTO {table_name} (channelId, videos_published, audience, views, whatch_time, total_likes, playlists, audience_retention, recent_videos, audience_by_countries, audience_by_demographics, traffic_source, external_source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        scraped_data_with_channelId = [channelId] + scraped_data
        mycursor.execute(sql, tuple(scraped_data_with_channelId[:13]))

        mydb.commit()

        mycursor.close()
        mydb.close()

        send_data_to_external_api(channelId, *scraped_data)

    except Exception as e:
        print("Error:", e)

@app.route('/fetch_youtube_data', methods=['POST'])
def fetch_youtube_data():
    try:
        data = request.json
        channelId = data.get('channelId')

        # Replace 'your_channel_url' with the actual URL pattern for your channel
        channel_url = f'https://socialblade.com/youtube/channel/{channelId}'

        fetch_and_store_youtube_data(channel_url, channelId)

        return jsonify({"success": True, "message": "YouTube data fetched and stored successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

if __name__ == '__main__':
    app.run(debug=True)

import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
import pymongo
from pymongo import MongoClient
from datetime import datetime
import re
import pymysql
from datetime import timedelta
import numpy as np
from googleapiclient.errors import HttpError

# SET PAGE LAYOUT
st.set_page_config(page_title="Youtube Data Handling", layout="wide", initial_sidebar_state="auto", menu_items=None)
st.title(":blue[Youtube Data Harvesting and Warehousing]")

# Connecting with Youtube API
Api_key = 'AIzaSyAOF5g9MKCxwLtpn08Hc4tjVDIKmA_zKjs'
youtube = build('youtube', 'v3', developerKey=Api_key)

# CHANNEL DETAILS
def Get_Channel_details(youtube, channel_ids):
    channel_list = []
    request = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_ids)
    response = request.execute()
    for i in range(len(response["items"])):
        data = dict(channel_name=response["items"][i]["snippet"]["title"],
                    channel_id=response["items"][i]["id"],
                    subscription_count=response["items"][i]['statistics']['subscriberCount'],
                    channel_views=response["items"][i]['statistics']['viewCount'],
                    channel_description=response["items"][i]["snippet"]["description"],
                    playlist_id=response["items"][i]['contentDetails']['relatedPlaylists']['uploads'],
                    video_count=response["items"][i]["statistics"]["videoCount"])
        channel_list.append(data)
    return channel_list


# PLAYLIST ID'S
def playlist_id(channel_data):
    playlist_id = []
    for i in channel_data:
        playlist_id.append(i["playlist_id"])
    return playlist_id


# PLAYLIST DETAILS
def play_list_(channel_ids):
    all_data = []
    request = youtube.playlists().list(part="snippet,id", channelId=channel_ids, maxResults=50)
    response = request.execute()
    for k in range(len(response["items"])):
        data = dict(playlist_id=response["items"][k]["id"],
                    channel_id=response["items"][k]["snippet"]["channelId"],
                    playlist_name=response["items"][k]["snippet"]["title"])
        all_data.append(data)
    return all_data


# VIDEO ID'S
def Get_video_details(youtube, channel_ids):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_ids,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# VIDEO DETAILS
def Get_video_data(youtube, video_ids):
    video_data = []
    next_page_token = None
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50]), maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for video in response["items"]:
            published_date_str = video["snippet"]["publishedAt"]
            published_date = datetime.strptime(published_date_str, '%Y-%m-%dT%H:%M:%SZ')
            formatted_published_date = published_date.strftime('%Y-%m-%d %H:%M:%S')

            # snippet = video.get("snippet", {})
            # statistics = video.get("statistics", {})
            content_details = video.get("contentDetails", {})

            duration = content_details.get("duration", "")
            duration = duration[2:]  # Remove "PT" from the beginning

            hours = 0
            minutes = 0
            seconds = 0

            if 'H' in duration:
                hours_index = duration.index('H')
                hours = int(duration[:hours_index])
                duration = duration[hours_index + 1:]

            if 'M' in duration:
                minutes_index = duration.index('M')
                minutes = int(duration[:minutes_index])
                duration = duration[minutes_index + 1:]

            if 'S' in duration:
                seconds_index = duration.index('S')
                seconds = int(duration[:seconds_index])

            duration_formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            data = dict(
                channel_id=video["snippet"]["channelId"],
                channel_name=video["snippet"]["channelTitle"],
                video_id=video["id"],
                video_name=video["snippet"]["title"],
                video_description=video["snippet"]["description"],
                published_At=formatted_published_date,
                view_count=video["statistics"]["viewCount"],
                like_count=video["statistics"].get("likeCount"),
                favorite_count=video["statistics"]["favoriteCount"],
                duration=duration_formatted,
                thumbnails=video["snippet"]["thumbnails"]["default"]["url"],
                comment_count=video["statistics"].get("commentCount"),
                caption_status=video["contentDetails"]["caption"])
            video_data.append(data)
        next_page_token = response.get("nextPageToken")
    return video_data


# COMMENT DETAILS
def comment_data(video_ids):
    comments_data = []
    for ids in video_ids:
        try:
            video_data_request = youtube.commentThreads().list(
                part="snippet",
                videoId=ids,
                maxResults=50
            ).execute()
            video_info = video_data_request['items']
            for comment in video_info:
                published_date_str = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                published_date = datetime.strptime(published_date_str, '%Y-%m-%dT%H:%M:%SZ')
                formatted_published_date = published_date.strftime('%Y-%m-%d %H:%M:%S')

                comment_info = dict(
                    video_id=comment['snippet']['videoId'],
                    comment_id=comment['snippet']['topLevelComment']['id'],
                    comment_text=comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published_At=formatted_published_date
                )

                comments_data.append(comment_info)
        except HttpError as e:
            if e.resp.status == 403 and 'disabled comments' in str(e):
                comment_info = {
                    'Video_id': ids,
                    'Comment_Id': 'comments_disabled',
                }
                comments_data.append(comment_info)
            else:
                print(f"An error occurred while retrieving comments for video: {ids}")
                print(f"Error details: {e}")
    return comments_data


# FRONT PAGE
tab1, tab2, tab3, tab4 = st.tabs(['Channel_id', 'Migrate to Mongodb', 'Migrate to MYSQl', 'Questions'])
with tab1:
    channel_ids = st.text_input("Enter Channel Id")
    if channel_ids and st.button("Submit"):
        channel_data = Get_Channel_details(youtube, channel_ids)
        channels_datas = pd.DataFrame(channel_data)
        # channels_data.to_csv('channels_data.csv', index=False)
        # print(channels_data)
        # st.write(channel_data)
        st.table(channel_data)
        st.success(f'### Channel Data Extracted Successfully')

# UPLOAD DATA TO MONGODB
with tab2:
    if st.button("Upload Data to MongoDB Atlas"):
        st.write('Fetching Data...')
        channel_data = Get_Channel_details(youtube, channel_ids)
        channels_datas = pd.DataFrame(channel_data)
        # channels_data.to_csv('channels_data.csv', index=False)
        # print(channels_data)
        # st.write(channel_data)

        playlist_ids = playlist_id(channel_data)
        playlist_details = play_list_(channel_ids)
        playlists_details = pd.DataFrame(playlist_details)
        # playlists_details.to_csv('playlists_details.csv', index=False)
        # print(playlists_details)

        video_ids = Get_video_details(youtube, channel_ids)
        video_details = Get_video_data(youtube, video_ids)
        videos_details = pd.DataFrame(video_details)
        # videos_details.to_csv('videos_details_trans.csv', index=False)
        # print(videos_details)

        comment_details = comment_data(video_ids)
        comments_details = pd.DataFrame(comment_details)
        # comments_details.to_csv('comments_details.csv', index=False)
        # print(comments_details)

        st.write(f"### Youtube Channel Data Extracted Successfully")
        with st.spinner('Uploading....'):
            client = MongoClient(
                f"mongodb+srv://kathirthilaak:Tikki%40123@cluster1.lm6wn.mongodb.net/")
            mydb = client["app_data"]
            mycol1 = mydb["channel_data"]
            mycol2 = mydb["playlist_data"]
            mycol3 = mydb["video_data"]
            mycol4 = mydb["comment_data"]

            mycol1.insert_many(channel_data)
            mycol2.insert_many(playlist_details)
            mycol3.insert_many(video_details)
            mycol4.insert_many(comment_details)

            st.success("Data Migrate to MongoDB Atlas is Successfully", icon="✅")

        # MIGRATE TO MYSQL
with tab3:
    client = MongoClient(f"mongodb+srv://kathirthilaak:Tikki%40123@cluster1.lm6wn.mongodb.net/")
    mydb = client["app_data"]


    def youtube_channel_names():
        channelname = []
        for i in mydb.channel_data.find():
            channelname.append(i['channel_name'])
        return channelname


    ch_names = youtube_channel_names()
    user_inp = st.selectbox("Select the channel for data migration :", options=ch_names)

    if user_inp:

        if st.button("Migarte to MYSQL"):
            st.write("### Data Migration from MongoDB Atlas to MySQL")
            with st.spinner(":Green[Data migrating...]"):

                client = MongoClient(
                    f"mongodb+srv://kathirthilaak:Tikki%40123@cluster1.lm6wn.mongodb.net/")
                mydb = client["app_data"]

                mydb = pymysql.connect(
                    host="localhost",
                    user="root",
                    password="Tikki@123"
                )
                mycursor = mydb.cursor()

                mycursor.execute("create database if not exists youtube_data_db")
                mydb.commit()

                mycursor.execute("use youtube_data_db")
                mydb.commit()

                # TABLES CREATION
                mycursor.execute("""create table if not exists channel_data(
                                    channel_name varchar(255),
                                    channel_id varchar(255),
                                    subscription_count INT,
                                    channel_views INT,
                                    channel_description TEXT,
                                    playlist_id varchar(255),
                                    video_count INT
                                    )
                                    """)
                mydb.commit()

                mycursor.execute("""create table if not exists playlist_data(
                                    playlist_id varchar(255), 
                                    channel_id varchar(255),
                                    playlist_name varchar(255)
                                    )
                                    """)
                mydb.commit()

                mycursor.execute("""create table if not exists video_data(

                                    channel_id varchar(255),
                                    channel_name varchar(255),
                                    video_id varchar(255),
                                    video_name varchar(255),
                                    video_description TEXT,
                                    published_At DateTime,
                                    view_count INT,
                                    like_count INT,
                                    favorite_count INT,
                                    duration Time,
                                    thumbnails varchar(255),
                                    comment_count INT,
                                    caption_status varchar(255)
                                    )
                                    """)
                mydb.commit()

                mycursor.execute("""create table if not exists comment_data(
                                    video_id varchar(255),
                                    comment_id varchar(255),
                                    comment_text TEXT,
                                    comment_author varchar(255),
                                    comment_published_At DateTime
                                    )
                                    """)
                mydb.commit()

                client = MongoClient(
                    f"mongodb+srv://kathirthilaak:Tikki%40123@cluster1.lm6wn.mongodb.net/")
                mydb = client["app_data"]
                mycol1 = mydb["channel_data"]
                mycol2 = mydb["playlist_data"]
                mycol3 = mydb["video_data"]
                mycol4 = mydb["comment_data"]

                # VALUES INSERTION INTO MYSQL

                for item in mycol1.find({"channel_name": user_inp}, {'_id': 0}):
                    values = (
                        item['channel_name'],
                        item['channel_id'],
                        item['subscription_count'],
                        item['channel_views'],
                        item['channel_description'],
                        item['playlist_id'],
                        item['video_count']
                    )

                    mydb = pymysql.connect(
                        host="localhost",
                        user="root",
                        password="Tikki@123"
                    )
                    mycursor = mydb.cursor()

                    mycursor.execute("use youtube_data_db")
                    mydb.commit()

                    mycursor.execute("INSERT INTO channel_data  VALUES (%s, %s, %s, %s, %s, %s, %s)", values)
                    mydb.commit()

                for item in mycol1.find({"channel_name": user_inp}, {'_id': 0}):
                    channel_id_s = (item['channel_id'])
                    for playlist_item in mycol2.find({"channel_id": channel_id_s}, {'_id': 0}):
                        values = (
                            playlist_item['playlist_id'],
                            playlist_item['channel_id'],
                            playlist_item['playlist_name']
                        )

                        mydb = pymysql.connect(
                            host="localhost",
                            user="root",
                            password="Tikki@123"
                        )
                        mycursor = mydb.cursor()

                        mycursor.execute("use youtube_data_db")
                        mydb.commit()

                        mycursor.execute("INSERT INTO playlist_data VALUES (%s, %s, %s)", values)
                        mydb.commit()

                for video_item in mycol3.find({"channel_name": user_inp}, {'_id': 0}):
                    values = (

                        video_item['channel_id'],
                        video_item['channel_name'],
                        video_item['video_id'],
                        video_item['video_name'],
                        video_item['video_description'],
                        video_item['published_At'],
                        video_item['view_count'],
                        video_item['like_count'],
                        video_item['favorite_count'],
                        video_item['duration'],
                        video_item['thumbnails'],
                        video_item['comment_count'],
                        video_item['caption_status']
                    )

                    mydb = pymysql.connect(
                        host="localhost",
                        user="root",
                        password="Tikki@123"
                    )
                    mycursor = mydb.cursor()

                    mycursor.execute("use youtube_data_db")
                    mydb.commit()

                    mycursor.execute(
                        "INSERT INTO video_data VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", values)
                    mydb.commit()

                for item in mycol3.find({"channel_name": user_inp}, {'_id': 0}):
                    video_id_s = (item['video_id'])
                    for comment_item in mycol4.find({"video_id": video_id_s}, {'_id': 0}):
                        values = (
                            comment_item['video_id'],
                            comment_item['comment_id'],
                            comment_item['comment_text'],
                            comment_item['comment_author'],
                            comment_item['comment_published_At']
                        )

                        mydb = pymysql.connect(
                            host="localhost",
                            user="root",
                            password="Tikki@123"
                        )
                        mycursor = mydb.cursor()

                        mycursor.execute("use youtube_data_db")
                        mydb.commit()

                        mycursor.execute("INSERT INTO comment_data VALUES (%s, %s, %s, %s, %s)", values)
                        mydb.commit()

                st.success("Data Migration from Mongodb to Msql is Successful", icon="✅")

            # QUESTIONS AND ANSWERS
with tab4:
    q1 = '1. What are the names of all the videos and their corresponding channels'

    q2 = '2. Which channels have the most number of videos, and how many videos do they have?'

    q3 = '3. What are the top 10 most viewed videos and their respective channels?'

    q4 = '4. How many comments were made on each video, and what are their corresponding video names?'

    q5 = '5. Which videos have the highest number of likes, and what are their corresponding channel names?'

    q6 = '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?'

    q7 = '7. What is the total number of views for each channel, and what are their corresponding channel names?'

    q8 = '8. What are the names of all the channels that have published videos in the year 2023?'

    q9 = '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?'

    q10 = '10. Which videos have the highest number of comments, and what are their corresponding channel names?'

    details = st.selectbox('Select any questions given below :',
                           ['Click the question that you would like to query', q1, q2, q3, q4, q5, q6, q7, q8, q9, q10])
    clicked = st.button("Get Answer")

    if clicked:
        mydb = pymysql.connect(
            host="localhost",
            user="root",
            password="Tikki@123"
        )
        mycursor = mydb.cursor()

        mycursor.execute("use youtube_data_db")
        mydb.commit()

        if details == q1:
            query = """SELECT video_name AS Video_name, Channel_name FROM video_data ORDER BY Channel_name"""
            mycursor.execute(query)
            results = mycursor.fetchall()
            df = pd.DataFrame(results, columns=['Video_name', 'Channel_name'], index=np.arange(1, len(results) + 1))
            st.write(df)

        elif details == q2:
            query = """SELECT channel_name, COUNT(video_id) AS Video_count FROM video_data GROUP BY channel_name ORDER BY video_count DESC"""
            mycursor.execute(query)
            results = mycursor.fetchall()
            df = pd.DataFrame(results, columns=['Channel_name', 'Video_count'], index=np.arange(1, len(results) + 1))
            st.write(df)

        elif details == q3:
            query = """SELECT channel_name, video_name ,view_count FROM video_data ORDER BY view_count DESC LIMIT 10"""
            mycursor.execute(query)
            results = mycursor.fetchall()
            df = pd.DataFrame(results, columns=['Channel_name', 'Top_10_viewd_videos', "View_count"],
                              index=np.arange(1, len(results) + 1))
            st.write(df)

        elif details == q4:
            query = """SELECT video_name, comment_count FROM video_data ORDER BY comment_count DESC"""
            mycursor.execute(query)
            results = mycursor.fetchall()
            df = pd.DataFrame(results, columns=['Video_name', 'Comment_count'], index=np.arange(1, len(results) + 1))
            st.write(df)

        elif details == q5:
            query = """SELECT channel_name, video_name, like_count FROM video_data ORDER BY like_count"""
            mycursor.execute(query)
            results = mycursor.fetchall()
            df = pd.DataFrame(results, columns=['Channel_name', 'Video_name', 'Like_count'],
                              index=np.arange(1, len(results) + 1))
            st.write(df)

        elif details == q6:
            query = """SELECT video_name, like_count FROM video_data ORDER BY like_count"""
            mycursor.execute(query)
            results = mycursor.fetchall()
            df = pd.DataFrame(results, columns=['Video_name', 'Like_count'], index=np.arange(1, len(results) + 1))
            st.write(df)

        elif details == q7:
            query = """SELECT channel_name,  channel_views FROM channel_data ORDER BY channel_views"""
            mycursor.execute(query)
            results = mycursor.fetchall()
            df = pd.DataFrame(results, columns=['Channel_name', 'total_Channel_views'],
                              index=np.arange(1, len(results) + 1))
            st.write(df)

        elif details == q8:
            query = """SELECT channel_name ,count(video_id) FROM video_data WHERE EXTRACT( YEAR FROM published_At) = 2023 GROUP BY channel_name"""
            mycursor.execute(query)
            results = mycursor.fetchall()
            df = pd.DataFrame(results, columns=['Channel_name', 'Total_videos'], index=np.arange(1, len(results) + 1))
            st.write(df)

        elif details == q9:
            query = '''SELECT channel_name, AVG(duration) AS Average_duration FROM video_data GROUP BY channel_name ORDER BY Average_duration DESC'''
            mycursor.execute(query)
            results = mycursor.fetchall()
            df = pd.DataFrame(results, columns=['Channel_name', 'Average_duration'],
                              index=np.arange(1, len(results) + 1))
            st.write(df)

        elif details == q10:
            query = """SELECT video_name, channel_name, comment_count FROM video_data ORDER BY comment_count DESC"""
            mycursor.execute(query)
            results = mycursor.fetchall()
            df = pd.DataFrame(results, columns=['Video_name', 'Channel_name', 'Comment_count'],
                              index=np.arange(1, len(results) + 1))
            st.write(df)



#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import pymongo
import psycopg2
import streamlit as st
from googleapiclient.discovery import build

def Api_connection():
    Api_Id="AIzaSyBtbRHTv0lcUv_Li4dHIHux52kCFLUZYgU"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connection()

def fetch_channel_info(channel_id):

    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)

    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data

def fetch_channel_videos(channel_id):
    video_ids = []

    res = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(
                                           part = 'snippet',
                                           playlistId = playlist_id,
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

def fetch_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics'].get('viewCount'),
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

def fetch_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()

                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass

        return Comment_Information

client=pymongo.MongoClient("mongodb+srv://rickyrayan412:Ricky412@ricky.qreas6k.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_Data"]

def channel_details(channel_id):
    ch_details = fetch_channel_info(channel_id)
    vi_ids = fetch_channel_videos(channel_id)
    vi_details = fetch_video_info(vi_ids)
    com_details = fetch_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                        "video_information":vi_details,
                        "comment_information":com_details})

    return "Stored in Database successfully"

def channels_table():
    mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="ricky412",
            database= "youtube_data",
            port = "5432"
            )
    cursor = mydb.cursor()
    
    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()
    
    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                        Channel_Id varchar(80) primary key, 
                        Subscription_Count bigint, 
                        Views bigint,
                        Total_Videos int,
                        Channel_Description text,
                        Playlist_Id varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Channels Table already created")    
    
    ch_list = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)
    
    for index,row in df.iterrows():
        insert_query = '''INSERT into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscription_Count,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''
            
    
        values =(
                row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit()    
        except:
            print("Channels values are already inserted")

def videos_table():
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="ricky412",
                database= "youtube_data",
                port = "5432"
                )
    cursor = mydb.cursor()

    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists videos(
                        Channel_Name varchar(150),
                        Channel_Id varchar(100),
                        Video_Id varchar(50) primary key, 
                        Title varchar(150), 
                        Tags text,
                        Thumbnail varchar(225),
                        Description text, 
                        Published_Date timestamp,
                        Duration interval, 
                        Views bigint, 
                        Likes bigint,
                        Comments int,
                        Favorite_Count int, 
                        Definition varchar(10), 
                        Caption_Status varchar(50) 
                        )''' 
                        
        cursor.execute(create_query)             
        mydb.commit()
    except:
        print("Videos Table already created")

    vi_list = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
        
    
    for index, row in df2.iterrows():
        insert_query = '''INSERT INTO videos(Channel_Name,
                        Channel_Id,
                        Video_Id, 
                        Title, 
                        Tags,
                        Thumbnail,
                        Description, 
                        Published_Date,
                        Duration, 
                        Views, 
                        Likes,
                        Comments,
                        Favorite_Count, 
                        Definition, 
                        Caption_Status 
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values = (
                    row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])
                                
        try:    
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("videos values already inserted in the table")

def comments_table():
    mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="ricky412",
            database= "youtube_data",
            port = "5432"
            )
    cursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()
        
    except:
        print("Comments Table already created")

    com_list = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_Id,
                                      Video_Id ,
                                      Comment_Text,
                                      Comment_Author,
                                      Comment_Published)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
            )
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
               print("This comments are already exist in comments table")

def tables():
    channels_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"

def display_channels_table():
    ch_list = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table
    st.write("Displaying channels table...")

def display_videos_table():
    vi_list = []
    db = client["Youtube_Data"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table
    st.write("Displaying videos table...")

def display_comments_table():
    com_list = []
    db = client["Youtube_Data"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table
    st.write("Displaying comments table...")


st.title("YouTube Data Harvesting and Warehousing")

with st.sidebar:
    st.markdown('<h1 style="color: red; font-family: Arial; text-align: center; font-weight: bold; font-size: 36px;">YouTube Data Harvesting and Warehousing</h1>', unsafe_allow_html=True)
    st.header("Project's Management Style")
    st.markdown('- <span style="font-size:16px; color:blue;"><b>Python scripts in Jupyter Notebook using Anaconda Navigator</b></span>', unsafe_allow_html=True)
    st.markdown('- <span style="font-size:16px; color:red;"><b>Connected to the YouTube API</b></span>', unsafe_allow_html=True)
    st.markdown('- <span style="font-size:16px; color:orange;"><b>Stored data in a MongoDB data lake</b></span>', unsafe_allow_html=True)
    st.markdown('- <span style="font-size:16px; color:blue;"><b>Migrated data to a PostgreSQL data warehouse</b></span>', unsafe_allow_html=True)
    st.markdown('- <span style="font-size:16px; color:green;"><b>Queryed the SQL data warehouse</b></span>', unsafe_allow_html=True)
    st.markdown('- <span style="font-size:16px; color:red;"><b>Displaying data in the Streamlit app</b></span>', unsafe_allow_html=True)


channel_id = st.text_input("Enter the Channel ID:")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

def store_data_mongodb(channel_Id): 
    for channel in channel_Id:
        ch_ids = []
        db = client["Youtube_Data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)

if st.button("Store Data in MongoDB"):
    if channels:
        store_data_mongodb(channels)
    else:
        st.warning("Please enter at least one channel ID.")

    
def migrate_to_sql_and_display_tables():
    display = tables()
    if display:
        st.success("Data migration to SQL and table display successful!")
        st.write(display)
    else:
        st.error("Failed to migrate data to SQL or no data available.")
if st.button("Migrate to SQL"):
    migrate_to_sql_and_display_tables()


show_table = st.radio("SELECT THE TABLE FOR VIEW", ("Channels", "Videos", "Comments"))

if st.button("View Table"):
    if show_table == "Channels":
        display_channels_table()
    elif show_table == "Videos":
        display_videos_table()
    elif show_table == "Comments":
        display_comments_table()
else:
    st.info("Please select a table and click 'View Table' to display.")

mydb = psycopg2.connect(host="localhost",
        user="postgres",
        password="ricky412",
        database= "youtube_data",
        port = "5432"
        )
cursor = mydb.cursor()

def execute_sql_query(question):
    if question == '1. Every video as well as the channel name':
        query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
        cursor.execute(query1)
        mydb.commit()
        t1=cursor.fetchall()
        st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))
        
    elif question == '2. Channels having the most videos':
        query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
        cursor.execute(query2)
        mydb.commit()
        t2=cursor.fetchall()
        st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))
        
    elif question == '3. Top 10 videos that have been viewed most':
        query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
        cursor.execute(query3)
        mydb.commit()
        t3 = cursor.fetchall()
        st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))
        
    elif question == '4. Commentary for every video':
        query4 = "select Comments as No_comments ,Title as VideoTitle, Channel_Name as ChannelName from videos where Comments is not null;"
        cursor.execute(query4)
        mydb.commit()
        t4=cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=["No of Comments", "Video Title", "Channel Name"]))
        
    elif question == '5. The most liked videos':
        query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
        cursor.execute(query5)
        mydb.commit()
        t5 = cursor.fetchall()
        st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))
        
    elif question == '6. Likes of all videos':
        query6 = '''select Likes as likeCount,Title as VideoTitle, Channel_Name as ChannelName from videos;'''
        cursor.execute(query6)
        mydb.commit()
        t6 = cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=["like count","video title","channel Name"]))
        
    elif question == '7. Views for every channel':
        query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
        cursor.execute(query7)
        mydb.commit()
        t7=cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=["channel name","total views"]))
        
    elif question == '8. Videos that were released in 2022':
        query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
        cursor.execute(query8)
        mydb.commit()
        t8=cursor.fetchall()
        st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))
        
    elif question == '9. The average length of each channel\'s videos':
        query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
        cursor.execute(query9)
        mydb.commit()
        t9=cursor.fetchall()
        t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
        T9=[]
        for index, row in t9.iterrows():
            channel_title = row['ChannelTitle']
            average_duration = row['Average Duration']
            average_duration_str = str(average_duration)
            T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
        st.write(pd.DataFrame(T9))
        
    elif question == '10. Videos with the most comments':
        query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
        cursor.execute(query10)
        mydb.commit()
        t10=cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))

question = st.selectbox(
    'Please Select Your Question',
    ('1. Every video as well as the channel name',
     '2. Channels having the most videos',
     '3. Top 10 videos that have been viewed most',
     '4. Commentary for every video',
     '5. The most liked videos',
     '6. Likes of all videos',
     '7. Views for every channel',
     '8. Videos that were released in 2022',
     '9. The average length of each channel\'s videos',
     '10. Videos with the most comments'))

if st.button("Execute Query"):
    results = execute_sql_query(question)
    st.write("Query executed successfully!")

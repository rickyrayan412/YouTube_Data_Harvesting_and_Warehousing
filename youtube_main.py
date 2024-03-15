import streamlit as st
import pandas as pd
import googleapiclient.discovery
from googleapiclient.discovery import *
import pymongo
from datetime import datetime
import isodate 
import psycopg2 
from psycopg2 import sql
from sqlalchemy import create_engine
from streamlit_option_menu import option_menu

client=pymongo.MongoClient("mongodb+srv://rickyrayan412:Ricky412@ricky.qreas6k.mongodb.net/?retryWrites=true&w=majority")
mydb=client['youtubedata']
mycollection=mydb.youtubedetails 

mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="ricky412",
            database= "youtube",
            port = "5432"
            )
cursor = mydb.cursor()

with st.sidebar:
    option_styles = {
        "nav-link": {
            "font-size": "18px",
            "text-align": "center",
            "margin": "0px",
            "--hover-color": "#C80101"
        },
        "icon": {
            "font-size": "20px"
        },
        "container": {
            "max-width": "250px"
        },
        "nav-link-selected": {
            "background-color": "red"
        }
    }
    selected = option_menu(None, 
                           ["Home", "Migrate into MongoDB and PostgreSQL", "SQL Queries"], 
                           icons=["house-door-fill", "cloud-upload", "card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles=option_styles)

def api_connect():
    api_srevice_name = 'youtube'
    api_version = 'v3'
    API_KEY ='AIzaSyBtbRHTv0lcUv_Li4dHIHux52kCFLUZYgU'
    youtube = googleapiclient.discovery.build(api_srevice_name,api_version,developerKey=API_KEY)
    return youtube

def channeldata(youtube,channel_id):
    finalchanneldata=[]
    request= youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
     )
    channel_data = request.execute()

    for i in range(len(channel_data['items'])):
        finalinfo=dict(channel_id=channel_data['items'][i]['id'],
                       channel_name=channel_data['items'][i]['snippet']['title'],
                       channel_description=channel_data['items'][i]['snippet']['description'],
                       channel_view=channel_data['items'][i]['statistics']['viewCount'],
                       channel_subscriber=channel_data['items'][i]['statistics']['subscriberCount'],
                       channel_playlistid=channel_data['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                       channel_videocount=channel_data['items'][i]['statistics']['videoCount'])
        finalchanneldata.append(finalinfo)
    
    return finalchanneldata

def playlist(youtube,channel_id):
    playlist=[]
    playlist_request = youtube.playlists().list(
         part="snippet,contentDetails",
         channelId=channel_id,
        maxResults=50
    )
    playlist_response = playlist_request.execute()
    

    for i in range(len(playlist_response['items'])):
        play=dict(playlist_id=playlist_response['items'][i]['id'],
                  channel_id=playlist_response['items'][i]['snippet']['channelId'],
                  playlist_title=playlist_response['items'][i]['snippet']['localized']['title'],
                  playlist_count=playlist_response['items'][i]['contentDetails']['itemCount'],
                  playlist_publishedate=playlist_response['items'][i]['snippet']['publishedAt'])
        playlist.append(play)

    next_page_token=playlist_response.get('nextPageToken')
    more_pages=True
    while more_pages:
        if next_page_token is None:
            more_pages=False
        else:
            playlist_request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            playlist_response=playlist_request.execute()
            for i in range(len(playlist_response['items'])):
                play=dict(playlist_id=playlist_response['items'][i]['id'],
                          channel_id=playlist_response['items'][i]['snippet']['channelId'],
                          playlist_count=playlist_response['items'][i]['contentDetails']['itemCount'],
                          playlist_title=playlist_response['items'][i]['snippet']['localized']['title'],
                          playlist_publishedate=playlist_response['items'][i]['snippet']['publishedAt'])
                playlist.append(play)
            next_page_token=playlist_response.get('nextPageToken')
    return playlist

def videoids_details(youtube,a):
    video_ids=[]
    request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=a,
            maxResults=50
    )
    channel_playlist = request.execute()
    
    for i in range(len(channel_playlist['items'])):
        video_ids.append(channel_playlist['items'][i]['contentDetails']['videoId'])
        
    next_page_token=channel_playlist.get('nextPageToken')
    more_pages=True
    while more_pages:
        if next_page_token is None:
            more_pages=False
        else:
            request = youtube.playlistItems().list(
                       part="snippet,contentDetails",
                       playlistId=a,
                       maxResults=50,
                       pageToken=next_page_token
            )
            channel_playlist = request.execute()
            for i in range(len(channel_playlist['items'])):
                video_ids.append(channel_playlist['items'][i]['contentDetails']['videoId'])

            next_page_token=channel_playlist.get('nextPageToken')
    return  video_ids

def videodetails(youtube,videoids):
    all_video_stats=[]
    for i in range(0,len(videoids),50):
        video_request = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=','.join(videoids[i:i+50])
              )
        
        video_response= video_request.execute()
        
        for i in range(len(video_response['items'])):
            video_stats=dict(video_id=video_response['items'][i]['id'],
                             channel_name=video_response['items'][i]['snippet']['channelTitle'],
                             video_title=video_response['items'][i]['snippet']['title'],
                             video_description=video_response['items'][i]['snippet']['description'],
                             video_published=video_response['items'][i]['snippet']['publishedAt'],
                             video_viewcount=video_response['items'][i]['statistics']['viewCount'],
                             video_likecount=video_response['items'][i]['statistics'].get('likeCount',0),
                             video_commentcount=video_response['items'][i]['statistics'].get('commentCount',0),
                             video_duration=video_response['items'][i]['contentDetails']['duration'],
                             video_thumbnail=video_response['items'][i]['snippet']['thumbnails']['default']['url'],
                             video_captionStatus=video_response['items'][i]['contentDetails']['caption']
                            )
                             
                            
            all_video_stats.append(video_stats)
      
    return all_video_stats

def comments_details(youtube,v):
    all_comments = []
    for video_id in v:
        try:
            comment_request = youtube.commentThreads().list(
                      part="snippet",
                      videoId=video_id,
                      maxResults=50
            )
            comment_response = comment_request.execute()
            for comment in comment_response['items']:
                comment=dict(comment_id = comment['snippet']['topLevelComment']['id'],
                             Video_id = comment['snippet']['videoId'],
                             comment_text= comment['snippet']['topLevelComment']['snippet']['textOriginal'],
                             comment_author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName'], 
                             comment_published =comment['snippet']['topLevelComment']['snippet']['publishedAt'] 
                 )
                all_comments.append(comment)
    
        except:
            print('Could not get comments for video ' + video_id)
    return all_comments

def main(channel_id):
    youtube = api_connect()
    CD =  channeldata(youtube,channel_id)
    playlist_details = playlist(youtube,channel_id)
    videoids=videoids_details(youtube,CD[0]['channel_playlistid'])
    video_details = videodetails(youtube,videoids) 
    comment_details = comments_details(youtube,videoids) 

    data = {"Channel_Details": CD,
            "Playlist_Details": playlist_details,
            "Video_Details": video_details,
            "Comments_Details": comment_details}
    return data

if selected == "Home":
    st.title("YouTube Data Harvessting and Warehousing")
    st.markdown("---")

    st.markdown("## Technologies Used:")
    st.markdown("- Python")
    st.markdown("- MongoDB")
    st.markdown("- YouTube Data API")
    st.markdown("- PostgreSQL")
    st.markdown("- Streamlit")
    st.markdown("---")

    st.markdown("## Overview:")
    st.markdown("First, we use the Google YouTube API to retrieve data from YouTube. The data is then pushed into MongoDB. After these procedures are finished, we retrieve the data from MongoDB once more, format it into a table, and then insert it into PostgreSQL.")
    st.markdown("---")

def create_tables():
    try:
        conn = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="ricky412",
                                database="youtube",
                                port="5432")
        cursor = conn.cursor()

        create_channel_details = """
        CREATE TABLE IF NOT EXISTS channel_details (
            id SERIAL PRIMARY KEY,
            channel_name VARCHAR(255),
            channel_id VARCHAR(255),
            channel_description TEXT,
            channel_view BIGINT,
            channel_subscriber BIGINT,
            channel_playlsitid VARCHAR(255),
            channel_videocount INT
        );
        """

        create_playlist_details = """
        CREATE TABLE IF NOT EXISTS playlist_details (
            id SERIAL PRIMARY KEY,
            playlist_id VARCHAR(255),
            channel_id VARCHAR(255),
            playlist_count INT,
            playlist_title VARCHAR(255),
            playlist_publishedate TIMESTAMP
        );
        """

        create_video_details = """
        CREATE TABLE IF NOT EXISTS video_details (
            id SERIAL PRIMARY KEY,
            video_id VARCHAR(255),
            channel_name VARCHAR(255),
            video_title VARCHAR(255),
            video_description TEXT,
            video_published TIMESTAMP,
            video_viewcount BIGINT,
            video_likecount BIGINT,
            video_commentcount BIGINT,
            video_duration INT,
            video_thumbnail TEXT,
            video_captionStatus VARCHAR(50)
        );
        """

        create_comment_details = """
        CREATE TABLE IF NOT EXISTS comment_details (
            id SERIAL PRIMARY KEY,
            comment_id VARCHAR(255),
            video_id VARCHAR(255),
            comment_text TEXT,
            comment_author VARCHAR(255),
            comment_published TIMESTAMP
        );
        """

        cursor.execute(create_channel_details)
        cursor.execute(create_playlist_details)
        cursor.execute(create_video_details)
        cursor.execute(create_comment_details)

        conn.commit()
        print("Tables created successfully!")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while creating tables:", error)

    finally:
        if conn is not None:
            cursor.close()
            conn.close()

create_tables()

if selected=="Migrate into MongoDB and PostgreSQL":
    st.title("Data harvesting")
    channel_id=st.text_input('Enter your channelID')
    if st.button('Get Data'):
        with st.spinner('Processing....'):
            st.success('Successfully data collected', icon="✅")

    if st.button('Migrate to Mongodb'):
        with st.spinner('Please Wait for while...'):
            data=main(channel_id)
            count=False
            for i in mycollection.find():
                if i['Channel_Details'][0]['channel_id']==channel_id:
                    st.warning('channel already exist',icon="⚠️")
                    count=True
                    break

            if count==False:
                mycollection.insert_one(data)
                st.success('Migrate into mongodb',icon="✅")

    st.markdown('## Select  channel name to insert into PostgreSql')

    ch=[]
    for i in mycollection.find():
        ch_details=i['Channel_Details'][0]['channel_name']
        ch.append(ch_details)
        
    user_input=st.selectbox('select channel name',options=ch)
    
    channel_data=[]
    channel_sql=[]
    playlist_sql=[]
    video_sql=[]
    comment_sql=[]

    for i in mycollection.find():
        if i['Channel_Details'][0]['channel_name']==user_input:
            channel_data.append(i)
    if st.button('insert into sql'):
        cursor.execute('select channel_name from channel_details')
        channelnames=[i[0] for i in cursor.fetchall()]
        
        if user_input in channelnames:
             st.warning('channel already inserted into PostgreSql',icon="⚠️")
        else:
            with st.spinner('please wait for a while'):
                    #Reterving channel data
                    channel_details={'channel_name':channel_data[0]['Channel_Details'][0]['channel_name'],
                                     'channel_id':channel_data[0]['Channel_Details'][0]['channel_id'],
                                     'channel_description':channel_data[0]['Channel_Details'][0]['channel_description'],
                                     'channel_view':channel_data[0]['Channel_Details'][0]['channel_view'],
                                     'channel_subscriber':channel_data[0]['Channel_Details'][0]['channel_subscriber'],
                                     'channel_playlsitid':channel_data[0]['Channel_Details'][0]['channel_playlistid'],
                                     'channel_videocount':channel_data[0]['Channel_Details'][0]['channel_videocount']}
                    channel_sql.append(channel_details)

                    for j in channel_data[0]['Playlist_Details']:
                         playlist_details={'playlist_id':j['playlist_id'],
                                           'channel_id':j['channel_id'],
                                           'playlist_count':j['playlist_count'],
                                           'playlist_title':j['playlist_title'],
                                           'playlist_publishedate':j['playlist_publishedate']}
                         playlist_sql.append(playlist_details)

                    for j in channel_data[0]['Video_Details']:
                        video_details={'video_id':j['video_id'],
                                       'channel_name':j['channel_name'],
                                       'video_title':j['video_title'],
                                       'video_description':j['video_description'],
                                       'video_published':j['video_published'],
                                       'video_viewcount':j['video_viewcount'],
                                       'video_likecount':j['video_likecount'],
                                       'video_commentcount':j['video_commentcount'],
                                       'video_duration':j['video_duration'],
                                       'video_thumbnail':j['video_thumbnail']}
                        video_sql.append(video_details)

                    for j in channel_data[0]['Comments_Details']:
                        comment_details={'comment_id':j['comment_id'],
                                         'video_id':j['Video_id'],
                                         'comment_text':j['comment_text'],
                                         'comment_author':j['comment_author'],
                                         'comment_published':j['comment_published']}
                        comment_sql.append(comment_details)

                    channel_sql=pd.DataFrame(channel_sql)  
                    playlist_sql=pd.DataFrame(playlist_sql) 
                    video_sql=pd.DataFrame(video_sql)
                    comment_sql=pd.DataFrame(comment_sql) 

                    channel_sql['channel_view']=pd.to_numeric(channel_sql['channel_view'])
                    channel_sql['channel_subscriber']=pd.to_numeric(channel_sql['channel_subscriber'])
                    channel_sql['channel_videocount']=pd.to_numeric(channel_sql['channel_videocount'])

                    playlist_sql['playlist_count']=pd.to_numeric(playlist_sql['playlist_count'])
                    playlist_sql['playlist_publishedate']=pd.to_datetime(playlist_sql['playlist_publishedate'])

                    video_sql['video_published']=pd.to_datetime(video_sql['video_published'])
                    video_sql['video_viewcount']=pd.to_numeric(video_sql['video_viewcount'])
                    video_sql['video_likecount']=pd.to_numeric(video_sql['video_likecount'])
                    video_sql['video_commentcount']=pd.to_numeric(video_sql['video_commentcount'])

                    for i in range(len(video_sql['video_duration'])):
                        duration=isodate.parse_duration(video_sql['video_duration'].loc[i])
                        seconds=duration.total_seconds()
                        video_sql.loc[i,'video_duration']=int(seconds)

                    video_sql['video_duration']=pd.to_numeric(video_sql['video_duration'])

                    comment_sql['comment_published']=pd.to_datetime(comment_sql['comment_published'])
    
                    db_url = 'postgresql://postgres:ricky412@localhost:5432/youtube'
                    engine = create_engine(db_url)
                
                    try:
                        channel_sql.to_sql('channel_details',engine,if_exists='append',index=False) 
                        playlist_sql.to_sql('playlist_details',engine,if_exists='append',index=False)
                        video_sql.to_sql('video_details',engine,if_exists='append',index=False)
                        comment_sql.to_sql('comment_details', engine, if_exists='append', index=False)
                        st.success('Successfully data push into PostgreSql', icon="✅")
                    except Exception as e:
                        st.error(f'Error inserting comment details into PostgreSql: {str(e)}')
    

if selected=='SQL Queries':
    st.write("## :black[Choose one of the questions below to see the answers.]")
    question=['1. Every video as well as the channel name',
             '2. Channels having the most videos',
             '3. Top 10 videos that have been viewed most',
             '4. Commentary for every video',
             '5. The most liked videos',
             '6. Likes of all videos',
             '7. Views for every channel',
             '8. Videos that were released in 2022',
             '9. The average length of each channel\'s videos',
             '10. Videos with the most comments']
    questions=st.selectbox('Select questions',question)
    
    if questions=='1. Every video as well as the channel name':
        if st.button('Get solution'):
            cursor.execute("select video_title,channel_name from video_details")
            df=[i for i in cursor.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['video_title','channel_name'],index=range(1, len(df) + 1)))
            st.success("DONE", icon="✅")

    elif questions=='2. Channels having the most videos':
        if st.button('Get solution'):
            cursor.execute("select channel_name,channel_videocount from channel_details order by channel_videocount desc")
            df=[i for i in cursor.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name','videocount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")

    elif questions=='3. Top 10 videos that have been viewed most':
        if st.button('Get solution'):
            cur.execute("select channel_name,video_viewcount from video_details order by video_viewcount desc limit 10")
            df=[i for i in cur.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name','video_viewcount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")

    elif questions=='4. Commentary for every video':
        if st.button('Get solution'):
            cursor.execute("select video_title,video_commentcount from video_details order by video_viewcount desc")
            df=[i for i in cursor.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['video_name','video_commentcount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")

    elif questions=='5. The most liked videos':
        if st.button('Get solution'):
            cursor.execute("select channel_name,video_likecount from video_details order by video_likecount desc limit 10")
            df=[i for i in cursor.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name','video_likecount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")

    elif questions=='6. Likes of all videos':
        if st.button('Get solution'):
            cursor.execute("select video_title as video_name, video_likecount as Like_count from video_details;")
            df=[i for i in cursor.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['video_name','likecount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")

    elif  questions=='7. Views for every channel':
        if st.button('Get solution'):
            cursor.execute("select channel_name,channel_view from channel_details order by channel_view desc")
            df=[i for i in cursor.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name','channel_view'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")

    elif questions=='8. Videos that were released in 2022':
        if st.button('Get solution'):
            cursor.execute("select distinct(channel_name) from video_details where year(video_published)=2022 order by channel_name")
            df=[i for i in cursor.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")

    elif questions == '9. The average length of each channel\'s videos':
        if st.button('Get solution'):
            cursor.execute("""
                SELECT 
                    channel_name,
                    CONCAT(
                        FLOOR(AVG(video_duration) / 3600), 'h ',
                        FLOOR(MOD(AVG(video_duration), 3600) / 60), 'm ',
                        MOD(AVG(video_duration), 60), 's'
                    ) AS average_duration
                FROM 
                    video_details 
                GROUP BY 
                    channel_name 
                ORDER BY 
                    average_duration DESC
            """)
            df = [i for i in cursor.fetchall()]
            st.dataframe(pd.DataFrame(df, columns=['channel_name', 'average_duration'], index=range(1, len(df) + 1)))
            st.success("DONE", icon="✅")

    elif questions=='10. Videos with the most comments':
        if st.button('Get solution'):
            cursor.execute("select channel_name,video_commentcount from video_details order by video_commentcount desc limit 200")
            df=[i for i in cursor.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name','video_commentcount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")

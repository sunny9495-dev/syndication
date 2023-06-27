import pickle
import random
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload
import io
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
import shutil
import json
import zipfile
import pandas as pd
from pathlib import Path
from moviepy.editor import *
from moviepy.config import change_settings
from PIL import Image
import os
import time
from datetime import datetime, timedelta
from moviepy.video.fx.all import crop
import requests
import logging
import logging.config
from moviepy.editor import VideoFileClip, concatenate_videoclips
import contextlib
# from multiprocessing import Pool
# import multiprocessing
from concurrent.futures import ProcessPoolExecutor as Executor
import itertools

change_settings({"IMAGEMAGICK_BINARY": r"imageMagick\\magick.exe"})

H = 1080
W = 1920
size = (1080, 1920)
HD_Size = (1280, 720)
SIZE = (W, H)
HX = H + H * .80
WX = W + W * .80
bold_font = 'Liberation-Sans-Bold'
plain_font = 'Liberation-Sans'
sansfont = r'Roboto-Black.ttf'

SCOPES = ['https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/documents.readonly',
          'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.apps.readonly',
          'https://www.googleapis.com/auth/drive.appdata']

def create_service(client_secret_file, api_name, api_version, *scopes, prefix=''):
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]

    creds = None
    working_dir = os.getcwd()
    token_dir = 'Output/Social'
    token_file = f'token_{API_SERVICE_NAME}_{API_VERSION}{prefix}.json'

    ### Check if token dir exists first, if not, create the folder
    if not os.path.exists(os.path.join(working_dir, token_dir)):
        os.mkdir(os.path.join(working_dir, token_dir))

    if os.path.exists(os.path.join(working_dir, token_dir, token_file)):
        creds = Credentials.from_authorized_user_file(os.path.join(working_dir, token_dir, token_file), SCOPES)
        # with open(os.path.join(working_dir, token_dir, token_file), 'rb') as token:
        #   cred = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(os.path.join(working_dir, token_dir, token_file), 'w') as token:
            token.write(creds.to_json())

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=creds, static_discovery=False)
        print(API_SERVICE_NAME, API_VERSION, 'service created successfully')
        return service
    except Exception as e:
        print(e)
        print(f'Failed to create service instance for {API_SERVICE_NAME}')
        os.remove(os.path.join(working_dir, token_dir, token_file))
        return None

def slide_out(clip, duration, height, counter):
    def calc(t, counter, duration, h):
        ts = t - (counter * duration)
        val = min(-45, h * (duration - ts))
        return ('center', val)

    return clip.set_pos(lambda t: calc(t, counter, duration, height))


def add_transition(clip_size, counter, clip):
    # reverse the count to get slide number.
    counter = clip_size - 1 - counter
    return slide_out(clip.resize(height=HX, width=WX), 3, HX, counter)


def get_gdrive_service():
    creds = None
    if os.path.exists('token11.pickle'):
        with open('token11.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                r'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token11.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return build('drive', 'v3', credentials=creds)

def get_files_from_folder(folderID):
    files = []
    for i in range(5):
        try:
            folderquery = "'" + folderID + "'" + " in parents"
            service = get_gdrive_service()
            request = service.files().list(q=folderquery,
                                           spaces='drive',
                                           fields='files(id, name)').execute()
            for j in request['files']:
                files.append(j['id'])
            if len(files):
                break
        except:
            pass
    return files

def download_file_from_drive(ID, save_to_loc):
    introdestination_file = ''
    for i in range(5):
        try:
            service = get_gdrive_service()
            request = service.files().get_media(fileId=ID)
            filename = os.path.splitext(service.files().get(fileId=ID,
                                                            fields='*').execute()['name'])[0]
            fh = io.BytesIO()

            downloader = MediaIoBaseDownload(fh, request)
            introdestination_file = ''
            done = False

            while done is False:
                status, done = downloader.next_chunk()
            fh.seek(0)
            introdestination_file = os.path.join(save_to_loc, filename)
            with open(fr'{introdestination_file}.mp4', 'wb') as f:
                shutil.copyfileobj(fh, f)
            break
        except:
            pass
    return introdestination_file

def upload_to_drive(upload_file, parentID):
    uploadID = ''
    for i in range(5):
        try:
            filename = os.path.basename(upload_file)
            print(filename)
            file_metadata = {
                'name': filename,
                'parents': [parentID]
            }
            media = MediaFileUpload(upload_file, mimetype='video/mp4')
            service = get_gdrive_service()
            file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            uploadID = file['id']
            media = None
            break
        except:
            pass
    return uploadID

def edit_intro_outro_videos(videoloc, foldertoSave):
    if not os.path.exists(os.path.join(foldertoSave) + "/vid-Edited1.mp4"):
        clip = VideoFileClip(fr'{videoloc}')
        (w, h) = clip.size
        crop_width = h * 9 / 16
        cropped_clip = crop(clip, width=crop_width, height=h, x_center=w / 2, y_center=h / 2)
        cropped_output = os.path.join(foldertoSave) + "/vid-Edited.mp4"
        final_cropped_output = os.path.join(foldertoSave) + "/vid-Edited1.mp4"
        cropped_clip.write_videofile(cropped_output, verbose=False, logger=None)
        clip = VideoFileClip(cropped_output)
        clip_resized = clip.resize(width=1080, height=1920)
        clip_resized.write_videofile(final_cropped_output, verbose=False, logger=None)
    if not os.path.exists(os.path.join(foldertoSave) + "/vid-HD-Edited1.mp4"):
        hd_final_cropped_output = os.path.join(foldertoSave) + "/vid-HD-Edited1.mp4"
        clip = VideoFileClip(fr'{videoloc}')
        resized = clip.resize((1280, 720))
        resized.write_videofile(hd_final_cropped_output, verbose=False, logger=None)
        resized.close()


def add_music_to_vid(videoloc, hdvid, foldertoSave):
    # List all audio files
    listDir = os.listdir(r"Audio")
    audiofiles = []
    for i in listDir:
        audiofiles.append(fr"Audio/{i}")
    videoclip = VideoFileClip(videoloc)
    audioclip = AudioFileClip(random.choice(audiofiles)).subclip(15, 55)
    new_audioclip = CompositeAudioClip([audioclip])
    videoclip.audio = new_audioclip
    save_as = os.path.join(foldertoSave) + '/new-video.mp4'
    videoclip.write_videofile(save_as, verbose=False, logger=None)
    videoclip.close()
    #Hd Vid
    videoclip = VideoFileClip(hdvid)
    audioclip = AudioFileClip(random.choice(audiofiles)).subclip(15, 55)
    new_audioclip = CompositeAudioClip([audioclip])
    videoclip.audio = new_audioclip
    save_as = os.path.join(foldertoSave) + '/new-hd-video.mp4'
    videoclip.write_videofile(save_as, verbose=False, logger=None)
    videoclip.close()

def join_video_clips(vidList, foldertoSave, agent):
    final_output = os.path.join(foldertoSave) + f"/{agent}-finaloutput.mp4"
    final_clip = concatenate_videoclips(vidList, method='compose')
    final_clip.to_videofile(final_output, fps=60, remove_temp=True, verbose=False, logger=None)

def join_hd_video_clips(vidList, foldertoSave, agent):
    final_output = os.path.join(foldertoSave) + f"/{agent}-hd-finaloutput.mp4"
    final_clip = concatenate_videoclips(vidList, method='compose')
    final_clip.to_videofile(final_output, fps=60, remove_temp=True, verbose=False, logger=None)

def uploadvideotodrive(videoloc, hdvid, agentname, introID, outroID, output, socialID, logID, videoTitle, videoDesc, videoTags):
    if os.path.exists(agentname):
        agentname = f"{agentname}_{random.randint(1, 99)}"
        os.makedirs(agentname)
    else:
        os.makedirs(agentname)
    print(f"{agentname}: introID: {introID}")
    print(f"{agentname}:outroID: {outroID}")
    introFolder = os.path.join(agentname) + '/Intros'
    outroFolder = os.path.join(agentname) + '/Outros'
    outputFolder = os.path.join(agentname) + '/Finaloutput'
    socialFolder = os.path.join(agentname) + '/Social'
    logFolder = os.path.join(agentname) + "/Log"
    os.makedirs(introFolder)
    os.makedirs(outroFolder)
    os.makedirs(outputFolder)
    os.makedirs(socialFolder)
    os.makedirs(logFolder)
    for i in range(10):
        try:
            # Start the process of adding intro and video
            timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            logname = rf'{logFolder}\Log-{timenow}.txt'
            file = open(logname, "w")
            file.close()
            logdata = []
            # Get Intro Files
            Intros = get_files_from_folder(introID)
            if len(Intros):
                timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                logdata.append(f"{timenow} - Download intro video")
                introVid = random.choice(Intros)
                introdestination_file = download_file_from_drive(introVid, introFolder)
                timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                logdata.append(f"{timenow} - Editing intro video to 9/16")
                edit_intro_outro_videos(fr'{introdestination_file}.mp4', introFolder)
            Outros = get_files_from_folder(outroID)
            if len(Outros):
                print(f"{agentname} downloading outro videos: {len(Outros)}")
                timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                logdata.append(f"{timenow} - Download outro video")
                outroVid = random.choice(Outros)
                introdestination_file = download_file_from_drive(outroVid, outroFolder)
                timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                logdata.append(f"{timenow} - Editing outro video to 9/16")
                edit_intro_outro_videos(fr'{introdestination_file}.mp4', outroFolder)
            timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            logdata.append(f"{timenow} - Add music to videos")
            # Add music to video
            add_music_to_vid(videoloc, hdvid, outputFolder)
            save_as = os.path.join(outputFolder) + '/new-video.mp4'
            introFile = os.path.join(introFolder) + "/vid-Edited1.mp4"
            outroFile = os.path.join(outroFolder) + "/vid-Edited1.mp4"
            if not os.path.exists(introFile) and not os.path.exists(outroFile):
                final_output = os.path.join(outputFolder) + f"/{agentname}-finaloutput.mp4"
                clip = VideoFileClip(save_as)
                clip.close()
                shutil.move(save_as, final_output)
            else:
                L = []
                if os.path.exists(introFile):
                    clip = VideoFileClip(introFile)
                    L.append(clip)
                clip = VideoFileClip(save_as)
                L.append(clip)
                if os.path.exists(outroFile):
                    clip = VideoFileClip(outroFile)
                    L.append(clip)
                final_output = os.path.join(outputFolder) + f"/{agentname}-finaloutput.mp4"
                join_video_clips(L, outputFolder, agentname)
            if os.path.exists(introFile):
                clip = VideoFileClip(introFile)
                clip.close()
            if os.path.exists(save_as):
                video = VideoFileClip(save_as)
                video.close()
            if os.path.exists(outroFile):
                clip = VideoFileClip(outroFile)
                clip.close()
            upload_id = upload_to_drive(final_output, output)
            timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            logdata.append(
                f"{timenow} - Uploaded video to Google Drive: https://drive.google.com/file/d/{upload_id}/view?usp=drive_link")
            save_as = os.path.join(outputFolder) + '/new-hd-video.mp4'
            introFile = os.path.join(introFolder) + "/vid-HD-Edited1.mp4"
            outroFile = os.path.join(outroFolder) + "/vid-HD-Edited1.mp4"
            if not os.path.exists(introFile) and not os.path.exists(outroFile):
                final_output = os.path.join(outputFolder) + f"/{agentname}-hd-finaloutput.mp4"
                clip = VideoFileClip(save_as)
                clip.close()
                shutil.move(save_as, final_output)
            else:
                L = []
                if os.path.exists(introFile):
                    clip = VideoFileClip(introFile)
                    L.append(clip)
                clip = VideoFileClip(save_as)
                L.append(clip)
                if os.path.exists(outroFile):
                    clip = VideoFileClip(outroFile)
                    L.append(clip)
                final_output = os.path.join(outputFolder) + f"/{agentname}-hd-finaloutput.mp4"
                join_hd_video_clips(L, outputFolder, agentname)
            if os.path.exists(introFile):
                clip = VideoFileClip(introFile)
                clip.close()
            if os.path.exists(save_as):
                video = VideoFileClip(save_as)
                video.close()
            if os.path.exists(outroFile):
                clip = VideoFileClip(outroFile)
                clip.close()
            upload_id = upload_to_drive(final_output, output)
            timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            logdata.append(f"{timenow} - Uploaded HD video to Google Drive: https://drive.google.com/file/d/{upload_id}/view?usp=drive_link")
            media = None
            # Get all socialmedia data
            folderquery = "'" + socialID + "'" + " in parents"
            service = get_gdrive_service()
            request = service.files().list(q=folderquery,
                                           spaces='drive',
                                           fields='files(id, name)').execute()
            youtubeID = ''
            fbID = ''
            if len(request):
                for i in request['files']:
                    if i['name'].endswith('.json'):
                        youtubeID = i['id']
                    else:
                        fbID = i['id']
            # Dowload YT Json File
            if len(youtubeID):
                introdestination_file = ''
                service = get_gdrive_service()
                request = service.files().get_media(fileId=youtubeID)
                filename = os.path.splitext(service.files().get(fileId=youtubeID,
                                                                fields='*').execute()['name'])[0]
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                try:
                    while done is False:
                        status, done = downloader.next_chunk()
                        print("Download %d%%." % int(status.progress() * 100))
                    fh.seek(0)
                    introdestination_file = os.path.join(socialFolder, filename)
                    with open(fr'{introdestination_file}.json', 'wb') as f:
                        shutil.copyfileobj(fh, f)
                except:
                    pass
                final_output = os.path.join(outputFolder) + f"/{agentname}-finaloutput.mp4"
                clip = VideoFileClip(final_output)
                duration = round(clip.duration)
                clip.close()
                if duration <= 60:
                    try:
                        # Upload video to YT
                        YTtitle = "#shorts" + " " + videoTitle
                        YTDesc = "#shorts" + " " + videoDesc
                        Tags = videoTags
                        upload_time = (datetime.now() + timedelta(days=10)).isoformat() + '.000Z'
                        request_body = {
                            'snippet': {
                                'title': YTtitle,
                                'description': YTDesc,
                                'categoryId': '22',
                                'tags': Tags
                            },

                            'status': {
                                'privacyStatus': 'public',
                                'publishedAt': upload_time,
                                'selfDeclaredMadeForKids': False
                            },
                            'notifySubscribers': True
                        }
                        API_NAME = 'youtube'
                        API_VERSION = 'v3'
                        SCOPES = ['https://www.googleapis.com/auth/youtube']
                        client_file = fr'{introdestination_file}.json'
                        service = create_service(client_file, API_NAME, API_VERSION, SCOPES)
                        media_file = MediaFileUpload(final_output)
                        response_video_upload = service.videos().insert(
                            part='snippet,status',
                            body=request_body,
                            media_body=media_file
                        ).execute()
                        uploaded_video_id = response_video_upload.get('id')
                        timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                        logdata.append(
                            f"{timenow} - Uploaded shorts video to YouTube: https://youtube.com/shorts/{uploaded_video_id}")
                        media_file = None
                    except:
                        timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                        logdata.append(
                            f"{timenow} - Uploaded to Youtube : Failed to upload to YouTube")
                    media_file = None
                media_file = None
                time.sleep(5)
                final_output = os.path.join(outputFolder) + f"/{agentname}-hd-finaloutput.mp4"
                try:
                    # Upload HD video to YT
                    YTtitle = videoTitle
                    YTDesc = videoDesc
                    Tags = videoTags
                    upload_time = (datetime.now() + timedelta(days=10)).isoformat() + '.000Z'
                    request_body = {
                        'snippet': {
                            'title': YTtitle,
                            'description': YTDesc,
                            'categoryId': '22',
                            'tags': Tags
                        },

                        'status': {
                            'privacyStatus': 'public',
                            'publishedAt': upload_time,
                            'selfDeclaredMadeForKids': False
                        },
                        'notifySubscribers': True
                    }
                    API_NAME = 'youtube'
                    API_VERSION = 'v3'
                    SCOPES = ['https://www.googleapis.com/auth/youtube']
                    client_file = fr'{introdestination_file}.json'
                    service = create_service(client_file, API_NAME, API_VERSION, SCOPES)
                    media_file = MediaFileUpload(final_output)
                    response_video_upload = service.videos().insert(
                        part='snippet,status',
                        body=request_body,
                        media_body=media_file
                    ).execute()
                    uploaded_video_id = response_video_upload.get('id')
                    timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                    logdata.append(
                        f"{timenow} - Uploaded HD video to YouTube: https://youtube.com/video/{uploaded_video_id}")
                    media_file = None
                except:
                    timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                    logdata.append(
                        f"{timenow} - Uploaded to Youtube : Failed to upload to YouTube")
                media_file = None
                time.sleep(5)
            media_file = None
            # Upload FaceBook
            if len(fbID):
                introdestination_file = ''
                service = get_gdrive_service()
                request = service.files().get_media(fileId=fbID)
                filename = os.path.splitext(service.files().get(fileId=fbID,
                                                                fields='*').execute()['name'])[0]
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                try:
                    while done is False:
                        status, done = downloader.next_chunk()
                        print("Download %d%%." % int(status.progress() * 100))
                    fh.seek(0)
                    introdestination_file = os.path.join(socialFolder, filename)
                    with open(fr'{introdestination_file}.xlsx', 'wb') as f:
                        shutil.copyfileobj(fh, f)
                except:
                    pass
                excel_path = fr'{introdestination_file}.xlsx'
                df = pd.read_excel(excel_path)
                pageID = df['PageID'].tolist()[0]
                access_token = df['Access Token'].tolist()[0]
                url = f'https://graph-video.facebook.com/v17.0/{pageID}/videos'
                files = {'source': open(final_output, 'rb')}
                try:
                    payload = {
                        'access_token': access_token,
                        'title': videoTitle,
                        'description': videoDesc,
                    }

                    response = requests.post(url, files=files, data=payload, verify=False).json()
                    video_id = response['id']
                    # response.close()
                    if video_id:
                        print(f'https://www.facebook.com/{pageID}/videos/{video_id}')
                        timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                        logdata.append(
                            f"{timenow} - Uploaded to Facebook: https://www.facebook.com/{pageID}/videos/{video_id}")
                except:
                    timenow = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                    logdata.append(
                        f"{timenow} - Failed to upload to Facebook")
                files.clear()
            # Upload Logs
            with open(logname, 'w') as f:
                for lines in logdata:
                    f.write(f"{lines}\n")
                f.close()
            print(f"{agentname}: at logs")
            filename = os.path.basename(logname)
            file_metadata = {
                'name': filename,
                'parents': [logID]
            }
            media = MediaFileUpload(logname, mimetype='text/plain')
            try:
                service = get_gdrive_service()
                file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            except:
                pass
            media = None
            final_output = os.path.join(outputFolder) + f"/{agentname}-finaloutput.mp4"
            if os.path.exists(final_output):
                clip = VideoFileClip(final_output)
                clip.close()
            final_output = os.path.join(outputFolder) + f"/{agentname}-hd-finaloutput.mp4"
            if os.path.exists(final_output):
                clip = VideoFileClip(final_output)
                clip.close()
            final_output = os.path.join(outputFolder) + "/new-hd-video.mp4"
            if os.path.exists(final_output):
                clip = VideoFileClip(final_output)
                clip.close()
            final_output = os.path.join(outputFolder) + "/new-video.mp4"
            if os.path.exists(final_output):
                clip = VideoFileClip(final_output)
                clip.close()
            final_output = os.path.join(introFolder) + "/vid-HD-Edited1.mp4"
            if os.path.exists(final_output):
                clip = VideoFileClip(final_output)
                clip.close()
            final_output = os.path.join(outroFolder) + "/vid-HD-Edited1.mp4"
            if os.path.exists(final_output):
                clip = VideoFileClip(final_output)
                clip.close()
            final_output = os.path.join(introFolder) + "/vid-Edited1.mp4"
            if os.path.exists(final_output):
                clip = VideoFileClip(final_output)
                clip.close()
            final_output = os.path.join(outroFolder) + "/vid-Edited1.mp4"
            if os.path.exists(final_output):
                clip = VideoFileClip(final_output)
                clip.close()
            #Delete all media
            vid_list = os.listdir(introFolder)
            with contextlib.ExitStack() as stack:
                for name in vid_list:
                    clip = VideoFileClip(fr'{introFolder}/{name}')
                    stack.enter_context(contextlib.closing(clip))
            for name in vid_list:
                os.remove(fr'{introFolder}/{name}')
            vid_list = os.listdir(outroFolder)
            with contextlib.ExitStack() as stack:
                for name in vid_list:
                    clip = VideoFileClip(fr'{outroFolder}/{name}')
                    stack.enter_context(contextlib.closing(clip))
            for name in vid_list:
                os.remove(fr'{outroFolder}/{name}')
            vid_list = os.listdir(outputFolder)
            with contextlib.ExitStack() as stack:
                for name in vid_list:
                    clip = VideoFileClip(fr'{outputFolder}/{name}')
                    stack.enter_context(contextlib.closing(clip))
            for name in vid_list:
                os.remove(fr'{outputFolder}/{name}')
            shutil.rmtree(agentname, ignore_errors=False, onerror=None)
            break
        except:
            pass

def get_main_folders():
    service = get_gdrive_service()
    folderid = '12y4sG1dlMN9nRTD0o7tu4rl4PiIKoax9'
    folderquery = "'" + folderid + "'" + " in parents and mimeType='application/vnd.google-apps.folder'"
    childrenFoldersDict = service.files().list(q=folderquery,
                                               spaces='drive',
                                               fields='files(id, name)').execute()
    return childrenFoldersDict['files']

def myprogramcallback(result):
    print("Process completed")

# Endless Loop Starts Here
def Main():
    while True:
        with open('camps.json') as j:
            json_data = json.load(j)

        get_all_folders = get_main_folders()
        all_folder_ids = []

        for i in get_all_folders:
            all_folder_ids.append((i['id'], i['name']))

        for idx, i in enumerate(all_folder_ids):
            if len(json_data['all_agents']) and len(json_data['all_agents']) > idx:
                for jdx, y in enumerate(json_data['all_agents']):
                    # print(json_data['all_agents'][jdx]['Agent ID'])
                    if json_data['all_agents'][jdx]['Agent ID'] == i[0]:
                        folderid = json_data['all_agents'][idx]['Real Estate ID']
                        folderquery = "'" + folderid + "'" + " in parents and mimeType='application/vnd.google-apps.folder'"
                        service = get_gdrive_service()
                        getFolders = service.files().list(q=folderquery,
                                                          spaces='drive',
                                                          fields='files(id, name)').execute()
                        for k in getFolders['files']:
                            dbIntros = []
                            if k['name'] == 'Listing Media' or k['name'] == 'New Listing Media':
                                folderquery = "'" + k['id'] + "'" + " in parents"
                                service = get_gdrive_service()
                                introFiles = service.files().list(q=folderquery,
                                                                  spaces='drive',
                                                                  fields='files(id)').execute()
                                for l in introFiles['files']:
                                    dbIntros.append(l['id'])
                                jsonMedia = json_data['all_agents'][idx]['Real Estate']['New Listing Media']
                                newMedia = [item for item in dbIntros if item not in jsonMedia]
                                print(folderid, k['id'])
                                print(newMedia)
                                if len(newMedia):
                                    print("Found new media")
                                    outputID = json_data['all_agents'][idx]['Output']
                                    json_data['all_agents'][idx]['Real Estate']['New Listing Media'] = jsonMedia + newMedia
                                    with open('camps.json', 'w') as f:
                                        json.dump(json_data, f)
                                    for z in newMedia:
                                        filename = os.path.splitext(
                                            service.files().get(fileId=z,
                                                                fields='*').execute()['name'])[0]
                                        print(filename)
                                        # download_zip_folder(z, filename)
                                        downloadname = filename
                                        downloadID = z
                                        if not os.path.exists(downloadname):
                                            os.makedirs(downloadname)
                                        service = get_gdrive_service()
                                        request = service.files().get_media(fileId=downloadID)
                                        fh = io.BytesIO()
                                        downloader = MediaIoBaseDownload(fh, request)
                                        done = False
                                        while done is False:
                                            status, done = downloader.next_chunk()
                                            print("Download %d%%." % int(status.progress() * 100))
                                        fh.seek(0)
                                        destination_file = os.path.join(downloadname)
                                        with open(fr'{destination_file}.zip', 'wb') as f:
                                            shutil.copyfileobj(fh, f)
                                        with zipfile.ZipFile(fr'{destination_file}.zip') as zf:
                                            zf.extractall(fr'{destination_file}')
                                        if os.path.exists(fr'{destination_file}.zip'):
                                            os.remove(fr'{destination_file}.zip')
                                        # change destination file if it is a folder
                                        list_files = os.listdir(destination_file)
                                        if len(list_files) == 1:
                                            for i in list_files:
                                                if os.path.isdir(fr'{list_files[0]}'):
                                                    destination_file = fr'{destination_file}/{list_files[0]}'
                                                    print("Destination file is", destination_file)

                                        # get excel sheet data
                                        x = [f for f in os.listdir(fr'{destination_file}') if f.endswith('.xlsx')][0]
                                        excel_path = fr'{destination_file}/{x}'
                                        df = pd.read_excel(excel_path)
                                        videotext = df['Video Text'].tolist()
                                        totalVids = df['No of Videos'].tolist()[0]
                                        videoTitle = df['YouTube/Facebook Title'].tolist()[0]
                                        videoDesc = df['YouTube/Facebook Description'].tolist()[0]
                                        videoTags = df['YouTube/Facebook Tags'].tolist()[0]
                                        # Load json file to get all parend ID's
                                        all_images = []
                                        img_list = os.listdir(destination_file)
                                        for a in img_list:
                                            all_images.append(fr'{destination_file}\{a}')
                                        all_images = [f for f in all_images if not f.endswith('.xlsx')]

                                        logging.info("Image cropping in progress")

                                        # Crop images and save to output
                                        cropoutput = f'Output/cropOutput'
                                        if not os.path.exists(cropoutput):
                                            os.mkdir(cropoutput)
                                        for ddx, d in enumerate(all_images):
                                            filename = Path(d).stem
                                            im = Image.open(d)
                                            # if len(all_images) > 10:
                                            #     im = Image.open(random.choice(all_images))
                                            # else:
                                            #     im = Image.open(d)
                                            if im.width >= 5469:
                                                cropping = (2160, 3840)
                                            else:
                                                cropping = (1080, 1920)
                                            left = round((im.size[0] - cropping[0]) / 2)
                                            top = round((im.size[1] - cropping[1]) / 2)
                                            im = im.crop((left, top, cropping[0] + left, cropping[1] + top))
                                            im.save(fr'{cropoutput}\{filename}.png')
                                            im.close()
                                            if ddx >= 7:
                                                break
                                        # Add all edited images to list
                                        img_list = os.listdir(cropoutput)
                                        all_images = []
                                        for d in img_list:
                                            all_images.append(fr'{cropoutput}\{d}')
                                        logging.info("Converting images to videos")

                                        # Convert images to video
                                        vidoutput = r'Output/vidOut'
                                        if not os.path.exists(vidoutput):
                                            os.mkdir(vidoutput)
                                        counter = 0
                                        setStart = 0
                                        setDuration = 5
                                        txtClipDuration = 1
                                        txtClipOne = 3
                                        txtClipTwo = 4
                                        clips = []
                                        for i in all_images:
                                            filename = Path(i).stem
                                            slide = ImageClip(i).set_fps(25).set_duration(5).resize(size)

                                            slide = slide.resize(lambda t: 1 + 0.04 * t)
                                            slide = slide.set_position(('center', 'center'))
                                            slide = CompositeVideoClip([slide], size=size)
                                            slide.write_videofile(f'{vidoutput}/{filename}.mp4')
                                            slide.close()
                                            clip = VideoFileClip(f'{vidoutput}/{filename}.mp4').set_duration(5).set_start(
                                                setStart)
                                            try:
                                                txt_2 = TextClip(str(videotext[counter]), font=sansfont, color='white',
                                                                 fontsize=80,
                                                                 interline=9).set_duration(6).set_start(
                                                    txtClipDuration).set_pos(('right', 360)).crossfadein(.3)
                                                counter += 1
                                            except:
                                                pass
                                            try:
                                                stxt_2 = TextClip(str(videotext[counter]), font=sansfont, color='red',
                                                                  bg_color='yellow', fontsize=80,
                                                                  interline=9).set_duration(6).set_start(
                                                    txtClipOne).set_pos(('right', 550)).crossfadein(.3)
                                                counter += 1
                                            except:
                                                pass
                                            try:
                                                stxt_3 = TextClip(str(videotext[counter]), font=sansfont, color='white',
                                                                  bg_color='blue', fontsize=80,
                                                                  interline=9).set_duration(6).set_start(
                                                    txtClipTwo).set_pos(('left', 1300)).crossfadein(.3)
                                                counter += 1
                                            except:
                                                pass
                                            clip.close()
                                            slide_2 = CompositeVideoClip([clip, txt_2, stxt_2, stxt_3]).set_duration(
                                                setDuration)
                                            clips.append(slide_2)
                                            slide_2.close()
                                            setStart += 5
                                            setDuration += 5
                                            txtClipDuration += 5
                                            txtClipOne += 5
                                            txtClipTwo += 5

                                        # Join Video Clips
                                        logging.info("Appending all video clips to one video")
                                        imgvidoutput = r'Output/ImagestoVideo'
                                        if not os.path.exists(imgvidoutput):
                                            os.mkdir(imgvidoutput)
                                        final_clip = CompositeVideoClip(clips, size=size).set_duration(40)
                                        final_clip.write_videofile(fr"{imgvidoutput}/video.mp4", fps=25, codec="h264_nvenc",
                                                                   audio_codec="aac")
                                        final_clip.close()
                                        videoloc = fr"{imgvidoutput}/video.mp4"
                                        #HD Vid
                                        img_list = os.listdir(cropoutput)
                                        all_images = []
                                        for d in img_list:
                                            all_images.append(fr'{cropoutput}\{d}')
                                        all_images = all_images[0:8]
                                        vidoutput = r'Output/vidOut'
                                        if not os.path.exists(vidoutput):
                                            os.mkdir(vidoutput)
                                        counter = 0
                                        setStart = 0
                                        setDuration = 5
                                        txtClipDuration = 1
                                        txtClipOne = 3
                                        txtClipTwo = 4
                                        clips = []
                                        for i in all_images:
                                            filename = Path(i).stem
                                            slide = ImageClip(i).set_fps(25).set_duration(5).resize(HD_Size)

                                            slide = slide.resize(lambda t: 1 + 0.04 * t)
                                            slide = slide.set_position(('center', 'center'))
                                            slide = CompositeVideoClip([slide], size=HD_Size)
                                            slide.write_videofile(f'{vidoutput}/{filename}.mp4')
                                            slide.close()
                                            clip = VideoFileClip(f'{vidoutput}/{filename}.mp4').set_duration(
                                                5).set_start(setStart)
                                            try:
                                                txt_2 = TextClip(str(videotext[counter]), font=sansfont, color='white',
                                                                 fontsize=80,
                                                                 interline=9).set_duration(6).set_start(
                                                    txtClipDuration).set_pos(((700, 100))).crossfadein(.3)
                                                counter += 1
                                            except:
                                                pass
                                            try:
                                                stxt_2 = TextClip(str(videotext[counter]), font=sansfont, color='red',
                                                                  bg_color='yellow', fontsize=80,
                                                                  interline=9).set_duration(6).set_start(
                                                    txtClipOne).set_pos((700, 350)).crossfadein(.3)
                                                counter += 1
                                            except:
                                                pass
                                            try:
                                                stxt_3 = TextClip(str(videotext[counter]), font=sansfont, color='white',
                                                                  bg_color='blue', fontsize=80,
                                                                  interline=9).set_duration(6).set_start(
                                                    txtClipTwo).set_pos((300, 600)).crossfadein(.3)
                                                counter += 1
                                            except:
                                                pass
                                            clip.close()
                                            slide_2 = CompositeVideoClip([clip, txt_2, stxt_2, stxt_3]).set_duration(
                                                setDuration)
                                            clips.append(slide_2)
                                            slide_2.close()
                                            setStart += 5
                                            setDuration += 5
                                            txtClipDuration += 5
                                            txtClipOne += 5
                                            txtClipTwo += 5
                                        # Join Video Clips
                                        logging.info("Appending all video clips to one video")
                                        imgvidoutput = r'Output/ImagestoVideo'
                                        if not os.path.exists(imgvidoutput):
                                            os.mkdir(imgvidoutput)
                                        final_clip = CompositeVideoClip(clips, size=HD_Size).set_duration(40)
                                        final_clip.write_videofile(fr"{imgvidoutput}/hd-video.mp4", fps=25,
                                                                   codec="h264_nvenc", audio_codec="aac")
                                        final_clip.close()
                                        hdvideoloc = fr"{imgvidoutput}/hd-video.mp4"

                                        with open('camps.json') as j:
                                            json_data = json.load(j)
                                        allagents = []
                                        allintroID = []
                                        alloutroID = []
                                        alloutput = []
                                        allsocialID = []
                                        alllogID = []
                                        for jdx, y in enumerate(json_data['all_agents']):
                                            allagents.append(json_data['all_agents'][jdx]['Agent'])
                                            allintroID.append(json_data['all_agents'][jdx]['Real Estate']['IntroID'])
                                            alloutroID.append(json_data['all_agents'][jdx]['Real Estate']['OutroID'])
                                            alloutput.append(json_data['all_agents'][jdx]['Output'])

                                            allsocialID.append(json_data['all_agents'][jdx]['Social Accounts'])
                                            alllogID.append(json_data['all_agents'][jdx]['Logs'])
                                        with Executor(max_workers=3) as executor:
                                            for agent, intro, outro, output, social, log in itertools.zip_longest(allagents, allintroID, alloutroID, alloutput, allsocialID, alllogID):
                                                executor.submit(uploadvideotodrive, videoloc, hdvideoloc, agent, intro, outro, output, social, log, videoTitle, videoDesc, videoTags)
                                            executor.shutdown(wait=True)
                                        videoloc = VideoFileClip(videoloc)
                                        videoloc.close()
                                        vid_list = os.listdir(r"Output/ImagestoVideo")
                                        with contextlib.ExitStack() as stack:
                                            for name in vid_list:
                                                clip = VideoFileClip(fr'Output/ImagestoVideo/{name}')
                                                stack.enter_context(contextlib.closing(clip))
                                        vid_list = os.listdir(r"Output/vidOut")
                                        with contextlib.ExitStack() as stack:
                                            for name in vid_list:
                                                clip = VideoFileClip(fr'Output/vidOut/{name}')
                                                stack.enter_context(contextlib.closing(clip))
                                        print("Deleting folders")
                                        shutil.rmtree(r"Output/cropOutput", ignore_errors=False, onerror=None)
                                        shutil.rmtree(r"Output/ImagestoVideo", ignore_errors=False, onerror=None)
                                        shutil.rmtree(r"Output/vidOut", ignore_errors=False, onerror=None)
            else:
                new_data = {
                    'Agent': i[1],
                    'Agent ID': i[0]
                }
                service = get_gdrive_service()
                folderid = i[0]
                folderquery = "'" + folderid + "'" + " in parents and mimeType='application/vnd.google-apps.folder'"
                getFolders = service.files().list(q=folderquery,
                                                  spaces='drive',
                                                  fields='files(id, name)').execute()
                all_subfolders = []
                subfolders = getFolders
                print(subfolders)
                for i in subfolders['files']:
                    if i['name'] == 'Real Estate' or i['name'] == 'Real estate':
                        new_data['Real Estate ID'] = i['id']
                        new_data['Real Estate'] = {}
                        folderid = i['id']
                        folderquery = "'" + folderid + "'" + " in parents and mimeType='application/vnd.google-apps.folder'"
                        getFolders = service.files().list(q=folderquery,
                                                          spaces='drive',
                                                          fields='files(id, name)').execute()
                        nest_subFolders = []
                        getsubfolders = getFolders
                        for j in getsubfolders['files']:
                            if j['name'] == 'Intro':
                                # new_data['Real Estate'] = {'IntroID': j['id']}
                                new_data['Real Estate'] = {**new_data['Real Estate'], **{'IntroID': j['id']}}

                                folderid = j['id']
                                new_data['Real Estate'][j['name']] = []
                                folderquery = "'" + folderid + "'" + " in parents"
                                getFolders = service.files().list(q=folderquery,
                                                                  spaces='drive',
                                                                  fields='files(id, name)').execute()
                                getallfiles = getFolders
                                for i in getallfiles['files']:
                                    new_data['Real Estate'][j['name']].append(i['id'])
                            elif j['name'] == 'Outro':
                                new_data['Real Estate'] = {**new_data['Real Estate'], **{'OutroID': j['id']}}
                                folderid = j['id']
                                new_data['Real Estate'][j['name']] = []
                                folderquery = "'" + folderid + "'" + " in parents"
                                getFolders = service.files().list(q=folderquery,
                                                                  spaces='drive',
                                                                  fields='files(id, name)').execute()
                                getallfiles = getFolders
                                for i in getallfiles['files']:
                                    new_data['Real Estate'][j['name']].append(i['id'])
                                # new_data['Real Estate'] = {'OutroID': j['id']}
                            elif j['name'] == 'Listing Media' or j['name'] == 'New Listing Media':
                                # new_data['Real Estate'].update({'MediaID': j['id']})
                                new_data['Real Estate'] = {**new_data['Real Estate'], **{'MediaID': j['id']}}
                                folderid = j['id']
                                new_data['Real Estate'][j['name']] = []
                                folderquery = "'" + folderid + "'" + " in parents and mimeType='application/vnd.google-apps.folder'"
                                getFolders = service.files().list(q=folderquery,
                                                                  spaces='drive',
                                                                  fields='files(id, name)').execute()
                                getallfiles = getFolders
                                for i in getallfiles['files']:
                                    new_data['Real Estate'][j['name']].append(i['id'])
                    elif i['name'] == 'Land':
                        new_data['Land ID'] = i['id']
                        new_data[i['name']] = {}
                        folderid = i['id']
                        folderquery = "'" + folderid + "'" + " in parents and mimeType='application/vnd.google-apps.folder'"
                        getFolders = service.files().list(q=folderquery,
                                                          spaces='drive',
                                                          fields='files(id, name)').execute()
                        nest_subFolders = []
                        getsubfolders = getFolders
                        for j in getsubfolders['files']:
                            if j['name'] == 'Intro':
                                # new_data['Real Estate'] = {'IntroID': j['id']}
                                new_data['Land'] = {**new_data['Land'], **{'IntroID': j['id']}}

                                folderid = j['id']
                                new_data['Land'][j['name']] = []
                                folderquery = "'" + folderid + "'" + " in parents"
                                getFolders = service.files().list(q=folderquery,
                                                                  spaces='drive',
                                                                  fields='files(id, name)').execute()
                                getallfiles = getFolders
                                for i in getallfiles['files']:
                                    new_data['Land'][j['name']].append(i['id'])
                            elif j['name'] == 'Outro':
                                new_data['Land'] = {**new_data['Land'], **{'OutroID': j['id']}}
                                folderid = j['id']
                                new_data['Land'][j['name']] = []
                                folderquery = "'" + folderid + "'" + " in parents"
                                getFolders = service.files().list(q=folderquery,
                                                                  spaces='drive',
                                                                  fields='files(id, name)').execute()
                                getallfiles = getFolders
                                for i in getallfiles['files']:
                                    new_data['Land'][j['name']].append(i['id'])
                                # new_data['Real Estate'] = {'OutroID': j['id']}
                            elif j['name'] == 'Listing Media' or j['name'] == 'New Listing Media':
                                # new_data['Real Estate'].update({'MediaID': j['id']})
                                new_data['Land'] = {**new_data['Land'], **{'MediaID': j['id']}}
                                folderid = j['id']
                                new_data['Land'][j['name']] = []
                                folderquery = "'" + folderid + "'" + " in parents"
                                getFolders = service.files().list(q=folderquery,
                                                                  spaces='drive',
                                                                  fields='files(id, name)').execute()
                                getallfiles = getFolders
                                for i in getallfiles['files']:
                                    new_data['Land'][j['name']].append(i['id'])
                    elif i['name'] == 'Head Shot':
                        new_data['Head Shot ID'] = i['id']
                    elif i['name'] == 'Commercial':
                        new_data['Commercial ID'] = i['id']
                        new_data[i['name']] = {}
                        folderid = i['id']
                        folderquery = "'" + folderid + "'" + " in parents and mimeType='application/vnd.google-apps.folder'"
                        getFolders = service.files().list(q=folderquery,
                                                          spaces='drive',
                                                          fields='files(id, name)').execute()
                        nest_subFolders = []
                        getsubfolders = getFolders
                        for j in getsubfolders['files']:
                            if j['name'] == 'Intro':
                                # new_data['Real Estate'] = {'IntroID': j['id']}
                                new_data['Commercial'] = {**new_data['Commercial'], **{'IntroID': j['id']}}

                                folderid = j['id']
                                new_data['Commercial'][j['name']] = []
                                folderquery = "'" + folderid + "'" + " in parents"
                                getFolders = service.files().list(q=folderquery,
                                                                  spaces='drive',
                                                                  fields='files(id, name)').execute()
                                getallfiles = getFolders
                                for i in getallfiles['files']:
                                    new_data['Commercial'][j['name']].append(i['id'])
                            elif j['name'] == 'Outro':
                                new_data['Commercial'] = {**new_data['Commercial'], **{'OutroID': j['id']}}
                                folderid = j['id']
                                new_data['Commercial'][j['name']] = []
                                folderquery = "'" + folderid + "'" + " in parents"
                                getFolders = service.files().list(q=folderquery,
                                                                  spaces='drive',
                                                                  fields='files(id, name)').execute()
                                getallfiles = getFolders
                                for i in getallfiles['files']:
                                    new_data['Commercial'][j['name']].append(i['id'])
                                # new_data['Real Estate'] = {'OutroID': j['id']}
                            elif j['name'] == 'Listing Media' or j['name'] == 'New Listing Media':
                                # new_data['Real Estate'].update({'MediaID': j['id']})
                                new_data['Commercial'] = {**new_data['Commercial'], **{'MediaID': j['id']}}
                                folderid = j['id']
                                new_data['Land'][j['name']] = []
                                folderquery = "'" + folderid + "'" + " in parents"
                                getFolders = service.files().list(q=folderquery,
                                                                  spaces='drive',
                                                                  fields='files(id, name)').execute()
                                getallfiles = getFolders
                                for i in getallfiles['files']:
                                    new_data['Commercial'][j['name']].append(i['id'])
                    elif i['name'] == 'Output':
                        new_data['Output'] = i['id']
                    elif i['name'] == 'Logs':
                        new_data['Logs'] = i['id']
                    elif i['name'] == 'Social Accounts':
                        new_data['Social Accounts'] = i['id']
                        # new_data['Social Accounts'] = {}
                        # new_data['Output'].append(i['id'])

                json_data['all_agents'].insert(0, new_data)
                with open('camps.json', 'w') as f:
                    json.dump(json_data, f)


if __name__ == '__main__':
    # multiprocessing.freeze_support()
    Main()














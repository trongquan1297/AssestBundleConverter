import os
import requests
import boto3
from zipfile import ZipFile
import subprocess
import shutil
import time
import logging
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def evaluate_process_time(start_time, end_time, step):
    time_taken = end_time - start_time
    logger.info("Step: " + step + f" - Time taken: {time_taken:.3f} seconds")

load_dotenv()

def noti_to_tele(message):

    token = os.getenv('TOKEN')
    chat_id = os.getenv('CHAT_ID')
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}"

    requests.get(url)

def unzip_file_and_delete(file):
    step = "Unzip File"
    start = time.time()
    filename = file.split('/')[-1][:-4]
    zip_file =  os.getenv('INPUT') + "/" + file.split('/')[-1]
    dest_folder = os.getenv('INPUT') + "/" + filename

    logger.info(step)
    with ZipFile(zip_file, 'r') as zObject:
        zObject.extractall(path=dest_folder)
    os.remove(zip_file)
    end = time.time()
    evaluate_process_time(start, end, step)
def build_asset_bundle():
    step = "Build Bundle"
    start = time.time()
    logger.info(step)

    args = "/home/devops/Unity/Hub/Editor/2022.1.10f1/Editor/Unity -executeMethod CreateAssetBundles -batchmode -quit"
    subprocess.call(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    end = time.time()
    evaluate_process_time(start, end, step)

class CustomException(Exception):
        def __init__(self, message):
            self.message = message

def upload_to_s3(file, bundle_type):
    step = "Upload Bundle"
    start = time.time()

    logger.info(step)
    bundle_file = file.split('/')[-1][:-4]
    ios_bundle = os.getenv('IOS_BUNDLE') + bundle_file + ".bundle"
    and_bundle = os.getenv('ANDROID_BUNDLE') + bundle_file + ".bundle"

    s3 = boto3.client('s3',aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.getenv('AWS_SECRET_KEY'))

    match bundle_type:
        case "story":
            ios_s3_bundle = os.getenv('STORY_IOS_S3_PATH') + bundle_file + ".bundle"
            and_s3_bundle = os.getenv('STORY_AND_S3_PATH') + bundle_file + ".bundle"
        case "word":
            ios_s3_bundle = os.getenv('WORD_IOS_S3_PATH') + bundle_file + ".bundle"
            and_s3_bundle = os.getenv('WORD_AND_S3_PATH') + bundle_file + ".bundle"

    try:

        s3.upload_file(Bucket=os.getenv('S3_BUCKET'), Key=ios_s3_bundle, Filename=ios_bundle)
        s3.upload_file(Bucket=os.getenv('S3_BUCKET'), Key=and_s3_bundle, Filename=and_bundle)
        return ios_s3_bundle, and_s3_bundle
    except Exception as e:
        raise CustomException("\nCannot upload bundle to S3")
    
    end = time.time()
    evaluate_process_time(start, end, step)

def update_api(file, bundle_type):

    step = "Update API"
    start = time.time()

    story_api = os.getenv('STORY_API')
    word_api = os.getenv('WORD_API')
    award_api = os.getenv('AWARD_API')
    form_1 = {
       "path_bundle": file
    }
    form_2 = {
        "type": "2",
        "path_bundle": file
    }
    form_3 = {
        "type": "1",
        "path_bundle": file
    }

    logger.info(step)
    match bundle_type:
        case "story":
            request = requests.put(story_api, data=form_1)
            if(request.status_code != 200):
                raise CustomException("Update APi " + request.text + "\n"+story_api)

    
    end = time.time()
    evaluate_process_time(start, end, step)



def count_file_in_queue():

    file_count = [0,0]

    src_story_folder = os.getenv('STORY_ZIP_PATH')
    src_word_folder = os.getenv('WORD_ZIP_PATH')

    folders = [src_story_folder, src_word_folder]

    i = 0
    for folder in folders:
        
        if os.path.isdir(folder):
        # Get a list of all files in the directory
            file_list = os.listdir(folder)

            # Count the files
            file_count[i] = len(file_list)
            i += 1

    return file_count



def main_process(file_path, bundle_type):
    try:
        done_message = "Done"
        fail_message = "Failed"
        start_time = time.time()     
        file_name = file_path.split('/')[-1][:-4]
        file_count = count_file_in_queue()

        noti_to_tele("Story: " +str(file_count[0]) +", Word: " + str(file_count[1]) + "\nStart convert: "+bundle_type+" - "+file_name)
        unzip_file_and_delete(file_path)
        build_asset_bundle()

        upload = upload_to_s3(file_path,bundle_type)

        if bundle_type == "story":
            update_api(file_name, bundle_type)

        end_time = time.time()
        total_time_taken = end_time - start_time

        noti_to_tele("Successfully: "+bundle_type+" - "+file_name+f"\nBuild time: {total_time_taken:.1f} seconds")
        
        return done_message, total_time_taken, upload[0], upload[1]
    except CustomException as ce:
        noti_to_tele("Failed: " + file_name +" "+ ce.message)
        return fail_message
    except Exception as e:
        noti_to_tele("Failed: " + str(e))
        return fail_message
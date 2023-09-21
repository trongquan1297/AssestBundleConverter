from file_handle import main_process
from file_handle import CustomException
import os
import time
import logging
import csv
import shutil
import dotenv
import subprocess
from file_handle import evaluate_process_time
from datetime import datetime
import redis
from write_result import insert_result_to_es

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

redis_client = redis.StrictRedis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))

def check_process(file_path):

    file_name = file_path.split('/')[-1]

    status = redis_client.get(file_name)

    if status:
        return status
    else:
        return None
    

def cache_process_status(file_path, status):
    file_name = file_path.split('/')[-1]
    redis_client.set(file_name, status)

def delete_cache(file_path):
    file_name = file_path.split('/')[-1]

    redis_client.delete(file_name)

def remove_folder():

    step = "Clean"
    start = time.time()
    input_folder = os.getenv('INPUT')
    output_folder = os.getenv('OUTPUT')
    folders = [input_folder, output_folder]

    logger.info(step)
    for folder in folders:
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(e)
    end = time.time()
    evaluate_process_time(start, end, step)

def get_file_in_folders():

    step = "Get Zip File"
    start = time.time()
    bundle_type = ""
    src_story_folder = os.getenv('STORY_ZIP_PATH')
    src_word_folder = os.getenv('WORD_ZIP_PATH')
    src_lesson_folder = os.getenv('LESSON_ZIP_PATH')
    src_courseinstall_folder = os.getenv('COURSEINSTALL_ZIP_PATH')
    src_item_folder = os.getenv('ITEM_ZIP_PATH')
    src_theme_folder = os.getenv('THEME_ZIP_PATH')
    src_category_folder = os.getenv('CATEGORY_ZIP_PATH')
    folders = [src_item_folder, src_story_folder, src_word_folder, src_lesson_folder, src_courseinstall_folder, src_theme_folder, src_category_folder]
    
    logger.info(step)
    for folder in folders:
        if os.path.exists(folder) and os.path.isdir(folder):
            for root, _, files in os.walk(folder):
                for file_name in files:
                    file_path = os.path.join(root, file_name)

                    if check_process(file_path):
                        continue
                    else:

                        if folder == src_story_folder:
                            bundle_type = "story"
                        elif folder == src_word_folder:
                            bundle_type = "word"
                        elif folder == src_lesson_folder:
                            bundle_type = "lesson"
                        elif folder == src_courseinstall_folder:
                            bundle_type = "courseinstall"
                        elif folder == src_item_folder:
                            bundle_type = "item"
                        elif folder == src_theme_folder:
                            bundle_type == "theme"
                        elif folder == src_category_folder:
                            bundle_type = "category"
                        cache_process_status(file_path, "Processing")
                        return file_path, bundle_type
                
    end = time.time()
    evaluate_process_time(start, end, step)
    return None
    
def copy_zip_file(file_path):

    step = "Copy zip to Input"
    start = time.time()

    logger.info(step)
    if file_path:
        try:
            shutil.copy(file_path, os.getenv('INPUT'))
        except Exception as e:
            print(f"Error: {e}")
    end = time.time()
    evaluate_process_time(start, end, step)

def delete_zip_file(file_path):

    step = "Delete Zip file"
    start = time.time()

    logger.info(step)

    try:
        os.remove(file_path)
    except FileNotFoundError:
        print(f"The file '{file_path}' does not exist.")
    except Exception as e:
        print(f"An error occurred while trying to delete the file: {e}")
    end = time.time()
    evaluate_process_time(start, end, step)

def move_file_to_dead_letter(file_path, bundle_type):

    step = "Move zip to Dead Letter"
    start = time.time()

    logger.info(step)
    file_name = file_path.split('/')[-1]
    command = "sudo mv " + file_path + " " + os.getenv('DL_PATH') + bundle_type + "/" + file_name

    try:
        subprocess.call(command , stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    except FileNotFoundError:
        print("The source file does not exist.")
    except Exception as e:
        print(f"An error occurred while moving the file: {str(e)}")

    end = time.time()
    evaluate_process_time(start, end, step)

max_retry = 3

def single_process():
    count_retry = 1
    process = "True"
    try:
        while process:
            remove_folder()

            f = get_file_in_folders()
            file_path = f[0]
            bundle_type = f[1]

            copy_zip_file(f[0])

            p = main_process(f[0], f[1])
            message = p[0]
            build_time = p[1]
            ios_bundle = p[2]
            and_bundle = p[3]
                   
            if message == "Done":
                delete_zip_file(f[0])
                delete_cache(f[0])
                insert_result_to_es(file_path, bundle_type, message, ios_bundle, and_bundle, build_time ) 
                
                count_retry = 1
                process = "True"
                return process
            else:
                build_time = "0"
                ios_bundle = "Not Exist"
                and_bundle = "Not Exits"
                fail_message = "Failed"
                delete_cache(f[0])
                insert_result_to_es(file_path, bundle_type, fail_message, ios_bundle, and_bundle, build_time ) 
                
                count_retry += 1
                if count_retry == max_retry:
                    move_file_to_dead_letter(f[0], f[1])
                    process = "True"
                    return process
    except FileNotFoundError as fe:
        process = None
        return process
    except CustomException as ce:
        process = None
        return process
    except Exception as e:
        process = None
        return process


if __name__ == '__main__':
    count = 0
    i = 0
    while i < 10:
        p = single_process()
        if p == None:
            logger.info("Nothing to do. Wating for zip file!")
            time.sleep(10)


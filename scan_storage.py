# Scan specified directory
import sys
import logging
import os
import json
import requests

logging.basicConfig(filename='scan_storage.log', format='%(asctime)s %(levelname)s %(message)s', encoding='utf-8',
                    level=logging.DEBUG)
logger = logging.getLogger()
SERVER_IP = "localhost"
PORT = "8080"
SERVER_URL = "http://" + SERVER_IP + ":" + PORT
URL_GET_STORAGE_TYPES = SERVER_URL + "/media_library/storage-types"
URL_GET_STORAGE = SERVER_URL + "/media_library/storage"
URL_ADD_STORAGE = SERVER_URL + "/media_library/storage"
URL_DELETE_STORAGE = SERVER_URL + "/media_library/storage"

URL_CONTENT = SERVER_URL + "/media_library/content"
URL_EMPTY_STORAGE = SERVER_URL + "/media_library/content/storage"

URL_GET_ALL_CONTENT = SERVER_URL + "/media_library/content"
URL_GET_STORAGE_CONTENT = SERVER_URL + "/media_library/content/storage"
URL_GET_CONTENT_TYPE = SERVER_URL + "/media_library/content-types"

CONTENT_BATCH_SIZE = 10

CONTENT_TYPE_FILE = "content_type.json"


def usage():
    print("At least 1 argument is required")
    exit(1)


def list_files(location):
    obj = os.scandir()
    pass


def scan_device(location):
    logger.info("scan_device:: Called")
    logger.info("scan_device:: location [%s]", location)
    device_scan_json_file_name = "json_file.json"

    # Get Content Types
    if not os.path.exists(CONTENT_TYPE_FILE):
        # Query to get content types from file
        get_content_types()

    with open(CONTENT_TYPE_FILE) as file_c:
        content_types = json.loads(file_c.read())

    file_records = []
    os.scandir(location)
    with os.scandir(location) as entries:
        for entry in entries:
            if entry.is_file():
                print("File at root level")
            elif entry.is_dir():
                #print(entry)
                #print(entry.name)
                for ctype in content_types:
                    if ctype in entry.name:
                        contentType = ctype
                        break;
                #print(contentType)

                for (dir_path, dir_names, file_names) in os.walk(os.path.join(location,entry.name)):
                    logger.info("dir_path %s", dir_path)
                    logger.info("dir_names %s", dir_names)
                    logger.info("file_names %s", file_names)
                    for file in file_names:
                        file_records.append({"contentLocation": dir_path, "contentName": file, "contentType": contentType, "file": True})
                    file_records.append({"contentLocation": dir_path, "contentName": os.path.basename(dir_path), "file": False})
                    logger.info("--")
                    #json_data = json.dumps(file_records)
                    #logger.info(json_data)
            else:
                print("Some wrong entry")

    with open(device_scan_json_file_name, "w") as file:
        json.dump(file_records, file)
    print(device_scan_json_file_name)


def get_storage_types():
    logger.info("Called function get_storage_types")
    logger.info("Calling API %s".format(URL_GET_STORAGE_TYPES))
    response = requests.get(URL_GET_STORAGE_TYPES)
    logger.info(response)
    if response.status_code == 200:
        logger.info(response.json())
        logger.info(response.json()['storage-types'])
        storage_types = response.json()['storage-types']
        print(storage_types)
    else:
        print("Error")


def get_content_types():
    logger.info("Called function get_content_types")
    logger.info("Calling API %s".format(URL_GET_CONTENT_TYPE))
    response = requests.get(URL_GET_CONTENT_TYPE)
    logger.info(response)
    if response.status_code == 200:
        logger.info(response.json())
        logger.info(response.json()['content-types'])
        content_types = response.json()['content-types']
        print(content_types)
        with open(CONTENT_TYPE_FILE, "w") as file:
            json.dump(content_types, file)
    else:
        print("Error")
        exit(1)


def get_storages():
    logger.info("Called function get_storages")
    logger.info("Calling API %s".format(URL_GET_STORAGE))
    response = requests.get(URL_GET_STORAGE)
    logger.info(response)
    if response.status_code == 200:
        logger.info(response.json())
        # logger.info(response.json()['storage-types'])
        storage_list = response.json()
        print(storage_list)
    else:
        print("Error")


def add_storage(storage_name, storage_type, storage_description):
    logger.info("Called function add_storage with storage name [%s]".format(URL_ADD_STORAGE))
    logger.info("Calling API " + URL_ADD_STORAGE)

    # Create new resource
    response = requests.post(URL_ADD_STORAGE,
                             headers={
                                 'Content-type': 'application/json',
                                 'Accept': 'application/json'
                             },
                             data=json.dumps(
                                 {'storageName': storage_name, 'description': storage_description, 'type': storage_type}
                             ))
    logger.info(response)
    if response.status_code == 200:
        logger.info(response.json())
        print(response.json())
    else:
        print(response.text)
        print("Error")


def delete_storage(storage_name):
    logger.info("Called function delete_storage with storage name [%s]".format(storage_name))
    logger.info("Calling API " + URL_DELETE_STORAGE)

    # Delete resource
    response = requests.delete(URL_DELETE_STORAGE,
                               headers={
                                   'Content-type': 'application/json',
                                   'Accept': 'application/json'
                               },
                               data=storage_name
                               )
    logger.info(response)
    if response.status_code == 200:
        logger.info(response.json())
        print(response.json())
    else:
        print(response.text)
        print("Error")


def push_storage_data(storage_id, storage_data_json_file):
    # json_data = json.dumps(file_records)
    # logger.info(json_data)
    with open(storage_data_json_file) as file_j:
        json_data = json.loads(file_j.read())

    print("json list size " + str(len(json_data)))

    # Delete resource
    response = requests.delete(URL_EMPTY_STORAGE + "/" + storage_id,
                               headers={
                                   'Content-type': 'application/json',
                                   'Accept': 'application/json'
                               }
                               )
    logger.info(response)
    if response.status_code == 200:
        logger.info(response.json())
        print(response.json())
    else:
        print(response.text)
        print("Error")
        exit(1)

    start_index = 0
    batches = {}
    while start_index < len(json_data):
        end_index = start_index + CONTENT_BATCH_SIZE;
        batch_data = json_data[start_index:end_index]
        logger.info("BATCH start "+str(start_index)+" end "+str(end_index))
        print("BATCH start "+str(start_index)+" end "+str(end_index))
        batches[storage_id] = batch_data
        logger.info(json.dumps(batches))

        # Create new resource
        response = requests.post(URL_CONTENT,
                                 headers={
                                     'Content-type': 'application/json',
                                     'Accept': 'application/json'
                                 },
                                 data=json.dumps(batches))
        logger.info(response)
        if response.status_code == 200:
            logger.info(response.json())
            print(response.json())
        else:
            print(response.text)
            print("Error")
        start_index += CONTENT_BATCH_SIZE


def get_storage_data(storage_id):
    logger.info("Called function get_storage_data")
    url = URL_GET_STORAGE_CONTENT + "/" + storage_id
    logger.info("Calling API %s".format(url))
    print(url)
    response = requests.get(url)
    logger.info(response)
    if response.status_code == 200:
        logger.info(response.json())
        print(response.json())
        # logger.info(response.json()['storage-types'])
        # storage_types = response.json()['storage-types']
        # print(storage_types)
    else:
        print("Error")


def get_all_data():
    logger.info("Called function get_all_data")
    logger.info("Calling API %s".format(URL_GET_ALL_CONTENT))
    response = requests.get(URL_GET_ALL_CONTENT)
    logger.info(response)
    if response.status_code == 200:
        logger.info(response.json())
        print(response.json())
        #logger.info(response.json()['storage-types'])
        #storage_types = response.json()['storage-types']
        #print(storage_types)
    else:
        print("Error")


if __name__ == "__main__":
    # print(sys.argv)
    if len(sys.argv) < 2:
        usage()
    operation_name = sys.argv[1]
    # print("Operation " + operation_name)
    match operation_name:
        case "SCAN_DEVICE":
            if len(sys.argv) < 3:
                usage()
            scan_device(sys.argv[2])
        case "GET_STORAGE_TYPES":
            get_storage_types()
        case "GET_CONTENT_TYPES":
            get_content_types()
        case "GET_STORAGE":
            get_storages()
        case "ADD_STORAGE":
            add_storage(sys.argv[2], sys.argv[3], sys.argv[4])
        case "DELETE_STORAGE":
            delete_storage(sys.argv[2])
        case "PUSH_STORAGE_DATA":
            push_storage_data(sys.argv[2], sys.argv[3])
        case "GET_STORAGE_DATA":
            get_storage_data(sys.argv[2])
        case "GET_ALL_DATA":
            get_all_data()
        case default:
            print("Matching Operation Not found. Exiting..\n")
            usage()
            exit(1)

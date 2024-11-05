from flask import Flask, Response, request, jsonify
from waitress import serve
from datetime import datetime
from datetime import timedelta
import os
import math
from pathlib import Path
import threading
import shutil
import json
import argparse

CACHE_FOLDER = 'cache'
DEFAULT_PORT = 8801
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

MIN_USAGE_METRIC_RANGE = (0.2, 0.4)
MAX_ITEM_COUNT_FOR_PLATFORM_PRODUCT = 15
USE_COUNT_HALF_LIFE_PERIOD = timedelta(days = 7)
UPDATE_TIME_INTERVAL = timedelta(hours = 1)

class MetaData:
    def __init__(self, post_time, use_count, aged_use_count, last_time):
        self.__post_time = post_time
        self.__use_count = use_count
        self.__aged_use_count = aged_use_count
        self.__last_time = last_time

    @classmethod
    def create(cls):
        now = datetime.now()
        return cls(now, 1, 1, now)

    @classmethod
    def load(cls, path):
        if not os.path.exists(os.path.join(path, "file")):
            raise FileNotFoundError("File not found")

        with open(os.path.join(path, "metadata.json"), "r") as file:
            data = json.load(file)  # Load JSON data from the file

        post_time = datetime.strptime(data["post_time"].strip(), DATE_FORMAT)
        last_time = datetime.strptime(data["last_time"].strip(), DATE_FORMAT)
        return cls(post_time, data["use_count"], data["aged_use_count"], last_time)

    def to_json(self):
        return { 
            "post_time": self.__post_time.strftime(DATE_FORMAT),
            "use_count": self.__use_count,
            "aged_use_count": self.__aged_use_count,
            "last_time": self.__last_time.strftime(DATE_FORMAT)
        }

    def save(self, path):
        json_data = self.to_json()

        with open(os.path.join(path, "metadata.json"), "w") as file:
            json.dump(json_data, file, indent = 4)

    def get_aged_use_count(self, now):
        delta_in_seconds = (now - self.__last_time).total_seconds()
        if delta_in_seconds > 0:
            return self.__aged_use_count * math.pow(2, -delta_in_seconds / USE_COUNT_HALF_LIFE_PERIOD.total_seconds())
        else:
            return self.__aged_use_count

    def get_usage_metric(self, now):
        return self.get_aged_use_count(now)

    def update_last_time(self):
        now = datetime.now()
        self.__use_count += 1
        self.__aged_use_count = self.get_aged_use_count(now) + 1
        self.__last_time = now


class Storage:
    def __init__(self):
        os.makedirs(CACHE_FOLDER, exist_ok = True)
        self.__metadata_map = {}

    def update(self):
        print("Update storage")

        now = datetime.now()

        self.__metadata_map = {}
        cache_entry = Path(CACHE_FOLDER)
        for product_entry in filter(lambda e: e.is_dir(), cache_entry.iterdir()):
            product = product_entry.name
            self.__metadata_map[product] = {}
            for platform_entry in filter(lambda e: e.is_dir(), product_entry.iterdir()):
                platform = platform_entry.name
                items = {}
                self.__metadata_map[product][platform] = items
                for key_entry in filter(lambda e: e.is_dir(), platform_entry.iterdir()):
                    path = str(key_entry)

                    try:
                        items[path] = MetaData.load(path)
                    except Exception as e:
                        print(f"The item {path} is incomplete. Removing it.")
                        shutil.rmtree(path)

                self.__remove_outdated_items(platform, product, now)

                if len(items) > 0:
                    print(f"    * Found {product}/{platform}, count: {len(items)}")
                else:
                    del self.__metadata_map[product][platform]
                    shutil.rmtree(platform_entry)

            if len(self.__metadata_map[product]) == 0:
                del self.__metadata_map[product]
                shutil.rmtree(product_entry)

        if len(self.__metadata_map) == 0:
            print("The storage is empty")

    def add_data(self, platform, key, product, version, data):
        path = Storage.__product_directory_path(platform, key, product, version)

        if self.__has_data(platform, product, path):
            raise FileNotFoundError(f"Not found.")

        self.__create_item_placeholder(platform, product, path)

        with open(os.path.join(path, "file"), "wb") as f:
            f.write(data) 

        metadata = MetaData.create()
        self.__metadata_map[product][platform][path] = metadata
        metadata.save(path)
        self.__remove_outdated_items(platform, product, now, min_items_count = MAX_ITEM_COUNT_FOR_PLATFORM_PRODUCT)

    def get_data(self, platform, key, product, version):
        path = Storage.__product_directory_path(platform, key, product, version)

        if not self.__has_data(platform, product, path):
            raise FileNotFoundError(f"Not found.")

        with open(os.path.join(path, "file"), "rb") as f:
            data = f.read()

        metadata = self.__metadata_map[product][platform][path]
        metadata.update_last_time()
        metadata.save(path)

        return data

    def to_string(self):
        data = {}

        for product, product_data in self.__metadata_map.items():
            data[product] = {}
            for platform, platform_data in product_data.items():
                data[product][platform] = {}
                for path, metadata in platform_data.items():
                    data[product][platform][path] = metadata.to_json()

        return json.dumps(data, indent = 4)

    @staticmethod
    def __get_min_usage_metric(items_count):
        filling_factor = (items_count - 1) / (MAX_ITEM_COUNT_FOR_PLATFORM_PRODUCT - 1)
        return MIN_USAGE_METRIC_RANGE[0] + filling_factor * (MIN_USAGE_METRIC_RANGE[1] - MIN_USAGE_METRIC_RANGE[0])

    def __remove_outdated_items(self, platform, product, now, min_items_count = 0):
        items = self.__metadata_map[product][platform]
        while len(items) > min_items_count:
            path = min(items, key = lambda k: items[k].get_usage_metric(now))
            usage_metric = items[path].get_usage_metric(now)

            if (len(items) > MAX_ITEM_COUNT_FOR_PLATFORM_PRODUCT):
                print(f"Exceeded max items count, removing the less actual item: {path}, usage metric: {usage_metric}")
            elif (usage_metric < Storage.__get_min_usage_metric(items_count = len(items))):
                print(f"An item has been outdated, removing it: {path}, usage metric: {usage_metric}")
            else:
                break

            del items[path]
            shutil.rmtree(path)


    def __has_data(self, platform, product, path):
        product_exists = product in self.__metadata_map
        platform_exists = product_exists and (platform in self.__metadata_map[product])
        return platform_exists and (path in self.__metadata_map[product][platform])

    def __create_item_placeholder(self, platform, product, path):
        if not product in self.__metadata_map:
            self.__metadata_map[product] = {}
        if not platform in self.__metadata_map[product]:
            self.__metadata_map[product][platform] = {}

        os.makedirs(path, exist_ok = True)

    @staticmethod
    def __product_directory_path(platform, key, product, version):
        return os.path.join(CACHE_FOLDER, product, platform, f"{version}_{key}")


def run_server(storage, lock, port, debug):
    app = Flask(__name__)

    @app.route('/products/<platform>/<key>/<product>/<version>', methods = ['POST'])
    def upload_file(platform, key, product, version):
        print(f"The product {platform}/{product}/{version} is posted by {request.remote_addr}")

        try:
            with lock:
                storage.add_data(platform = platform, key = key, product = product, version = version, data = request.get_data())
            print(f"The product {platform}/{product}/{version} has been saved.")
            return jsonify({"message": f"The product {platform}/{product}/{version} has been uploaded successfully"}), 201
        except FileExistsError:
            print(f"Cannot add data: the product {platform}/{product}/{version} exists")
            return jsonify({"error": f"The product {platform}/{product}/{version} already exists"}), 409


    @app.route('/products/<platform>/<key>/<product>/<version>', methods = ['GET'])
    def get_file(platform, key, product, version):
        print(f"The product {platform}/{product}/{version} is requested by {request.remote_addr}")

        try:
            with lock:
                data = storage.get_data(platform = platform, key = key, product = product, version = version)
            print(f"Sending the product {platform}/{product}/{version}")
            return Response(data, mimetype = 'application/octet-stream')
        except FileNotFoundError:
            print(f"Cannot get {product}/{version}, not found")
            return jsonify({"error": f"The product {platform}/{product}/{version} is not found in the cache"}), 404

    @app.route('/products/metadata', methods = ['GET'])
    def dump_metadata():
        print(f"The product metadata is requested by {request.remote_addr}")
        with lock:
            return storage.to_string()


    print(f"Running the files caching server on the port {port}...")
    if debug:
        app.run(host = '0.0.0.0', port = port, debug = True)
    else:
        serve(app, host = "0.0.0.0", port = port)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = "A server for caching files.")
    parser.add_argument("--debug", action = "store_true", help = "Enable server debug mode")
    parser.add_argument("--port", type = int, default = DEFAULT_PORT, help = f"Server port ({DEFAULT_PORT})")
    args = parser.parse_args()

    storage = Storage()
    lock = threading.Lock()

    def start_update_timer():
        with lock:
            storage.update()
        threading.Timer(UPDATE_TIME_INTERVAL.total_seconds(), start_update_timer).start()

    start_update_timer()

    run_server(storage, lock, port = args.port, debug = args.debug)

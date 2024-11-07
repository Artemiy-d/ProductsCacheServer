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
MAX_ELEMENT_COUNT_FOR_PLATFORM_PRODUCT = 15
USE_COUNT_HALF_LIFE_PERIOD = timedelta(days = 7)
UPDATE_TIME_INTERVAL = timedelta(hours = 1)

class ElementsSet:
    def __init__(self):
        self.elements = {}
        self.aliases = {}

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

    def get_last_time(self):
        return self.__last_time

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
                elements, aliases = self.__enshure_elements(product, platform)

                for key_entry in filter(lambda e: e.is_dir() and not e.is_symlink(), platform_entry.iterdir()):
                    path = str(key_entry)

                    try:
                        elements[path] = MetaData.load(path)
                    except Exception as e:
                        print(f"The element {path} is incomplete. Removing it.")
                        shutil.rmtree(path)

                for key_entry in filter(lambda e: e.is_dir() and e.is_symlink(), platform_entry.iterdir()):
                    alias_path = str(key_entry)

                    source_folder = os.readlink(alias_path)
                    source_path = os.path.join(str(platform_entry), source_folder)
                    if source_path in elements:
                        aliases[alias_path] = source_path
                    else:
                        print(f"Removing invalid link {alias_path} -> {source_folder} from {product}/{platform}")
                        key_entry.unlink()

                self.__remove_outdated_elements(product, platform, now)

                if len(elements) > 0:
                    print(f"    * Found {product}/{platform}, elements count: {len(elements)}, aliases count: {len(aliases)}")
                else:
                    del self.__metadata_map[product][platform]
                    shutil.rmtree(platform_entry)

            if len(self.__metadata_map[product]) == 0:
                del self.__metadata_map[product]
                shutil.rmtree(product_entry)

        if len(self.__metadata_map) == 0:
            print("The storage is empty")

    def add_data(self, platform, key, product, version, data):
        path, exists = self.__resolved_product_directory_path(product, platform, key, version)

        if exists:
            raise FileExistsError(f"Not found.")

        os.makedirs(path, exist_ok = True)

        with open(os.path.join(path, "file"), "wb") as f:
            f.write(data) 

        elements, aliases = self.__enshure_elements(product, platform)
        elements[path] = MetaData.create()
        elements[path].save(path)
        self.__remove_outdated_elements(product, platform, now = elements[path].get_last_time(), min_element_count = MAX_ELEMENT_COUNT_FOR_PLATFORM_PRODUCT)

    def add_alias(self, platform, key, product, version, key_alias):
        source_path, exists = self.__resolved_product_directory_path(product, platform, key, version)

        if not exists:
            raise FileNotFoundError(f"Source not found.")

        elements, aliases = self.__get_elements_and_aliases(product, platform)
        alias_path = self.__product_directory_path(product, platform, key_alias, version)

        if alias_path in elements or alias_path in aliases or os.path.exists(alias_path):
            raise FileExistsError(f"Target file already exists.")

        os.symlink(os.path.basename(source_path), alias_path)
        aliases[alias_path] = source_path


    def get_data(self, platform, key, product, version):
        path, exists = self.__resolved_product_directory_path(product, platform, key, version)

        if not exists:
            raise FileNotFoundError(f"Not found.")

        with open(os.path.join(path, "file"), "rb") as f:
            data = f.read()

        metadata = self.__metadata_map[product][platform].elements[path]
        metadata.update_last_time()
        metadata.save(path)

        return data

    def to_string(self):
        result = {}

        for product, product_data in self.__metadata_map.items():
            data[product] = {}
            for platform, platform_data in product_data.items():
                result[product][platform] = {}

                for path, metadata in platform_data.elements.items():
                    result[product][platform][os.path.basename(path)] = metadata.to_json()

                for alias_path, source_path in platform_data.elements.items():
                    result[product][platform][source_path].set_default("aliases", []).append(os.path.basename(alias_path))

        return json.dumps(result, indent = 4)

    @staticmethod
    def __get_min_usage_metric(items_count):
        filling_factor = (items_count - 1) / (MAX_ELEMENT_COUNT_FOR_PLATFORM_PRODUCT - 1)
        return MIN_USAGE_METRIC_RANGE[0] + filling_factor * (MIN_USAGE_METRIC_RANGE[1] - MIN_USAGE_METRIC_RANGE[0])

    def __remove_outdated_elements(self, product, platform, now, min_element_count = 0):
        elements, aliases = self.__get_elements_and_aliases(product, platform)
        while len(elements) > min_element_count:
            path = min(elements, key = lambda k: elements[k].get_usage_metric(now))
            usage_metric = elements[path].get_usage_metric(now)

            if (len(elements) > MAX_ELEMENT_COUNT_FOR_PLATFORM_PRODUCT):
                print(f"Exceeded max items count, removing the less actual item: {path}, usage metric: {usage_metric}")
            elif (usage_metric < Storage.__get_min_usage_metric(items_count = len(elements))):
                print(f"An item has been outdated, removing it: {path}, usage metric: {usage_metric}")
            else:
                break

            for alias_path in filter(lambda k: aliases[k] == path, aliases.keys()):
                del aliases[alias_path]
                if os.path.islink(alias_path):
                    os.unlink(alias_path)

            del elements[path]
            shutil.rmtree(path)

    def __get_elements_and_aliases(self, product, platform):
        result = self.__metadata_map[product][platform]
        return result.elements, result.aliases

    def __enshure_elements(self, product, platform):
        if not product in self.__metadata_map:
            self.__metadata_map[product] = {}
        if not platform in self.__metadata_map[product]:
            self.__metadata_map[product][platform] = ElementsSet()

        return self.__get_elements_and_aliases(product, platform)

    def __product_directory_path(self, product, platform, key, version):
        return os.path.join(CACHE_FOLDER, product, platform, f"{version}_{key}")

    def __resolved_product_directory_path(self, product, platform, key, version):
        path = self.__product_directory_path(product, platform, key, version)

        product_exists = product in self.__metadata_map
        platform_exists = product_exists and (platform in self.__metadata_map[product])

        if not platform_exists:
            return path, False

        elements, aliases = self.__get_elements_and_aliases(product, platform)
        while path in aliases:
            path = aliases[path]

        return path, path in elements


def run_server(storage, lock, port, debug):
    app = Flask(__name__)

    @app.route('/products/<product>/<version>/<platform>/<key>', methods = ['POST'])
    def upload_file(product, version, platform, key):
        print(f"The product {platform}/{product}/{version} is posted by {request.remote_addr}")

        try:
            with lock:
                storage.add_data(platform = platform, key = key, product = product, version = version, data = request.get_data())
            print(f"The product {platform}/{product}/{version} has been saved.")
            return jsonify({"message": f"The product {platform}/{product}/{version} with the key {key} has been uploaded successfully"}), 201
        except FileExistsError:
            print(f"Cannot add data: the product {platform}/{product}/{version} the key {key} exists")
            return jsonify({"error": f"The product {platform}/{product}/{version} with the key {key} already exists"}), 409


    @app.route('/products/<product>/<version>/<platform>/<key>/add_alias/<key_alias>', methods = ['POST'])
    def add_alias(product, version, platform, key, key_alias):
        print(f"The product key alias {key_alias} is posted for {platform}/{product}/{version} by {request.remote_addr}")
        try:
            with lock:
                storage.add_alias(platform = platform, key = key, product = product, version = version, key_alias = key_alias)
            print(f"The alias has been added.")
            return jsonify({"message": f"The product {platform}/{product}/{version} has been uploaded successfully"}), 201
        except FileNotFoundError:
            return jsonify({"error": f"The source product {platform}/{product}/{version} with the key {key} doesn't exist"}), 409
        except FileExistsError:
            return jsonify({"error": f"The key {key_alias} already exists for the product {platform}/{product}/{version}"}), 409


    @app.route('/products/<product>/<version>/<platform>/<key>', methods = ['GET'])
    def get_file(product, version, platform, key):
        print(f"The product {platform}/{product}/{version} is requested by {request.remote_addr}")

        try:
            with lock:
                data = storage.get_data(platform = platform, key = key, product = product, version = version)
            print(f"Sending the product {platform}/{product}/{version} with the key {key}")
            return Response(data, mimetype = 'application/octet-stream')
        except FileNotFoundError:
            print(f"Cannot find {platform}/{product}/{version} with the key {key}")
            return jsonify({"error": f"The product {platform}/{product}/{version} with the key {key} is not found in the cache"}), 404


    @app.route('/products/metadata', methods = ['GET'])
    def dump_metadata():
        print(f"The product metadata is requested by {request.remote_addr}")
        with lock:
            return storage.to_string()

    @app.route('/help', methods = ['GET'])
    def get_help():
        print(f"Help is requested by {request.remote_addr}")
        return ('GET   /products/metadata  returns a dump of products metadata'
                'GET   /products/<product>/<version>/<platform>/<key>  downloads the specified product'
                'POST  /products/<product>/<version>/<platform>/<key>  uploads the specified product'
                'POST  /products/<product>/<version>/<platform>/<key>/add_alias/  add the specified alias key to an existing product')

  

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

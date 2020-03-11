#!/usr/bin/env python3
import datetime
import os

import logging
import pymongo
import time
from bson import ObjectId

from utils import json_from_file, MyHTMLParser, json_to_file, _get

config_file_name = 'config.json'
config = {}

try:
    config = json_from_file(config_file_name, "Can't open ss-config file.")
except RuntimeError as e:
    print(e)
    exit()

ss_ad_collection = config['ss_ad_collection']
geodata_collection = config['geodata_collection']
resolved = []
not_exist_resolver = []
not_found = []

if not os.path.exists('requests'):
    os.makedirs('requests')

formatter = logging.Formatter(config['logging.format'])
# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler(config['logging.file'])

# Create formatters and add it to handlers
c_handler.setFormatter(formatter)
f_handler.setFormatter(formatter)

logging_level = config["logging.level"] if 'logging.level' in config else 20
print("Selecting logging level", logging_level)
print("Selecting logging format", config["logging.format"])
print("Selecting logging file \"%s\"" % config['logging.file'])

logging.basicConfig(format=config["logging.format"], handlers=[c_handler, f_handler])
logger = logging.getLogger(config["logging.name"])
logger.setLevel(logging_level)


def extract_pages(data):
    pages = []
    for line in data:
        if isinstance(line, tuple) and line[0] == 'a' and len(line[1]) == 4 and line[1][0][1] == 'nav_id':
            pages.append(line[1][3][1])
    return pages, pages.pop(0)


def is_item(item):
    return len(item) >= 3 and item[0] == 'td' and len(item[1]) > 0 and len(item[1][0]) > 1 and item[1][0][1] == config[
        "sscom.class"]


def is_url(item):
    return len(item) >= 2 and item[0] == 'a' and len(item[1]) > 2 and len(item[1][2]) > 1 and item[1][2][1] == config[
        "sscom.class.url"]


def generate_report(ads={}, new_ads=[], new_address=[]):
    try:
        for a in ads:
            for i in ads[a]['items']:
                print("{0:>30} {1:7}".format(a, str(i)))
        print("______________________________________________________")
        print("_____________  New Records  __________________________")
        for a in new_ads:
            print(a)
        print("______________________________________________________")
        print(len(new_ads), "New records found.")

        print("______________________________________________________")
        print("_____________  New Address not in GeoData DB  ________")
        for a in new_address:
            print(a)
        print("______________________________________________________")
        print(len(new_address), "New records found.")
    except RuntimeError as e:
        logger.error(e)


def uload_new_records(new_ads):
    try:
        db[ss_ad_collection].insert_many(new_ads)
    except RuntimeError as e:
        logger.error(e)


def request_ss_records():
    data = []
    try:
        for url in config["sites"]:
            logger.info(f"Looking for new records in {url}")
            parser_config = {'valid_tags': ['tr', 'td', 'a', 'br', 'b'], 'skip_tags': ['b']}
            page = MyHTMLParser(parser_config).feed_and_return(_get(url).text)
            pages, last = extract_pages(page.data)
            data += page.data
            pages_max = last.split('page')[1].split('.')[0]

            for p in range(2, int(pages_max)):
                _url = f"{config['sscom.url']}{last.replace(pages_max, str(p))}"
                logger.debug(f"Looking for new records in rest of pages {_url}")
                data += MyHTMLParser(parser_config).feed_and_return(_get(_url).text).data
    except RuntimeError as e:
        logger.debug(e)
    return data


def build_db_record(items):
    a = {}
    try:
        a = {'url': config['sscom.url'] + items[0], 'address': items[1],
             'date': datetime.datetime.utcnow()}
        if len(items) == 6:
            a.update({'m2': items[2], 'level': items[3], 'type': config['house.marker'],
                      'price_m2': items[4], 'price': items[5]})
        elif len(items) == 8:
            a.update({'rooms': items[2], 'm2': items[3], 'level': items[4], 'type': items[5],
                      'price_m2': items[6], 'price': items[7]})
    except RuntimeError as e:
        logger.debug(e)
    return a


def is_property(param: str) -> bool:
    return param in config and config[param]


def to_buffer(buffer, d):
    if is_url(d):
        buffer.append(d[1][3][1])
    elif is_item(d):
        buffer.append(d[len(d) - 1])


def to_ads(ads, a):
    try:
        _addr = ads[a['address']]
        _addr['items'].append(a)
    except:
        ads[a['address']] = {'items': [a]}


def build_model(data):
    ads = {}
    buffer = []
    i = 0
    while i <= len(data) - 1:
        d = data[i]
        if is_url(d) or is_item(d):
            to_buffer(buffer, d)
        elif buffer:
            a = build_db_record(buffer)
            buffer = []

            to_ads(ads, a)

        i += 1
    return ads


def find_by_url(url, address, ads):
    for a in ads:
        for i in ads[a]['items']:
            if i['url'] == url and i['address'] == address:
                return i
    raise NotFound(url, address)


def resolve_diff_key(ad_old, ad_new, key):
    global db, resolved
    print('old_' + key, ad_old[key], ad_new[key])
    # resolved.append({'kind': 'old_' + key, 'old': ad_old, 'new': ad_new})
    try:
        old_price_record = {'kind': 'old_' + key, 'ad_id': ObjectId(ad_old['_id']), 'price': ad_old['price'],
                            'date': datetime.datetime.utcnow()}
        # del ad_old['_id']
        # result = db[ss_ad_collection].insert_one(ad_old)
        result = db[ss_ad_collection].insert_one(old_price_record)
        if not result.inserted_id:
            raise Exception('Not inserted', old_price_record)
        result = db[ss_ad_collection].update_one({'_id': ad_old['_id']}, {'$set': {key: ad_new[key]}})
        if not result.matched_count:
            raise Exception('Not updated record', ad_old['_id'])
    except Exception as e:
        logger.error(e)


def resolve_update_key(ad_old, ad_new, key):
    global db, resolved
    print('old_' + key, ad_old[key], ad_new[key])
    resolved.append({'kind': 'old_' + key, 'old': ad_old, 'new': ad_new})
    result = db[ss_ad_collection].update_one({'_id': ObjectId(ad_old['_id'])}, {'$set': {key:ad_new[key]}})
    if not result.matched_count:
        raise Exception('Not updated record', ad_old['_id'])


def skip(*args, **kwargs): pass


class NotFound(Exception): pass


mapping = {
    'price': resolve_diff_key,
    'price_m2': resolve_update_key,
    'm2': resolve_update_key,
    'level': resolve_update_key,
    'rooms': resolve_update_key
}


def get(d: dict, key: str) -> object:
    try:
        return d[key]
    except KeyError as e:
        if key in ['_id', 'date', 'kind']:
            return skip
        raise e


def compare(my_ad, remote_ad):
    for key in my_ad.keys():
        if get(my_ad, key) != get(remote_ad, key):
            try:
                get(mapping, key)(my_ad, remote_ad, key=key)
            except KeyError as e:
                logger.error('Key error:' + key)
                not_exist_resolver.append({'kind': 'old_' + key, 'old': my_ad, 'new': remote_ad})


while True:
    try:
        myclient = pymongo.MongoClient(config["db.url"])

        with myclient:
            db = myclient.ss_ads

            data = request_ss_records()

            remote_ads = build_model(data)

            my_ads = list(db[ss_ad_collection].find({'kind': 'ad'}))

            for my_ad in my_ads:
                try:
                    remote_ad = find_by_url(my_ad['url'], my_ad['address_lv'], remote_ads)
                    result = compare(my_ad, remote_ad)
                except NotFound as e:
                    not_found.append(my_ad)

            for my_ad in resolved:
                print(my_ad)

            print('Resolved', len(resolved))
            print('Not found', len(not_found))
            print('Not exist resolver', len(not_exist_resolver))

    except RuntimeError as e:
        logger.error(e)

    if 'restart' in config and config['restart'] > 0:
        logger.info("Waiting %s seconds.", config['restart'])
        time.sleep(config['restart'])
    else:
        break

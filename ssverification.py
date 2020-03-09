#!/usr/bin/env python3
import pymongo, logging, time
import datetime, os

from bson import ObjectId

from utils import json_from_file, MyHTMLParser, json_to_file, _get

config_file_name = 'config.json'
config = {}

try:
    config = json_from_file(config_file_name, "Can't open ss-config file.")
except RuntimeError as e:
    print(e)
    exit()

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
    return len(item) >= 3 and item[0] == 'a' and len(item[1]) > 2 and len(item[1][2]) > 1 and item[1][2][1] == config[
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
        ss_ads.ads.insert_many(new_ads)
    except RuntimeError as e:
        logger.error(e)


def export_to_file(ads):
    try:
        ads_for_json = ads.copy()
        for a in ads_for_json:
            for i in ads_for_json[a]['items']:
                i['date'] = str(i['date'])
        json_to_file(config['export.filename'], ads_for_json)
    except RuntimeError as e:
        logger.error(e)


def request_ss_records():
    data = []
    try:
        for url in config["sites"]:
            logger.info(f"Looking for new records in {url}")
            page = MyHTMLParser({'valid_tags': ['tr', 'td', 'a']}).feed_and_return(_get(url).text)
            pages, last = extract_pages(page.data)
            data += page.data
            pages_max = last.split('page')[1].split('.')[0]

            for p in range(2, int(pages_max)):
                _url = f"{config['sscom.url']}{last.replace(pages_max, str(p))}"
                logger.debug(f"Looking for new records in rest of pages {_url}")
                data += MyHTMLParser({'valid_tags': ['tr', 'td', 'a', 'br']}).feed_and_return(_get(_url).text).data
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


def verify_address(url, address):
    logger.debug(f"Verifying {address} url: {url}")
    return list(ss_ads.ads.find({"url": f"{url}", "address": f"{address}"}))


def verify_geodata(address):
    logger.debug(f"Verifying Geodata: {address}")
    return list(ss_ads.geodata.find({"address": f"{address}"}))


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

resolved = []

def resolve_diff_price(ad_old, ad_new):
    global ss_ads, resolved
    print('old_price', ad_old['price'], ad_new['price'])
    resolved.append({'old':ad_old, 'new':ad_new})
    # ss_ads.insert_one({'kind': 'old_price', 'ad_id': ObjectId(ad_old['_id']), 'price': ad_old['price'], 'date': datetime.datetime.utcnow()})
    # ss_ads.update_one({'_id': ObjectId(ad_old['_id'])}, {'$set': {'price':ad_new['price']}})


def skip(*args): pass
def skipError(*args): raise SkipError()
class SkipError(Exception):pass
class NotFoundResolver(Exception):pass
class NotFound(Exception):pass

mapping = {
    'price':resolve_diff_price,
    'price_m2':skip,
    'm2':skip,
    'level':skip,
    'rooms':skip,
    'kind':skip,
    'date':skipError,
    '_id':skipError,
}

def get(d:dict, key:str) -> object:
    try:
        return d[key]
    except KeyError as e:
        if key in ['_id', 'date', 'kind']:
            return None
        raise e


def compare(a, ad):
    for key in a.keys():
        if get(a, key) != get(ad, key):
            try:
                mapping[key](a, ad)
            except SkipError as e:
                pass
            except Exception as e:
                print(key, a[key], ad[key])
                raise NotFoundResolver()


while True:
    try:
        myclient = pymongo.MongoClient(config["db.url"])

        with myclient:
            ss_ads = myclient.ss_ads
            diff = ss_ads['diff']

            data = request_ss_records()

            ads = build_model(data)

            my_ads = list(ss_ads.ads.find({'kind':'ad'}))

            not_exist_resolver = []
            not_found = []
            for a in my_ads:
                try:
                    ad = find_by_url(a['url'], a['address'], ads)
                    result = compare(a, ad)
                except NotFoundResolver as e:
                    not_exist_resolver.append(a)
                except NotFound as e:
                    not_found.append(a)


            print('------Resolved. ----------------------------------')
            for a in resolved:
                print(a)
                ad_old = a['old']
                ad_new = a['new']
                ss_ads.ads.insert_one({'kind': 'old_price', 'ad_id': ObjectId(ad_old['_id']), 'price': ad_old['price'], 'date': datetime.datetime.utcnow()})
                ss_ads.ads.update_one({'_id': ObjectId(ad_old['_id'])}, {'$set': {'price':ad_new['price']}})
            # print('---------------------------------------------------')
            print('Resolved', len(resolved))

            print('------Not found record on ss.-----------------------')
            # for a in not_found:
            #     print(a)
            # print('---------------------------------------------------')
            print('Not found', len(not_found))

            print('------Not existing resolver.-----------------------')
            # for a in not_exist_resolver:
            #     print(a)
            # print('---------------------------------------------------')
            print('Not exist resolver', len(not_exist_resolver))

            # if is_property('report'):
            #     generate_report(ads, new_ads, new_address)
            #
            # if is_property('export') and 'export.filename' in config:
            #     logger.info("Exporting to file: %s", config['export.filename'])
            #     export_to_file(ads)
    except RuntimeError as e:
        logger.error(e)

    if 'restart' in config and config['restart'] > 0:
        logger.info("Waiting %s seconds.", config['restart'])
        time.sleep(config['restart'])
    else:
        break

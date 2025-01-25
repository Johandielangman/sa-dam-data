# ~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~
#      /\_/\
#     ( o.o )
#      > ^ <
#
# Author: Johan Hanekom
# Date: January 2024
#
# ~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~

# =========== // STANDARD IMPORTS // ===========

import re
import os
import pytz
import json
import copy
import hashlib
import tempfile
import threading
import concurrent.futures
from urllib.parse import quote
from dataclasses import dataclass
from datetime import datetime, date
from typing import (
    Dict,
    List,
    Any
)

# =========== // 3rd PARTY IMPORTS // ===========

import boto3
import requests
from loguru import logger
from pymongo import UpdateOne
from pymongo import MongoClient
from bs4 import BeautifulSoup, element
from botocore.exceptions import ClientError

# =========== // CONSTANTS // ===========

REPORT_BASE_URL: str = "https://www.dws.gov.za/hydrology/Weekly/ProvinceWeek.aspx"

PROVINCE_CODE_MAPPING: Dict[str, str] = {
    "LP": "Limpopo",
    "M": "Mpumalanga",
    "G": "Gauteng",
    "NW": "North-West",
    "KN": "KwaZulu-Natal",
    "FS": "Free State",
    "NC": "Northern Cape",
    "EC": "Eastern Cape",
    "WC": "Western Cape"
}

HEADER_TABLE_ID: str = "ContentPlaceHolder1_twh"
DAM_LEVEL_TABLE_ID: str = "ContentPlaceHolder1_tw"

DEFAULT_TIMEOUT: int = 30  # s

# https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
DEFAULT_DATE_FORMAT: str = "%Y-%M-%d"
ALWAYS_UPSERT_MONGO: bool = True
DATA_HEALTH_CHECK: bool = True

stop_event: threading.Event = threading.Event()


# =========== // ENV VARIABLES // ===========

def get_secrets() -> None:
    logger.debug("Getting environment variables")
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name="us-east-1"
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId="sa-dam-dashboard"
        )
    except ClientError as e:
        raise e

    for k, v in json.loads(
        get_secret_value_response['SecretString']
    ).items():
        os.environ[k] = v
    logger.debug("Environment Variables Loaded")


get_secrets()
S3_BUCKET: str = os.getenv("S3_BUCKET")

if not os.getenv("MONGO_USERNAME", ""):
    raise EnvironmentError("Could not get Mongo Credentials")


class Mongo:
    def __init__(
        self,
        username: str,
        password: str,
        cluster: str,
        database: str,
    ) -> None:
        logger.debug("Connecting to Mongo...")
        self.client: MongoClient = MongoClient(
            f"mongodb+srv://{quote(username, safe='')}:{quote(password, safe='')}@{cluster}?retryWrites=true&w=majority"
        )
        self.db = self.client[database]
        logger.debug("Connected to Mongo!")


mongo: Mongo = Mongo(
    username=os.getenv("MONGO_USERNAME", ""),
    password=os.getenv("MONGO_PASSWORD", ""),
    cluster=os.getenv("MONGO_CLUSTER", ""),
    database=os.getenv("MONGO_DB", "")
)


# =========== // HELPER FUNCTIONS // ===========

def to_numeric(
    string: str
) -> float:
    return float(string.replace("#", ""))


def get_date(
    text: str
) -> date:
    date: re.Match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if date:
        return datetime.strptime(
            date.group(),
            DEFAULT_DATE_FORMAT
        ).date()
    else:
        raise RuntimeError("Could not extract date")


def does_report_exist(
    date: date,
    overwrite: bool = ALWAYS_UPSERT_MONGO
) -> bool:
    if overwrite:
        return False
    return False  # TODO -- Implement!


class ReportExistsError(Exception):
    pass


def load_metadata() -> Dict[str, Dict]:
    logger.debug("Loading Dam Metadata...")
    metadata: List[Dict] = mongo.db['meta'].find({}, {"_id": 0})
    return {
        row['name']: row
        for row in metadata
    }


DAM_META: Dict[str, Dict] = load_metadata()


def data_health_check(
    dam_data: List[List[str]],
    province_code: str
):
    province: str = PROVINCE_CODE_MAPPING[province_code]
    for row in dam_data:
        if row[0] not in DAM_META:
            logger.warning(f"[{province}] Dam metadata not found for {row[0]}")
        else:
            dam_meta: Dict = DAM_META[row[0]]
            if (
                not isinstance(dam_meta['lat'], (float, int)) or
                not isinstance(dam_meta['long'], (float, int))
            ):
                logger.warning(f"[{province}] Lat-long data not found for {row[0]}")


def get_s3_key(
    dam_data: 'DamData'
) -> str:
    return f"{dam_data.report_date}/{dam_data.province}/{dam_data.dam}.jpg"


def get_s3_link(
    dam_data: 'DamData'
) -> str:
    return f"https://{S3_BUCKET}.s3.amazonaws.com/{quote(get_s3_key(dam_data))}"


def set_s3_link(data_set: List['DamData']):
    for dam_data in data_set:
        dam_data.s3_link = get_s3_link(dam_data)


def s3_key_exists(
    s3_client: object,
    s3_key: str
) -> bool:
    try:
        s3_client.head_object(
            Bucket=S3_BUCKET,
            Key=s3_key
        )
    except ClientError as e:
        return int(e.response['Error']['Code']) != 404
    return True


def process_dam_photos(
    data_set: List['DamData']
) -> None:
    logger.debug("Uploading Photos to S3...")
    if not S3_BUCKET:
        raise EnvironmentError("S3_BUCKET environment variable is not set.")

    s3_client = boto3.client("s3")
    uploaded_counter: int = 0
    already_exist_counter: int = 0

    with tempfile.TemporaryDirectory() as temp_dir:
        for dam_data in data_set:
            if dam_data.photo:
                try:
                    response: requests.Response = requests.get(dam_data.photo)
                    response.raise_for_status()

                    province_dir: str = os.path.join(temp_dir, dam_data.province)
                    os.makedirs(province_dir, exist_ok=True)

                    photo_path: str = os.path.join(province_dir, f"{dam_data.dam}.jpg")

                    # Save the photo locally
                    with open(photo_path, "wb") as file:
                        file.write(response.content)

                    # Upload the photo to S3
                    s3_key: str = get_s3_key(dam_data)
                    if not s3_key_exists(
                        s3_client=s3_client,
                        s3_key=s3_key
                    ):
                        s3_client.upload_file(
                            photo_path,
                            S3_BUCKET,
                            s3_key
                        )

                        uploaded_counter += 1
                    else:
                        already_exist_counter += 1

                except requests.RequestException as e:
                    logger.warning(f"Failed to download photo for dam {dam_data.dam}: {e}")
                except boto3.exceptions.S3UploadFailedError as e:
                    logger.warning(f"Failed to upload photo for dam {dam_data.dam} to S3: {e}")
                except Exception as e:
                    logger.critical(f"Unexpected Error: {e}")

    logger.debug(f"Finished uploading {uploaded_counter} images. {already_exist_counter} already existed")


def convert_to_serializable(obj):
    if isinstance(obj, date):
        return datetime(obj.year, obj.month, obj.day)
    if isinstance(obj, datetime):
        return obj
    return obj


def generate_unique_id(dam_data: 'DamData'):
    dam_copy: Dict = copy.deepcopy(dam_data.__dict__)
    dam_copy.pop('scrape_datetime')  # Remove the thing that keeps making it unique!
    hashable_content = str(dam_copy)
    return hashlib.sha256(hashable_content.encode()).hexdigest()


def upload_to_mongo(
    data_set: List['DamData']
) -> None:
    logger.debug("Uploading to Mongo")

    operations: List[UpdateOne] = []
    for dam_data in data_set:
        serialized_data: Dict[str, Any] = {
            key: convert_to_serializable(value)
            for key, value in dam_data.__dict__.items()
        }
        unique_id: str = generate_unique_id(dam_data)
        serialized_data["_id"] = unique_id

        operations.append(
            UpdateOne(
                {"_id": unique_id},
                {"$set": serialized_data},
                upsert=True
            )
        )

    try:
        results = mongo.db['reports'].bulk_write(operations)
        logger.debug(f"Finished uploading {len(operations)} records to Mongo")
        logger.info(
            f"Summary:"
            f"Deleted: {results.deleted_count}; "
            f"Inserted: {results.inserted_count}; "
            f"Matched: {results.matched_count}; "
            f"Modified: {results.modified_count}; "
            f"Upserted: {results.upserted_count}; "
        )
    except Exception as e:
        logger.error(f"Error uploading to MongoDB: {e}")


# =========== // Data Classes // ===========


@dataclass
class DamData:
    report_date: date = None
    province: str = ""
    dam: str = ""
    river: str = ""
    photo: str = ""
    s3_link: str = "#"
    full_storage_capacity: int = 0  # cubic meters
    this_week: int = 0
    last_week: int = 0
    last_year: int = 0
    scrape_datetime: datetime = datetime.now(pytz.timezone('Africa/Johannesburg'))
    nearest_locale: str = ""
    year_completed: str = ""
    wall_height_m: str = ""
    lat_long: List[float] = None


# =========== // SCRAPING FUNCTIONS // ===========


def get_soup(
    url: str
) -> BeautifulSoup:
    try:
        response: requests.Response = requests.get(
            url,
            timeout=DEFAULT_TIMEOUT
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching the URL: {e}")
        raise e

    return BeautifulSoup(
        response.text,
        'html.parser'
    )


def fetch_report_date(
    soup: BeautifulSoup,
    header_id: str = HEADER_TABLE_ID
) -> List[List[str]]:
    logger.debug("Fetching report Date")
    header_data: element.Tag = soup.find(
        'table',
        attrs={
            'id': header_id
        }
    )

    header: List[List[str]] = []
    header_rows: element.ResultSet = header_data.find_all('tr')
    for row in header_rows:
        cols: element.ResultSet = row.find_all(['td', 'th'])
        cols: List[str] = [ele.get_text(strip=True) for ele in cols]
        if cols:
            header.append(cols)
    return get_date(header[0][0])


def fetch_dam_data(
    soup: BeautifulSoup,
    table_id: str = DAM_LEVEL_TABLE_ID
) -> List[List[str]]:
    table: element.Tag = soup.find(
        'table',
        attrs={
            'id': table_id
        }
    )

    data: List[List[str]] = []
    rows: element.ResultSet = table.find_all('tr')
    for row in rows:
        cols: element.ResultSet = row.find_all(['td', 'th'])
        cols: List[str] = [
            ele.find('a')['href'] if ele.find('a') else ele.get_text(strip=True)
            for ele in cols
        ]
        if cols:
            data.append(cols)

    return data[1:-1]  # Remove header and footer


def load_data(
    province_code: str
) -> List[DamData]:
    try:
        if stop_event.is_set():
            return []

        soup: BeautifulSoup = get_soup(
            url=f"{REPORT_BASE_URL}?region={province_code}"
        )

        report_date: date = fetch_report_date(soup)
        logger.info(f"Latest report date: {report_date}")
        if does_report_exist(report_date):
            logger.warning("Report already exists in Mongo. Stopping Execution.")
            raise ReportExistsError

        dam_data: List[List[str]] = fetch_dam_data(soup)

        if DATA_HEALTH_CHECK:
            data_health_check(
                dam_data=dam_data,
                province_code=province_code
            )

        return [
            DamData(
                report_date=report_date,
                province=PROVINCE_CODE_MAPPING[province_code],
                dam=row[0],
                river=row[1],
                photo=f"https://www.dws.gov.za/Hydrology/Photos/{row[2].split('=')[1]}" if ".jpg" in row[2] else "",
                full_storage_capacity=to_numeric(row[4]) * 1e6,
                this_week=to_numeric(row[5]),
                last_week=to_numeric(row[6]),
                last_year=to_numeric(row[7]),
                nearest_locale=str(DAM_META.get(row[0], {}).get("nearest_locale", "NA")),
                year_completed=str(DAM_META.get(row[0], {}).get("year_completed", "NA")),
                wall_height_m=str(DAM_META.get(row[0], {}).get("wall_height_m", "NA")),
                lat_long=[
                    DAM_META.get(row[0], {}).get("lat"),
                    DAM_META.get(row[0], {}).get("long")
                ]
            )
            for row in dam_data
        ]
    except Exception as e:
        logger.error("An error occurred. Stopping all other workers")
        stop_event.set()
        raise e


# =========== // ENTRY // ===========

def main() -> None:

    logger.info("Starting dam data fetch...")

    data_set: List[DamData] = []
    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(
                1,
                (os.cpu_count() or 1) + 4
            )
        ) as executor:
            futures: List[concurrent.futures.Future] = [
                executor.submit(load_data, province_code)
                for province_code in PROVINCE_CODE_MAPPING.keys()
            ]
            for future in futures:
                data_set.extend(future.result())

        # Set the data for Mongo!
        set_s3_link(
            data_set=data_set
        )

        # Upload it to Mongo
        upload_to_mongo(
            data_set=data_set
        )

        # This one for last -- since it takes the longest!
        process_dam_photos(
            data_set=data_set
        )
        logger.info("Done 🚀")
    except ReportExistsError:
        logger.warning("Execution stopped. Report already exists in Mongo!")


def lambda_handler(event, context):
    main()

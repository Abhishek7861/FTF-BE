from fastapi import APIRouter
from app.mongo_client import getClient, getDatabase, getCollection
from fastapi import HTTPException, Query, Depends
from pytrends.request import TrendReq
from datetime import datetime, timedelta
from pymongo import DESCENDING
import httpx
import pytz
import logging
import asyncio
from logging.handlers import RotatingFileHandler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ...
# Add a rotating file handler to log to a file
handler = RotatingFileHandler('myapp.log', maxBytes=10 * 1024 * 1024,
                              backupCount=5)  # Log file size limited to 10MB with 5 backup copies
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s'))
logger.addHandler(handler)

router = APIRouter()


# Define a function to fetch data from the FTF API
async def fetch_ftf_data(page, gender, limit=20):
    try:
        # FTF API URL and authorization header
        url = "https://trends.fastfashion.live/trend/userid"
        headers = {"Authorization": "Bearer pbmNlaWhiY2tqcWJqa2NoM2d"}

        # Make a GET request to the FTF API with pagination parameters
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params={"gender": gender, "page": page, "limit": limit}, headers=headers)
            response.raise_for_status()

        # Return the JSON data from the FTF API response
        return response.json()

    except Exception as e:
        raise e


from fastapi import HTTPException, Query, Depends


def custom_key_param_checker(key_ftf: str = Query(..., description="Your custom query parameter")):
    # Define the expected value for your custom query parameter
    expected_value = "ajio-ftf"

    if key_ftf != expected_value:
        raise HTTPException(status_code=400, detail="Invalid request: The custom query parameter is invalid.")

    # If the custom parameter value is valid, return it
    return key_ftf


# Define a function to parse ISO format datetime strings with +05:30 timezone offset
def parse_iso_datetime(iso_datetime):
    try:
        # Remove the last 'Z' from the datetime string
        if iso_datetime.endswith('Z'):
            iso_datetime = iso_datetime[:-1]

        # Parse ISO format datetime string and add +05:30 offset
        parsed_datetime = datetime.fromisoformat(iso_datetime) + timedelta(hours=5, minutes=30)
        return parsed_datetime
    except Exception as e:
        print(f"Error parsing datetime: {str(e)}")
        return None


custom_description = """
Please don't try this api
"""


@router.get("/fetch_and_store_ftf_data/", summary="Fetch and Store FTF Data for all trends as per given gender",
            description=custom_description)
async def fetch_and_store_ftf_data(
        gender: str = Query(..., title="Gender"),
        key: str = Depends(custom_key_param_checker)  # Use the custom dependency for the "key" parameter
):
    try:
        # Access the MongoDB client and database
        db = getDatabase()

        # Fetch the first page of data to get the total count
        first_page_data = await fetch_ftf_data(page=1, gender=gender)

        # Extract the total count and calculate the number of pages
        total_count = first_page_data["count"]
        limit = 20  # The limit per page, adjust as needed
        num_pages = (total_count + limit - 1) // limit

        # Iterate through pages and fetch data
        for page in range(1, num_pages + 1):
            ftf_data = await fetch_ftf_data(page=page, gender=gender)

            # Convert timestamps to datetime objects before inserting

            for trend_data in ftf_data["data"]:
                for image in trend_data.get("images", []):
                    image["gender"] = gender
                    if "timeStamp" in image:
                        print("timestamp:", image["timeStamp"])
                        image["timeStamp"] = parse_iso_datetime(image["timeStamp"])
                        print("timestamp after convert:", image["timeStamp"])

            # Save the entire fetched data to MongoDB
            inserted_id = db["entire_data"].insert_one(ftf_data).inserted_id
            # Extract unique trends and save them with all their data
            # unique_trends = {item["name"]: item for item in ftf_data["data"]}
            # documents = [{"_id": unique_identifier, "data": trend_data} for unique_identifier, trend_data in unique_trends.items()]
            # db["unique_trends"].insert_many(documents)

            unique_trends = {item["name"]: item for item in ftf_data["data"]}
            documents = [{"name": trend_data["name"], "gender": gender, "data": trend_data} for trend_data in
                         unique_trends.values()]
            db["unique_trends"].insert_many(documents)

        return {"message": "Data saved to MongoDB", "document_id": str(inserted_id)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Initialize the pytrends object
pytrends = TrendReq(hl='en-US', tz=330)


@router.get("/get_trend_data")
async def get_trend_data(keyword: str = Query(..., title="Keyword")):
    try:
        # Build the payload for the keyword
        pytrends.build_payload(kw_list=[keyword], timeframe='today 3-m', geo='US')

        # Get the interest over time data
        interest_over_time_df = pytrends.interest_over_time()

        # Extract the relevant data
        trend_data = interest_over_time_df[keyword].to_dict()

        return {"keyword": keyword, "trend_data": trend_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Define a function to fetch product details for a trend
async def fetch_product_details(category: str, trend_id: str, trend_name: str, trend_gender: str):
    try:
        # FTF Product API URL and authorization header
        url = f"https://trends.fastfashion.live/products/trend/{category}/{trend_id}"
        headers = {"Authorization": "Bearer pbmNlaWhiY2tqcWJqa2NoM2d"}  # Replace with your actual authorization token
        logger.info("Fetching product details for trend: %s", trend_name)
        timeout = httpx.Timeout(timeout=60.0)  # Increase the timeout value as needed

        # Make a GET request to the FTF Product API with authorization
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()

        # Return the JSON data from the FTF Product API response
        product_details = response.json()
        logger.info("Product details fetched for trend: %s", trend_name)

        # Append category, trend ID, and trend name to each product object
        for product in product_details["results"][category]:
            product["category"] = category
            product["trendId"] = trend_id
            product["trendName"] = trend_name
            product["trendGender"] = trend_gender

        # Save all products in one batch to MongoDB for efficiency
        if product_details["results"][category]:
            db = getDatabase()
            db["product_details"].insert_many(product_details["results"][category])
            logger.info("Saved %d products for trend: %s", len(product_details["results"][category]), trend_name)

        return product_details["results"][category]

    except httpx.HTTPStatusError as e:
        logger.error("HTTP Error while fetching product details for trend %s: %s", trend_name, e)
        # You can handle specific HTTP errors here if needed
        raise HTTPException(status_code=500, detail=f"HTTP Error: {str(e)}")

    except Exception as e:
        logger.exception("An error occurred while fetching product details for trend %s", trend_name)
        raise HTTPException(status_code=500, detail="An error occurred while processing the request")


@router.get("/fetch_and_store_product_details/")
async def fetch_and_store_product_details(category: str, key: str = Depends(custom_key_param_checker)):
    try:
        db = getDatabase()

        # Get the list of trend IDs and names for the given category from the unique_trends collection
        trend_data = db["unique_trends"].find(
            {
                "data.category": category,
                "data.name": {"$exists": True, "$ne": None}
            },
            {"data.id": 1, "data.name": 1, "gender": 1}
        ).sort([("data.images.length", DESCENDING)]).limit(10)

        # Iterate through trend data and fetch product details
        for trend in trend_data:
            trend_id = trend["data"]["id"]
            trend_name = trend["data"]["name"]
            trend_gender = trend["gender"]
            await fetch_product_details(category, trend_id, trend_name, trend_gender)
            logger.info("Processed trend: %s", trend_name)

            # Introduce a 10-second sleep before making the next API call
            await asyncio.sleep(10)  # Sleep for 10 seconds

        return {"message": "Product details saved to MongoDB"}

    except Exception as e:
        logger.exception("An error occurred while processing the request")
        raise HTTPException(status_code=500, detail="An error occurred while processing the request")

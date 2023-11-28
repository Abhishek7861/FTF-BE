from fastapi import APIRouter, Query
from app.mongo_client import getClient,getDatabase,getCollection
from fastapi import HTTPException
from datetime import datetime, timedelta
from typing import Optional
from typing import List

from bson import ObjectId
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from bson.json_util import dumps

from starlette.responses import JSONResponse


import pandas as pd
import json



class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)

router = APIRouter()

@router.get("/get_unique_trends/")
def get_unique_trends():
    try:
        # Retrieve unique trend names from the MongoDB collection
        unique_trends = getCollection("unique_trends")

        return unique_trends

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/get_unique_categories_with_trends/")
def get_unique_categories_with_trends():
    try:
        # Retrieve unique trends with their categories from the MongoDB collection
        unique_categories_with_trends =  getCollection("unique_trends").aggregate([
            {
                "$group": {
                    "_id": "$data.category",
                    "trends": {
                        "$addToSet": "$data.name"
                    }
                }
            }
        ])

        return list(unique_categories_with_trends)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    






from typing import Optional

# ...

@router.get("/get_top_trends_and_distribution_by_category/")
def get_top_trends_and_distribution_by_category(
    category: str = Query(..., description="Category name"),
    top_n: int = Query(..., description="Number of top trends to return"),
    geography: Optional[str] = Query(None, description="Geography filter")
):
    try:
        # Access the MongoDB database
        db = getDatabase()

        # Define a filter query based on the geography parameter
        filter_query = {"data.category": category}
        if geography:
            filter_query["data.images.geography"] = geography

        # Aggregate and sort trends by the number of images in descending order
        pipeline = [
            {"$match": filter_query},
            {"$unwind": "$data.images"},
            {"$group": {
                "_id": "$data.name",
                "imageCount": {"$sum": 1},
                "firstImage": {"$first": "$data.images"}  # Include the first image
            }},
            {"$sort": {"imageCount": -1}},
            {"$limit": top_n}
        ]

        top_trends = list(db["unique_trends"].aggregate(pipeline))

        # Calculate the total number of images in the category
        total_images_in_category = list(db["unique_trends"].aggregate([
            {"$match": filter_query},
            {"$group": {
                "_id": None,
                "totalImages": {"$sum": {"$size": "$data.images"}}
            }}
        ]))

        total_images = total_images_in_category[0]["totalImages"] if total_images_in_category else 0

        # Calculate the percentage distribution for each trend
        for trend in top_trends:
            trend["percentageDistribution"] = (trend["imageCount"] / total_images) * 100

        return top_trends

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


    

@router.get("/get_time_series_data1/")
def get_time_series_data1(trend_name: str, timewindow: int):
    try:
        # Access the MongoDB database
        db = getDatabase()

        # Calculate the start date as timewindow days ago from the current date
        start_date = datetime.utcnow() - timedelta(days=timewindow)

        # Query MongoDB for data within the specified time window and matching trend name

        #start_date_iso = start_date.replace(microsecond=0).isoformat() + "Z"
        query = {
            "data.name": trend_name,
            "data.images.timeStamp": {"$gte": start_date}
        }

        # Projection to get only the required fields
        projection = {
            "data.images.timeStamp": 1
        }

        # # Use aggregation to group by date and count the number of images for each date
        pipeline = [
            {"$match": query},
            {"$unwind": "$data.images"},
            {
                "$project": {
                    "date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$data.images.timeStamp"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$date",
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ]

        # pipeline = [
        #     {"$match": query}
            # {"$unwind": "$data.images"},
            # {
            #      "$project": {
            #      "date": "$data.images.timeStamp"  # Use the timestamp directly
            #      }
            # },
            # {
            #      "$group": {
            #         "_id": "$date",
            #         "count": {"$sum": 1}
            #      }
            # },
            #  {"$sort": {"_id": 1}}
        #]


        # Execute the aggregation pipeline
        result = list(db["unique_trends"].aggregate(pipeline))

        # Create a dictionary with date as key and count as value
        time_series_data = {item["_id"]: item["count"] for item in result}

        return {"time_series_data": time_series_data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    



@router.get("/get_time_series_data/")
def get_time_series_data(trend_name: str, timewindow: int):
    try:
        # Access the MongoDB database
        db = getDatabase()

        # Calculate the start date as timewindow days ago from the current date
        start_date = datetime.utcnow() - timedelta(days=timewindow)

        # Query MongoDB for data within the specified time window and matching trend name
        query = {
            "data.name": trend_name,
            "data.images": {
                "$elemMatch": {
                    "timeStamp": {"$gte": start_date}
                }
            }
        }

        # Projection to get only the required fields
        projection = {
            "data.images": 1
        }

        # Execute the query
        cursor = db["unique_trends"].find(query, projection)

        # Initialize a dictionary to store time series data
        time_series_data = {}

        # Iterate through the cursor and count images per date
        for document in cursor:
            images = document.get("data", {}).get("images", [])
            for image in images:
                timestamp = image.get("timeStamp")
                if timestamp and timestamp >= start_date:
                    date_str = timestamp.strftime("%Y-%m-%d")
                    if date_str not in time_series_data:
                        time_series_data[date_str] = 1
                    else:
                        time_series_data[date_str] += 1

        return {"time_series_data": time_series_data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/generate_csv/")
async def generate_csv():
    try:
        # Fetch top 500 products based on score in descending order with "ajio" e-commerce
        # Access the MongoDB database
        db = getDatabase()

        collection = db["product_details"]
        products = collection.find(
            {"ecommerce": "Ajio"},
            {
                "_id": 0,
                "id": 1,
                "title": 1,
                "description": 1,
                "product_url": 1,
                "category": 1,
                "image_url": 1,
            },
        ).sort("score", -1).limit(50)
       # Define a mapping of column names
        column_mapping = {
            "id": "id",
            "product_url": "product_url",
            "title": "product_title",
            "image_url": "image_url_1",
            "description": "description",
            "category": "category",
        }

        # Create a DataFrame from the MongoDB cursor, mapping column names
        df = pd.DataFrame(list(products)).rename(columns=column_mapping)

        # Define CSV file path
        csv_file_path = "top_products.csv"

        # Save DataFrame to CSV
        df.to_csv(csv_file_path, index=False)

        return FileResponse(csv_file_path, headers={"Content-Disposition": "attachment; filename=top_products.csv"})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@router.get("/generate_json/")
async def generate_json():
    try:
        # Fetch top 500 products based on score in descending order with "ajio" e-commerce
        # Access the MongoDB database
        db = getDatabase()
        collection = db["product_details"]
        products = collection.find(
            {"ecommerce": "Ajio"},
            {
                "_id": 0,
                "id": 1,    
                "image_url": 1,
            },
        ).sort("score", -1).skip(100).limit(399)

        # Define a mapping of column names
        column_mapping = {
            "id": "id",            
            "image_url": "image_url_1",
            
        }

        # Create a list of dictionaries from the MongoDB cursor, mapping column names
        product_list = [dict((column_mapping.get(key, key), value) for key, value in product.items()) for product in products]

        return JSONResponse(content=product_list, media_type="application/json", headers={"Content-Disposition": "attachment; filename=top_products.json"})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/get_trend_data_by_name_and_gender/")
def get_trend_data_by_name_and_gender(
    trend_name: str = Query(..., title="Trend Name"),
    gender: str = Query(..., title="Gender")
):
    try:
        # Access the MongoDB database
        db = getDatabase()

        # Query MongoDB to fetch data for the given trend name and gender
        trend_data = db["unique_trends"].find_one({
            "data.name": trend_name,
            "gender": gender
        })

        if trend_data:
            # Convert the MongoDB document to a Python dictionary
            trend_data_dict = dict(trend_data)

            # Remove the ObjectId field if it exists
            if "_id" in trend_data_dict:
                trend_data_dict.pop("_id")

            # Serialize the dictionary using the custom encoder
            response_data = json.dumps(trend_data_dict, cls=DateTimeEncoder)

            # Parse the JSON string back into a JSON object
            response_json = json.loads(response_data)

            return JSONResponse(content=response_json)
        else:
            return {"message": f"No data found for trend '{trend_name}' and gender '{gender}'"}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    





@router.get("/percentage_contribution/")
def get_percentage_contribution(
    category: str = Query(..., description="Category name"),
    trendName: str = Query(None, description="Trend name (optional)")
):
    try:
        # Access the MongoDB database
        db = getDatabase()

        # Create a filter query based on the provided parameters
        filter_query = {"category": category}
        

        # Group by e-commerce and calculate the count of records
        aggregation_pipeline = [
            {"$match": filter_query},
            {"$group": {"_id": "$ecommerce", "count": {"$sum": 1}}}
        ]

        # Execute the aggregation pipeline
        result = list(db["product_details"].aggregate(aggregation_pipeline))

        # Calculate the total count of records
        total_records = sum(item["count"] for item in result)

        # Calculate the percentage contribution for each e-commerce entry at the category level
        category_level_contributions = [
            {"ecommerce": item["_id"], "percentage_contribution": (item["count"] / total_records) * 100}
            for item in result
        ]

        # Calculate the percentage contribution for each e-commerce entry at the trend level (if trendName provided)
        trend_level_contributions = []
        if trendName:
            trend_filter_query = {"category": category, "trendName": trendName}
            trend_result = list(db["product_details"].aggregate([
                {"$match": trend_filter_query},
                {"$group": {"_id": "$ecommerce", "count": {"$sum": 1}}}
            ]))
            trend_total_records = sum(item["count"] for item in trend_result)

            trend_level_contributions = [
                {"ecommerce": item["_id"], "percentage_contribution": (item["count"] / trend_total_records) * 100}
                for item in trend_result
            ]

        return {
            "category": category,
            "trendName": trendName,
            "category_level_contributions": category_level_contributions,
            "trend_level_contributions": trend_level_contributions
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





@router.get("/get_products_by_filters/")
def get_products_by_filters(
    trendName: str = Query(..., description="Trend name"),
    ecommerce: str = Query(..., description="E-commerce name"),
    count: int = Query(..., description="Number of records to fetch")
):
    try:
        # Access the MongoDB database
        db = getDatabase()

        # Create a filter query based on the provided parameters
        filter_query = {"trendName": trendName, "ecommerce": ecommerce}

        # Fetch products matching the filter query, limit to 'count', and sort by 'score' in descending order
        products = list(db["product_details"].find(filter_query, {"_id": 0}).sort("score", -1).limit(count))

        return {"products": products}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





@router.get("/get_product_counts_by_price_range/")
def get_product_counts_by_price_range(
    trendName: str = Query(..., description="Trend name"),
    ecommerce: str = Query(..., description="E-commerce name")
):
    try:
        # Access the MongoDB database
        db = getDatabase()
        
        # Create a filter query based on the provided parameters
        filter_query = {"trendName": trendName, "ecommerce": ecommerce}

        # Define price ranges
        price_ranges = [
            {"min": 0, "max": 1000},
            {"min": 1000, "max": 2000},
            {"min": 2000, "max": 3000},
            {"min": 3000, "max": 4000},
            {"min": 4000, "max": 5000},
            {"min": 5000, "max": float("inf")}
        ]

        # Initialize a dictionary to store product counts for each price range
        price_range_counts = {}

        # Iterate over the price ranges and count products in each range
        for price_range in price_ranges:
            min_price = price_range["min"]
            max_price = price_range["max"]

            # Count products within the current price range
            count = db["product_details"].count_documents(
                {
                    **filter_query,
                    "price": {"$gte": min_price, "$lt": max_price}
                }
            )

            # Store the count in the dictionary
            price_range_counts[f"INR {min_price}-{max_price}"] = count

        return {"product_counts_by_price_range": price_range_counts}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





@router.get("/get_tags_data/")
async def get_tags_data():
    try:
        # Read the JSON file
        with open('1697200732229584.json', 'r') as file:
            data = json.load(file)
        # Return the JSON data
        return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

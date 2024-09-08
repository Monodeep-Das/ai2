from fastapi import FastAPI, HTTPException, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Tuple
import pandas as pd
from opencage.geocoder import OpenCageGeocode
from geopy.distance import geodesic
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

app = FastAPI()

# Set up CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Your React app URL or "*" to allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (POST, GET, etc.)
    allow_headers=["*"],  # Allow all headers
)

# Define the data model
class Location(BaseModel):
    address: str  # Real address string

class CurrentLocation(BaseModel):
    address: str  # User's current address for finding the nearest delivery location

process_data = APIRouter()

# Initialize OpenCage geocoder
geocoder = OpenCageGeocode("49f2530ec11943f092d25acb9bfaff2b")

# Function to get latitude and longitude from address
def get_lat_lon(address: str) -> Tuple[float, float]:
    try:
        results = geocoder.geocode(address)
        if results and len(results) > 0:
            location = results[0]  # Get the first result
            return location['geometry']['lat'], location['geometry']['lng']
        else:
            raise ValueError(f"Could not find coordinates for address: {address}")
    except Exception as e:
        raise ValueError(f"Error occurred while geocoding address: {str(e)}")

# Function to calculate the nearest delivery location
def find_nearest_location(user_lat: float, user_lon: float, locations: pd.DataFrame) -> Tuple[str, float]:
    # Print DataFrame columns for debugging
    logging.info("DataFrame columns: %s", locations.columns)
    
    # Check if the required columns exist
    if 'latitude' not in locations.columns or 'longitude' not in locations.columns:
        raise ValueError("DataFrame must contain 'latitude' and 'longitude' columns")
    
    # Calculate distances
    locations['distance'] = locations.apply(
        lambda row: geodesic((user_lat, user_lon), (row['latitude'], row['longitude'])).km, axis=1
    )
    
    # Find the nearest location
    nearest_location = locations.loc[locations['distance'].idxmin()]
    return nearest_location['address'], nearest_location['distance']

@process_data.post("/process-data/")
async def process_data_endpoint(current_location: CurrentLocation):
    try:
        # Load the delivery data from JSON file
        with open('delivery.json', 'r') as file:
            delivery_data = json.load(file)

        # Convert delivery data to DataFrame
        df = pd.DataFrame(delivery_data)
        logging.info("DataFrame loaded: %s", df.head())  # Debugging line to see the DataFrame
        
        # Check if DataFrame is empty
        if df.empty:
            raise HTTPException(status_code=404, detail="No delivery addresses found")

        # Geocode delivery addresses to get latitude and longitude
        df[['latitude', 'longitude']] = df['address'].apply(lambda addr: pd.Series(get_lat_lon(addr)))

        # Geocode the user's current address
        user_lat, user_lon = get_lat_lon(current_location.address)
        logging.info("User location: Latitude %f, Longitude %f", user_lat, user_lon)

        # Find the nearest delivery location
        nearest_location, distance = find_nearest_location(user_lat, user_lon, df)
        logging.info("Nearest location: %s, Distance: %f", nearest_location, distance)

        return {
            'nearest_location': {
                'location': nearest_location,
                'distance_km': distance
            }
        }
    except Exception as e:
        logging.error("Exception occurred: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))

app.include_router(process_data)

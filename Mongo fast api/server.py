from fastapi import FastAPI
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient

application = FastAPI()

mongo_client = AsyncIOMotorClient("mongodb://database:27017")
database = mongo_client["storedb"]
products_collection = database["products"]

class Product(BaseModel):
    title: str
    cost: float
    stock: int

default_products = [
    {"title": "Chees", "cost": 2.5, "stock": 20},
    {"title": "Chocolate", "cost": 1.0, "stock": 40},
    {"title": "Biscuit", "cost": 3.0, "stock": 12},
   
]

@application.on_event("startup")
async def load_default_data():
    total = await products_collection.count_documents({})
    if total == 0:
        await products_collection.insert_many(default_products)

@application.post("/product/add")
async def create_product(product: Product):
    record = await products_collection.insert_one(product.dict())
    return {"id": str(record.inserted_id), "status": "added"}

@application.get("/product/all")
async def fetch_all_products():
    result = []
    async for prod in products_collection.find():
        prod["_id"] = str(prod["_id"])
        result.append(prod)
    return {"items": result}

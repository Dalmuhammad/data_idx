import duckdb
import logging
import pandas as pd
import boto3

from io import BytesIO
from datetime import datetime, timedelta, date
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BUCKET_NAME = "afdal-idx-stock-data-s3"
DIM_STOCK_KEY = "modeled/dim_stock/data.parquet"


def upload_df_to_s3(df, bucket_name, key, s3_client=None):
    s3_client = s3_client or boto3.client("s3")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())

def init_dim_sector_industry():
    #ini untuk sementara masih di-update manual mappingannya (mengikuti kebijakan IDX) dan menggunakan SDC type 1.
    key = "modeled/dim_sector_industry/data.parquet"
    idx_ic_flat_list = [
    {
        "code": "A111",
        "sector": "Energy",
        "sub_sector": "Oil, Gas & Coal",
        "industry": "Oil & Gas",
        "sub_industry": "Oil & Gas Production & Refinery"
    },
    {
        "code": "A112",
        "sector": "Energy",
        "sub_sector": "Oil, Gas & Coal",
        "industry": "Oil & Gas",
        "sub_industry": "Oil & Gas Storage & Distribution"
    },
    {
        "code": "A121",
        "sector": "Energy",
        "sub_sector": "Oil, Gas & Coal",
        "industry": "Coal",
        "sub_industry": "Coal Production"
    },
    {
        "code": "A122",
        "sector": "Energy",
        "sub_sector": "Oil, Gas & Coal",
        "industry": "Coal",
        "sub_industry": "Coal Distribution"
    },
    {
        "code": "A131",
        "sector": "Energy",
        "sub_sector": "Oil, Gas & Coal",
        "industry": "Oil, Gas & Coal Supports",
        "sub_industry": "Oil & Gas Drilling Service"
    },
    {
        "code": "A132",
        "sector": "Energy",
        "sub_sector": "Oil, Gas & Coal",
        "industry": "Oil, Gas & Coal Supports",
        "sub_industry": "Oil, Gas & Coal Equipment & Services"
    },
    {
        "code": "A211",
        "sector": "Energy",
        "sub_sector": "Alternative Energy",
        "industry": "Alternative Energy Equipment",
        "sub_industry": "Alternative Energy Equipment"
    },
    {
        "code": "A221",
        "sector": "Energy",
        "sub_sector": "Alternative Energy",
        "industry": "Alternative Fuels",
        "sub_industry": "Alternative Fuels"
    },
    {
        "code": "B111",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Chemicals",
        "sub_industry": "Basic Chemicals"
    },
    {
        "code": "B112",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Chemicals",
        "sub_industry": "Agricultural Chemicals"
    },
    {
        "code": "B113",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Chemicals",
        "sub_industry": "Specialty Chemicals"
    },
    {
        "code": "B121",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Construction Materials",
        "sub_industry": "Construction Materials"
    },
    {
        "code": "B131",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Containers & Packaging",
        "sub_industry": "Containers & Packaging"
    },
    {
        "code": "B141",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Metals & Minerals",
        "sub_industry": "Aluminum"
    },
    {
        "code": "B142",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Metals & Minerals",
        "sub_industry": "Cooper"
    },
    {
        "code": "B143",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Metals & Minerals",
        "sub_industry": "Gold"
    },
    {
        "code": "B144",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Metals & Minerals",
        "sub_industry": "Iron & Steel"
    },
    {
        "code": "B145",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Metals & Minerals",
        "sub_industry": "Precious Metals & Minerals"
    },
    {
        "code": "B146",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Metals & Minerals",
        "sub_industry": "Diversified Metals & Minerals"
    },
    {
        "code": "B147",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Metals & Minerals",
        "sub_industry": "Mining Equipment & Services"
    },
    {
        "code": "B151",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Forestry & Paper",
        "sub_industry": "Timber"
    },
    {
        "code": "B152",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Forestry & Paper",
        "sub_industry": "Paper"
    },
    {
        "code": "B153",
        "sector": "Basic Materials",
        "sub_sector": "Basic Materials",
        "industry": "Forestry & Paper",
        "sub_industry": "Diversified Forest Products"
    },
    {
        "code": "C111",
        "sector": "industryals",
        "sub_sector": "industryal Goods",
        "industry": "Aerospace & Defense",
        "sub_industry": "Aerospace & Defense"
    },
    {
        "code": "C121",
        "sector": "industryals",
        "sub_sector": "industryal Goods",
        "industry": "Building Products & Fixtures",
        "sub_industry": "Building Products & Fixtures"
    },
    {
        "code": "C131",
        "sector": "industryals",
        "sub_sector": "industryal Goods",
        "industry": "Electrical",
        "sub_industry": "Electrical Components & Equipment"
    },
    {
        "code": "C132",
        "sector": "industryals",
        "sub_sector": "industryal Goods",
        "industry": "Electrical",
        "sub_industry": "Heavy Electrical Equipment"
    },
    {
        "code": "C141",
        "sector": "industryals",
        "sub_sector": "industryal Goods",
        "industry": "Machinery",
        "sub_industry": "Construction Machinery & Heavy Vehicles"
    },
    {
        "code": "C142",
        "sector": "industryals",
        "sub_sector": "industryal Goods",
        "industry": "Machinery",
        "sub_industry": "Agricultural & Farm Machinery"
    },
    {
        "code": "C143",
        "sector": "industryals",
        "sub_sector": "industryal Goods",
        "industry": "Machinery",
        "sub_industry": "industryal Machinery & Components"
    },
    {
        "code": "C211",
        "sector": "industryals",
        "sub_sector": "industryal Services",
        "industry": "Diversified industryal Trading",
        "sub_industry": "Diversified industryal Trading"
    },
    {
        "code": "C221",
        "sector": "industryals",
        "sub_sector": "industryal Services",
        "industry": "Commercial Services",
        "sub_industry": "Commercial Printing"
    },
    {
        "code": "C222",
        "sector": "industryals",
        "sub_sector": "industryal Services",
        "industry": "Commercial Services",
        "sub_industry": "Environmental & Facilities Services"
    },
    {
        "code": "C223",
        "sector": "industryals",
        "sub_sector": "industryal Services",
        "industry": "Commercial Services",
        "sub_industry": "Office Supplies"
    },
    {
        "code": "C224",
        "sector": "industryals",
        "sub_sector": "industryal Services",
        "industry": "Commercial Services",
        "sub_industry": "Business Support Services"
    },
    {
        "code": "C231",
        "sector": "industryals",
        "sub_sector": "industryal Services",
        "industry": "Professional Services",
        "sub_industry": "Human Resource & Employment Services"
    },
    {
        "code": "C232",
        "sector": "industryals",
        "sub_sector": "industryal Services",
        "industry": "Professional Services",
        "sub_industry": "Research & Consulting Services"
    },
    {
        "code": "C311",
        "sector": "industryals",
        "sub_sector": "Multi-sector Holdings",
        "industry": "Multi-sector Holdings",
        "sub_industry": "Multi-sector Holdings"
    },
    {
        "code": "D111",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Food & Staples Retailing",
        "industry": "Food & Staples Retailing",
        "sub_industry": "Drug Retail & Distributors"
    },
    {
        "code": "D112",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Food & Staples Retailing",
        "industry": "Food & Staples Retailing",
        "sub_industry": "Food Retail & Distributors"
    },
    {
        "code": "D113",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Food & Staples Retailing",
        "industry": "Food & Staples Retailing",
        "sub_industry": "Supermarkets & Convenience Store"
    },
    {
        "code": "D211",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Food & Beverage",
        "industry": "Beverages",
        "sub_industry": "Liquors"
    },
    {
        "code": "D212",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Food & Beverage",
        "industry": "Beverages",
        "sub_industry": "Soft Drinks"
    },
    {
        "code": "D221",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Food & Beverage",
        "industry": "Processed Foods",
        "sub_industry": "Dairy Products"
    },
    {
        "code": "D222",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Food & Beverage",
        "industry": "Processed Foods",
        "sub_industry": "Processed Foods"
    },
    {
        "code": "D231",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Food & Beverage",
        "industry": "Agricultural Products",
        "sub_industry": "Fish, Meat, & Poultry"
    },
    {
        "code": "D232",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Food & Beverage",
        "industry": "Agricultural Products",
        "sub_industry": "Plantations & Crops"
    },
    {
        "code": "D311",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Tobacco",
        "industry": "Tobacco",
        "sub_industry": "Tobacco"
    },
    {
        "code": "D411",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Nondurable Household Products",
        "industry": "Household Products",
        "sub_industry": "Household Products"
    },
    {
        "code": "D421",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Nondurable Household Products",
        "industry": "Personal Care Products",
        "sub_industry": "Personal Care Products"
    },
    {
        "code": "E111",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Automobiles & Components",
        "industry": "Auto Components",
        "sub_industry": "Auto Parts & Equipment"
    },
    {
        "code": "E112",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Automobiles & Components",
        "industry": "Auto Components",
        "sub_industry": "Tires"
    },
    {
        "code": "E121",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Automobiles & Components",
        "industry": "Automobiles",
        "sub_industry": "Car Manufacturers"
    },
    {
        "code": "E122",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Automobiles & Components",
        "industry": "Automobiles",
        "sub_industry": "Motorcycle Manufacturers"
    },
    {
        "code": "E211",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Household Goods",
        "industry": "Household Goods",
        "sub_industry": "Home Furnishings"
    },
    {
        "code": "E212",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Household Goods",
        "industry": "Household Goods",
        "sub_industry": "Household Appliances"
    },
    {
        "code": "E213",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Household Goods",
        "industry": "Household Goods",
        "sub_industry": "Housewares & Specialties"
    },
    {
        "code": "E311",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Leisure Goods",
        "industry": "Consumer Electronics",
        "sub_industry": "Consumer Electronics"
    },
    {
        "code": "E321",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Leisure Goods",
        "industry": "Sport Equipment & Hobbies Goods",
        "sub_industry": "Sport Equipment & Hobbies Goods"
    },
    {
        "code": "E411",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Apparel & Luxury Goods",
        "industry": "Apparel & Luxury Goods",
        "sub_industry": "Clothing, Accessories & Bags"
    },
    {
        "code": "E412",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Apparel & Luxury Goods",
        "industry": "Apparel & Luxury Goods",
        "sub_industry": "Footwear"
    },
    {
        "code": "E413",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Apparel & Luxury Goods",
        "industry": "Apparel & Luxury Goods",
        "sub_industry": "Textiles"
    },
    {
        "code": "E511",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Consumer Services",
        "industry": "Tourism & Recreation",
        "sub_industry": "Gaming Venue"
    },
    {
        "code": "E512",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Consumer Services",
        "industry": "Tourism & Recreation",
        "sub_industry": "Hotels, Resorts & Cruise Lines"
    },
    {
        "code": "E513",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Consumer Services",
        "industry": "Tourism & Recreation",
        "sub_industry": "Travel Agencies"
    },
    {
        "code": "E514",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Consumer Services",
        "industry": "Tourism & Recreation",
        "sub_industry": "Recreational & Sports Facilities"
    },
    {
        "code": "E515",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Consumer Services",
        "industry": "Tourism & Recreation",
        "sub_industry": "Restaurants"
    },
    {
        "code": "E521",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Consumer Services",
        "industry": "Education & Support Services",
        "sub_industry": "Education Services"
    },
    {
        "code": "E522",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Consumer Services",
        "industry": "Education & Support Services",
        "sub_industry": "Consumer Support Services"
    },
    {
        "code": "E611",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Media & Entertainment",
        "industry": "Media",
        "sub_industry": "Advertising"
    },
    {
        "code": "E612",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Media & Entertainment",
        "industry": "Media",
        "sub_industry": "Broadcasting"
    },
    {
        "code": "E613",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Media & Entertainment",
        "industry": "Media",
        "sub_industry": "Cable & Satellite"
    },
    {
        "code": "E614",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Media & Entertainment",
        "industry": "Media",
        "sub_industry": "Consumer Publishing"
    },
    {
        "code": "E621",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Media & Entertainment",
        "industry": "Entertainment & Movie Production",
        "sub_industry": "Entertainment & Movie Production"
    },
    {
        "code": "E711",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Retailing",
        "industry": "Consumer Distributors",
        "sub_industry": "Consumer Distributors"
    },
    {
        "code": "E721",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Retailing",
        "industry": "Internet & Homeshop Retail",
        "sub_industry": "Internet & Homeshop Retail"
    },
    {
        "code": "E731",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Retailing",
        "industry": "Department Stores",
        "sub_industry": "Department Stores"
    },
    {
        "code": "E741",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Retailing",
        "industry": "Specialty Retail",
        "sub_industry": "Apparel & Textile Retail"
    },
    {
        "code": "E742",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Retailing",
        "industry": "Specialty Retail",
        "sub_industry": "Electronics Retail"
    },
    {
        "code": "E743",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Retailing",
        "industry": "Specialty Retail",
        "sub_industry": "Home Improvement Retail"
    },
    {
        "code": "E744",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Retailing",
        "industry": "Specialty Retail",
        "sub_industry": "Specialty Stores"
    },
    {
        "code": "E745",
        "sector": "Consumer Cyclicals",
        "sub_sector": "Retailing",
        "industry": "Specialty Retail",
        "sub_industry": "Automotive Retail"
    },
    {
        "code": "F111",
        "sector": "Healthcare",
        "sub_sector": "Healthcare Equipment & Providers",
        "industry": "Healthcare Equipment & Supplies",
        "sub_industry": "Healthcare Equipment"
    },
    {
        "code": "F112",
        "sector": "Healthcare",
        "sub_sector": "Healthcare Equipment & Providers",
        "industry": "Healthcare Equipment & Supplies",
        "sub_industry": "Healthcare Supplies & Distributions"
    },
    {
        "code": "F121",
        "sector": "Healthcare",
        "sub_sector": "Healthcare Equipment & Providers",
        "industry": "Healthcare Providers",
        "sub_industry": "Healthcare Providers"
    },
    {
        "code": "F211",
        "sector": "Healthcare",
        "sub_sector": "Pharmaceuticals & Health Care Research",
        "industry": "Pharmaceuticals",
        "sub_industry": "Pharmaceuticals"
    },
    {
        "code": "F221",
        "sector": "Healthcare",
        "sub_sector": "Pharmaceuticals & Health Care Research",
        "industry": "Healthcare Research",
        "sub_industry": "Healthcare Research"
    },
    {
        "code": "G111",
        "sector": "Financials",
        "sub_sector": "Banks",
        "industry": "Banks",
        "sub_industry": "Banks"
    },
    {
        "code": "G211",
        "sector": "Financials",
        "sub_sector": "Financing Service",
        "industry": "Consumer Financing",
        "sub_industry": "Consumer Financing"
    },
    {
        "code": "G221",
        "sector": "Financials",
        "sub_sector": "Financing Service",
        "industry": "Business Financing",
        "sub_industry": "Venture Capital"
    },
    {
        "code": "G222",
        "sector": "Financials",
        "sub_sector": "Financing Service",
        "industry": "Business Financing",
        "sub_industry": "Specialize Business Financing"
    },
    {
        "code": "G311",
        "sector": "Financials",
        "sub_sector": "Investment Service",
        "industry": "Investment Services",
        "sub_industry": "Investment Management"
    },
    {
        "code": "G312",
        "sector": "Financials",
        "sub_sector": "Investment Service",
        "industry": "Investment Services",
        "sub_industry": "Investment Banking & Brokerage Services"
    },
    {
        "code": "G313",
        "sector": "Financials",
        "sub_sector": "Investment Service",
        "industry": "Investment Services",
        "sub_industry": "Market Operators"
    },
    {
        "code": "G314",
        "sector": "Financials",
        "sub_sector": "Investment Service",
        "industry": "Investment Services",
        "sub_industry": "Investment Support Service"
    },
    {
        "code": "G411",
        "sector": "Financials",
        "sub_sector": "Insurance",
        "industry": "Insurance",
        "sub_industry": "Insurance Brokers"
    },
    {
        "code": "G412",
        "sector": "Financials",
        "sub_sector": "Insurance",
        "industry": "Insurance",
        "sub_industry": "General Insurance"
    },
    {
        "code": "G413",
        "sector": "Financials",
        "sub_sector": "Insurance",
        "industry": "Insurance",
        "sub_industry": "Life Insurance"
    },
    {
        "code": "G414",
        "sector": "Financials",
        "sub_sector": "Insurance",
        "industry": "Insurance",
        "sub_industry": "Reinsurance"
    },
    {
        "code": "G511",
        "sector": "Financials",
        "sub_sector": "Holding & Investment Companies",
        "industry": "Holding & Investment Companies",
        "sub_industry": "Financial Holdings"
    },
    {
        "code": "G512",
        "sector": "Financials",
        "sub_sector": "Holding & Investment Companies",
        "industry": "Holding & Investment Companies",
        "sub_industry": "Investment Companies"
    },
    {
        "code": "H111",
        "sector": "Properties & Real Estate",
        "sub_sector": "Properties & Real Estate",
        "industry": "Real Estate Management & Development",
        "sub_industry": "Real Estate Development & Management"
    },
    {
        "code": "H112",
        "sector": "Properties & Real Estate",
        "sub_sector": "Properties & Real Estate",
        "industry": "Real Estate Management & Development",
        "sub_industry": "Real Estate Services"
    },
    {
        "code": "I111",
        "sector": "Technology",
        "sub_sector": "Software & IT Services",
        "industry": "Online Applications & Services",
        "sub_industry": "Online Applications & Services"
    },
    {
        "code": "I121",
        "sector": "Technology",
        "sub_sector": "Software & IT Services",
        "industry": "IT Services & Consulting",
        "sub_industry": "IT Services & Consulting"
    },
    {
        "code": "I131",
        "sector": "Technology",
        "sub_sector": "Software & IT Services",
        "industry": "Software",
        "sub_industry": "Software"
    },
    {
        "code": "I211",
        "sector": "Technology",
        "sub_sector": "Technology Hardware & Equipment",
        "industry": "Networking Equipment",
        "sub_industry": "Networking Equipment"
    },
    {
        "code": "I221",
        "sector": "Technology",
        "sub_sector": "Technology Hardware & Equipment",
        "industry": "Computer Hardware",
        "sub_industry": "Computer Hardware"
    },
    {
        "code": "I231",
        "sector": "Technology",
        "sub_sector": "Technology Hardware & Equipment",
        "industry": "Electronic Equipment, Instruments & Components",
        "sub_industry": "Electronic Equipment & Instruments"
    },
    {
        "code": "I232",
        "sector": "Technology",
        "sub_sector": "Technology Hardware & Equipment",
        "industry": "Electronic Equipment, Instruments & Components",
        "sub_industry": "Electronic Components & Semiconductors"
    },
    {
        "code": "J111",
        "sector": "Infrastructures",
        "sub_sector": "Transportation Infrastructure",
        "industry": "Transport Infrastructure Operator",
        "sub_industry": "Airport Operators"
    },
    {
        "code": "J112",
        "sector": "Infrastructures",
        "sub_sector": "Transportation Infrastructure",
        "industry": "Transport Infrastructure Operator",
        "sub_industry": "Highways & Railtracks"
    },
    {
        "code": "J113",
        "sector": "Infrastructures",
        "sub_sector": "Transportation Infrastructure",
        "industry": "Transport Infrastructure Operator",
        "sub_industry": "Marine Ports & Services"
    },
    {
        "code": "J211",
        "sector": "Infrastructures",
        "sub_sector": "Heavy Constructions & Civil Engineering",
        "industry": "Heavy Constructions & Civil Engineering",
        "sub_industry": "Heavy Constructions & Civil Engineering"
    },
    {
        "code": "J311",
        "sector": "Infrastructures",
        "sub_sector": "Telecommunication",
        "industry": "Telecommunication Service",
        "sub_industry": "Wired Telecommunication Service"
    },
    {
        "code": "J312",
        "sector": "Infrastructures",
        "sub_sector": "Telecommunication",
        "industry": "Telecommunication Service",
        "sub_industry": "Integrated Telecommunication Service"
    },
    {
        "code": "J321",
        "sector": "Infrastructures",
        "sub_sector": "Telecommunication",
        "industry": "Wireless Telecommunication Services",
        "sub_industry": "Wireless Telecommunication Services"
    },
    {
        "code": "J411",
        "sector": "Infrastructures",
        "sub_sector": "Utilities",
        "industry": "Electric Utilities",
        "sub_industry": "Electric Utilities"
    },
    {
        "code": "J421",
        "sector": "Infrastructures",
        "sub_sector": "Utilities",
        "industry": "Gas Utilities",
        "sub_industry": "Gas Utilities"
    },
    {
        "code": "J431",
        "sector": "Infrastructures",
        "sub_sector": "Utilities",
        "industry": "Water Utilities",
        "sub_industry": "Water Utilities"
    },
    {
        "code": "K111",
        "sector": "Transportation & Logistic",
        "sub_sector": "Transportation",
        "industry": "Airlines",
        "sub_industry": "Airlines"
    },
    {
        "code": "K121",
        "sector": "Transportation & Logistic",
        "sub_sector": "Transportation",
        "industry": "Passenger Marine Transportation",
        "sub_industry": "Passenger Marine Transportation"
    },
    {
        "code": "K131",
        "sector": "Transportation & Logistic",
        "sub_sector": "Transportation",
        "industry": "Passenger Land Transportation",
        "sub_industry": "Rail"
    },
    {
        "code": "K132",
        "sector": "Transportation & Logistic",
        "sub_sector": "Transportation",
        "industry": "Passenger Land Transportation",
        "sub_industry": "Road Transportation"
    },
    {
        "code": "K211",
        "sector": "Transportation & Logistic",
        "sub_sector": "Logistics & Deliveries",
        "industry": "Logistics & Deliveries",
        "sub_industry": "Logistics & Deliveries"
    },
    {
        "code": "Z111",
        "sector": "Listed Investment Product",
        "sub_sector": "Investment Trusts",
        "industry": "Investment Trusts",
        "sub_industry": "Mutual Fund/ETFS"
    },
    {
        "code": "Z112",
        "sector": "Listed Investment Product",
        "sub_sector": "Investment Trusts",
        "industry": "Investment Trusts",
        "sub_industry": "Real Estate Investment Trusts"
    },
    {
        "code": "Z113",
        "sector": "Listed Investment Product",
        "sub_sector": "Investment Trusts",
        "industry": "Investment Trusts",
        "sub_industry": "Infrastructure Investment Trusts"
    },
    {
        "code": "Z211",
        "sector": "Listed Investment Product",
        "sub_sector": "Bonds",
        "industry": "Bonds",
        "sub_industry": "Government Bonds"
    },
    {
        "code": "Z212",
        "sector": "Listed Investment Product",
        "sub_sector": "Bonds",
        "industry": "Bonds",
        "sub_industry": "Corporate Bonds"
    }
]
    df_sector_industry = pd.DataFrame(idx_ic_flat_list)
    upload_df_to_s3(df_sector_industry,BUCKET_NAME,key)

def init_dim_date():
    # Rentangnya bisa disesuaikan, dan cukup sekali diinitial load aja.
    start_date = "2020-01-01"
    end_date = "2040-12-31"
    date_range = pd.date_range(start=start_date, end=end_date)

    dim_date = pd.DataFrame({"Date": date_range})

    dim_date["date_key"] = dim_date["Date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["date"] = dim_date["Date"].dt.date
    dim_date["year"] = dim_date["Date"].dt.year
    dim_date["month"] = dim_date["Date"].dt.month
    dim_date["quarter"] = dim_date["Date"].dt.quarter
    dim_date["day_name"] = dim_date["Date"].dt.day_name()

    dim_date = dim_date.drop(columns=["Date"])

    key = "modeled/dim_date/data.parquet"
    upload_df_to_s3(dim_date, BUCKET_NAME, key)

def _get_duckdb_connection():
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("""
        CREATE OR REPLACE SECRET (
            TYPE S3,
            PROVIDER credential_chain
        );
    """)
    return con

def init_dim_stock():
    # One-time init: bangun dim_stock SCD2 dari seluruh histori curated (2020 s.d. bulan terakhir yang sudah tercompact)
    con = _get_duckdb_connection()
    source = f"s3://{BUCKET_NAME}/curated/idx/year=*/month=*/data.parquet"

    dim_stock = con.execute(
        f"""
        WITH changes AS (
            SELECT
                trade_date,
                stock_code,
                stock_name,
                COALESCE(
                    stock_name != LAG(stock_name) OVER (
                        PARTITION BY stock_code ORDER BY trade_date
                    ),
                    TRUE
                ) AS name_changed
            FROM read_parquet('{source}', hive_partitioning = true)
--            WHERE year >= 2020
--              AND (year < 2026 OR (year = 2026 AND CAST(month AS INTEGER) <= 7))
        ), last_date AS (
            SELECT MAX(trade_date) as last_update_date
            FROM changes
        ),
        versions AS (
            SELECT *,
                SUM(CAST(name_changed AS INTEGER)) OVER (
                    PARTITION BY stock_code ORDER BY trade_date
                    ROWS UNBOUNDED PRECEDING
                ) AS version
            FROM changes
        ),
        history AS (
            SELECT
                stock_code,
                version,
                FIRST(stock_name ORDER BY trade_date) AS stock_name,
                MIN(trade_date) AS valid_from,
                MAX(trade_date) AS valid_to
            FROM versions
            GROUP BY stock_code, version
        )
        SELECT
            stock_code,
            stock_name,
            valid_from,
            CASE
                WHEN ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY valid_from DESC) = 1
                THEN NULL ELSE valid_to
            END AS valid_to,
            (ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY valid_from DESC) = 1)::INTEGER AS is_active,
            (SELECT *
            FROM last_date
            ) as last_update_date
        FROM history
        ORDER BY stock_code, valid_from
        """
    ).df()

    logger.info(
        f"init_dim_stock: generated {len(dim_stock)} rows for "
        f"{dim_stock['stock_code'].nunique()} stocks"
    )
    upload_df_to_s3(dim_stock, BUCKET_NAME, DIM_STOCK_KEY)

def update_dim_stock(key):
    # SCD2 incremental update dim_stock
    con = _get_duckdb_connection()
    dim_stock_path = f"s3://{BUCKET_NAME}/{DIM_STOCK_KEY}"
    new_data_path = ", ".join(f"'s3://{BUCKET_NAME}/{path}'" for path in key)
    # new_data_path = (
    #     f"s3://{BUCKET_NAME}/{key}"
    # )

    df_stock = con.execute(
        f"""
        WITH dim_current AS (
            SELECT * FROM read_parquet('{dim_stock_path}')
        ),
        current_active AS (
            SELECT stock_code, stock_name AS current_name, valid_from AS current_valid_from, last_update_date
            FROM dim_current
            WHERE is_active = 1
        ),
        get_last_update AS (
            SELECT MAX(last_update_date) as last_update_date
            FROM current_active
        ),
        new_changes AS (
            SELECT
                trade_date,
                stock_code,
                stock_name,
                COALESCE(
                    stock_name != LAG(stock_name) OVER (
                        PARTITION BY stock_code ORDER BY trade_date
                    ),
                    TRUE
                ) AS name_changed
            FROM read_parquet([{new_data_path}], union_by_name = True)
            WHERE trade_date>(SELECT * FROM get_last_update)
        ),
        new_versions AS (
            SELECT *,
                SUM(CAST(name_changed AS INTEGER)) OVER (
                    PARTITION BY stock_code ORDER BY trade_date
                    ROWS UNBOUNDED PRECEDING
                ) AS version
            FROM new_changes
        ),
        new_segments AS (
            SELECT
                stock_code,
                version,
                FIRST(stock_name ORDER BY trade_date) AS stock_name,
                MIN(trade_date) AS seg_start,
                MAX(trade_date) AS seg_end,
                MAX(version) OVER (PARTITION BY stock_code) AS max_version
            FROM new_versions
            GROUP BY stock_code, version
        ),
        merged AS (
            -- segmen pertama tiap stock di batch baru, digabung sama record aktif eksisting
            SELECT
                s.stock_code,
                s.stock_name,
                CASE WHEN s.stock_name = c.current_name THEN c.current_valid_from ELSE s.seg_start END AS valid_from,
                CASE WHEN s.version = s.max_version THEN NULL ELSE s.seg_end END AS valid_to
            FROM new_segments s
            LEFT JOIN current_active c ON s.stock_code = c.stock_code
            WHERE s.version = 1

            UNION ALL

            -- ganti nama yang kejadian di tengah2 batch baru
            SELECT
                s.stock_code,
                s.stock_name,
                s.seg_start AS valid_from,
                CASE WHEN s.version = s.max_version THEN NULL ELSE s.seg_end END AS valid_to
            FROM new_segments s
            WHERE s.version > 1

            UNION ALL

            -- tutup record aktif lama kalau nama udah beda dari H1 batch baru
            SELECT
                c.stock_code,
                c.current_name AS stock_name,
                c.current_valid_from AS valid_from,
                s.seg_start - 1 AS valid_to
            FROM new_segments s
            JOIN current_active c ON s.stock_code = c.stock_code
            WHERE s.version = 1 AND s.stock_name != c.current_name

            UNION ALL

            -- stock tanpa data baru di batch ini (suspend dll): biarin apa adanya
            SELECT
                c.stock_code,
                c.current_name AS stock_name,
                c.current_valid_from AS valid_from,
                NULL AS valid_to
            FROM current_active c
            WHERE NOT EXISTS (
                SELECT 1 FROM new_segments s WHERE s.stock_code = c.stock_code
            )
        ),
        final_active AS (
            SELECT
                stock_code,
                stock_name,
                valid_from,
                valid_to,
                (ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY valid_from DESC) = 1)::INTEGER AS is_active
            FROM merged
        )
        SELECT stock_code, stock_name, valid_from, valid_to, is_active FROM final_active
        UNION ALL
        SELECT stock_code, stock_name, valid_from, valid_to, is_active FROM dim_current WHERE is_active = 0
        ORDER BY stock_code, valid_from
        """
    ).df()
    # cek ada perubahan beneran atau engga, sebelum upload
    changed = con.execute(
        """
        SELECT COUNT(*) FROM (
            (SELECT * FROM df_stock EXCEPT 
                SELECT stock_code, stock_name, valid_from, valid_to, is_active 
                FROM read_parquet(?))
            UNION ALL
            (SELECT stock_code, stock_name, valid_from, valid_to, is_active 
            FROM read_parquet(?) 
            EXCEPT SELECT * FROM df_stock)
        )
        """,
        [dim_stock_path, dim_stock_path],
    ).fetchone()[0]

    if changed == 0:
        logger.info(f"update_dim_stock: no changes, skip upload")
        return 

    df_last_update = con.execute(
            f"""
                SELECT max(trade_date) as last_update FROM read_parquet([{new_data_path}], union_by_name = True)
            """).fetchone()[0]

    df_stock["last_update_date"] = df_last_update
    logger.info(f"update_dim_stock: {changed} row(s), uploading")
    # return df_stock
    upload_df_to_s3(df_stock, BUCKET_NAME, DIM_STOCK_KEY)
import logging
import re
from collections import defaultdict
from functools import lru_cache

import duckdb
import pandas as pd

from idx_common import (
    BUCKET_NAME,
    list_layer_keys,
    object_exists,
    run_stage,
    setup_logging,
    upload_df_to_s3,
)

logger = logging.getLogger(__name__)

DIM_STOCK_KEY = "modeled/dim_stock/data.parquet"


def s3_uri(key):
    return f"s3://{BUCKET_NAME}/{key}"


def sql_paths(keys):
    return ", ".join(f"'{s3_uri(k)}'" for k in keys)


@lru_cache(maxsize=1)
def get_duckdb_connection():
    """Satu koneksi DuckDB untuk seluruh run (INSTALL/LOAD httpfs + secret cuma sekali)."""
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


def copy_to_s3_parquet(con, select_sql, key):
    """Tulis hasil query langsung ke S3 sebagai parquet (tanpa lewat pandas -> tipe kolom tetap sesuai DDL Athena)."""
    con.execute(f"COPY ({select_sql}) TO '{s3_uri(key)}' (FORMAT PARQUET)")


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

    # tipe eksplisit supaya cocok dengan DDL Athena (date_key BIGINT, year/month/quarter INT)
    dim_date["date_key"] = dim_date["Date"].dt.strftime("%Y%m%d").astype("int64")
    dim_date["date"] = dim_date["Date"].dt.date
    dim_date["year"] = dim_date["Date"].dt.year.astype("int32")
    dim_date["month"] = dim_date["Date"].dt.month.astype("int32")
    dim_date["quarter"] = dim_date["Date"].dt.quarter.astype("int32")
    dim_date["day_name"] = dim_date["Date"].dt.day_name()

    dim_date = dim_date.drop(columns=["Date"])

    upload_df_to_s3(dim_date, BUCKET_NAME, "modeled/dim_date/data.parquet")


def init_dim_stock(keys):
    """One-time init: bangun dim_stock SCD2 dari seluruh histori curated (keys = semua file curated).

    Konvensi interval: valid_to = (valid_from versi berikutnya) - 1 hari, NULL untuk versi aktif.
    Jadi interval selalu menyambung tanpa gap/overlap: date BETWEEN valid_from AND COALESCE(valid_to, date).
    """
    if not keys:
        logger.warning("init_dim_stock: nggak ada file curated, skip")
        return

    con = get_duckdb_connection()
    source = f"read_parquet([{sql_paths(keys)}], union_by_name = true)"

    sql = f"""
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
            FROM {source}
        ),
        last_date AS (
            SELECT MAX(trade_date) AS d FROM changes
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
                MIN(trade_date) AS valid_from
            FROM versions
            GROUP BY stock_code, version
        )
        SELECT
            stock_code,
            stock_name,
            CAST(valid_from AS DATE) AS valid_from,
            CAST((LEAD(valid_from) OVER w) - 1 AS DATE) AS valid_to,
            (LEAD(valid_from) OVER w IS NULL)::INTEGER AS is_active,
            CAST((SELECT d FROM last_date) AS DATE) AS last_update_date
        FROM history
        WINDOW w AS (PARTITION BY stock_code ORDER BY valid_from)
        ORDER BY stock_code, valid_from
    """
    copy_to_s3_parquet(con, sql, DIM_STOCK_KEY)

    n_rows, n_stocks = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT stock_code) FROM read_parquet('{s3_uri(DIM_STOCK_KEY)}')"
    ).fetchone()
    logger.info(f"init_dim_stock: generated {n_rows} rows for {n_stocks} stocks")


def update_dim_stock(keys):
    """SCD2 incremental dim_stock. Hanya upload kalau ada perubahan (nama baru / saham baru) atau tipe kolom perlu diperbaiki."""
    if not keys:
        logger.info("update_dim_stock: nggak ada file curated, skip")
        return
    if not object_exists(BUCKET_NAME, DIM_STOCK_KEY):
        raise RuntimeError("dim_stock belum ada. Jalankan init_dim_stock() dulu (first run).")

    con = get_duckdb_connection()
    dim_path = s3_uri(DIM_STOCK_KEY)
    new_data = f"read_parquet([{sql_paths(keys)}], union_by_name = true)"

    # Semua "titik awal versi" (stock_code, stock_name, valid_from) = yang sudah ada + titik ganti nama baru.
    # valid_to & is_active dihitung ulang dari urutan valid_from, jadi konsisten dengan init_dim_stock.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim_new AS
        WITH dim_current AS (
            SELECT
                stock_code,
                stock_name,
                CAST(valid_from AS DATE) AS valid_from,
                is_active,
                CAST(last_update_date AS DATE) AS last_update_date
            FROM read_parquet('{dim_path}')
        ),
        current_active AS (
            SELECT stock_code, stock_name AS current_name
            FROM dim_current
            WHERE is_active = 1
        ),
        last_upd AS (
            SELECT MAX(last_update_date) AS d FROM dim_current
        ),
        new_rows AS (
            SELECT
                n.trade_date,
                n.stock_code,
                n.stock_name,
                -- baris pertama tiap saham dibandingkan dengan nama aktif eksisting (NULL kalau saham baru)
                COALESCE(
                    LAG(n.stock_name) OVER (PARTITION BY n.stock_code ORDER BY n.trade_date),
                    c.current_name
                ) AS prev_name
            FROM {new_data} n
            LEFT JOIN current_active c ON c.stock_code = n.stock_code
            WHERE n.trade_date > (SELECT d FROM last_upd)
        ),
        new_starts AS (
            SELECT stock_code, stock_name, trade_date AS valid_from
            FROM new_rows
            WHERE prev_name IS NULL OR stock_name != prev_name
        ),
        all_starts AS (
            SELECT stock_code, stock_name, valid_from FROM dim_current
            UNION ALL
            SELECT stock_code, stock_name, valid_from FROM new_starts
        )
        SELECT
            stock_code,
            stock_name,
            valid_from,
            CAST((LEAD(valid_from) OVER w) - 1 AS DATE) AS valid_to,
            (LEAD(valid_from) OVER w IS NULL)::INTEGER AS is_active
        FROM all_starts
        WINDOW w AS (PARTITION BY stock_code ORDER BY valid_from)
    """)

    # ada perubahan beneran atau engga?
    changed = con.execute(f"""
        SELECT COUNT(*) FROM (
            (SELECT * FROM dim_new
             EXCEPT
             SELECT stock_code, stock_name, CAST(valid_from AS DATE), CAST(valid_to AS DATE), CAST(is_active AS INTEGER)
             FROM read_parquet('{dim_path}'))
            UNION ALL
            (SELECT stock_code, stock_name, CAST(valid_from AS DATE), CAST(valid_to AS DATE), CAST(is_active AS INTEGER)
             FROM read_parquet('{dim_path}')
             EXCEPT
             SELECT * FROM dim_new)
        )
    """).fetchone()[0]

    # file lama (versi pandas) bisa menyimpan tanggal sebagai TIMESTAMP -> Athena (DDL: DATE) gagal baca. Paksa tulis ulang.
    existing_types = {
        row[0]: row[1] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{dim_path}')").fetchall()
    }
    schema_ok = all(existing_types.get(c) == "DATE" for c in ("valid_from", "valid_to", "last_update_date"))

    if changed == 0 and schema_ok:
        logger.info("update_dim_stock: no changes, skip upload")
        return

    last_update = con.execute(f"SELECT MAX(trade_date) FROM {new_data}").fetchone()[0]
    if last_update is None:
        logger.info("update_dim_stock: data baru kosong, skip upload")
        return

    reason = f"{changed} row(s) changed" if changed else "fix tipe kolom"
    logger.info(f"update_dim_stock: {reason}, uploading")
    copy_to_s3_parquet(
        con,
        f"""
        SELECT stock_code, stock_name, valid_from, valid_to, is_active,
               CAST('{last_update}' AS DATE) AS last_update_date
        FROM dim_new
        ORDER BY stock_code, valid_from
        """,
        DIM_STOCK_KEY,
    )


def fact_select(source):
    """SELECT untuk fact_stock_daily. Tipe di-cast eksplisit supaya cocok dengan DDL Athena."""
    return f"""
        SELECT
            CAST(strftime(trade_date, '%Y%m%d') AS INTEGER) AS date_key,
            stock_code,
            NULLIF(regexp_extract(remarks, '00([A-Z][0-9]{{3}})', 1), '') AS idx_ic_code,
            CAST(previous_price AS DOUBLE) AS previous_price,
            CAST(open_price AS DOUBLE) AS open_price,
            CAST(first_trade_price AS DOUBLE) AS first_trade_price,
            CAST(high_price AS DOUBLE) AS high_price,
            CAST(low_price AS DOUBLE) AS low_price,
            CAST(close_price AS DOUBLE) AS close_price,
            CAST(price_change AS DOUBLE) AS price_change,
            CAST(volume AS BIGINT) AS volume,
            CAST(trading_value AS DOUBLE) AS trading_value,
            CAST(frequency AS BIGINT) AS frequency,
            CAST(foreign_buy AS BIGINT) AS foreign_buy,
            CAST(foreign_sell AS BIGINT) AS foreign_sell,
            CAST(non_regular_volume AS BIGINT) AS non_regular_volume,
            CAST(non_regular_value AS DOUBLE) AS non_regular_value,
            CAST(non_regular_frequency AS BIGINT) AS non_regular_frequency
        FROM {source}
    """


def build_fact(keys):
    """Rebuild fact_stock_daily per bulan dari SEMUA file curated bulan itu (bulan berjalan = gabungan seluruh daily)."""
    con = get_duckdb_connection()

    keys_by_month = defaultdict(list)
    for key in keys:
        m = re.search(r"year=(\d{4})/month=(\d{2})", key)
        keys_by_month[(m.group(1), m.group(2))].append(key)

    for (yyyy, mm), month_keys in sorted(keys_by_month.items()):
        source = f"read_parquet([{sql_paths(month_keys)}], union_by_name = true)"
        fact_key = f"modeled/fact_stock_daily/year={yyyy}/month={mm}/data.parquet"
        copy_to_s3_parquet(con, fact_select(source), fact_key)
        logger.info(f"Uploading: {fact_key} (DONE, {len(month_keys)} source file)")


def dimensional(start_date, end_date, first_run):
    # Init dim_sector_industry dan dim_date hanya sekali (first run)
    if first_run:
        init_dim_sector_industry()
        init_dim_date()

    # full_month_daily=True: fact bulan berjalan harus dibangun dari SELURUH daily bulan itu, bukan cuma yang baru
    keys = list_layer_keys("curated", start_date, end_date, full_month_daily=True)
    if not keys:
        logger.warning("Nggak ada file curated di rentang ini, nothing to do")
        return

    # dim_stock (SCD type 2)
    if first_run:
        init_dim_stock(keys)
    else:
        update_dim_stock(keys)

    # fact_stock_daily
    build_fact(keys)


def main_dimentional():
    run_stage("dwh_idx", dimensional, prev_etl_name="curated_idx")


if __name__ == "__main__":
    setup_logging()
    main_dimentional()

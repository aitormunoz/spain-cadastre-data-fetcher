import pandas as pd
import requests
import xmltodict
from zipfile import ZipFile
import os
import geopandas as gpd

# Constants
PROVINCES = ["02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
             "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38",
             "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52"]


def download_catastro(province):
    base_url = "https://www.catastro.hacienda.gob.es/INSPIRE/Buildings/{province}/ES.SDGC.BU.atom_{province}.xml?tipo=Buildings"
    url = base_url.format(province=province)
    response = requests.get(url)
    if response.status_code == 200:
        file_content = response.content
        content = xmltodict.parse(file_content)
        return pd.DataFrame.from_dict(content['feed']['entry'])


def download_unzip_data(row):
    zip_url = row["id"]
    zip_response = requests.get(zip_url)
    zip_filename = os.path.join("/tmp", f"{row['title']}_building.zip")

    with open(zip_filename, "wb") as zip_file:
        zip_file.write(zip_response.content)

    with ZipFile(zip_filename, "r") as zip_ref:
        zip_ref.extractall("/tmp")
        os.remove(zip_filename)
        return zip_ref.namelist()


def process_province(province):
    buildings_df = download_catastro(province)
    print(f"fetch province {province}")

    for (index, row) in buildings_df.iterrows():
        zip_content = download_unzip_data(row)
        for file in zip_content:
            if file.endswith("building.gml"):
                gdf = gpd.read_file(os.path.join("/tmp", file))
                gdf = gdf.to_crs(epsg=4326)
                file_name = f"{row['title']}_building.parquet"
                print(file_name)
                parquet_path = os.path.join("/tmp", file_name)
                gdf.to_parquet(parquet_path)
            if file.endswith(".gml"):
                os.remove(os.path.join("/tmp", file))


def main():
    for province in PROVINCES:
        process_province(province)


if __name__ == "__main__":
    main()

# import packages
import os
import requests
import zipfile
import geopandas
import pandas


# load shapefile
def load_shapefile(shp_fp):
    # shp_fp = path to the shapefile
    shp = geopandas.read_file(shp_fp)
    # return the load/read in shapefile
    return shp


# create folder if not already existing and unzip files into it     # new functions
def create_and_unzip(folder_path, zip_files):
    # create folder
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    # extract zip files into the folder
    for zip_file in zip_files:
        # add .zip if necessary
        if ".zip" not in zip_file:
            zip_file = zip_file + ".zip"
        zipped_data = zipfile.ZipFile(zip_file, "r")
        zipped_data.extractall(path=folder_path)
        zipped_data.close()


# intersect shapefiles function                                     # renamed
def intersect_shps(shp_1, shp_2):
    # Re-project shp_1 if necessary
    re = shp_2.crs == shp_1.crs
    if re is False:
        shp_1 = shp_1.to_crs(shp_2.crs)
    # execute join
    intersected_shp = geopandas.sjoin(shp_1, shp_2, how="inner")
    # return the result
    return intersected_shp


# ---------- user input ---------- #
# set working directory
os.chdir("C:/Users/frank/Desktop/Test")
# set aoi file path
aoi_fp = "C:/Users/frank/Desktop/Data/AOI_/AOI_04.shp"
# user request year
start_year = 2016
end_year = 2019

# ---------- development ---------- #
# load shapefile function (aoi)
aoi = load_shapefile(shp_fp=aoi_fp)

# download shp with tile numbers
# set url
url = "https://geoportal.geoportal-th.de/hoehendaten/Uebersichten/Stand_2010-2013.zip"
# set the name of the zip file
zip_name = "tile_numbers_shp" + ".zip"
# download the data file if there is no file with the name that it would get
if not os.path.exists(zip_name):
    # set variables for the loop
    response = requests.get(url, stream=True)
    data = open(zip_name, "wb")
    # download and write data (downloading the data file in chunks is useful to save ram)
    for chunk in response.iter_content(chunk_size=1024):
        data.write(chunk)
    # close data
    data.close()

# extract shp with tile numbers into a new folder
# create folder and unzip function
create_and_unzip(folder_path="auxiliary_files", zip_files=[zip_name])

# load shapefile function (tile_number_shp)
tile_number_shp = load_shapefile(shp_fp="auxiliary_files/" +
                                        "DGM2_2010-2013_Erfass-lt-Meta_UTM32-UTM_2014-12-10.shp")

# intersect shapefiles function (aoi, tile_number_shp)
aoi_tile_numbers_shp = intersect_shps(shp_1=aoi, shp_2=tile_number_shp)

# get the relevant tile_numbers
tile_number_list = list()
temp_list = list(aoi_tile_numbers_shp["DGM_1X1"])
for tile_number in temp_list:
    tile_number_list.append(tile_number[2:])

# create a tile number df
data = {"tile_number": tile_number_list}
tile_number_df = pandas.DataFrame(data)

# download url_id_file
# set url
url = "https://raw.githubusercontent.com/Jon-Fr/Geo_419b/main/url_id_file.zip?token=ANQNA6GYPM3LPVQLVUFK6Y3BOPI5A"
# set the name of the zip file
zip_name = "url_id_file" + ".zip"
# download the data file if there is no file with the name that it would get
if not os.path.exists(zip_name):
    # set variables for the loop
    response = requests.get(url, stream=True)
    data = open(zip_name, "wb")
    # download and write data (downloading the data file in chunks is useful to save ram)
    for chunk in response.iter_content(chunk_size=1024):
        data.write(chunk)
    # close data
    data.close()

# extract url_id_file
# create folder and unzip function
create_and_unzip(folder_path="auxiliary_files", zip_files=[zip_name])

# load url_id_file as df and add df to list
url_id_df = pandas.read_csv("auxiliary_files/" + "url_id_file.csv")
url_id_df_list = list()
url_id_df_list.append(url_id_df)

# before 2019 the orthophoto covers a 2x2 km area and this affects the tile numbers
# split url_id_df (if necessary)
if end_year >= 2019:
    # clear list
    url_id_df_list = list()
    # create df for the years before 2019 and for 2019 and later
    url_id_df_before_2019 = url_id_df[url_id_df["year"] < 2019]
    url_id_df_after_2018 = url_id_df[url_id_df["year"] >= 2019]
    # append dfs to list
    url_id_df_list.append(url_id_df_after_2018)
    url_id_df_list.append(url_id_df_before_2019)


# loop to take care of more than one df if a spilt was necessary
for url_id_df in url_id_df_list:
    # check if the years are before 2019
    if url_id_df.iloc[0]["year"] < 2019:
        # add additional tile numbers to the tile_number_df if necessary
        for i in range(0, len(tile_number_df)):
            # split the tile number
            split = tile_number_df.iloc[i]["tile_number"]
            # check if it could be necessary to add a tile number and if yes create it
            # check if the "new" tile number is already in the df if no replace the unnecessary tile number
            # if yes delete the the unnecessary tile number
            if int(split[0]) % 2 != 0 and int(split[1]) % 2 == 0:
                new_part_1 = int(split[0]) - 1
                new_tile_number = str(new_part_1) + "_" + split[1]
            elif int(split[0]) % 2 == 0 and int(split[1]) % 2 != 0:
                new_part_2 = int(split[1]) - 1
                new_tile_number = split[10] + "_" + str(new_part_2)
            elif int(split[0]) % 2 != 0 and int(split[1]) % 2 != 0:
                new_part_1 = int(split[0]) - 1
                new_part_2 = int(split[1]) - 1
                new_tile_number = str(new_part_1) + "_" + str(new_part_2)
            else:
                continue

    # execute inner join
    joined_df = pandas.merge(url_id_df, tile_number_df)

    # filter relevant years
    filtered_df = joined_df[(joined_df["year"] >= start_year) & (joined_df["year"] <= end_year)]

    # store relevant url_ids in a list
    url_id_list = list(filtered_df["url_id"])

    # download of the orthophotos
    zip_name_list = list()
    for url_id in url_id_list:
        # set url
        url_id = str(url_id)
        url = "https://geoportal.geoportal-th.de/gaialight-th/_apps/dladownload/download.php?type=op&id=" + url_id
        # set the name of the zip file and append it to zip file name list
        zip_name = url_id + ".zip"
        zip_name_list.append(zip_name)
        # download the data file if there is no file with the name that it would get
        if not os.path.exists(zip_name):
            # set variables for the loop
            response = requests.get(url, stream=True)
            data = open(zip_name, "wb")
            # download and write data (downloading the data file in chunks is useful to save ram)
            for chunk in response.iter_content(chunk_size=1024):
                data.write(chunk)
            # close data
            data.close()

    # extract the orthophotos
    # create folder and unzip function
    create_and_unzip(folder_path="orthophotos/2008_2014", zip_files=zip_name_list)

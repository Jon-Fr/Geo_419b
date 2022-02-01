# import packages
import os
import requests
import zipfile
import geopandas
import pandas
from osgeo import gdal


def load_shapefile(shp_fp):
    """
    Reads in a shapefile from the specified file path and returns it.

    Parameter
    ----------
    shp_fp: str
        Path to the shapefile.
    Returns
    -------
    shp: geopandas.geodataframe.GeoDataFrame
        The read in shapefile.
    """
    # reading in the shapefile
    shp = geopandas.read_file(shp_fp)
    # return the read in shapefile
    return shp


def set_elev_variables(year):
    """
    Sets some variables that change depending on the specified year and returns them. However, if it is certain that
    there is no data for the specified year "stop" is returned.

    Parameter
    ----------
    year: int
        The year of interest or one of the years ot interest.

    Returns
    -------
    url_year: str
        Part of the URL for the download of the elevation data.
    dem_n: str
        Part of the URL for the download of the elevation data.
    elev_data_file: str
        Name of the meta data shapefile for the elevation data.
    """
    # set the variables if there could be data available for this year
    if 2011 <= year <= 2013:
        url_year = "2010-2013"
        dem_n = "2"
        elev_data_file = "DGM2_2010-2013_Erfass-lt-Meta_UTM32-UTM_2014-12-10.shp"
    elif 2014 <= year <= 2019:
        url_year = "2014-2019"
        dem_n = "1"
        elev_data_file = "DGM1_2014-2019_Erfass-lt-Meta_UTM_2020-04-20--17127.shp"
    elif 2020 <= year <= 2025:
        url_year = "2020-2025"
        dem_n = "1"
        elev_data_file = "DGM1_2020-2025_Erfass-lt-Meta_UTM_2021-03--17127/" \
                         "DGM1_2020-2025_Erfass-lt-Meta_UTM_2021-03--17127.shp"
    # return stop if there is certainly no data available for this year
    else:
        return "stop", "stop", "stop"
    # return the defined variables
    return url_year, dem_n, elev_data_file


def data_download(type_to_download, data_list_to_download, url_year="", year=0, dem_n="", year_list=None,
                  tile_number_list=None):
    """
    Loops trough a list of data to download puts the URL(s) together and download the ZIP file(s). A list with the
    name(s) of the downloaded file(s) is returned, if no files were downloaded "no_new_data" is returned.
    Files are only downloaded, if the file or the content of the file is not already in the working directory.

    Parameter
    ----------
    type_to_download: str
        The type of the data to be downloaded.
    data_list_to_download: list of str
        A list that contains the part of the URL that is different for each data tile for all data tiles to be
        downloaded.
    url_year: str
        Part of the URL for the download of the elevation data
    year: int
        The year of interest or one of the years ot interest.
    dem_n: str
        Part of the URL for the download of the elevation data
    year_list: list of int
        A list which contains the year of capture of each orthophoto to be downloaded.
    tile_number_list: list of str
         A list which contains the tile number of each orthophoto to be downloaded.

    Returns
    -------
    zip_data_list: list of str
        A list with the name(s) of of the downloaded file(s).
    """
    # the two following variables are used for the naming of the zip files (data kind = dgm, dom or las)
    data_year = "_" + str(year)
    data_kind = ""
    # in this list the names of the zip files are stored
    zip_data_list = []
    # necessary for the orthophoto lists
    index = 0
    # for loop to download more than one file
    for i in data_list_to_download:
        # set the url, the one or two of the variables  used for the naming of the zip files and
        # get the name of the zip file content and its hypothetically file path
        # to be able to check if the file is  already present
        url = ""
        hy_file_path = """elevation_data/{}/{}/""".format(type_to_download, str(year))
        file_name = ""
        file_name_part_1 = ""
        file_name_part_2 = ""
        if type_to_download == "meta_data" or type_to_download == "auxiliary_data":
            data_year = ""
            if i == "url_id_data":
                url = "https://raw.githubusercontent.com/Jon-Fr/Geo_419b/main/url_id_file.zip"
                file_name = "url_id_file.csv"
            else:
                url = """https://geoportal.geoportal-th.de/hoehendaten/Uebersichten/Stand_{}.zip""".format(url_year)
            if type_to_download == "auxiliary_data":
                hy_file_path = """image_data/{}/""".format(type_to_download)
            else:
                hy_file_path = """elevation_data/{}/""".format(type_to_download)
            if url_year == "2010-2013":
                file_name = "DGM2_2010-2013_Erfass-lt-Meta_UTM32-UTM_2014-12-10.shp"
            elif url_year == "2014-2019":
                file_name = "DGM1_2014-2019_Erfass-lt-Meta_UTM_2020-04-20--17127.shp"
            elif url_year == "2020-2025":
                file_name = "DGM1_2020-2025_Erfass-lt-Meta_UTM_2021-03--17127"
        if type_to_download == "dgm":
            url = """https://geoportal.geoportal-th.de/hoehendaten/DGM/dgm_{}/dgm{}_{}_1_th_{}.zip""" \
                .format(url_year, dem_n, i, url_year)
            data_kind = "dgm_"
            file_name = url[len(url) - 32:len(url) - 3] + "xyz"
            if year == 2020:
                file_name = url[len(url) - 35:len(url) - 3] + "xyz"
        if type_to_download == "dom":
            url = """http://geoportal.geoportal-th.de/hoehendaten/DOM/dom_{}/dom{}_{}_1_th_{}.zip""" \
                .format(url_year, dem_n, i, url_year)
            data_kind = "dom_"
            file_name = url[len(url) - 32:len(url) - 3] + "xyz"
            if year == 2020:
                file_name = url[len(url) - 35:len(url) - 3] + "xyz"
        if type_to_download == "las":
            url = """http://geoportal.geoportal-th.de/hoehendaten/LAS/las_{}/las_{}_1_th_{}.zip""" \
                .format(url_year, i, url_year)
            data_kind = "las_"
            file_name = url[len(url) - 32:len(url) - 3] + "laz"
            if year == 2020:
                file_name = url[len(url) - 35:len(url) - 3] + "laz"
        if type_to_download == "ortho":
            data_year = ""
            i = str(i)
            url = """https://geoportal.geoportal-th.de/gaialight-th/_apps/dladownload/download.php?type=op&id={}""" \
                .format(i)
            hy_file_path = """image_data/orthophotos/{}""".format(str(year_list[index]))
            file_name_part_1 = tile_number_list[index]
            file_name_part_2 = str(year_list[index])
        # set the name of the zip file
        zip_name = data_kind + i + data_year + ".zip"
        if type_to_download == "ortho":
            zip_name = "orthophoto_" + tile_number_list[index] + "_" + str(year_list[index]) + ".zip"
            index = index + 1
        # append the zip file name to zip file name list
        zip_data_list.append(zip_name[0:len(zip_name) - 4])
        # download the zip data file if there is no file with the name that it would get and
        # if the content of the zip file is not already present
        if not os.path.exists(zip_name) and \
                (not os.path.exists(hy_file_path + file_name) or type_to_download == "ortho"):
            # extra check for orthophotos (necessary because the full filename is harder to predict/construct)
            stop = False
            if type_to_download == "ortho" and os.path.exists(hy_file_path):
                file_list = os.listdir(hy_file_path)
                for file in file_list:
                    if file_name_part_1 in file and file_name_part_2 in file:
                        stop = True
                        break
            if stop is True:
                continue
            # set variables for the loop
            response = requests.get(url, stream=True)
            data = open(zip_name, "wb")
            # download and write data (downloading the data file in chunks is useful to save ram)
            for chunk in response.iter_content(chunk_size=1024):
                data.write(chunk)
            # close data
            data.close()
    # return the zip file name list if it is not empty
    if len(zip_data_list) > 0:
        return zip_data_list
    else:
        return "no_new_data"


def create_and_unzip(folder_path, zip_files):
    """
    Creates a folder (if it is not already existing) and unzip a list of ZIP files into it.
    Before the function tries to unpacks a file, it checks whether this file actually exists in the working directory.

    Parameter
    ----------
    folder_path: str
        Path to / name of the folder to create.
    zip_files: list of str
        A list containing the names of the ZIP files to be unzipped.

    Returns
    -------
    """
    # create folder
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    # loop through the list
    for zip_file in zip_files:
        # add .zip if necessary
        if ".zip" not in zip_file:
            zip_file = zip_file + ".zip"
        # check if file exist before trying to unzip it
        if os.path.exists(zip_file):
            # unzip the file
            zipped_data = zipfile.ZipFile(zip_file, "r")
            zipped_data.extractall(path=folder_path)
            zipped_data.close()


def intersect_shps(shp_1, shp_2):
    """
    Intersects two shapefiles and returns the intersected shapefile. If the coordinate reference system (crs) of the
    shapefiles is different the first shapefile is re-projected to the crs of the second shapefile.

    Parameter
    ----------
    shp_1: geopandas.geodataframe.GeoDataFrame
        shapefile 1
    shp_2: geopandas.geodataframe.GeoDataFrame
        shapefile 2

    Returns
    -------
    intersected_shp: geopandas.geodataframe.GeoDataFrame
        intersected shapefile
    """
    # Re-project shp_1 if necessary
    re = shp_2.crs == shp_1.crs
    if re is False:
        shp_1 = shp_1.to_crs(shp_2.crs)
    # execute join / intersect
    intersected_shp = geopandas.sjoin(shp_1, shp_2, how="inner")
    # return the result
    return intersected_shp


def create_elev_download_lists(elev_aoi, year, start_year, end_year, month_start_year, month_end_year,
                               additional_check):
    """
    Creates a list that contains the part of the URL that is different for each data tile for all data tiles to be
    downloaded and returns that list. If there is no data for the specified year, stop is returned.

    Parameter
    ----------
    elev_aoi: geopandas.geodataframe.GeoDataFrame
        The Intersected shapefile of the area of interest and the metadata shapefile.
    year: int
        The year of interest or one of the years ot interest.
    start_year: int
        First year of interest.
    end_year: int
        Last year of interest.
    month_start_year: int
        First month of interest.
    month_end_year: int
        Last month of interest.
    additional_check: str
        Is this an additional check.
    Returns
    -------
    elev_download_list: list of str
         A list that contains the part of the URL that is different for each data tile for all data tiles to be
         downloaded.
    """
    # month list
    m_l = ["-01", "-02", "-03", "-04", "-05", "-06", "-07", "-08", "-09", "-10", "-11", "-12"]
    # exclude the months that should not be checked
    if year == start_year and month_start_year != 1:
        for i in range(0, month_start_year - 1):
            m_l[i] = m_l[month_start_year - 1]
    if year == end_year and month_end_year != 12:
        for i in range(month_end_year, 12):
            m_l[i] = m_l[month_end_year - 1]
    # filter by date to find the relevant tiles
    year = str(year)
    filtered_data = elev_aoi[(elev_aoi["ERFASSUNG"] == year + m_l[0]) | (elev_aoi["ERFASSUNG"] == year + m_l[1]) |
                             (elev_aoi["ERFASSUNG"] == year + m_l[2]) | (elev_aoi["ERFASSUNG"] == year + m_l[3]) |
                             (elev_aoi["ERFASSUNG"] == year + m_l[4]) | (elev_aoi["ERFASSUNG"] == year + m_l[5]) |
                             (elev_aoi["ERFASSUNG"] == year + m_l[6]) | (elev_aoi["ERFASSUNG"] == year + m_l[7]) |
                             (elev_aoi["ERFASSUNG"] == year + m_l[8]) | (elev_aoi["ERFASSUNG"] == year + m_l[9]) |
                             (elev_aoi["ERFASSUNG"] == year + m_l[10]) | (elev_aoi["ERFASSUNG"] == year + m_l[11])]
    # if there is no data return stop
    if len(filtered_data) == 0:
        return "stop"
    # create list of the relevant tiles
    # since the meta data is not uniform, an if else statement is necessary
    if year < "2014" and additional_check != "check" or year == "2014" and additional_check == "check":
        temp_list = list(filtered_data["DGM_1X1"])
        elev_download_list = []
        for i in temp_list:
            name = i[2:len(i)]
            elev_download_list.append(name)
    else:
        elev_download_list = list(filtered_data["NAME"])
    # return the list of relevant tiles
    return elev_download_list


def delete_zip_files(zip_files):
    """
    Deletes one or more ZIP files. Before the function tries to delete a file, it checks whether
    this file actually exists in the working directory.

    Parameter
    ----------
    zip_files: list of str
        A list containing the names of the ZIP files to be deleted.

    Returns
    -------

    """
    # for loop to cycle through the whole list
    for i in zip_files:
        # check if the zip file to delete actually exist
        if os.path.exists(i + ".zip"):
            # delete the zip file
            os.remove(i + ".zip")


def c_tile_number_df(shp):
    """
    Creates and returns a dataframe that contains the tile numbers of a shapefile/geodataframe.

    Parameter
    ----------
    shp: geopandas.geodataframe.GeoDataFrame
        The Intersected shapefile of the area of interest and the tile number shapefile.

    Returns
    -------
    df: pandas.core.frame.DataFrame
         That contains the tile numbers.
    """
    # get the relevant tile_numbers
    tile_number_list = list()
    temp_list = list(shp["DGM_1X1"])
    for tile_number in temp_list:
        tile_number_list.append(tile_number[2:])
    # create the df
    data = {"tile_number": tile_number_list}
    df = pandas.DataFrame(data)
    return df


def csv_to_df(fp):
    """
    Reads in a CSV file as a dataframe and returns the dataframe.

    Parameter
    ----------
    fp: str
        Path to the CSV file.

    Returns
    -------
    df: pandas.core.frame.DataFrame
        Dataframe containing the content of the CSV file.
    """
    df = pandas.read_csv(fp)
    return df


def split_df(df):
    """
    Splits a dataframe into two (based on the year) and returns a list that contains the two new dataframes.

    Parameter
    ----------
    df: pandas.core.frame.DataFrame
        The dataframe to be split.
    Returns
    -------
    list_
        A list containing the new dataframes.
    """
    list_ = list()
    # create df for the years before 2019 and for 2019 and later
    df_before_2019 = df[df["year"] < 2019]
    df_after_2018 = df[df["year"] >= 2019]
    # append dfs to list
    list_.append(df_after_2018)
    list_.append(df_before_2019)
    return list_


def get_relevant_url_ids(url_id_df, tile_number_df, start_year, end_year):
    """
    Creates and returns three list that are needed for the download of the orthophotos and one list contacting the years
    where orthophotos are available only for a part of the area of interest. To accomplish this, several dataframe
    operations are performed.

    Parameter
    ----------
    url_id_df: pandas.core.frame.DataFrame
        Dataframe with the the ID part of all URLs, the years and the tile numbers as columns.
    tile_number_df: pandas.core.frame.DataFrame
        Dataframe containing all relevant tile numbers.
    start_year: int
        First year or interest.
    end_year:
        Last year of interest.

    Returns
    -------
    url_id_list: list of str
        A list which contains the ID part of the URL for each orthophoto downloaded.
    year_list: list of int
        A list which contains the year of capture of each orthophoto to be downloaded.
    tile_number_list: list of str
         A list which contains the tile number of each orthophoto to be downloaded.
    partly_data_list: list of int
        A list contacting the years where orthophotos are available only for a part of the area of interest.
    """
    # check if the years are before 2019
    if end_year < 2019 or url_id_df.iloc[0]["year"] < 2019 and start_year < 2019:
        # add additional tile numbers to the tile_number_df if necessary
        i = 0
        while i < len(tile_number_df):
            # split the tile number df
            split = tile_number_df.iloc[i]["tile_number"].split("_")
            # check if it could be necessary to add a tile number and if yes create it
            # check if the "new" tile number is already in the df if no replace the unnecessary tile number
            # if yes delete the the unnecessary tile number
            if int(split[0]) % 2 != 0 and int(split[1]) % 2 == 0:
                new_part_1 = int(split[0]) - 1
                new_tile_number = str(new_part_1) + "_" + split[1]
                if new_tile_number not in tile_number_df.values:
                    tile_number_df.iloc[i]["tile_number"] = new_tile_number
                    i = i + 1
                else:
                    tile_number_df = tile_number_df.drop(tile_number_df.index[i])
            elif int(split[0]) % 2 == 0 and int(split[1]) % 2 != 0:
                new_part_2 = int(split[1]) - 1
                new_tile_number = split[0] + "_" + str(new_part_2)
                if new_tile_number not in tile_number_df.values:
                    tile_number_df.iloc[i]["tile_number"] = new_tile_number
                    i = i + 1
                else:
                    tile_number_df = tile_number_df.drop(tile_number_df.index[i])
            elif int(split[0]) % 2 != 0 and int(split[1]) % 2 != 0:
                new_part_1 = int(split[0]) - 1
                new_part_2 = int(split[1]) - 1
                new_tile_number = str(new_part_1) + "_" + str(new_part_2)
                if new_tile_number not in tile_number_df.values:
                    tile_number_df.iloc[i]["tile_number"] = new_tile_number
                    i = i + 1
                else:
                    tile_number_df = tile_number_df.drop(tile_number_df.index[i])
            else:
                i = i + 1
                continue
    # execute inner join
    joined_df = pandas.merge(url_id_df, tile_number_df)
    # filter relevant years
    filtered_df = joined_df[(joined_df["year"] >= start_year) & (joined_df["year"] <= end_year)]
    # store relevant url_ids acquisition years and tile numbers in lists and return them
    url_id_list = list(filtered_df["url_id"])
    year_list = list(filtered_df["year"])
    tile_number_list = list(filtered_df["tile_number"])
    # check if there are orthophotos available for the whole aoi for each year (only necessary for years before 2018)
    # add years where this is not the case to a list and return that list
    partly_data_list = []
    if end_year < 2019 or url_id_df.iloc[0]["year"] < 2019 and start_year < 2019:
        for year in range(start_year, end_year + 1):
            if year < 2018:
                year_df = filtered_df[(filtered_df["year"] == year)]
                if len(year_df) < len(tile_number_df) and len(year_df) != 0:
                    partly_data_list.append(year)
    return url_id_list, year_list, tile_number_list, partly_data_list


class GeoFile:
    def __init__(self, file, extent):
        self.file = file
        self.extent = extent


class GeoFileHandler:
    def __init__(self, folder, geo_file_list):
        self.folder = folder
        self.geo_file_list = geo_file_list
        full_extent = []
        self.file_list = []
        for i in range(len(geo_file_list)):
            self.file_list.append(geo_file_list[i]["file"])
            if i == 0:
                full_extent = geo_file_list[i]["extent"]
            else:
                for key in [0, 1]:
                    if geo_file_list[i]["extent"][key] < full_extent[key]:
                        full_extent[key] = geo_file_list[i]["extent"][key]
                for key in [2, 3]:
                    if geo_file_list[i]["extent"][key] > full_extent[key]:
                        full_extent[key] = geo_file_list[i]["extent"][key]
        self.extent = full_extent

    def create_vrt(self):
        opts = gdal.BuildVRTOptions(outputBounds=self.extent)
        vrt = gdal.BuildVRT(self.folder + "/dgm_mosaic.vrt", self.file_list, options=opts)
        gdal.Translate(self.folder + "/dgm_mosaic.tif", vrt, format='GTiff',
                       creationOptions=['COMPRESS:DEFLATE', 'TILED:YES'])


def correct_all_dgm(dir, file_cor):
    folder_list = os.listdir(dir)
    geo_file_handler_list = []
    for i1 in range(len(folder_list)):
        file_list = os.listdir(dir + "/" + folder_list[i1])
        out_file_list = []
        for i2 in range(len(file_list)):
            if file_list[i2].endswith(".xyz"):
                out_file_list.append(dgm_correction(dir + "/" + folder_list[i1], file_list[i2], file_cor))
        geo_file_handler_list.append(GeoFileHandler(dir + "/" + folder_list[i1], out_file_list))
    return geo_file_handler_list


def dgm_correction(dir, file_dgm, file_cor):
    dgm_str = dir + "/" + file_dgm
    out_file = dgm_str.replace(".xyz", "_UTM_cor.tif")
    dgm = gdal.Open(dgm_str)
    gt = dgm.GetGeoTransform()
    x_size = dgm.RasterXSize
    y_size = dgm.RasterYSize
    extent = [gt[0], gt[3] + gt[5] * y_size,
              gt[0] + gt[1] * x_size, gt[3]]
    # [minX, minY, maxX, maxY]
    cor_warp = gdal.Warp("",
                         file_cor,
                         dstSRS="EPSG: 25832",
                         xRes=gt[1],
                         yRes=gt[5],
                         resampleAlg='bilinear',
                         outputBounds=extent,
                         format="vrt")
    data_out = dgm.GetRasterBand(1).ReadAsArray() - cor_warp.GetRasterBand(1).ReadAsArray()
    driver = gdal.GetDriverByName("GTiff")
    ds_out = driver.Create(out_file, x_size, y_size, 1, gdal.GDT_UInt16)
    ds_out.SetGeoTransform(cor_warp.GetGeoTransform())  # sets same geotransform as input
    ds_out.SetProjection(cor_warp.GetProjection())  # sets same projection as input
    band_out = ds_out.GetRasterBand(1)
    band_out.WriteArray(data_out)
    r_d = {"file": out_file, "extent": extent}
    return r_d


# main function
def auto_download():
    # ---------- user input ---------- #

    # set working directory
    os.chdir("D:/FabianHDD/Uni_Master/modulare Programmierung/dataB")

    # set aoi file path
    aoi_fp = "D:/FabianHDD/Uni_Master/modulare Programmierung/" \
             "clip.shp"

    # user request data
    dgm = True
    dom = True
    las = True
    ortho = True

    # dgm_correction
    dgm_cor = True
    file_cor_dgm = "D:/FabianHDD/Uni_Master/modulare Programmierung/documentsB/GCG2016.tif"
    merge_dgm = True
    # delete zip files
    delete = True

    start_year = 2012
    month_start_year = 1
    end_year = 2016
    month_end_year = 12

    # user request years for the image data
    start_year_2 = 2019
    end_year_2 = 2019

    # ---------- function ---------- #
    # ---------- both ---------- #
    # load shapefile function (aoi)
    aoi = load_shapefile(shp_fp=aoi_fp)
    # create a list in which the names of all zip files are stored
    # so that they can be deleted at the end of the function
    zip_files_to_delete = []

    # ---------- elevation data ---------- #
    # Create some variables that are needed, because for some years an additional check is needed.
    # This is due to the fact that the data collection periods partly overlap.
    additional_check = "false"
    length_of_download_list = 0
    # no data available
    no_data_av = "?"
    # loop to cover each year
    year = start_year
    while year <= end_year:
        # check if there could be elevation data available for this year if not give the user feedback
        if dgm is True or dom is True or las is True:
            url_year, dem_n, elev_data_file = set_elev_variables(year=year)
            if url_year == "stop":
                print("There is no elevation data available prior to 2011.")
                # prevents unnecessary loop cycles
                if year < 2011 <= end_year:
                    year = 2010
                else:
                    break
            else:
                # changes because of additional check
                if additional_check == 2014:
                    url_year, dem_n, elev_data_file = set_elev_variables(year=year - 1)
                    additional_check = "check"
                    end_year = end_year - 1
                # download meta data
                meta_data_name = data_download(data_list_to_download=["meta_data_elevation_data_" + url_year],
                                               year=year, type_to_download="meta_data", url_year=url_year)
                # create folder for elevation data and unzip meta_data if necessary
                # add the zip name to the list of zip files that are to delete
                if meta_data_name != "no_new_data" or not os.path.exists("elevation_data/meta_data"):
                    zip_files_to_delete.extend(meta_data_name)
                    create_and_unzip(folder_path="elevation_data/meta_data",
                                     zip_files=["meta_data_elevation_data_" + url_year])
                # load meta_data shapefile
                elev_meta_data_shp = load_shapefile(shp_fp="elevation_data/meta_data/" + elev_data_file)
                # intersect meta data and aoi shp
                elev_meta_data_aoi = intersect_shps(shp_1=aoi, shp_2=elev_meta_data_shp)
                # changes because of additional check
                if additional_check == 2013 or additional_check == 2019:
                    year = year - 1
                    additional_check = "check"
                    end_year = end_year - 1
                # get download list
                elev_download_list = create_elev_download_lists(elev_aoi=elev_meta_data_aoi, year=year,
                                                                start_year=start_year, end_year=end_year,
                                                                month_start_year=month_start_year,
                                                                month_end_year=month_end_year,
                                                                additional_check=additional_check)
                # detect if an additional check should be executed for this year
                if year == 2013 and additional_check != 2013 and additional_check != "check":
                    if end_year == 2013 and month_end_year != 12:
                        additional_check = "false"
                    else:
                        additional_check = 2013
                        end_year = end_year + 1
                if year == 2014 and additional_check != 2014 and additional_check != "check":
                    if end_year == 2014 and month_end_year == 1 or start_year == 2014 and month_start_year > 2:
                        additional_check = "false"
                    else:
                        additional_check = 2014
                        end_year = end_year + 1
                if year == 2019 and additional_check != 2019 and additional_check != "check":
                    if end_year == 2019 and month_end_year < 11:
                        additional_check = "false"
                    else:
                        additional_check = 2019
                        end_year = end_year + 1
                # if there is only data for part of the AOI inform the user about it
                # but if there is an additional check pending for this year give no feedback yet because there is still
                # a chance that data for this year and region is available
                if len(elev_download_list) + length_of_download_list < len(elev_meta_data_aoi):
                    if additional_check != 2013 and additional_check != 2014 and additional_check != 2019 and \
                            additional_check != "check":
                        print("Only for a part of this area there is elevation data available for " + str(year) + ".")
                    # save the length of the download list before the additional check if there is a check pending
                    else:
                        length_of_download_list = len(elev_download_list)
                # if the download list is empty give the user feedback about it and skip the rest of this loop cycle
                # but if there is an additional check pending for this year give no feedback yet because there is still
                # a chance that data for this year and region is available
                if elev_download_list == "stop":
                    if additional_check != 2013 and additional_check != 2014 and additional_check != 2019 and \
                            (no_data_av == "probably" or additional_check != "check"):
                        print("There is no elevation data available for this area for " + str(year) + ".")
                        if year == start_year and month_start_year != 1 or year == end_year and month_end_year != 12:
                            print("At least for the selected months.")
                        additional_check = "false"
                        no_data_av = "yes"
                    else:
                        no_data_av = "probably"
                    # adjustments due to the additional check
                    if additional_check == 2014:
                        year = year - 1
                    year = year + 1
                    continue
                # download the data,
                # if data was downloaded and add the zip names to the list of zip files that are to delete
                if dgm is True:
                    elev_data_list = data_download(type_to_download="dgm", url_year=url_year, year=year,
                                                   data_list_to_download=elev_download_list, dem_n=dem_n)
                    if elev_data_list != "no_new_data":
                        create_and_unzip(folder_path="elevation_data/dgm/" + str(year), zip_files=elev_data_list)
                        zip_files_to_delete.extend(elev_data_list)

                if dom is True:
                    elev_data_list = data_download(type_to_download="dom", url_year=url_year, year=year,
                                                   data_list_to_download=elev_download_list, dem_n=dem_n)
                    if elev_data_list != "no_new_data":
                        create_and_unzip(folder_path="elevation_data/dom/" + str(year), zip_files=elev_data_list)
                        zip_files_to_delete.extend(elev_data_list)
                if las is True:
                    elev_data_list = data_download(type_to_download="las", url_year=url_year, year=year,
                                                   data_list_to_download=elev_download_list, dem_n=dem_n)
                    if elev_data_list != "no_new_data":
                        create_and_unzip(folder_path="elevation_data/las/" + str(year), zip_files=elev_data_list)
                        zip_files_to_delete.extend(elev_data_list)
        year = year + 1
        # adjustments due to the additional check
        if additional_check == 2014:
            year = year - 1
        if additional_check == "check":
            additional_check = "false"
            length_of_download_list = 0

    # ---------- image data ---------- #
    if ortho is True:
        # download shp with tile numbers if necessary(auxiliary data)
        aux_data_name = data_download(data_list_to_download=["meta_data_elevation_data_" + "2010-2013"],
                                      type_to_download="auxiliary_data", url_year="2010-2013")
        # create folder for image data and unzip auxiliary data if necessary
        if aux_data_name != "no_new_data" or not os.path.exists("image_data/auxiliary_data"):
            zip_files_to_delete.extend(aux_data_name)
            create_and_unzip(folder_path="image_data/auxiliary_data",
                             zip_files=["meta_data_elevation_data_" + "2010-2013"])
        # load tile_number_shp
        tile_number_shp = load_shapefile(shp_fp="image_data/auxiliary_data/" +
                                                "DGM2_2010-2013_Erfass-lt-Meta_UTM32-UTM_2014-12-10.shp")
        # intersect aoi and tile_number_shp
        aoi_tile_numbers_shp = intersect_shps(shp_1=aoi, shp_2=tile_number_shp)
        # create tile number df
        tile_number_df = c_tile_number_df(shp=aoi_tile_numbers_shp)
        # download url_id_file if necessary
        url_id_data = data_download(data_list_to_download=["url_id_data"], type_to_download="auxiliary_data")
        # extract url_id_file if necessary
        # add the zip name to the list of zip files that are to delete
        if url_id_data != "no_new_data" or not os.path.exists("image_data/auxiliary_data/url_id_file.csv"):
            zip_files_to_delete.extend(url_id_data)
            create_and_unzip(folder_path="image_data/auxiliary_data/", zip_files=["url_id_data"])
        # load url id file as df
        url_id_df = csv_to_df(fp="image_data/auxiliary_data/url_id_file.csv")
        # before 2019 the orthophoto covers a 2x2 km area and this affects the tile numbers
        # split url_id_df (if necessary)
        if end_year_2 >= 2019 and start_year_2 < 2019:
            url_id_df_list = split_df(df=url_id_df)
            # get the relevant url ids, years and tile numbers
            url_id_list, year_list, tile_number_list, partly_data_list = get_relevant_url_ids(
                url_id_df=url_id_df_list[0],
                tile_number_df=tile_number_df,
                start_year=start_year_2,
                end_year=end_year_2)
            url_id_list_2, year_list_2, tile_number_list_2, partly_data_list_2 = get_relevant_url_ids(
                url_id_df=url_id_df_list[1],
                tile_number_df=tile_number_df,
                start_year=start_year_2,
                end_year=end_year_2)
            url_id_list.extend(url_id_list_2)
            year_list.extend(year_list_2)
            tile_number_list.extend(tile_number_list_2)
            partly_data_list.extend(partly_data_list_2)
        else:
            url_id_list, year_list, tile_number_list, partly_data_list = get_relevant_url_ids(
                url_id_df=url_id_df,
                tile_number_df=tile_number_df,
                start_year=start_year_2,
                end_year=end_year_2)
        # inform the user if there are no orthophotos available
        for year in range(start_year_2, end_year_2 + 1):
            if (year in year_list) is False:
                print("There are no orthophotos available for this area for " + str(year) + ".")
        # inform the user if the orthophotos available for a year does not cover the whole aoi
        for year in partly_data_list:
            print("Only for a part of this area there are orthophotos available available for " + str(year) + ".")
        # download orthophotos
        image_data = data_download(type_to_download="ortho", data_list_to_download=url_id_list,
                                   year_list=year_list, tile_number_list=tile_number_list)
        # if data was downloaded unzip it and add the zip names to the list of zip files that are to delete
        # loop to create a new folder for each year
        if image_data != "no_new_data":
            zip_files_to_delete.extend(image_data)
            # loop to create a new folder for each year
            index = 0
            for zip_file_name in image_data:
                create_and_unzip(folder_path="image_data/orthophotos/" + str(year_list[index]),
                                 zip_files=[zip_file_name])
                index = index + 1

    # ---------- both ---------- #
    # if it is wanted delete the zip files
    if dgm_cor is True:
        geo_file_handler_list = correct_all_dgm("./elevation_data/dgm", file_cor_dgm)
        if merge_dgm is True:
            for i in range(len(geo_file_handler_list)):
                geo_file_handler_list[i].create_vrt()
    if delete:
        delete_zip_files(zip_files=zip_files_to_delete)


# test = auto_download()
if __name__ == "__main__":
    auto_download()

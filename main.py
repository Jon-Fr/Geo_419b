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


# set year and so on for the elevation data urls + set the name of the meta data shapefile
def set_elev_variables(year):
    # the variables url_year and dem_n are used to store parts needed for the download urls.
    # under the variable elev_data_file the name of the meta data shapefile is stored
    # define the variables if there could be data available for this year
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
    # return stop if there is certainly data available for this year
    else:
        return "stop", "stop", "stop"
    # return the defined variables
    return url_year, dem_n, elev_data_file


# data download
def data_download(type_to_download, data_list_to_download, url_year="", year=0, dem_n="", year_list=None,
                  tile_number_list=None):
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
        if not os.path.exists(zip_name) and not os.path.exists(hy_file_path + file_name):
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


# create folder if not already existing and unzip files into it
def create_and_unzip(folder_path, zip_files):
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


# intersect shapefiles function
def intersect_shps(shp_1, shp_2):
    # Re-project shp_1 if necessary
    re = shp_2.crs == shp_1.crs
    if re is False:
        shp_1 = shp_1.to_crs(shp_2.crs)
    # execute join
    intersected_shp = geopandas.sjoin(shp_1, shp_2, how="inner")
    # return the result
    return intersected_shp


# creates a list of elevation data to be downloaded
def create_elev_download_lists(elev_aoi, year, start_year, end_year, month_start_year, month_end_year,
                               additional_check):
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


# deletes zip files
def delete_zip_files(zip_files):
    # for loop to cycle through the whole list
    for i in zip_files:
        # check if the zip file to delete actually exist
        if os.path.exists(i + ".zip"):
            # delete the zip file
            os.remove(i + ".zip")


# create tile number df
def c_tile_number_df(shp):
    # get the relevant tile_numbers
    tile_number_list = list()
    temp_list = list(shp["DGM_1X1"])
    for tile_number in temp_list:
        tile_number_list.append(tile_number[2:])
    # create the df
    data = {"tile_number": tile_number_list}
    df = pandas.DataFrame(data)
    return df


# load csv as df
def csv_to_df(fp):
    df = pandas.read_csv(fp)
    return df


# split df
def split_df(df):
    list_ = list()
    # create df for the years before 2019 and for 2019 and later
    df_before_2019 = df[df["year"] < 2019]
    df_after_2018 = df[df["year"] >= 2019]
    # append dfs to list
    list_.append(df_after_2018)
    list_.append(df_before_2019)
    return list_


def get_relevant_urls(url_id_df, tile_number_df, start_year, end_year):
    # check if the years are before 2019
    if end_year < 2019 or url_id_df.iloc[0]["year"] < 2019 and start_year < 2019:
        # add additional tile numbers to the tile_number_df if necessary
        for i in range(0, len(tile_number_df)):
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
                else:
                    tile_number_df = tile_number_df.drop([i])
            elif int(split[0]) % 2 == 0 and int(split[1]) % 2 != 0:
                new_part_2 = int(split[1]) - 1
                new_tile_number = split[0] + "_" + str(new_part_2)
                if new_tile_number not in tile_number_df.values:
                    tile_number_df.iloc[i]["tile_number"] = new_tile_number
                else:
                    tile_number_df = tile_number_df.drop([i])
            elif int(split[0]) % 2 != 0 and int(split[1]) % 2 != 0:
                new_part_1 = int(split[0]) - 1
                new_part_2 = int(split[1]) - 1
                new_tile_number = str(new_part_1) + "_" + str(new_part_2)
                if new_tile_number not in tile_number_df.values:
                    tile_number_df.iloc[i]["tile_number"] = new_tile_number
                else:
                    tile_number_df = tile_number_df.drop([i])
            else:
                continue
    # execute inner join
    joined_df = pandas.merge(url_id_df, tile_number_df)
    # filter relevant years
    filtered_df = joined_df[(joined_df["year"] >= start_year) & (joined_df["year"] <= end_year)]
    # store relevant url_ids acquisition years and tile numbers in lists and return them
    url_id_list = list(filtered_df["url_id"])
    year_list = list(filtered_df["year"])
    tile_number_list = list(filtered_df["tile_number"])
    return url_id_list, year_list, tile_number_list


# main function
def auto_download():
    # ---------- user input ---------- #

    # set working directory
    os.chdir("C:/Users/frank/Desktop/Test")

    # set aoi file path
    aoi_fp = "C:/Users/frank/Desktop/Data/AOI_/AOI_04.shp"

    # user request data
    dgm = True
    dom = True
    las = True
    ortho = True

    # delete zip files
    delete = True

    # user request year and month for the elevation data
    start_year = 2014
    month_start_year = 1
    end_year = 2014
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
                # if the download list is empty give the user feedback about it and skip the rest of this loop cycle
                # but if there is an additional check pending for this year give no feedback yet because there is still
                # a chance that data for this year and region is available
                if elev_download_list == "stop":
                    if additional_check != 2013 and additional_check != 2014 and additional_check != 2019 and \
                            (no_data_av == "probably" or additional_check != "check"):
                        print("There is no elevation data available for this specific region for " + str(year) + ".")
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
        # load stile_number_shp
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
            url_id_list, year_list, tile_number_list = get_relevant_urls(url_id_df=url_id_df_list[0],
                                                                         tile_number_df=tile_number_df,
                                                                         start_year=start_year_2, end_year=end_year_2)
            url_id_list_2, year_list_2, tile_number_list_2 = get_relevant_urls(url_id_df=url_id_df_list[1],
                                                                               tile_number_df=tile_number_df,
                                                                               start_year=start_year_2,
                                                                               end_year=end_year_2)
            url_id_list.extend(url_id_list_2)
            year_list.extend(year_list_2)
            tile_number_list.extend(tile_number_list_2)
        else:
            url_id_list, year_list, tile_number_list = get_relevant_urls(url_id_df=url_id_df,
                                                                         tile_number_df=tile_number_df,
                                                                         start_year=start_year_2, end_year=end_year_2)
        #
        if len(url_id_list) == 0:
            if start_year_2 != end_year_2:
                print("""There are no orthophotos available for this specific region for the time period from {} to {}.
                """.format(str(start_year_2), str(end_year_2)))
            else:
                print("""There are no orthophotos available for this specific region for {}."""
                      .format(str(start_year_2)))
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
    if delete:
        delete_zip_files(zip_files=zip_files_to_delete)


# test = auto_download()
if __name__ == "__main__":
    auto_download()

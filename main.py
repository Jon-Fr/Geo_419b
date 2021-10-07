# import packages
import os
import requests
import zipfile
import geopandas


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


# elevation data download
def elev_data_download(type_to_download, url_year, data_list_to_download, year, dem_n=""):
    # the two following variables are used for the naming of the zip files (data kind = dgm, dom or las)
    data_year = "_" + str(year)
    data_kind = ""
    # in this list the names of the zip files are stored
    elev_data_list = []
    # for loop to download more than one file
    for i in data_list_to_download:
        # set the url and the one or two of the variables  used for the naming of the zip files
        url = ""
        if type_to_download == "meta_data":
            url = """https://geoportal.geoportal-th.de/hoehendaten/Uebersichten/Stand_{}.zip""".format(url_year)
            data_year = ""
        elif type_to_download == "dgm":
            url = """https://geoportal.geoportal-th.de/hoehendaten/DGM/dgm_{}/dgm{}_{}_1_th_{}.zip""" \
                .format(url_year, dem_n, i, url_year)
            data_kind = "dgm_"
        elif type_to_download == "dom":
            url = """http://geoportal.geoportal-th.de/hoehendaten/DOM/dom_{}/dom{}_{}_1_th_{}.zip""" \
                .format(url_year, dem_n, i, url_year)
            data_kind = "dom_"
        elif type_to_download == "las":
            url = """http://geoportal.geoportal-th.de/hoehendaten/LAS/las_{}/las_{}_1_th_{}.zip""" \
                .format(url_year, i, url_year)
            data_kind = "las_"
        # set the name of the zip file and append it to zip file name list
        zip_name = data_kind + i + data_year + ".zip"
        elev_data_list.append(zip_name[0:len(zip_name) - 4])
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
    # return the zip file name list
    return elev_data_list


# create folder if not already existing and unzip files into it
def create_and_unzip(folder_path, zip_files):
    # create folder
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    # extract zip files into the folder
    for i in zip_files:
        zipped_data = zipfile.ZipFile(i + ".zip", "r")
        zipped_data.extractall(path=folder_path)
        zipped_data.close()


# intersect aoi and elevation data shapefile
def elev_aoi_intersect(aoi, elev_meta_data_shp):
    # Re-project aoi shapefile if necessary
    re = elev_meta_data_shp.crs == aoi.crs
    if re is False:
        aoi = aoi.to_crs(elev_meta_data_shp.crs)
    # execute join
    elev_aoi = geopandas.sjoin(aoi, elev_meta_data_shp, how="inner")
    # return the result
    return elev_aoi


# creates a list of elevation data to be downloaded
def create_elev_data_lists(elev_aoi, year, start_year, end_year, month_start_year, month_end_year, additional_check):
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


# main function
def auto_download():
    # ---------- user input ---------- #

    # set working directory
    os.chdir("C:/Users/frank/Desktop/Test")

    # load aoi shapefile
    aoi_fp = "C:/Users/frank/Desktop/Data/AOI_/AOI_03.shp"
    aoi = load_shapefile(aoi_fp)

    # user request data
    dgm = True
    dom = False
    las = False

    # user request year and month
    start_year = 2014
    month_start_year = 1
    end_year = 2014
    month_end_year = 12

    # ---------- function ---------- #
    # create a list in which the names of all zip files are stored
    # so that they can be deleted at the end of the function
    zip_files_to_delete = []
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
                # download and extract meta data
                meta_data_name = elev_data_download(data_list_to_download=["meta_data_elevation_data_" + url_year],
                                                    year=year, type_to_download="meta_data", url_year=url_year)
                zip_files_to_delete.extend(meta_data_name)
                # create folder for elevation data and unzip meta_data
                create_and_unzip(folder_path="elevation_data/meta_data",
                                 zip_files=["meta_data_elevation_data_" + url_year])
                # load meta_data shapefile
                elev_meta_data_shp = load_shapefile(shp_fp="elevation_data/meta_data/" + elev_data_file)
                # intersect meta data and aoi shp
                elev_meta_data_aoi = elev_aoi_intersect(aoi=aoi, elev_meta_data_shp=elev_meta_data_shp)
                # changes because of additional check
                if additional_check == 2013 or additional_check == 2019:
                    year = year - 1
                    additional_check = "check"
                    end_year = end_year - 1
                # get download list
                elev_download_list = create_elev_data_lists(elev_aoi=elev_meta_data_aoi, year=year,
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
                        if year == start_year or year == end_year:
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
                # download the data, unzip it and add the zip name to the list of zip files that are to delete
                if dgm is True:
                    elev_data_list = elev_data_download(type_to_download="dgm", url_year=url_year, year=year,
                                                        data_list_to_download=elev_download_list, dem_n=dem_n)
                    create_and_unzip(folder_path="elevation_data/dgm/" + str(year), zip_files=elev_data_list)
                    zip_files_to_delete.extend(elev_data_list)
                if dom is True:
                    elev_data_list = elev_data_download(type_to_download="dom", url_year=url_year, year=year,
                                                        data_list_to_download=elev_download_list, dem_n=dem_n)
                    create_and_unzip(folder_path="elevation_data/dom/" + str(year), zip_files=elev_data_list)
                    zip_files_to_delete.extend(elev_data_list)
                if las is True:
                    elev_data_list = elev_data_download(type_to_download="las", url_year=url_year, year=year,
                                                        data_list_to_download=elev_download_list, dem_n=dem_n)
                    create_and_unzip(folder_path="elevation_data/las/" + str(year), zip_files=elev_data_list)
                    zip_files_to_delete.extend(elev_data_list)
        year = year + 1
        # adjustments due to the additional check
        if additional_check == 2014:
            year = year - 1
        if additional_check == "check":
            additional_check = "false"
    # delete zip files
    delete_zip_files(zip_files=zip_files_to_delete)


# test = auto_download()
if __name__ == "__main__":
    auto_download()

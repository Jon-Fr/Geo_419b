import os
import requests
import zipfile
import geopandas


# load shapefile
def load_shapefile(shp_fp):
    shp = geopandas.read_file(shp_fp)

    return shp


# set year and so on for the elevation data urls
def set_elev_variables(year):
    if 2010 <= year <= 2013:
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
    else:
        return "stop", "stop", "stop"

    return url_year, dem_n, elev_data_file


# elevation data download
def elev_data_download(type_to_download, url_year, data_list_to_download, year, dem_n=""):
    data_year = "_" + str(year)
    data_kind = ""
    elev_data_list = []
    if type(data_list_to_download) != list:
        data_list_to_download = list(data_list_to_download)
    for i in data_list_to_download:
        # set url
        if type_to_download == "overview":
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
        else:
            return "stop"
        zip_name = data_kind + i + data_year + ".zip"
        elev_data_list.append(zip_name[0:len(zip_name) - 4])
        if not os.path.exists(zip_name):
            # download data
            response = requests.get(url, stream=True)
            # write data
            data = open(zip_name, "wb")
            for chunk in response.iter_content(chunk_size=1024):
                data.write(chunk)
            # close data
            data.close()

    return elev_data_list


# create folder if not already existing and unzip files into it
def create_and_unzip(folder_path, zip_files):
    # create folder
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    # extract zip files into the folder
    if type(zip_files) != list:
        zip_files = list(zip_files)
    for i in zip_files:
        zipped_data = zipfile.ZipFile(i + ".zip", "r")
        zipped_data.extractall(path=folder_path)
        zipped_data.close()


# intersect aoi and elevation data shapefile
def elev_aoi_intersect(aoi, elev_overview_shp):
    # Re-project aoi shapefile if necessary
    re = elev_overview_shp.crs == aoi.crs
    if re is False:
        aoi = aoi.to_crs(elev_overview_shp.crs)
    # execute join
    elev_aoi = geopandas.sjoin(aoi, elev_overview_shp, how="inner")

    return elev_aoi


# creates a list of elevation data to be downloaded
def create_elev_data_lists(elev_aoi, year):
    year = str(year)
    # filter year
    filtered_year = elev_aoi[(elev_aoi["ERFASSUNG"] == year + "-01") | (elev_aoi["ERFASSUNG"] == year + "-02") |
                             (elev_aoi["ERFASSUNG"] == year + "-03") | (elev_aoi["ERFASSUNG"] == year + "-04") |
                             (elev_aoi["ERFASSUNG"] == year + "-05") | (elev_aoi["ERFASSUNG"] == year + "-06") |
                             (elev_aoi["ERFASSUNG"] == year + "-07") | (elev_aoi["ERFASSUNG"] == year + "-08") |
                             (elev_aoi["ERFASSUNG"] == year + "-09") | (elev_aoi["ERFASSUNG"] == year + "-10") |
                             (elev_aoi["ERFASSUNG"] == year + "-11") | (elev_aoi["ERFASSUNG"] == year + "-12")]
    if len(filtered_year) == 0:
        return "stop"
    # create list
    if year < "2014":
        temp_list = list(filtered_year["DGM_1X1"])
        elev_download_list = []
        for i in temp_list:
            name = i[2:len(i)]
            elev_download_list.append(name)
    else:
        elev_download_list = list(filtered_year["NAME"])

    return elev_download_list


# delete zip files
def delete_zip_files(zip_files):
    if type(zip_files) != list:
        zip_files = list(zip_files)
    for i in zip_files:
        if os.path.exists(i + ".zip"):
            os.remove(i + ".zip")


# main function
def auto_download():

    # ---------- user input ---------- #

    # set working directory
    os.chdir("C:/Users/frank/Desktop/Test")

    # load aoi shapefile
    aoi_fp = "C:/Users/frank/Desktop/Data/AOI_/AOI_02.shp"
    aoi = load_shapefile(aoi_fp)

    # user request data
    dgm = True
    dom = False
    las = False

    # user request year
    start_year = 2016
    end_year = 2020

    # ---------- function ---------- #
    zip_files_to_delete = []
    additional_check = "False"
    # loop to cover each year
    year = start_year
    while year <= end_year:
        # check if there could be elevation data available for this year
        if dgm is True or dom is True or las is True:
            url_year, dem_n, elev_data_file = set_elev_variables(year=year)
            if url_year == "stop":
                print("The oldest elevation data available is from 2010.")
                # prevents unnecessary loop cycles
                if end_year >= 2010:
                    year = 2009
                else:
                    break
            else:
                # download
                if additional_check == 2014:
                    url_year, dem_n, elev_data_file = set_elev_variables(year=year-1)
                overview_name = elev_data_download(data_list_to_download=["overview_elevation_data_" + url_year],
                                                   year=year, type_to_download="overview", url_year=url_year)
                zip_files_to_delete.extend(overview_name)
                # create folder for elevation data and unzip overview
                create_and_unzip(folder_path="elevation_data/overview",
                                 zip_files=["overview_elevation_data_" + url_year])
                # load overview shapefile
                elev_overview_shp = load_shapefile(shp_fp="elevation_data/overview/" + elev_data_file)
                # intersect
                elev_overview_aoi = elev_aoi_intersect(aoi=aoi, elev_overview_shp=elev_overview_shp)
                # download list
                if additional_check == 2013 or additional_check == 2019:
                    year = year - 1
                elev_download_list = create_elev_data_lists(elev_aoi=elev_overview_aoi, year=year)
                # check if list is empty or not
                if elev_download_list == "stop":
                    if year == 2013 and additional_check != 2013:
                        additional_check = 2013
                        year = year + 1
                    elif year == 2014 and additional_check != 2014:
                        additional_check = 2014
                    elif year == 2019 and additional_check != 2019:
                        additional_check = 2019
                        year = year + 1
                    else:
                        print("There is no elevation data available for this specific region for " + str(year) + ".")
                        additional_check = "False"
                        year = year + 1
                    continue
                else:
                    additional_check = "False"
                    # download and unzip
                    if dgm is True:
                        elev_data_list = elev_data_download(type_to_download="dgm", url_year=url_year, year=year,
                                                            data_list_to_download=elev_download_list, dem_n=dem_n)
                        create_and_unzip(folder_path="elevation_data/dgm/"+str(year), zip_files=elev_data_list)
                        zip_files_to_delete.extend(elev_data_list)
                    if dom is True:
                        elev_data_list = elev_data_download(type_to_download="dom", url_year=url_year, year=year,
                                                            data_list_to_download=elev_download_list, dem_n=dem_n)
                        create_and_unzip(folder_path="elevation_data/dom/"+str(year), zip_files=elev_data_list)
                        zip_files_to_delete.extend(elev_data_list)
                    if las is True:
                        elev_data_list = elev_data_download(type_to_download="las", url_year=url_year, year=year,
                                                            data_list_to_download=elev_download_list, dem_n=dem_n)
                        create_and_unzip(folder_path="elevation_data/las/"+str(year), zip_files=elev_data_list)
                        zip_files_to_delete.extend(elev_data_list)
        year = year + 1
    # delete zip filex
    delete_zip_files(zip_files=zip_files_to_delete)


# test = auto_download()
if __name__ == "__main__":
    auto_download()

import os
import requests
import zipfile

# set working directory
os.chdir("C:/Users/frank/Desktop/Data")

# user request
dgm_list = ["561_5610", "563_5616", "567_5621"]
year = 2017

# set year
if 2010 <= year <= 2013:
    year = "2010-2013"
    dgm_n = "dgm2"
elif 2014 <= year <= 2019:
    year = "2014-2019"
    dgm_n = "dgm1"
elif 2020 <= year <= 2025:
    year = "2020-2025"
    dgm_n = "dgm1"

# for loop to download more than one dgm
for i in dgm_list:
    # set url
    url = """https://geoportal.geoportal-th.de/hoehendaten/DGM/dgm_{}/{}_{}_1_th_{}.zip""".format(year, dgm_n, i, year)
    # download data
    response = requests.get(url, stream=True)
    # write data
    data = open(i+".zip", "wb")
    for chunk in response.iter_content(chunk_size=1024):
        data.write(chunk)
    # close data
    data.close()

# create folder for the DGMs
if not os.path.exists("DGMs"):
    os.makedirs("DGMs")

# extract zip files into one folder
for i in dgm_list:
    zipped_data = zipfile.ZipFile(i+".zip", "r")
    zipped_data.extractall(path="DGMs")
    zipped_data.close()

# delete zip files
for i in dgm_list:
    if os.path.exists(i+".zip"):
        os.remove(i+".zip")



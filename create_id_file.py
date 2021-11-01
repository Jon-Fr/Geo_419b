# import packages
import asyncio
import aiohttp
import pandas
import os

# set working directory
os.chdir("path")

# prevents RuntimeError: Event loop is closed
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# __________ define functions for the requests __________ #


# request function
async def get_hcd(session, url):
    # if there is no answer to the request a key error will occur
    try:
        # request head
        async with session.head(url) as resp:
            # select hcd
            head_content_disposition = resp.headers["content-disposition"]
            # return hcd + url
            return head_content_disposition + "__" + url
    # if an key error occurs return the url
    except KeyError:
        return url


# framework function for the request function
async def main(start=0, stop=0, list_of_ids=None):
    # set timeout time
    timeout = aiohttp.ClientTimeout(total=12000)
    # create client session so that not every request will open an new connection
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # create a tasks, hcd + url and only url(KeyError) list
        tasks = list()
        hcd__url_list = list()
        url_key_error_list = list()
        # check if there is a list of ids
        if list_of_ids is not None:
            # loop through the list
            for number in list_of_ids:
                # set url
                url = "https://geoportal.geoportal-th.de/gaialight-th/_apps/dladownload/download.php?type=op&id=" + \
                      str(number)
                # append task to tasks list asyncio.ensure_future schedules the execution of the task
                tasks.append(asyncio.ensure_future(get_hcd(session, url)))
        # if there is no list there should be a range of values
        else:
            for number in range(start, stop):
                url = "https://geoportal.geoportal-th.de/gaialight-th/_apps/dladownload/download.php?type=op&id=" + \
                      str(number)
                tasks.append(asyncio.ensure_future(get_hcd(session, url)))
        # gather all Future tasks(executions of request function) and wait till they are finished
        hcd__url_all = await asyncio.gather(*tasks)
        # loop through the returns
        for hcd__url in hcd__url_all:
            # check if the return is hcd + url or only the url and append them to the appropriate  list
            if hcd__url[0] != "h":
                hcd__url_list.append(hcd__url)
            else:
                url_key_error_list.append(hcd__url)
        # return hcd + url and only url(KeyError) list
        return hcd__url_list, url_key_error_list


# __________ run the function, sort the return and export csv files with the relevant data __________ #


# set the range for the ids that should be requested
# set the number of concurrent request
# loop through the returned lists
for i1 in range(200000, 200300, 100):
    # run framework function
    hcd__url_re_list, url_key_error_re_list = asyncio.run(main(start=i1, stop=i1+100))
    # cause the urls are sometimes not reachable for a short time
    # each url that is not reachable is checked for a second an if necessary even a third time
    for i2 in range(0, 2):
        if len(url_key_error_re_list) > 0:
            # create list of error url ids
            error_url_id_list = list()
            # get url ids
            for error_url_re in url_key_error_re_list:
                error_url_id = ""
                for i3 in range(6, 1, -1):
                    if error_url_re[len(error_url_re) - i3:].isdigit() is True:
                        error_url_id = int(error_url_re[len(error_url_re) - i3:])
                        break
                # append url id to url id list
                error_url_id_list.append(error_url_id)
            # run framework function with the url id list as input
            hcd__url_re_list2, url_key_error_re_list = asyncio.run(main(list_of_ids=error_url_id_list))
            # add hcd + ulr to the already existing hcd + ulr list
            hcd__url_re_list = hcd__url_re_list + hcd__url_re_list2
    # if the returned hcd + url list is not empty loop though the list
    if len(hcd__url_re_list) != 0:
        # create a list for the url ids, the years and the tile numbers
        url_id_list = list()
        year_list = list()
        tile_number_list = list()
        # get url id
        for hcd__url_re in hcd__url_re_list:
            url_id = ""
            for i4 in range(6, 1, -1):
                if hcd__url_re[len(hcd__url_re) - i4:].isdigit() is True:
                    url_id = int(hcd__url_re[len(hcd__url_re) - i4:])
                    break
            # get year
            th_position = hcd__url_re.find("th")
            year = hcd__url_re[th_position + 3:th_position + 7]
            # get tile number
            _32_position = hcd__url_re.find("_32_")
            if _32_position != -1:
                tile_number = hcd__url_re[_32_position + 4:_32_position + 12]
            else:
                _32_position = hcd__url_re.find("_32")
                tile_number = hcd__url_re[_32_position + 3:_32_position + 11]
            # sort out some data that is not relevant
            if tile_number == "tachment":
                continue
            # append url ids an so on to the appropriate list
            url_id_list.append(url_id)
            year_list.append(year)
            tile_number_list.append(tile_number)
        # create df with the url id, year and tile number as columns
        data = {"url_id": url_id_list, "year": year_list, "tile_number": tile_number_list}
        url_id_df = pandas.DataFrame(data)
        # set name for the csv file
        name = str(i1) + "_" + str(i1+100) + "_url_id_file.csv"
        # export df as csv
        url_id_df.to_csv(name, index=False)
    # if url(KeyError) list is not empty export a csv data with the "error" urls
    if len(url_key_error_re_list) != 0:
        error_data = {"error_urls": url_key_error_re_list}
        error_df = pandas.DataFrame(error_data)
        name = str(i1) + "_" + str(i1+100) + "_url_KeyError.csv"
        error_df.to_csv(name, index=False)


# __________ merge the csv files __________ #


# create list for the filenames
filenames_list = list()
# get path to the working directory
directory = os.getcwd()

# loop through files in the working directory
for file in os.scandir(directory):
    # convert to string cause argument of type 'nt.DirEntry' is not iterable
    filename = str(file)
    # check if it is a KeyError file or not
    if "KeyError" not in filename:
        # add filename to filename list
        filenames_list.append(filename[11:len(filename)-2])

# combine the content of all the files
combined_content = pandas.concat([pandas.read_csv(filename) for filename in filenames_list])
# create a csv file with the combined content
combined_content.to_csv("url_id_file.csv", index=False)

from functools import partial
from multiprocessing import Pool
from tqdm import tqdm
from queuedwriters.csvWriteQueue import CSVQueueWriter

import re
import pandas as pd
import numpy as np
import csv


def combine_words(array):
    result = ""
    for word in array:
        result += rf"\b{word}\b|"
    return result[:-1]


def get_tags():
    with open("./keywords_info.csv", mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header_row = next(reader, None)  # skip the headers
        return header_row[1:]


def load_keyword_info():
    '''
    Loads the keyword info file and returns the tagging array for each keyword as well as the tag header.
    '''
    keyword_df = pd.read_csv("./keywords_info.csv")
    header = list(keyword_df.columns)
    tags = header[1:]
    new_df = keyword_df.groupby(tags).agg(
        {'keyword': combine_words}).reset_index()

    new_df = new_df.set_index("keyword")

    keyword_tagging_dict = {}

    i = 0

    for index, row in new_df.iterrows():
        keyword_tagging_dict[i] = {
            "array": row.to_list(),
            "regex": re.compile(index, flags=re.IGNORECASE)
        }
        i += 1

    return keyword_tagging_dict, tags


def parse(t, keyword_tagging_array, tag_array_header):
    if t["text"] is None or t["text"] == "":
        return

    result = np.zeros(len(tag_array_header), dtype=int)
    matched = False

    for i in keyword_tagging_array:

        regex = keyword_tagging_array[i]["regex"]
        array = keyword_tagging_array[i]["array"]

        total_matches = len(regex.findall(t["text"]))

        if (total_matches > 0):
            result = np.add(result, np.multiply(array, total_matches))
            matched = True

    if (matched):
        return [t["status_id"], t["user_id"], t["screen_name"], t["created_at"],
                t["text"]] + result.tolist()


def parse_tweets(tweet_list, writer):
    keyword_tagging_array, tag_array_header = load_keyword_info()

    with Pool(14) as executor:
        parse_x = partial(
            parse, keyword_tagging_array=keyword_tagging_array, tag_array_header=tag_array_header)
        parsed_tweets = executor.map(parse_x, tweet_list)

    for parsed_results in parsed_tweets:
        if (parsed_results is None):
            continue
        writer.append(parsed_results)


if __name__ == "__main__":

    tweet_count = 0

    total_tweets = 231841790
    tweet_buffer = total_tweets/15

    tweet_data_file = "YOUR TWEET FILE"
    output_file = "OUTPUT FILE"

    tag_array_header = get_tags()

    writer = CSVQueueWriter("test", output_file, 200000, True)
    writer.append(["tid", "uid", "screen_name", "created_at",
                   "text"] + tag_array_header)

    with open(tweet_data_file, mode="r", encoding="utf-8") as f:
        tweets = csv.DictReader(f)
        tweet_list = []
        for t in tqdm(tweets, total=total_tweets):
            tweet_list.append(t)
            tweet_count += 1

            if (tweet_count > tweet_buffer):
                parse_tweets(tweet_list, writer)
                writer.flush()
                tweet_count = 0
                tweet_list = []

        parse_tweets(tweet_list, writer)
    writer.flush()

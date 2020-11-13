import os
import csv
import json
from collections import OrderedDict

if __name__ == '__main__':
    input_file_name = "/Users/avinash/Downloads/1Jan2017-30Oct2020.json"
    output_file_name = "/Users/avinash/Downloads/1Jan2017-30Oct2020.txt"
    separator = " === "
    columns_needed = ["create_at", "text", "favorite_count", "retweet_count", "is_retweet", "screen_name",
                      "location", "description", "followers_count", "friends_count", "country", "stateName",
                      "countyName", "cityName"]
    with open(input_file_name, "r") as read_file, open(output_file_name, "w") as write_file:
        header = ""
        for col in columns_needed:
            header += col
            header += separator
        write_file.write(header+"\n")

        line = read_file.readline()
        while line:
            output_line = ""
            for col in columns_needed:
                pos_start = line.find(col)
                if pos_start != -1:
                    pos_end = line.find(", \"", pos_start)
                    substring = line[pos_start + len(col) + 2:pos_end]
                    output_line += substring
                output_line += separator
            write_file.write(output_line + "\n")
            line = read_file.readline()
        read_file.close()
        write_file.close()

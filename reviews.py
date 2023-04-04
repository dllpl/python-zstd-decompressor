
import gzip
import json

with gzip.open('feed_ru.json.gz', 'r') as fin:
    i = 0
    for line in fin:
        raw_data = line.decode("utf-8", 'ignore')
        line = '{' + raw_data[:-2] + '}'
        asda = json.loads(line)
        print(asda, '\n')
        # lines = raw_data.split("\n")
        # lines = json.loads(raw_data)
        # print(lines, '\n')
        # if (i == 50):
        #     exit()
        # else:
        #     i += 1

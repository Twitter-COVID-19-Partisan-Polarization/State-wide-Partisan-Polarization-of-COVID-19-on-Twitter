# State-wide Partisan Polarization of COVID-19 on Twitter

## COVID-19 Tweet Topic Classification

Using keywords found in previous studies cited in [topic and sentiment keywords](./COVID-19_Topic_And_Sentiment_Keywords.pdf), we found an initial set of COVID-19 tweets. From this set of tweets, we analyzed the frequency of all hashtags and supplemented the list of keywords found in our dataset that were also related to COVID-19. In this study, we discarded the stance and only used the topic. We also introduced the `Conspiracy` subtopic. Therefore, each keyword represented a topic from `Mask`, `Lockdown`, `Vaccine` or `Miscellaneou` and could be tagged for `Conspiracy`. The following [COVID-19 Keyword Tag Array](./covid-19_keywords_tag_array.csv) is thus constructed, where `1` represents the existence of the topic / subtopic.

### Scaling Keyword Based Classification

[COVID-19 Keyword Tag Array](./covid-19_keywords_tag_array.csv) is created in a way to represent a bit array. Each row represents a keyword and the topics and subtopics they can signify. For example, a `10001` signify `Mask` and `Conspiracy` topic. Technically, you could have `11111` which signifies all topic and subtopic. [Tweet Tagger](./scripts/tag_tweets.py) is sample script that shows an optimized way to retrieve the total matches per topic per tweet through the following ways

1. Squashing Regexes: Each keyword has a bit array. Instead of creating a regex per keyword, we can group all keywords that have the same bit array, and create one singular complex regex. As regexes are already highly optimized, multiple singular regexes are slower than using one complex regex. An example new regex is `maskdontwork|Talesoftheunmaskedpatriot`. This squashing decreases the extrapolated runtime from 18 to 2 days.

2. Precompiling Regexes & Multi-processing: Instead of creating the regex each time for each tweet (in this case 387M), we can precompile and use the instance before the loop. Due to the GIL, we use multi-processing instead of multi-threaded.

3. Memory Efficient: Instead of reading the file all at once, we can read the file in chunks. In the script, we read the file in 10 chunks. After processing each chunk, we write out the whole result.

#yelpcrawler
A python multithreaded crawler to scrape yelp listings built on a weekend as a fun programming exercise.

##Prerequisite packages

    sudo easy_install httplib2 beautifulsoup4

--OR--

    sudo pip install httplib2 beautifulsoup4

##Usage

    python crawler.py --crawlspeed=10 --verbose=no

- **verbose:** yes/no. Set to yes to see detailed logs
- **crawlspeed:** Higher the number, faster the crawl.

**Note:**  
- Recommended crawlspeed value is to start at 10. IP might get blocked after about 15 mins depending on your IP address. Use high rates only if you plan to run for a very short duration or if you are whitelisted. You might have to move to a new machine and lower the number if the IP gets blocked.
- If you want to run for a longer duration without getting blocked, set crawlspeed to 1. Crawl will be slowwwww.
- This is usually not a problem when you crawl a large number of domains since req/sec/domain can be managed to be low, but since we are just crawling yelp, it might become necessary to throttle requests


##Output:

* **Crawl Stats:** *REPORTS/crawl_stats.tsv*
* **Crawl Report:** *REPORTS/crawled_urls.tsv*
* **Downloaded HTML:** *CRAWLED_DOCS/*

##Kill Script:

    CTRL + Z
    kill %1

##Crawl Strategy:

1. Generate deep seed urls by querying yelp for categories combined with zip codes.
2. From the seed pages, discover and download listing pages and follow links that lead to listings. Discard all other links.
3. Avoid visited links for performance and canonicalize urls to be more efficient with de-dup. Content level dedup not yet implemented.

##Results:

On EC2 medium sized instance, single machine:
- crawlspeed=3 ( 3 threads ), the average performance was as below:
    - **Throughput:** 290 listing pages dowloaded per min
    - **Focussed Crawl Efficiency:** 70 % ( Listing pages downloaded / Total pages crawled )
- crawlspeed=10 ( 10 threads ), the average performance was as below:
    - **Throughput:** 610 listing pages dowloaded per min
    - **Focussed Crawl Efficiency:** 70 % ( Listing pages downloaded / Total pages crawled )

Higher throughput is acheivable with higher number of threads

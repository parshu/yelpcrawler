yelpcrawler
===========

A python multithreaded crawler to scrape yelp listings built on a weekend as a fun programming exercise.

Prerequisite packages:
=============

    sudo easy_install httplib2 beautifulsoup4

--OR--

    sudo pip install httplib2 beautifulsoup4

Usage:
=======

    python crawler.py --crawlspeed=10 --verbose=no

- verbose: yes/no. Set to yes to see detailed logs
- crawlspeed: Higher the number, faster the crawl.

Note: Yelp may block IP at high crawl frequencies. Set to 10 for moderate speed. 
      You might have to move to a new machine and lower the number if the IP gets blocked.

Output:
=========
* Crawl Stats: REPORTS/crawl_stats.tsv
* Crawl Report: REPORTS/crawled_urls.tsv
* Downloaded HTML: CRAWLED_DOCS/

Kill Script:
==========

    CTRL + Z
    kill %1


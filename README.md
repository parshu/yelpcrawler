yelpcrawler
===========

A python multithreaded crawler to scrape yelp listings built on a weekend as a fun programming exercise.

Prerequisite packages:
=============

sudo easy_install httplib2 -or- sudo pip install httplib2

sudo easy_install beautifulsoup4 -or- sudo pip install beautifulsoup4

Usage:
=======
python crawler.py --crawlspeed=10

    crawlspeed: higher the number, faster the crawl

Note: Yelp may block IP at high crawl frequencies. 
      Keep at 1 for long duration crawls.
      Set to 10 or more for high speed crawl at the risk of being blocked

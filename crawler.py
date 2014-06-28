''' ------------------------------------------------------
++Yelp Listing Crawler++
Usage: python crawler.py --crawlspeed=10
       crawlspeed: higher the number, faster the crawl
Note: Yelp may block IP at high crawl frequencies. 
      Keep at 1 for long duration crawls.
      Set to 10 or more for high speed crawl
----------------------------------------------------------'''

import threading, urllib, urlparse
import sys
import httplib2
import urllib2
from bs4 import BeautifulSoup, SoupStrainer
import hashlib
import os
import time

# Configs
YELP_URL_TEMPLATE = "http://www.yelp.com/search?find_desc=%s&find_loc=%s&start=%s"
YELP_CATEGORIES_FILE = "categories.txt"
ZIP_CODES_FILE = "zipcodes.txt"
OUTPUT_DIR = 'CRAWLED_DOCS'
REPORTS_DIR = 'REPORTS'
USER_AGENT = 'Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)'
THREADS = 10

# Global URL hashmap to avoid fetching duplicate urls. Simple implementation is not content based
globalURLs = {}


class CrawlerThread(threading.Thread):
	global globalURLs
	
	def __init__(self, binarySemaphore, url, startIndex):
		self.binarySemaphore = binarySemaphore
		self.url = url
		self.startIndex = startIndex
		self.threadId = hash(self)
		threading.Thread.__init__(self)

	# identify whether a link points to a yelp listing
	def is_listing_link(self, link):
		if (link.find('http://www.yelp.com/biz/') == 0):
			return True
		return False

	# identify whether the link is the 'next' navigation in yelp srp
	def is_next_page(self, link, startIndex):
		params = urlparse.parse_qs(urlparse.urlparse(link).query)
		if(params.has_key('start')):		
			if (params.get('start',['-100'])[0] == str((startIndex + 10))):
				return True
		return False
		
	# downloads listing html into local disk and adds entry to report file		
	def download_listing(self, link):
		digest = hashlib.sha224(link).hexdigest()
		if(not globalURLs.has_key(digest)):
			globalURLs[digest] = link
			reports_fp.write(digest + '\t' + link + '\n') # add entry to reports file
			print "Downloading: " + link
			content = self.fetch_content(link)
			f = open(OUTPUT_DIR + '/' + digest + '.html', 'w')
			f.write(content) # dump html to disk, filename = <hash of url>.html
			f.close()
		
		pass
	
	def fetch_content(self, url):
		req = urllib2.Request(url, headers={ 'User-Agent': USER_AGENT })
		content = urllib2.urlopen(req).read()
		return content
		
	def run(self):

		#Fetch the page				
		soup = None
		try:
			content = self.fetch_content(self.url)
			soup = BeautifulSoup(content)
			print "Fetching: " + self.url
		except urllib2.HTTPError, e:
			print >> sys.stderr, 'HTTPError ' + str(e.code) + ': ' + self.url
		
		if(soup is not None):
			self.binarySemaphore.acquire() 
			nextUrl = None # There is only one 'next' navigation link per srp page
			
			for linkobj in soup.findAll('a',href=True):
				link = linkobj['href']		
				link = urlparse.urljoin(self.url, link)
				
				#download all listing pages
				if self.is_listing_link(link):
					try:
						self.download_listing(link)
					except urllib2.HTTPError, e:
						print >> sys.stderr, 'HTTPError ' + str(e.code) + ': ' + self.url
				#follow the next link in srp ( add to queue )
				elif self.is_next_page(link, self.startIndex):
					if nextUrl is None:
						nextUrl = link
							
			self.binarySemaphore.release()	     
			if nextUrl is not None:
				CrawlerThread(binarySemaphore, nextUrl, self.startIndex + 10).start()

# Argument parsing for crawl speed		
argError = False		
if(len(sys.argv) == 2):
	arg = sys.argv[1]
	try:
		if(arg.split('=')[0] == '--crawlspeed'):
			THREADS = int(arg.split('=')[1])
		else:
			argError = True
	except:
		argError = True
else:
	argError = True

if argError:
	print "Improper arguments: python crawler.py --crawlspeed=10"
	sys.exit(1)
	  
	  
# Main function
if __name__ == "__main__":   
	
	# Initializations
	try:
		os.remove(REPORTS_DIR + '/crawled_urls.tsv')
	except:
		pass
	if not os.path.exists(OUTPUT_DIR):
		os.makedirs(OUTPUT_DIR)
	if not os.path.exists(REPORTS_DIR):
		os.makedirs(REPORTS_DIR)
	reports_fp = open(REPORTS_DIR + '/crawled_urls.tsv', 'a')


	binarySemaphore = threading.Semaphore(THREADS)

	# Crawl Strategy: 
	# 1. Generate deep links by querying yelp for categories combined with zip codes 
	# 2. Discover and download only listing pages
	# 3. Deep crawl srp pages by following pagination links
	# 4. Avoid visited links

	categories = []
	for category in open(YELP_CATEGORIES_FILE):
		categories.append(category.strip())
		
	for zip in open(ZIP_CODES_FILE):
		for category in categories:
			category = urllib.quote_plus(category)
			url = YELP_URL_TEMPLATE % (category, zip.strip(), '0')
			CrawlerThread(binarySemaphore, url, 0).start()
	

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
YELP_CATEGORIES_FILE = "categories_test.txt"
ZIP_CODES_FILE = "zipcodes_test.txt"
OUTPUT_DIR = 'CRAWLED_DOCS'
REPORTS_DIR = 'REPORTS'
USER_AGENT = 'Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)'
THREADS = 10

# Global URL hashmap to avoid fetching duplicate urls. Simple implementation is not content based
downloadedURLs = {}
visitedLinks = {}

def was_visited(link):
	if(visitedLinks.has_key(link)):
		return True
	visitedLinks[link] = True
	return False

class CrawlerThread(threading.Thread):
	global downloadedURLs
	global visitedLinks
	
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


	# canonicalizes links to a standard format with only desired params to avoid duplicates more efficiently
	def canonicalize(self, link):
		url_p = urlparse.urlparse(link)
		base = url_p.scheme + '://' + url_p.netloc + url_p.path
		params = urlparse.parse_qs(urlparse.urlparse(link).query)
		param_str = ''
		
		# These are the only 3 parameters that we are interested for yelp's focussed crawl
		if(params.has_key('find_desc')):
			param_str = param_str + 'find_desc=' + urllib.quote_plus(params['find_desc'][0]) + '&'
		if(params.has_key('find_loc')):
			param_str = param_str + 'find_loc=' + urllib.quote_plus(params['find_loc'][0]) + '&'
		if(params.has_key('start')):
			if(params['start'][0] != '0'):
				param_str = param_str + 'start=' + params['start'][0] + '&'
		
		link = base + '?' + param_str.rstrip('&')
		return link
		
		
		
		
		

	# identify whether the link is the 'next' navigation in yelp srp
	def follow_link(self, link, startIndex):
		
		#params = urlparse.parse_qs(urlparse.urlparse(link).query)
		if( (link.find('http://www.yelp.com/search?') == 0) ):	
			
			# and params.has_key('start')
			#if (params.get('start',['-100'])[0] == str((startIndex + 10))):
			return True
		return False
		
	# downloads listing html into local disk and adds entry to report file		
	def download_listing(self, link):
		digest = hashlib.sha224(link).hexdigest()
		if(not downloadedURLs.has_key(digest)):
			downloadedURLs[digest] = link
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
			crawlLinks = [] # Links to follow on the page
			
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
				elif self.follow_link(link, self.startIndex):
					link = self.canonicalize(link)
					crawlLinks.append(link)
				
							
			self.binarySemaphore.release()	     
			for link in crawlLinks:
				if(not was_visited(link)):
					CrawlerThread(binarySemaphore, link, self.startIndex + 10).start()

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
	
	print "Crawl Report: " + REPORTS_DIR + '/crawled_urls.tsv'
	print "Downloaded HTML: " + OUTPUT_DIR + '/'
	print "-------------------------------------------------"
	
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
	# 1. Generate deep seed urls by querying yelp for categories combined with zip codes 
	# 2. Discover and download only listing pages
	# 3. Deep crawl srp pages by discovering and crawling only srp and pagination links ( that lead to listings ).
	# 4. Avoid visited links for performance.
	# 5. Canonicalize urls to be more efficient with de-dup

	categories = []
	for category in open(YELP_CATEGORIES_FILE):
		categories.append(category.strip())
		
	for zip in open(ZIP_CODES_FILE):
		for category in categories:
			category = urllib.quote_plus(category)
			url = YELP_URL_TEMPLATE % (category, zip.strip(), '0')
			if(not was_visited(url)):
				CrawlerThread(binarySemaphore, url, 0).start()
	
	
	
''' ------------------------------------------------------
++Yelp Listing Crawler++
Usage: python crawler.py --crawlspeed=10
       crawlspeed: higher the number, faster the crawl
Note: Yelp may block IP at high crawl frequencies. 
      Set at 10 and reduce if blocked. 
----------------------------------------------------------'''

import threading, urllib, urlparse
import sys
import httplib2
import urllib2
from bs4 import BeautifulSoup, SoupStrainer
import hashlib
import os
import time
import datetime

# Configs
YELP_URL_TEMPLATE = "http://www.yelp.com/search?find_desc=%s&find_loc=%s&start=%s"
YELP_CATEGORIES_FILE = "categories.txt"
ZIP_CODES_FILE = "zipcodes.txt"
OUTPUT_DIR = 'CRAWLED_DOCS'
REPORTS_DIR = 'REPORTS'
USER_AGENT = 'Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)'
THREADS = 10
VERBOSE = 'no'

# Global URL hashmap to avoid fetching duplicate urls. Simple implementation is not content based
downloadedURLs = {}
visitedLinks = {}
stats = {
	'listingsDownloaded' : 0,
	'pagesCrawled' : 0,
	'pagesRejected' : 0,
	'startTime': datetime.datetime.utcnow()
}

# Utility function to check if a link was already visited before. Used after canonicalization
def was_visited(link):
	if(visitedLinks.has_key(link)):
		return True
	visitedLinks[link] = True
	return False

class CrawlerInstance(threading.Thread):
	global downloadedURLs
	global visitedLinks
	global stats
	
	
	def __init__(self, bSemaphore, url):
		self.bSemaphore = bSemaphore
		self.url = url
		self.tid = hash(self)
		threading.Thread.__init__(self)

	# Identify whether a link points to a yelp listing
	def is_listing_link(self, link):
		if (link.find('http://www.yelp.com/biz/') == 0):
			return True
		return False


	# Canonicalizes links to a standard format with only desired params to avoid duplicates more efficiently
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
	

	# Decide whether a link is worth following. SRP pages always lead to listing pages
	def follow_link(self, link):		
		if( (link.find('http://www.yelp.com/search?') == 0) ):	
			return True
		return False
		
	# Downloads listing html to local disk and adds entry to report file		
	def download_listing(self, link):
		digest = hashlib.sha224(link).hexdigest()
		if(not downloadedURLs.has_key(digest)):
			downloadedURLs[digest] = link
			reports_fp.write(digest + '\t' + link + '\n') # add entry to reports file
			if(VERBOSE == 'yes'):
				print "Downloading: " + link
			try:
				content = self.fetch_content(link, 'listing')
				f = open(OUTPUT_DIR + '/' + digest + '.html', 'w')
				f.write(content) # dump html to disk, filename = <hash of url>.html
				f.close()	
			except:
				# Usually happens when Yelp block the IP for frequent requests.
				print >> sys.stderr, 'HTTPError: ' + self.url	
	
	# Utility function to fetch page content & update crawl stats
	def fetch_content(self, url, type):
		req = urllib2.Request(url, headers={ 'User-Agent': USER_AGENT })
		content = urllib2.urlopen(req).read()
		
		if(type == 'listing'):
			stats['listingsDownloaded'] = stats['listingsDownloaded'] + 1
			if( (stats['listingsDownloaded'] % 10) == 0):
					
				report_str = 'Downloaded\t%d\nCrawled/Visited\t%d\nRejected\t%d\nFocus Crawl Efficiency\t%.2f %%\nThroughput\t%d pages/min\n' % (stats['listingsDownloaded'],  stats['pagesCrawled'],  stats['pagesRejected'], stats['listingsDownloaded'] * 100.0 / stats['pagesCrawled'] ,  int((stats['pagesCrawled'] * 60.0) / (datetime.datetime.utcnow() - stats['startTime']).seconds) )
				
				# Log stats once in a while
				print report_str.replace('\t', ':').replace('\n',', ')
				
				# Dump crawl stats once in a while
				if( (stats['listingsDownloaded'] % 50) == 0):
					tdelta = (datetime.datetime.utcnow() - stats['startTime'])
					report_str = report_str + "Run Time\t%d:%d:%d\n" % (tdelta.seconds/3600, tdelta.seconds/60, tdelta.seconds)
					reports_fp = open(REPORTS_DIR + '/crawl_stats.tsv', 'w')
					reports_fp.write(report_str)
					reports_fp.close()
				
		stats['pagesCrawled'] = stats['pagesCrawled'] + 1
		
		return content
		
	def run(self):

		#Fetch the page				
		soup = None
		try:
			content = self.fetch_content(self.url, 'page')
			soup = BeautifulSoup(content)
			if(VERBOSE == 'yes'):
				print "Fetching: " + self.url
		except:
			# Usually happens when Yelp block the IP for frequent requests.
			print >> sys.stderr, 'HTTPError: ' + self.url
		
		if(soup is not None):
			self.bSemaphore.acquire() 
			crawlLinks = [] # Links to follow on the page
			
			for linkobj in soup.findAll('a',href=True):
				link = linkobj['href']		
				link = urlparse.urljoin(self.url, link)

				# Download all listing pages
				if self.is_listing_link(link):
					try:
						self.download_listing(link)
					except urllib2.HTTPError, e:
						print >> sys.stderr, 'HTTPError ' + str(e.code) + ': ' + self.url
				# Follow links that will lead to listing pages ( add to queue )
				elif self.follow_link(link):
					# Canonicalize links since many links can point to same page.
					# Not doing content level deduping yet
					link = self.canonicalize(link)
					crawlLinks.append(link)
				else:
					# These are links that don't directly lead to listing pages
					stats['pagesRejected'] = stats['pagesRejected'] + 1
				
			self.bSemaphore.release()	     
			for link in crawlLinks:
				# Only visit links that have not been visited before
				if(not was_visited(link)):
					time.sleep(1) # Throttling creation of threads to prevent too many connection requests to yelp.com
					CrawlerInstance(bSemaphore, link).start()

# Argument parsing for crawl speed		
argError = False		
if(len(sys.argv) == 3):
	arg = sys.argv[1]
	try:
		if(arg.split('=')[0] == '--crawlspeed'):
			THREADS = int(arg.split('=')[1])
		else:
			argError = True
	except:
		argError = True
	arg = sys.argv[2]
	try:
		if(arg.split('=')[0] == '--verbose'):
			VERBOSE = arg.split('=')[1]
		else:
			argError = True
	except:
		argError = True
else:
	argError = True

if argError:
	print "Improper arguments: python crawler.py --crawlspeed=10 --verbose=no"
	sys.exit(1)
	  
	  
# Main function
if __name__ == "__main__":   
	print "-------------------------------------------------"
	print "Crawl Report: " + REPORTS_DIR + '/crawled_urls.tsv'
	print "Downloaded HTML: " + OUTPUT_DIR + '/'
	print "Crawl Stats: " + REPORTS_DIR + '/crawl_stats.tsv'
	print "-------------------------------------------------"
	print "Generating deep seed URLs..."
	
	# Initializations
	try:
		os.remove(REPORTS_DIR + '/crawled_urls.tsv')
		os.remove(REPORTS_DIR + '/crawl_stats.tsv')
	except:
		pass
	if not os.path.exists(OUTPUT_DIR):
		os.makedirs(OUTPUT_DIR)
	if not os.path.exists(REPORTS_DIR):
		os.makedirs(REPORTS_DIR)
	reports_fp = open(REPORTS_DIR + '/crawled_urls.tsv', 'a')
	

	bSemaphore = threading.Semaphore(THREADS)

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
				time.sleep(1) # Throttling creation of threads to prevent too many connection requests to yelp.com
				CrawlerInstance(bSemaphore, url).start()
	
	
	

'''
   This is a "real" version of fake_crawler, which fetches real pages from
   the internet.  It is a manifistation of PFcrawler.py, which was the original
   sequential version.

   It needs two functions:

  (1)  [timestamp, canonical_url, page_contents] = get_webpage(url)

  (2)  links_to_follow = process_webpage(num_page, timestamp, 
                            url, canonical_url, 
                            page_contents, links_already_dispatched):

'''
import time, random
import socket  #for setting timeout for fecthing webpages
from thread_pool import thread_pool
from datetime import datetime
import hashlib
import sys
import PCcrawler
import readwg
from parallel_crawler import parallel_crawler



links_dispatched = set([])
hash_codes_visited = set([])
url_matching_pattern = ''
page_num  = 0
filestream = sys.stdout


def process_webpage(url, canonical_url, page_contents):
    global links_dispatched
    global hash_codes_visited
    global page_num

    links_dispatched.add(url)  # needed? url came from links_dispatched...
    if (url != canonical_url):
        links_dispatched.add(canonical_url)

    page_num += 1

    links_to_follow = PCcrawler.modular_process_webpage(page_num, \
          url, canonical_url, page_contents, links_dispatched, \
          hash_codes_visited, url_matching_pattern, filestream)

    new_links_to_follow = set([]) 
    for link in links_to_follow:
        if link not in links_dispatched:
            new_links_to_follow.add(link)
            links_dispatched.add(link)
            
    return new_links_to_follow
 

def main():

   global url_matching_pattern
   global links_dispatched
   global hash_codes_visited


   timeout = 10  # 10-second timeout for fetching URLs
   socket.setdefaulttimeout(timeout)

   # NUM_THREADS = 128
   # NUM_THREADS = 32
   # NUM_THREADS = 8

   NUM_THREADS = 2
   
 
   # read wg files,figure out url_matching_pattern, and max_num_pages, if needed
   #
   if len(sys.argv) <= 2  :
     print "usage is domain-pattern seed-url  [max-num-pages-visited] "
     print "     -w  domain-pattern"
     print "              | "
     print "              ^ "
     print " Ex:  nist.gov http://math.nist.gov 100 "
     print "    -w means to continue from a webcrawl dump  (fed into stdin)"
     print " "
 
     sys.exit(2)
 

   max_pages_to_visit = 0     #if 0, then there is no limit
 

   seed_urls = set([])

   if sys.argv[1] == "-w":    #sart from a previous crawl
      readwg.process_wg_file(sys.stdin, links_dispatched, \
                     hash_codes_visited, seed_urls )
      url_matching_pattern = sys.argv[2]
      if len(sys.argv) > 3 :
         max_pages_to_visit = int(sys.argv[3])
   else :
      url_matching_pattern = sys.argv[1]
      starting_url = sys.argv[2]
      seed_urls.add(starting_url)
      if len(sys.argv) > 3 :
         max_pages_to_visit = int(sys.argv[3])
  
   print "#!#  domain pattern: ", url_matching_pattern
   print " "
 
  
   links_dispatched  = seed_urls

   try:

      C = parallel_crawler(NUM_THREADS, process_webpage, seed_urls, \
        max_pages_to_visit)

   except (KeyboardInterrupt, SystemExit):

      #C.terminate()
      print "Ctrl-C hit: exiting... "
      print ""
      exit

   print "[-- DONE --]"
   print "num_pages:", C.num_pages_crawled()
   print ""
   print "Active urls being fetched: "
   for a in C.active_urls_being_fetched():
      print a


'''
   frontier = C.frontier()
   print "frontier has ", len(frontier), "element(s)"
   for link in frontier:
      print "frontier: ", link

   PCcrawler.print_frontier(sys.stdout, C.frontier())
'''




if __name__ == "__main__":
   main()

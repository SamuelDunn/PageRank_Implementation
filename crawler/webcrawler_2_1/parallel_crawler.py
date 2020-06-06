
import PCcrawler
from thread_pool import thread_pool




class parallel_crawler(object):


  def __init__(self, num_threads, process_webpage_func, seed_urls, \
                  max_num_pages_to_fetch):


      self.num_pages_requested = 0
      P = thread_pool(num_threads, PCcrawler.get_webpage, queue_size=0)
      self.P =  P;
    
    
      # feed seed_urls
      #
      for s in seed_urls:
        if (self.num_pages_requested < max_num_pages_to_fetch):
            self.P.eval(s)
            self.num_pages_requested += 1
        else:
            break
    
    
      while P.num_pending_results() > 0 :
    
        [url,[timestamp, canonical_url, page_contents]] = P.result()
        links_to_follow = process_webpage_func(url,canonical_url,page_contents)
        for link in links_to_follow:
          if (self.num_pages_requested < max_num_pages_to_fetch):
              P.eval(link)
              self.num_pages_requested +=1
          else:
            break

      #  print out remaining pages without generating any more request
    
      while P.num_pending_results() > 0 :
        [url,[timestamp, canonical_url, page_contents]] = P.result()
        links_to_follow = process_webpage_func(url,canonical_url,page_contents)

    
      # Now frontier must be empty
      P.terminate()



  def num_pages_crawled(self):
      return self.num_pages_requested

  def active_urls_being_fetched(self):
      return self.P.current_args()

  # this is meant to be called after all threads in P have stopped.
  #
  def frontier(self):
    frontier_links = set([])
    while not self.P.Qin.empty():
        frontier_links.add(self.P.Qin.get_nowait()[1])
    return frontier_links


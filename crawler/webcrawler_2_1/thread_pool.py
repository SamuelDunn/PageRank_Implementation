from threading import Thread
import Queue, time, sys


#STOP_COMMAND = '!stop!'
#RUN_COMMAND  = '!process!'

STOP_COMMAND = 'S'
RUN_COMMAND  = 'P'

class parallel_function(Thread):
 
  waiting = False

  def waiting():
    return self.waiting

  def report_error(self, Qerr):
    error_msg = sys.exc_info()[:2]
    Qerr.put(error_msg)
    print  error_msg
      
  def __init__(self, thread_id, func, Qin, Qout, Qerr, debug_print):
    Thread.__init__(self)
    self.tid = thread_id
    self.func = func
    self.Qin = Qin
    self.Qout = Qout
    self.Qerr = Qerr
    self.debug_print  = debug_print
    self.arg = None

  def run( self ):
     #print "   Thread: entered parallel_function.run()"
     if (self.debug_print) : print "  ... Thread #", self.tid, "starting."
     while True:
          if (self.debug_print) : print "... Thread #", self.tid, \
               ": waiting for arguments ... "
          self.waiting = True
          command, arg = self.Qin.get()       # implicitly stops and waits
          self.arg = arg
          self.waiting = False
          #print "... Thread #", self.tid, ": got arg = ", str(arg)
          if command == STOP_COMMAND:
              if (self.debug_print) : print " ... thread_pool #", self.tid, \
                    "shutting down... "
              break
          try:
              # simulated work functionality of a worker thread
              if command ==  RUN_COMMAND:
                  if (self.debug_print) : print ' ... Thread #', self.tid,\
                       ' (', str(arg), \
                                    ') is now processing...'
                  #result = self.func(*args)
                  result = self.func(arg)
                  if (self.debug_print) : print ' ... Thread #', self.tid,\
                       ': f(',str(arg),')=',str(result)
                  if (self.debug_print) : print
              else:
                  raise ValueError, 'Unknown command %r' % command
          except:
              # unconditional except is right, since we report _all_ errors
              self.report_error(self.Qerr)
              if (self.debug_print) : print ' ... Thread #', self.tid, \
                   ': exception thrown.'
          else:
              self.Qout.put( (arg, result) )
              self.arg = None
              if (self.debug_print) : print ' ... Thread #', self.tid, \
                   ': f(',   str(arg),  ') on Qout.'

  def arg(self):
     return self.arg

#end of parrallel_function


class thread_pool(object):
 
  
  def __init__ (self, num_threads, func, queue_size=0, debug_print=False):
     if (queue_size == 0):
       self.Qout = Queue.Queue()
     else:
       self.Qout = Queue.Queue(num_threads*2)
     
     self.Qin = Queue.Queue()
     self.Qerr = Queue.Queue()
     self.Pool = []
     self.func = func
     self.num_threads = num_threads
     self.num_pending_results_ = 0
     #self.Thread_waiting = dict([])
     self.debug_print = debug_print

     if (self.debug_print) : print "Thead_Pool starting with ", num_threads, \
         " threads, q =", q_size

     for i in range(num_threads):
         new_thread = parallel_function(i, func, self.Qin, self.Qout, \
                self.Qerr, self.debug_print)
         # if (self.debug_print): print "... created thread_pool thread # ", i
         self.Pool.append(new_thread)
         new_thread.start()


  def current_args(self):
    args  = []
    for t in self.Pool:
        args.append(t.arg())
    return args
        

  # True only if *all* threads are waiting for new work
  #  and the input queue is empty.
  #
  def all_waiting(self) :
     for t in self.Pool:
        if (t.waiting == False):
           return False
     return True

  def put_on_work_queue(self, arg, command=RUN_COMMAND):
    ''' work requests are posted as (command, data) pairs to Qin '''
    self.Qin.put((command, arg))
    #print command, arg
  
  def eval(self, arg):
    self.num_pending_results_ += 1
    self.put_on_work_queue(arg)
  

  def result(self):
    self.num_pending_results_ -= 1
    return self.Qout.get()     # implicitly stops and waits


  def num_pending_results(self):
    return self.num_pending_results_

  def get_all_from_queue(self, Q):
    ''' generator to yield one after the others all items currently
        in the Queue Q, without any waiting
    '''
    try:
        while True:
            yield Q.get_nowait()
    except Queue.Empty:
        raise StopIteration

  def eval_queue_size(self):
      return self.Qin.qsize()

  def result_queue_size(self):
      return self.Qout.qsize()



  def show_all_results(self):
    for result in get_all_from_queue(self.Qout):
        print 'Result:', result
  def show_all_errors(self):
    for etyp, err in self.get_all_from_queue(self.Qerr):
        print 'Error:', etyp, err

  # this only makes sense if the input queue (Qin) is empty!
  #
  def terminate(self):
    # order is important: first, request all threads to stop...:
    for i in range(len(self.Pool)):
        self.put_on_work_queue(None, STOP_COMMAND)
    # ...then, wait for each of them to terminate:
    for existing_thread in self.Pool:
        existing_thread.join()
    # clean up the pool from now-unused thread objects
    del self.Pool[:]
    Running = False



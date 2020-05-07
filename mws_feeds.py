import argparse
import hashlib
import logging
import os
import sched
import threading
import time
from collections import namedtuple
from datetime import datetime, timedelta, timezone, tzinfo
from concurrent.futures import ThreadPoolExecutor
import mws
import pytz
import xmlschema
import errno
from detect_file_encoding import predict_encoding




# Flat File Price and Quantity Update Feed
# Enumeration value: _POST_FLAT_FILE_PRICEANDQUANTITYONLY_UPDATE_DATA_
MIN_MAX_TEMP = '_POST_FLAT_FILE_PRICEANDQUANTITYONLY_UPDATE_DATA_'
# Flat File Listings Feed
# Enumeration value: _POST_FLAT_FILE_LISTINGS_DATA_
LISTING_TEMP = '_POST_FLAT_FILE_LISTINGS_DATA_'

# Pricing Feed
# Enumeration value: _POST_PRODUCT_PRICING_DATA_
PRICING_FEED = '_POST_PRODUCT_PRICING_DATA_'
# Flat File FBA Create Removal Feed
REMOVAL_FEED = '_POST_FLAT_FILE_FBA_CREATE_REMOVAL_'

AUTOMATED_PRICING_FEED = '_MARS_AUTOMATE_PRICING_FEED_'


log_out = os.path.join(os.getenv('userprofile'),'/Documents/Logs')

def check_dir(path):
    if not os.path.exists(path):
        print('Creating Directory {}'.format(path))
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
check_dir(log_out)
LOG_FILE = os.path.normpath(os.path.join(log_out,'mws_feeds.log'))
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG,format='%(asctime)s %(message)s')
class FeedHandler():
    '''
    Implement the MWS Feeds class
    MWS FEEDS DOCUMENTATION
    http://docs.developer.amazonservices.com/en_UK/feeds/Feeds_Overview.html
    
    feed should be a full filepath
    feed_type needs to be one of the constant uppercase variables above
    logger has to implement the logging module, the log function is prefered
    '''
    def __init__(self, feed,feed_type,logger):
        enums = FeedTypeEnumeration()
        if not feed_type in enums.valid_enumerations.values():
            if feed_type in enums.valid_enumerations.keys():
                feed_type = enums[feed_type]
            else:
                print('invalid feed_type {}'.format(feed_type))
                return None
        # if not '_' in feed_type:
        #     return None
        content = "text/xml"
        
        if os.path.isfile(feed):
            ext = os.path.splitext(feed)
            if not ext[1] in ('.xml', '.xlsx'):
                content = 'text/plain'
        self.reports_api = mws.Feeds(access_key = os.environ['MWS_ACCESS_KEY'],
                                    secret_key = os.environ['MWS_SECRET_KEY'],
                                    account_id = os.environ['MWS_ACCOUNT_ID'],
                                    auth_token = os.environ['MWS_TOKEN'])
        self.marketplace_id = os.environ['MWS_MARKETPLACE_ID']
        self.file_name = os.path.basename(os.path.normpath(feed))
        submit_out = os.path.join(os.path.dirname(feed),'Submission Results')
        check_dir(submit_out)
        self.results_save_dir = submit_out
        # if feed_type == PROMO_TEMP:
        #     self.feed = open(feed,"r").read().encode('utf-8')
        # else:
        if 'xml' in content:
            self.feed = open(feed,"rb").read()#.encode('utf-8')
        else:    
            self.feed = open(feed,"r",encoding=predict_encoding(feed, n_lines=20)).read().encode('utf-8')
        # Do NOT need to hash the feed, but keeping for possible future needs
        # self.feed = hash_feed(feed)
        self.feed_type = feed_type
        self.content_type = content
        self.logger = logger

    def submit_feed(self):
        '''
        Returns a FeedSubmissionId , which you can use to periodically
        check the status of the feed using get_feed_submit_list()
        '''
        return self.reports_api.submit_feed(feed=self.feed,feed_type=self.feed_type,
            marketplaceids=self.marketplace_id,content_type=self.content_type)
    def get_feed_submit_list(self,feedids,max_count=None,
            feedtypes=None, processingstatuses=None, fromdate=None,
            todate=None, next_token=None):
        '''
        If Amazon MWS is still processing a request, the FeedProcessingStatus
        element of the get_feed_submit_list() operation returns a status
        of _IN_PROGRESS_. If the processing is complete, a status of _DONE_
        is returned.
        '''
        return self.reports_api.get_feed_submission_list(feedids,
                max_count=max_count,feedtypes=feedtypes,
                processingstatuses=processingstatuses, fromdate=fromdate,
                todate=todate, next_token=next_token)
    def cancel_submission(self,feedids=None, feedtypes=None, fromdate=None,
                            todate=None):
        '''
        Cancels one or more feed submissions and returns a count of the
        canceled feed submissions and the feed submission information.
        Note that if you do not specify a FeedSubmmissionId,
        all feed submissions are canceled. Information is returned
        for the first 100 feed submissions in the list.
        To return information for more than 100 canceled feed submissions,
        use the get_feed_submit_list() operation.
        If a feed has begun processing, it cannot be canceled.
        '''
        return self.cancel_feed_submission(feedids=feedids, feedtypes=feedtypes,
            fromdate=fromdate, todate=todate)
    def get_feed_result(self,feedid):
        '''
        Returns the feed processing report and MD5 hash if they do not match.
        You should discard the result and automatically retry the request
        for up to **three more times.** 
        '''
        return self.reports_api.get_feed_submission_result(feedid)
    def submit_feed_workflow(self,feedid=None):
        try:
            if not feedid:
                feedid = self.submit_feed().parsed.FeedSubmissionInfo.FeedSubmissionId
                print('feedid: ',feedid)
                self.logger.debug('Feed ID: {}'.format(feedid))
                time.sleep(60)
            submission_list = self.get_feed_submit_list(feedid).parsed.FeedSubmissionInfo.FeedProcessingStatus
            print(submission_list)
            self.logger.debug('Submission list Status: {}'.format(submission_list))
            # while submission_list == '_IN_PROGRESS_':
            while submission_list != '_DONE_':
                if submission_list in ('_CANCELLED_','_IN_SAFETY_NET_'):
                    raise SubmissionError("Feed submission result",submission_list)
                time.sleep(60)
                submission_list = self.get_feed_submit_list(feedid).parsed.FeedSubmissionInfo.FeedProcessingStatus
                print(submission_list)
                self.logger.debug('Submission list Status: {}'.format(submission_list))
            # if submission_list == '_DONE_':
            self.logger.debug('Submission Status: {} for Feed ID: {}'.format(submission_list,feedid))
            result = self.get_feed_result(feedid).parsed
            # print(result)
            if not self.save_feed_results(result,feedid):
                time.sleep(60)
                self.save_feed_results(self.get_feed_result(feedid).parsed)
                # if result:
                #     self.logger.debug("Saving results to: Submission result for ID {}.txt".format(feedid))
                #     if os.path.isdir(result_output):
                #         out = os.path.normpath(os.path.join(result_output,'Submission result for ID {}.txt'.format(feedid)))
                #     else:
                #         out = os.path.normpath('Submission result for ID {}.txt'.format(feedid))
                #     with open(out,'wb') as file:
                #         file.write(result.decode('ISO-8859-1').encode('utf-8'))
                #         print(result.decode('ISO-8859-1'))
                # self.logger.debug('Feed result: {} for Feed ID {}'.format(result,feedid))
                # return result
            self.email_feed_results(result,feedid)
        # except Exception as e:
        except KeyboardInterrupt:
            return self.cancel_feed_submission(feedid)
        except SubmissionError as e:
            self.logger.error('could not submit feed {}'.format(self.feed_type))
            self.logger.exception(str(e))
        except mws.MWSError as e:
            self.logger.error('could not get feed result {}'.format(self.feed_type))
            self.logger.exception(str(e))
            if feedid:
                submit_feed_workflow(feedid=feedid)
    def save_feed_results(self,result,feedid):
        if result:
            self.logger.debug("Saving results to: Submission result for ID {}.txt".format(feedid))
            if os.path.isdir(self.results_save_dir):
                out = os.path.normpath(os.path.join(self.results_save_dir,'Submission result for ID {}.txt'.format(feedid)))
            else:
                out = os.path.normpath('Submission result for ID {}.txt'.format(feedid))
            with open(out,'wb') as file:
                file.write(result.decode('ISO-8859-1').encode('utf-8'))
                print(result.decode('ISO-8859-1'))
            self.logger.debug('Feed result: {} for Feed ID {}'.format(result,feedid))
            return True
        self.logger.debug('Empty Feed result: for Feed ID {}'.format(feedid))
        return False
    def email_feed_results(self,result,feedid):
        if not 'robert' in os.getenv('username'):
            return False
        try:
            from my_email import EmailServer
            es = EmailServer()
            es.send_email(to=es.my_email,subject='Submission result for ID {}'.format(feedid),body='See attached submission results.\n\n',attachment={result:'ISO-8859-1'})
            return True
        except Exception as e:
            print(e)
            return False

def hash_feed(feed):
    '''
    Calculate md5 hash for a feed (text file)
    '''
    # BUF_SIZE is totally arbitrary, change for your app!
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
    md5 = hashlib.md5()
    with open(feed, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)
    return md5.digest()
def log():
    logger = logging.getLogger('mws_feeds')
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(LOG_FILE)
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    # ch = logging.StreamHandler()
    ch = ClosingStreamHandler()
    ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger
class ClosingStreamHandler(logging.StreamHandler):
    def close(self):
        self.flush()
        super().close()
class SubmissionError(Exception):
    '''
    CRITICAL ERROR: _IN_SAFETY_NET_
    The request is being processed, but the system has determined that there is a potential error with the feed
    (for example, the request will remove all inventory from a seller's account.)
    An Amazon seller support associate will contact the seller to confirm whether the feed should be processed.
    
    _CANCELLED_: The request has been aborted due to a fatal error.
    '''
    def __init__(self, message, errors):

        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        self.errors = errors
class FeedTypeEnumeration(object):
    
    def __init__(self):
        # self.valid_enumerations = namedtuple('Enumerations', [MIN_MAX_TEMP, LISTING_TEMP,PRICING_FEED,REMOVAL_FEED])
        self.valid_enumerations = {'MIN_MAX_TEMP': '_POST_FLAT_FILE_PRICEANDQUANTITYONLY_UPDATE_DATA_',
                                    'LISTING_TEMP': '_POST_FLAT_FILE_LISTINGS_DATA_',
                                    'PRICING_FEED': '_POST_PRODUCT_PRICING_DATA_',
                                    'REMOVAL_FEED': '_POST_FLAT_FILE_FBA_CREATE_REMOVAL_',
                                    'AUTOMATED_PRICING_FEED': '_MARS_AUTOMATE_PRICING_FEED_'}
def new_loader_feed(path):
    '''
    Inventory Loader file works
    first_feed = os.path.join('G:\My Drive\GC Inventory Glory\Personal Work Folders\Robbie\Season Coding and Price Updating\F19','kulkea is worse than tnf F19.txt')
    feed = FeedHandler(feed=first_feed,feed_type=MIN_MAX_TEMP,logger=log())
    '''
    
    feed = FeedHandler(feed=path,feed_type=MIN_MAX_TEMP,logger=log())
    print('submit_feed_workflow()')
    feed.submit_feed_workflow()
    
def new_removal(path):
    feed = FeedHandler(feed=path,feed_type=REMOVAL_FEED,logger=log())
    print('submit_feed_workflow()')
    feed.submit_feed_workflow()
def new_xlsx_feed(path):
    feed = FeedHandler(feed=path,feed_type=LISTING_TEMP,logger=log())
    print('submit_feed_workflow()')
    feed.submit_feed_workflow()
def new_auto_feed(path):
    feed = FeedHandler(feed=path,feed_type=AUTOMATED_PRICING_FEED,logger=log())
    print('submit_feed_workflow()')
    feed.submit_feed_workflow()
def feed_finder(short_hand_feed_type):
    func_dict = {'xlsx':new_xlsx_feed,'loader':new_loader_feed,'rem':new_removal,'auto':new_auto_feed}
    if short_hand_feed_type in func_dict.keys():
        return func_dict[short_hand_feed_type]
    return None
class PriceSchema():
    
    def __init__(self):
        self.schema = xmlschema.XMLSchema('https://images-na.ssl-images-amazon.com/images/G/01/rainier/help/xsd/release_4_1/Price.xsd')
        # print(schema.types)
        # print(schema.attributes)

class MWSUploadScheduler(ThreadPoolExecutor):
    '''
    https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor
    See Future Objects for additional methods
    '''
    def __init__(self,logger=log()):
        ThreadPoolExecutor.__init__(self,max_workers=min(32,os.cpu_count() + 4),thread_name_prefix='Feed-')
        self.logger = logger
    def add_thread(self,mws_thread):
        if not isinstance(mws_thread,MWSUploadThread):
            raise TypeError("must pass in a MWSUploadThread class")
        self.submit(mws_thread.start_thread)
        self.logger.debug('submitted thread {}'.format(mws_thread.name))
    # def thread_status()
    def shutdown_scheduler(self):
        self.shutdown()
        self.cancel_mws_threads()
    def get_mws_threads(self):
        ts = active_threads()
        return [t for t in ts if isinstance(t,MWSUploadThread)]
    def cancel_mws_threads(self):
        mws_t = self.get_mws_threads()
        if mws_t:
            for t in mws_t:
                if t.active():
                    t.cancel_thread()
    def wait_for_threads(self):
        self.logger.debug('waiting for completion of {} threads'.format(active_threads(count_only=True) - 1))
        try:
            [t.join() for t in self.get_mws_threads()]
        except Exception as e:
            self.logger.exception(e)


class MWSUploadThread(threading.Timer):
    '''
    Schedules future uploads at the appropriate start_time
    https://docs.python.org/3/library/threading.html#threading.Thread
    '''
    def __init__(self,short_hand_feed_type,feed_path,start_time,logger=log(),thread_name='',pacific_tz=True,tz=''):
        if not tz:
            if pacific_tz:
                self.tz = 'US/Pacific'
            else:
                self.tz = 'US/Eastern'
        else:
            if tz in pytz.all_timezones:
                self.tz = tz
            else:
                self.tz = 'US/Pacific'
        now = pytz.timezone(self.tz).localize(datetime.now()) 
        self.logger = logger
        self.short_feed_type = short_hand_feed_type
        self.feed_path = feed_path
        self.feed_func = feed_finder(self.short_feed_type)
        # self.local_start_time = pytz.timezone('US/Pacific').localize(start_time)
        # self.start_time = start_time
        self.start_time = pytz.timezone(self.tz).localize(start_time)
        self.total_delay = (self.start_time - now).total_seconds() 
        # self.scheduler = sched.scheduler(time.time,time.sleep)
        # if self.total_delay > threading.TIMEOUT_MAX:
        #     self.delay = int(threading.TIMEOUT_MAX) - 1
        # else:
        #     self.delay = self.total_delay
        self.delay = self.total_delay
        # self = threading.Timer(self.delay,self.feed_func,args=[self.feed_path]).start()
        # threading.Thread.__init__(threading.Timer(self.delay,self.feed_func,args=[self.feed_path]))
        threading.Timer.__init__(self,self.delay,self.feed_func,args=[self.feed_path])
        self.time_remaining = self.countdown()
        # self.scheduled_start()
        self.set_thread_name(os.path.splitext(os.path.basename(self.feed_path))[0])

    def start_thread(self):
        '''
        convenience method for starting a new thread
        '''
        
        now = pytz.timezone(self.tz).localize(datetime.now())
        delay = (self.start_time - now).total_seconds() 
        if delay > threading.TIMEOUT_MAX:
            self.logger.debug('cannot start a new thread, start_time is too far in the future and would cause an OverflowError')
            print('cannot start a new thread, start_time is too far in the future and would cause an OverflowError. Retry in {}'.format(str(timedelta(seconds=delay - threading.TIMEOUT_MAX))))
            return None
        self.logger.debug('starting a new thread named {}'.format(self.getName()))
        self.logger.debug('thread {} is scheduled to upload {} as a {} at {}'.format(
            self.getName(),self.feed_path,self.short_feed_type,self.start_time))
        self.start()
        
    def cancel_thread(self):
        '''
        convenience method for cancelling a thread
        '''
        self.logger.info('cancelling thread {}'.format(self.getName()))
        self.cancel()
        # if not self.active():
        #     self.logger.debug('thread {} sucessfully cancelled'.format(self.getName()))
        while self.active():
            self.logger.warning('thread {} is still active and has not yet been cancelled, retrying in 5 seconds'.format(self.getName()))
            time.sleep(5)
        self.logger.info('thread {} sucessfully cancelled'.format(self.getName()))
    def active(self):
        '''
        convenience method for checking if a thread is alive
        '''
        return self.is_alive()
    def status(self):
        '''
        convenience method for logging thread status
        '''
        if self.active():
            self.logger.info('{} is active'.format(self.getName()))
            return True
        self.logger.info('{} is not active'.format(self.getName()))
        return False
    def set_thread_name(self,thread_name):
        '''
        convenience method for naming threads
        '''
        self.name = thread_name
    def scheduled_start(self):
        '''
        convenience method for scheduling thread start times
        if the total delay in seconds exceeds threading.TIMEOUT_MAX
        OverflowError is thrown
        '''
        if self.total_delay > threading.TIMEOUT_MAX:
            self.scheduler.enter(self.total_delay - self.delay,1,self.start_thread)#self.native_id
        else:
            self.scheduler.enter(1,1,self.start_thread)#self.native_id
        self.scheduler.run()
    def countdown(self):
        delta = (self.start_time - pytz.timezone(self.tz).localize(datetime.now()))
        secs = delta.seconds
        hours, remainder = divmod(secs, 3600)
        minutes, seconds = divmod(remainder, 60)
        # print('time remaining until upload: {:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds)))
        self.logger.info('time remaining until upload: {:02} days {:02}:{:02}:{:02}'.format(delta.days,int(hours), int(minutes), int(seconds)))
        time_remaining = '{:02}:{:02}:{:02}:{:02}'.format(delta.days,int(hours), int(minutes), int(seconds))
        self.time_remaining = time_remaining
        return time_remaining
    
def active_threads(count_only=False):
    if count_only:
        return threading.active_count()
    return threading.enumerate()

def check_dir(path):
    if not os.path.exists(path):
        print('Creating Directory {}'.format(path))
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
def file_iterator(path,file_fix):
    files = glob.glob(path+file_fix)
    if not files:
        return "NO files match {}{}".format(path,file_fix)
    return [f for f in files if os.path.isfile(f) if os.path.splitext(f)[1] in ('csv','xlsx')]
def will_exceed_timeout_max(dt):
    delta = (dt - datetime.now()).total_seconds()
    if delta > threading.TIMEOUT_MAX:
        return True
    return False
def wait_duratiion_to_start_thread(dt):
    delta = (dt - datetime.now()).total_seconds()
    if delta > threading.TIMEOUT_MAX:
        return 'Wait {} to start thread'.format(str(timedelta(seconds=delta-threading.TIMEOUT_MAX)))
    return "Start thread"
def main_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-x','--xlsx', action='store',type=str)
    parser.add_argument('-l','--loader', action='store',type=str)
    parser.add_argument('-r','--rem', action='store',type=str)
    parser.add_argument('-a','--auto', action='store',type=str)
    args = parser.parse_args()
    return args
if __name__ == "__main__":
    # arg_dict = vars(args)
    arg_dict = vars(main_args())
    print(arg_dict)
    # func_dict = {'xlsx':new_xlsx_feed,'loader':new_loader_feed,'rem':new_removal}
    if arg_dict:
        for k,v in arg_dict.items():
            if v:
                # func_dict[k](v)
                feed_finder(k)(v)

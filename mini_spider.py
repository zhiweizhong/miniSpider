#!/usr/bin/python
#-*- coding: utf-8 -*-


################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
This module is used to grab the web pages that meet specific pattern.

Authors: zhongzhiwei(zhongzhiwei01@baidu.com)
Date:    2014/07/22 16:30
"""

    
import argparse
import codecs
import ConfigParser
import HTMLParser
import logging
import logging.handlers
import os
import Queue
import re
import sys
import string
import threading
import time
import urllib
import urllib2
import urlparse

import threadpool


def init_log(log_path, level=logging.INFO, when="D", backup=7,
    format="%(levelname)s: %(asctime)s: %(filename)s:%(lineno)d * %(thread)d %(message)s",
    datefmt="%m-%d %H:%M:%S"):
    """
    init_log - initialize log module

    Args:
        log_path    - Log file path prefix.
                      Log data will go to two files: log_path.log and log_path.log.wf
                      Any non-exist parent directories will be created automatically
        level       - msg above the level will be displayed
                      DEBUG < INFO < WARNING < ERROR < CRITICAL
                      the default value is logging.INFO
        when        - how to split the log file by time interval
                      'S' : Seconds
                      'M' : Minutes
                      'H' : Hours
                      'D' : Days
                      'W' : Week day
                      default value: 'D'
        format      - format of the log
                      fefault format:
                      %(levelname)s: %(asctime)s: %(filename)s:%(lineno)d * %(thread)d %(message)s
                      INFO: 12-09 18:02:42: log.py:40 * 139814749787872 HELLO WORLD
        backup      - how many backup file to keep
                      default value: 7

    Rauses:
        OSError: fail to create log directories
        IOError: fail to open log file
    """

    formatter = logging.Formatter(format, datefmt)
    logger = logging.getLogger()
    logger.setLevel(level)

    dir = os.path.dirname(log_path)
    if not os.path.isdir(dir):
        os.makedirs(dir)
    
    handler = logging.handlers.TimedRotatingFileHandler(log_path 
            + ".log", when=when, backupCount=backup)

    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class MyHtmlParser(HTMLParser.HTMLParser):
    """
    MyHtmlParser    - the class that parse the web pages
                      parse the url address in the web page, 
                      and save the url addresses in the list of the class

    Attributes:
        links: the list the save the url address in the web page.
    """

    def __init__(self):
        HTMLParser.HTMLParser.__init__(self)
        self.links = []

    def handle_starttag(self, tag, attrs):
        if (tag == "a" or tag == "src" or tag == "link" 
        or tag == "base" or tag == "script" or tag == "img"):
            if len(attrs) == 0:
                pass
            else:
                for (variable, value) in attrs:
                    if variable == "href" or variable == "src":
                        if value.find('javascript:location.href=') >= 0:
                            value=value.replace('javascript:location.href='
                                    , '').lstrip('"').rstrip('"')
                        self.links.append(value)


class Node(object):
    """
    Node    - define the queue node, include the url and depth.

    Attributes:
        url:The url address.
        depth:The depth of the url.
    """
    def __init__(self, url="", depth=0):
        self.url = url
        self.depth = depth


class MySpider(object):
    """
    MySpider    - grab the web page that meet specific pattern.

    Attributes:
        out_dir:the directory that save the web pages.
        max_depth:the deepest that the spider can grab.
        crawl_interval:the interval that grab the web pages.
        crawl_timeout:the timeout that grab the web pages.
        target_url:the url pattern that need to saved.
        url_addr:the root url address.
        dir:current directory of the program.
        current_depth:current depth of the root url address.
    """
    def __init__(self, configfile):
        self.configfile = configfile
        self.out_dir = ""
        self.max_depth = 0
        self.crawl_interval = 1
        self.crawl_timeout = 1
        self.target_url_reg = ""
        self.thread_count = 0
        self.seen_url = set()
        self.url_queue = Queue.Queue()
        self.dir = os.getcwd()
        self.current_depth = 0
        self.rlock = threading.RLock()
        self.event = threading.Event()
        self.html_parse = MyHtmlParser()
    
    def get_parameter(self):
        """
        get_parameter   - get the parameter from the config file.
        
        Args:
            configfile:The config file.
        """
        
        cf = ConfigParser.ConfigParser()
        try:
            cf.read(self.configfile)
            url_list_file = cf.get("spider", "url_list_file")
            self.out_dir = cf.get("spider", "output_directory")
            self.max_depth = cf.get("spider", "max_depth")
            self.crawl_interval = cf.get("spider", "crawl_interval")
            self.crawl_timeout = cf.get("spider", "crawl_timeout")
            self.target_url_reg = cf.get("spider", "target_url")
            self.thread_count = cf.get("spider", "thread_count")
            for line in open(cf.get("spider", "url_list_file"), "r"):
                if line.startswith('http'):
                    self.url_queue.put(Node(url=line.strip(' /\n\r')))
            self.event.set() 
        except (ConfigParser.NoSectionError, ConfigParser.MissingSectionHeaderError) as e:
            logging.error(e)
            return False
            sys.exit(1)
        else:
            if not os.path.exists(url_list_file):
                logging.error('url_list_file is not exist.')
                return False
                sys.exit(1)
            if not self.max_depth.isdigit():
                logging.error('max_depth is not digit.')
                return False
                sys.exit(1)
            if not self.crawl_interval.isdigit():
                logging.error('crawl_interval is not digit.')
                return False
                sys.exit(1)
            if not self.crawl_timeout.isdigit():
                logging.error('crawl_timeout is not digit.')
                return False
                sys.exit()
            if not self.thread_count.isdigit():
                logging.error('thread_count is not digit.')
                return False
                sys.exit(1)
        return True

    def download_page(self, url_addr):
        """
        download_page   - download the specified url address.

        Args:
            url_addr    - the specified url address that need download the web page.

        Returns:
            The web page will return back.
            If the url address can not be open, a False bool will return.

        Raises:
            urllib2.URLError:Faild to open the web page.
        """

        try:
            request = urllib2.Request(url_addr)
            request.add_header('User-agent', 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)')
            response = urllib2.urlopen(request, timeout=int(self.crawl_timeout))   
        except urllib2.URLError as e:
            print url_addr
            logging.error('[%s] occur error %s ', url_addr, str(e))
            return False
        else:
            old_page = response.read()
            regex = 'meta.*charset=(?P<charset>\w+)'
            pattern = re.compile(regex)
            match = pattern.search(old_page)
            if match:
                new_page = old_page.decode(match.group('charset')).encode('utf-8')
                return new_page
            else:
                return old_page
	    
    def get_urllist(self, base_url, url_page):
        """
        get_urllist    - Get all the url address in one web page.

        Args:
            base_url   - The root url address.
            url_page   - The web page that get the url list.
        """

        try:
            self.html_parse.feed(url_page)
        except HTMLParser.HTMLParseError as e:
            logging.error('[%s] web page occur error %s', base_url, str(e))
        else:
            self.html_parse.close()
            if len(self.html_parse.links) > 0:
                for url in self.html_parse.links:
                    if not url.startswith('http'):
                        addr = urlparse.urljoin(base_url.strip('/ '), url.strip('/ '))
                        yield addr
                    else:
                        yield addr.strip('/ ')
   
    def is_meet(self, url_addr):
        """
        is_meet    - Judge whether the web page meet the pattern.

        Args:
            url_addr    - The web address that need to checked.
        
        Returns:
            It will return True if the web page meet the pattern,
            else return False.
        """

        regex = re.compile(self.target_url_reg)
        is_meet = regex.match(url_addr)
        if is_meet:
            return True
        else:
            return False
     
    def save_page(self, curr_dir, url_addr, url_page):
        """
        save_page   - Save the specified web page in specified directory.

        Args:
            curr_dir:Current directory that the program in.
            url_addr:The specified web address.
        """
        
        new_dir = curr_dir + self.out_dir.lstrip('.')
        filename = urllib.quote_plus(url_addr)
        if not os.path.isdir(new_dir):
            os.mkdir(new_dir)
        self.rlock.acquire()
        f = open(os.path.join(new_dir, filename), 'w')
        f.write('%s' % url_page)
        f.close()
        self.rlock.release()
    
    def crawing(self, node):
        """
        crawing   - the BFS capture that grab the web pages.
                    search the web pages using the BFS, and save the web pages 
                    that meet the specific pattern.
        """
        logging.info('The url [%s], depth [%d ]start crawling ...', node.url, node.depth)
        url_page = self.download_page(node.url)
        if url_page is not False:
            if self.is_meet(node.url):   
                logging.info('The url [%s] is meet specific pattern.', node.url)
                self.save_page(self.dir, node.url, url_page)
            if self.get_urllist(node.url, url_page) is None:
                logging.info('the url page [%s] is the bottom', node.url)
            else:
                self.rlock.acquire()
                for next_url in self.get_urllist(node.url, url_page):
                    if node.depth < int(self.max_depth) and next_url not in self.seen_url:
                        self.seen_url.add(next_url)
                        self.url_queue.put(Node(next_url, node.depth + 1))
                self.rlock.release()
        logging.info('The url [%s], depth [%d] is crawled.', node.url, node.depth)
        if not self.event.isSet():
            self.event.set()
        time.sleep(int(self.crawl_interval))

    def start(self):
        """
        start   - Start to crawl the urls in url file. 
        """
        self.get_parameter()
        self.thread_pool = threadpool.ThreadPool(self.thread_count)
        self.thread_pool.thread_start()
        logging.info('Begin crawling...')
        while True:
            if self.url_queue.empty():
                if not self.event.wait(int(self.crawl_timeout)):
                    break
            self.event.clear()
            try:
                node = self.url_queue.get(timeout=int(self.crawl_timeout))
            except Queue.Empty:
                logging.info("url_queue is empty.")
                continue
            self.thread_pool.add_job(self.crawing, node)
            self.url_queue.task_done()
        self.url_queue.join()
        logging.info('Crawl work is done.')


def main():
    """
    main    - The main function that run the program.
    """

    init_log("./log/mini_spider")
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--version", 
            help="display the version of the program", action="store_true")
    parser.add_argument("-c", help="read the config file")
    args = parser.parse_args()
    configfile = args.c
    if args.version:
        print "version 1.0.0"
    elif args.c: 
        global spider 
        spider = MySpider(configfile)
        spider.start()         


if __name__ == '__main__':
    main()
    


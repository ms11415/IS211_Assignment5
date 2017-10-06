#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""IS 211 Assignment 5, Network Latency Simulator"""

from __future__ import division
from collections import deque
import argparse
import csv
import urllib2

class Queue:
    """Custom queue class, base taken from textbook."""
    def __init__(self):
        self.items = []
    def is_empty(self):
        return self.items == []
    def enqueue(self, item):
        self.items.insert(0,item)
    def dequeue(self):
        return self.items.pop()
    def get_length(self):
        return len(self.items)

class Server:
    def __init__(self):
        self.time_completed = 0
    def get_completed(self, next_request):
        if self.time_completed > 0:
            self.time_completed += next_request.get_duration()
        else:
            self.time_completed = next_request.get_combined()
        return self.time_completed
    def get_time(self):
        return self.time_completed

class Request:
    def __init__(self, request_time, duration):
        self.request_time = int(request_time)
        self.duration = int(duration)
    def get_duration(self):
        return self.duration
    def get_combined(self):
        return (self.request_time + self.duration)

def simulateOneServer(url):
    """Returns the average time a request is in queue before processing."""

    # Initalize server and associated data variables
    server = Server()
    server_queue = Queue()
    waiting_times = []
    # Download and read URL data
    response = urllib2.urlopen(url)
    data = csv.reader(response)
    for row in data:
    # Create one request from each line of CSV data
        request = Request(row[0], row[2])
    # Add request to server queue
        server_queue.enqueue(request)
    # Process server queue
    while not server_queue.is_empty():
        next_request = server_queue.dequeue()
        waiting_times.append(server.get_completed(next_request)-
                             next_request.get_combined())

    average_wait = sum(waiting_times) / len(waiting_times)
    print 'Average Wait for one server to process {} requests is ' \
          '{:5.2f} secs.'.format(len(waiting_times), average_wait)

def simulateManyServers(url, servers):
    """Returns the average time a request is in queue before processing."""

    # Create array of servers and associated data
    server_data = []
    for i in xrange(servers):
        server = Server()
        server_queue = Queue()
        waiting_times = []
        server_data.append([server, server_queue, waiting_times])
    # Convert list to deque so that it can be rotated for round robin
    server_list = deque(server_data)
    # Download and read URL data
    response = urllib2.urlopen(url)
    data = csv.reader(response)
    for row in data:
    # Create one request from each line of CSV data
        request = Request(row[0], row[2])
    # Add request to first server queue in deque, rotate deque for round robin
        server_list[0][1].enqueue(request)
        server_list.rotate(-1)

    total_wait = []
    # Process server queues
    for i in xrange(servers):
        while not server_list[0][1].is_empty():
            next_request = server_list[0][1].dequeue()
            server_list[0][2].append(server_list[0][0].get_completed(next_request)-
                                 next_request.get_combined())
        average_wait = sum(server_list[0][2]) / len(server_list[0][2])
        total_wait.append(average_wait)
        print 'Average Wait for one server to process {} requests is ' \
                  '{:5.2f} secs.'.format(len(server_list[0][2]), average_wait)
        server_list.rotate(1)

    print 'Average Wait for all {} servers is {:5.2f}'.format(servers,
                    sum(total_wait) / len(total_wait))

def main():
    """Main function."""
    # Parses required URL argument
    # Source data http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, required=True)
    parser.add_argument("--servers", type=int, default=1)
    args = parser.parse_args()
    url = args.url
    servers = args.servers

    if servers == 1:
        simulateOneServer(url)
    else:
        simulateManyServers(url, servers)

if __name__ == '__main__':
    main()
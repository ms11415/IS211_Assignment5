#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""IS 211 Assignment 5, Network Latency Simulator"""

from __future__ import division
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


class Server:
    def __init__(self):
        self.time_completed = 0

    def get_completed(self, next_request):
        if self.time_completed > 0:
            self.time_completed += next_request.get_duration()
        else:
            self.time_completed = next_request.get_combined()

        return self.time_completed

class Request:
    def __init__(self, request_time, duration):
        self.request_time = int(request_time)
        self.duration = int(duration)

    def get_duration(self):
        return self.duration

    def get_combined(self):
        return (self.request_time + self.duration)


def simulateOneServer(file):
    """Returns the average time a request is in queue before processing."""

    server = Server()
    server_queue = Queue()
    waiting_times = []

    data = csv.reader(file)
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

def main(file):
    """Main function."""

    simulateOneServer(file)

if __name__ == '__main__':
    file = urllib2.urlopen(
        'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv')
    main(file)
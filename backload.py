#!/usr/bin/env python3

import json
import gzip
import git
import argparse
from gobits import Gobits
from datetime import datetime
from google.cloud import storage
from google.cloud import pubsub_v1


def get_bucket_name(topic):
    return '{}-history-stg'.format(topic)


def create_pubsub_topic(project_id, topic_name):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    try:
        publisher.create_topic(request={"name": topic_path})
    except Exception as e:
        print(e)


def delete_pubsub_topic(project_id, topic_name):

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    try:
        publisher.delete_topic(request={"topic": topic_path})
    except Exception as e:
        print(e)


def create_pubsub_subscription(project_id, topic_name, subscription_name):

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    topic_path = publisher.topic_path(project_id, topic_name)

    try:
        subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
    except Exception as e:
        print(e)


def list_blobs(bucket_name, start_from):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    return list(bucket.list_blobs(start_offset=start_from.strftime('%Y/%m/%d')))


class Publisher:

    def __init__(self, project_id: str, topic_name: str, subscription_name: str):
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.project_id = project_id

        self.publisherClient = pubsub_v1.PublisherClient()
        self.subscriberClient = pubsub_v1.SubscriberClient()
        self.backloadTopic = None

    def createTopic(self):
        topic_path = self.publisherClient.topic_path(self.project_id, self.topic_name)

        try:
            self.backloadTopic = self.publisherClient.create_topic(request={"name": topic_path})
        except Exception as e:
            print(e)

    def createSubscription(self):
        subscription_path = self.subscriberClient.subscription_path(self.project_id, self.subscription_name)
        topic_path = self.publisherClient.topic_path(self.project_id, self.topic_name)

        try:
            self.subscriberClient.create_subscription(request={"name": subscription_path, "topic": topic_path})
        except Exception as e:
            print(e)

    def publish(self, message):
        self.publisherClient.publish(self.backloadTopic, json.dumps(message).encode("utf-8"))


class Request:
    def __init__(self, request):
        self.request = request

    def dataset_identifier(self):
        return self.request['dataset_identifier']

    def subscription(self):
        return self.request['subscription']

    def start_from(self):
        return datetime.fromisoformat(self.request['start_from'].replace('Z', '+00:00'))


class DataCatalog:
    def __init__(self, datacatalog_filename):
        with open(datacatalog_filename) as data_catalog_file:
            self.data_catalog = json.load(data_catalog_file)

    def getDataset(self, search_identifier):
        for dataset in self.data_catalog['dataset']:
            if dataset['identifier'] == search_identifier:
                return dataset


class DataSet:
    def __init__(self, dataset):
        self.dataset = dataset

    def topic(self):
        for distribution in self.dataset['distribution']:
            if distribution['format'] == 'topic':
                return distribution

    def subscription(self, search_subscription):
        for distribution in self.dataset['distribution']:
            if distribution['title'] == search_subscription:
                return distribution


def publish(bucket, start_from, publisher, topic):
    start_time = datetime.now()
    message_cnt = 0

    blobs = list_blobs(bucket, start_from)
    for blob in blobs:
        print('==== BLOB {} ===='.format(blob.name))
        blob.download_to_filename('file.gz')

        with gzip.open('file.gz') as archive:
            data = json.load(archive)
            for message in data:
                gobits = message['gobits']
                gobits.append(Gobits().to_json())
                message['gobits'] = gobits

                publisher.publish(topic, json.dumps(message).encode("utf-8"))
                message_cnt += 1

    print('Published {} messages during {}'.format(message_cnt, datetime.now() - start_time))


def git_changed_files(project_id):
    """Returns commit info for the last commmit in the current repo."""

    repo = git.Repo('')
    # repo = git.Repo('../odh-backload-requests')
    branch = str(repo.active_branch)

    files = []

    if branch == 'develop':
        last_commit = list(repo.iter_commits(paths='config/{}'.format(project_id)))[0]
        files = [file for file in last_commit.stats.files.keys() if project_id in file]

    elif branch == 'master':
        headcommit = repo.head.commit
        while True:
            headcommit = headcommit.parents[0]
            if len(headcommit.parents) != 1:
                break

        last_commits = list(repo.iter_commits(rev='{}..{}'.format(headcommit, branch)))

        for commit in last_commits:
            for file in commit.stats.files.keys():
                if project_id in file:
                    files.append(file)

    return list(set(files))


def parse_args():
    """A simple function to parse command line arguments."""

    parser = argparse.ArgumentParser(description='Backload odh messages')
    parser.add_argument('-p', '--project-id', required=True, help='name of the GCP project')
    parser.add_argument('-d', '--data-catalog', required=True, help='Data catalog file')

    return parser.parse_args()


def main(args):

    datacatalog = DataCatalog(args.data_catalog)

    for changed_file in git_changed_files(args.project_id):
        print(changed_file)

        with open(changed_file) as backload_request_file:
            request = json.load(backload_request_file)

        for request in request['request']:
            backload_request = Request(request['backload'])

            print('=== REQUEST ===')
            print(backload_request.request)

            dataset = DataSet(datacatalog.getDataset(backload_request.dataset_identifier()))

            topic = dataset.topic()
            print('=== TOPIC ===')
            print(topic)

            subscription = dataset.subscription(backload_request.subscription())
            print('=== SUBSCRIPTION ===')
            print(subscription)

            backload_topic = '{}-backload'.format(topic['title'])
            backload_subscription = '{}-backload'.format(subscription['title'])

            create_pubsub_topic(args.project_id, backload_topic)
            create_pubsub_subscription(args.project_id, backload_topic, backload_subscription)

            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(args.project_id, backload_topic)

            publish(get_bucket_name(topic['title']), backload_request.start_from(), publisher, topic_path)

            # delete_pubsub_topic(args.project_id, backload_topic)


if __name__ == "__main__":
    exit(main(parse_args()))

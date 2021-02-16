#!/usr/bin/env python3

import json
import zlib
import tarfile
import git
import argparse
import time
from gobits import Gobits
from datetime import datetime
from google.cloud import storage
from google.cloud import pubsub_v1


def create_pubsub_topic(project_id, topic):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, '{}-backload'.format(topic['title']))

    try:
        publisher.create_topic(request={"name": topic_path})
    except Exception as e:
        print(e)


def delete_pubsub_topic(project_id, topic):

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, '{}-backload'.format(topic['title']))

    try:
        publisher.delete_topic(request={"topic": topic_path})
    except Exception as e:
        print(e)


def delete_pubsub_subscription(project_id, subscription):

    subscriber = pubsub_v1.SubscriberClient()

    request = dict()
    request['subscription'] = subscriber.subscription_path(project_id, '{}-backload'.format(subscription['title']))

    print('===== DELETE SUBSCRIPTION ======')
    print(request)

    try:
        subscriber.delete_subscription(request=request)
    except Exception as e:
        print(e)


def create_pubsub_subscription(project_id, topic, subscription):

    subscriber = pubsub_v1.SubscriberClient()

    request = dict()
    request['name'] = subscriber.subscription_path(project_id, '{}-backload'.format(subscription['title']))
    request['topic'] = pubsub_v1.PublisherClient().topic_path(project_id, '{}-backload'.format(topic['title']))
    request['expiration_policy'] = {"ttl": {"seconds": 86400}}
    request['ack_deadline_seconds'] = subscription['deploymentProperties']['ackDeadlineSeconds']

    push_config = subscription['deploymentProperties']['pushConfig']
    request['push_config'] = {"push_endpoint": push_config['pushEndpoint'],
                              "oidc_token": {"service_account_email": push_config['oidcToken']['serviceAccountEmail'],
                                             "audience": push_config['oidcToken']['audience']}}

    print('==== CREATE SUBSCRIPTION (REQUEST) ====')
    print(request)

    try:
        subscriber.create_subscription(request=request)
    except Exception as e:
        print(e)


def list_blobs(bucket_name, start_from):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    return list(bucket.list_blobs(start_offset=start_from.strftime('%Y/%m/%d')))


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
        try:
            with open(datacatalog_filename) as data_catalog_file:
                self.data_catalog = json.load(data_catalog_file)
        except Exception:
            print('Data catalog [{}] not found'.format(datacatalog_filename))
            raise

    def getDataset(self, search_identifier):
        for dataset in self.data_catalog['dataset']:
            if dataset['identifier'] == search_identifier:
                return dataset

        raise ValueError('Dataset with name [{}] not found'.format(search_identifier))


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

        raise ValueError('Subscription [{}] not found'.format(search_subscription))


def publish(bucket, start_from, publisher, topic):

    futures = dict()

    def get_callback(f, data):
        def callback(f):
            try:
                futures.pop(data)
            except:  # noqa
                print("Please handle {} for {}.".format(f.exception(), data))
                raise

        return callback

    start_time = datetime.now()
    message_cnt = 0

    for blob in list_blobs(bucket, start_from):
        print('==== BLOB {} ===='.format(blob.name))

        data = []

        if 'archive.gz' in blob.name:
            data = json.loads(zlib.decompress(blob.download_as_string(), 16+zlib.MAX_WBITS))

        if 'tar.xz' in blob.name:
            blob.download_to_filename('tempfile.xz')
            with tarfile.open('tempfile.xz', mode='r:xz') as tar:
                for member in tar.getmembers():
                    f = tar.extractfile(member)
                    data.extend(json.loads(f.read()))

        for message in data:
            gobits = message['gobits']
            gobits.append(Gobits().to_json())
            message['gobits'] = gobits

            data = str(message_cnt)
            futures.update({data: None})

            future = publisher.publish(topic, json.dumps(message).encode("utf-8"))
            futures[data] = future
            future.add_done_callback(get_callback(future, data))

            message_cnt += 1

    while futures:
        time.sleep(1)

    print('Published {} messages during {}'.format(message_cnt, datetime.now() - start_time))


def git_changed_files(project_id):
    """Returns commit info for the last commmit in the current repo."""

    repo = git.Repo('')
    # repo = git.Repo('../odh-backload-requests')
    branch = str(repo.active_branch)

    files = []

    if branch == 'develop':
        last_commit = list(repo.iter_commits(paths='config/{}'.format(project_id)))[0]

        # Only handle recent commits (to make sure the last commit is not reused when the code is re-deployed)
        commit_time = datetime.fromtimestamp(last_commit.committed_date)

        if (datetime.utcnow() - commit_time).total_seconds() < 600:
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
    parser = argparse.ArgumentParser(description='Backload odh messages')
    parser.add_argument('-p', '--project-id', required=True, help='name of the GCP project')
    parser.add_argument('-d', '--data-catalog', required=True, help='Data catalog file')

    return parser.parse_args()


def main(args):

    try:
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

                create_pubsub_topic(args.project_id, topic)
                delete_pubsub_subscription(args.project_id, subscription)
                create_pubsub_subscription(args.project_id, topic, subscription)

                publisher = pubsub_v1.PublisherClient()
                topic_path = publisher.topic_path(args.project_id, '{}-backload'.format(topic['title']))

                publish('{}-history-stg'.format(topic['title']), backload_request.start_from(), publisher, topic_path)

                delete_pubsub_topic(args.project_id, topic)

                return 0

    except Exception as e:
        print(e)
        return 1


if __name__ == "__main__":
    exit(main(parse_args()))


#SPDX-License-Identifier: MIT
# import ast
# import json
# import logging
# import os
# import sys
# import time
# import traceback
# import requests
# import copy
# from datetime import datetime
# from multiprocessing import Process, Queue
# import pandas as pd
# import sqlalchemy as s
# from sqlalchemy.sql.expression import bindparam
from workers.worker_git_integration import WorkerGitInterfaceable

## Don't forget to change Config.py for housekeeper and workers

class GerritChangeRequestWorker(WorkerGitInterfaceable):
    """
    Worker that collects Pull Request related data from the
    Github API and stores it in our database.

    :param task: most recent task the broker added to the worker's queue
    :param config: holds info like api keys, descriptions, and database connection strings
    """
    def __init__(self, config={}):

        worker_type = "gerrit_change_request_worker"

        # Define what this worker can be given and know how to interpret
        given = [['git_url']]
        models = ['change_requests']

        # Define the tables needed to insert, update, or delete on
        data_tables = ['change_requests', 'change_requests_messages', 'change_request_reviewers', 'change_request_labels']
        operations_tables = ['worker_history', 'worker_job']

        self.deep_collection = True
        self.platform_id = 25152 #Gerrit

        # Run the general worker initialization
        super().__init__(worker_type, config, given, models, data_tables, operations_tables)

        # Define data collection info
        ## This is metadata at the end of the table for GERRIT
        self.tool_source = 'Gerrit Change Request Worker'
        self.tool_version = '0.0.1'
        self.data_source = 'Gerrit API'

## Bsic pull request model
    def change_requests_model(self, entry_info, repo_id):
        """Pull Request data collection function. Query GitHub API for PhubRs.

        :param entry_info: A dictionary consisiting of 'git_url' and 'repo_id'
        :type entry_info: dict
        """

        self.logger.info("Beginning collection of Change Requests...\n")

        cr_url = (
            "https://gerrit.automotivelinux.org/gerrit/changes/?q=changes&no-limit"
        )

        cr_action_amp = {
            'insert': {
                'source': ['id'],
                'augur': ['change_src_id']
            },
            'update': {
                'source': ['status'],
                'augur': ['change_src_state']
            }
        }

        source_crs = self.paginate_endpoint(
            cr_url, action_map=cr_action_amp, table=self.change_requests_table, platform="gerrit")

        self.logger.info(f"{len(source_crs['insert'])} change requests to insert")

        self.write_debug_data(source_crs, 'source_prs')

        if len(source_crs['all']) == 0:
            self.logger.info("There are no prs for this repository.\n")
            self.register_task_completion(self.task_info, self.repo_id, 'change_requests')
            return

        crs_insert = [
            {
                'change_src_id': cr['id'],
                'change_project': cr['project'],
                'change_branch': cr['branch'],
                'change_id': cr['change_id'],
                'change_src_state': cr['status'],
                'change_created_at': cr['created'],
                'change_updated_at': cr['updated'],
                'change_submitted_at': cr['submitted'] if 'submitted' in cr.keys() else None,
                'tool_source': self.tool_source,
                'tool_version': self.tool_version,
                'data_source': 'Gerrit API'
            } for cr in source_crs['insert']
        ]

        if len(source_crs['insert']) > 0 or len(source_crs['update']) > 0:
            pr_insert_result, pr_update_result = self.bulk_insert(
                self.change_requests_table,
                update=source_crs['update'], unique_columns=cr_action_amp['insert']['augur'],
                insert=crs_insert, update_columns=cr_action_amp['update']['augur']
            )

        self.change_ids = []
        for cr in source_crs['insert']:
            self.change_ids.append(cr['id'])

        self.logger.info("Finished gathering change requests")

        if self.change_ids:
            self.change_request_comments_model()
            self.change_request_nested_data_model()

        self.register_task_completion(self.task_info, self.repo_id, 'change_requests')

## Comment out whole method if not available.
    def change_request_comments_model(self):

        self.logger.info("Starting change request message collection")

        self.logger.info(f"{len(self.change_ids)} change requests to collect messages for")

        for index, change_id in enumerate(self.change_ids, start=1):

            self.logger.info(f"Message collection {index} of {len(self.change_ids)}")

            comments_url = (
                'https://gerrit.automotivelinux.org/gerrit/changes/{}/comments'.format(change_id)
            )

            comment_action_map = {
                'insert': {
                    'source': ['id'],
                    'augur': ['msg_id']
                }
            }

            cr_comments = self.paginate_endpoint(
                comments_url, action_map=comment_action_map, table=self.change_requests_messages_table, platform="gerrit"
            )

            self.write_debug_data(cr_comments, 'cr_comments')

            # pr_comments['insert'] = self.text_clean(pr_comments['insert'], 'message')

            cr_comments_insert = [
                {
                    'msg_id': comment['id'],
                    'change_src_id': change_id,
                    'change_project': change_id.split('~')[0],
                    'change_branch': change_id.split('~')[1],
                    'change_id': change_id.split('~')[2],
                    'msg_text': comment['message'],
                    'msg_updated': comment['updated'],
                    'author_id': comment['author']['_account_id'],
                    'tool_source': self.tool_source,
                    'tool_version': self.tool_version,
                    'data_source': self.data_source
                } for comment in cr_comments['insert']
            ]

            self.bulk_insert(self.change_requests_messages_table, insert=cr_comments_insert)

        self.logger.info("Finished change request message collection")

# ### If you could comment this so we knew what the fuck it did, that would be great. :)
    def change_request_nested_data_model(self):

            self.logger.info("Starting Labels Colletion")

            labels_url = (
                'https://gerrit.automotivelinux.org/gerrit/changes/?q=changes&o=LABELS'
            )

            labels_action_map = {
                'insert': {
                    'source': ['id', 'label'],
                    'augur': ['change_src_id', 'label']
                }
            }

            cr_labels = self.paginate_endpoint(
                labels_url, action_map=labels_action_map, table=self.change_request_labels_table, platform="gerrit"
            )

            self.write_debug_data(cr_labels, 'cr_labels')

            cr_labels_insert = [
                {
                    'change_src_id': label['id'],
                    'change_project': label['project'],
                    'change_branch': label['branch'],
                    'change_id': label['change_id'],
                    'label': label['label'],
                    'tool_source': self.tool_source,
                    'tool_version': self.tool_version,
                    'data_source': self.data_source
                } for label in cr_labels['insert']
            ]


            self.bulk_insert(self.change_request_labels_table, insert=cr_labels_insert)

            self.logger.info("Finished Labels Connection")

            self.logger.info("Starting change request reviewers collection")

            self.logger.info(f"{len(self.change_ids)} change requests to collect reviewers for")

            for index, change_id in enumerate(self.change_ids, start=1):

                self.logger.info(f"Reviewers collection {index} of {len(self.change_ids)}")

                reviewers_url = (
                    'https://gerrit.automotivelinux.org/gerrit/changes/{}/reviewers'.format(change_id)
                )

                reviewer_action_map = {
                    'insert': {
                        'source': ['_account_id'],
                        'augur': ['reviewer_id']
                    }
                }

                # TODO: add relational table so we can include a where_clause here
                cr_reviewers = self.paginate_endpoint(
                    reviewers_url, action_map=reviewer_action_map, table=self.change_request_reviewers_table, platform="gerrit"
                )

                self.write_debug_data(cr_reviewers, 'cr_reviewers')

                cr_reviewers_insert = [
                    {
                        'reviewer_id': int(reviewer['_account_id']),
                        'change_src_id': change_id,
                        'change_project': change_id.split('~')[0],
                        'change_branch': change_id.split('~')[1],
                        'change_id': change_id.split('~')[2],
                        'reviewer_name': reviewer['name'],
                        'reviewer_email': reviewer['email'] if 'email' in reviewer.keys() else None,
                        'reviewer_username': reviewer['username'],
                        'tool_source': self.tool_source,
                        'tool_version': self.tool_version,
                        'data_source': self.data_source
                    } for reviewer in cr_reviewers['insert']
                ]

                self.bulk_insert(self.change_request_reviewers_table, insert=cr_reviewers_insert)

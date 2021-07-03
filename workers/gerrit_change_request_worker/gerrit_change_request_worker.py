
#SPDX-License-Identifier: MIT
import ast
import json
import logging
import os
import sys
import time
import traceback
import requests
import copy
from datetime import datetime
from multiprocessing import Process, Queue
import pandas as pd
import sqlalchemy as s
from sqlalchemy.sql.expression import bindparam
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

### Changes to "change Request" stopped here
### data_tables will be thesame. The goal is to populate whatever data we can get that we already have for GithUb


        self.deep_collection = True

## TODO: You'll need a new platform ID in the platform table
        self.platform_id = 25152 # GitHub

        # Run the general worker initialization
        super().__init__(worker_type, config, given, models, data_tables, operations_tables)

        # Define data collection info
        ## This is metadata at the end of the table for GERRIT
        self.tool_source = 'Gerrit Change Request Worker'
        self.tool_version = '0.0.1'
        self.data_source = 'Gerrit API'

    def get_crs(self):

        cr_url = (
            "https://gerrit.automotivelinux.org/gerrit/changes/?q=changes&no-limit"
        )

## Source columns in action_map may need to change to the ones in gerrit

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
                'change_project': cr['id'].split('~')[0],
                'change_branch': cr['id'].split('~')[1],
                'change_id': cr['id'].split('~')[2],
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

        return source_crs

## Bsic pull request model
    def change_requests_model(self, entry_info, repo_id):
        """Pull Request data collection function. Query GitHub API for PhubRs.

        :param entry_info: A dictionary consisiting of 'git_url' and 'repo_id'
        :type entry_info: dict
        """

        self.logger.info("Beginning collection of Change Requests...\n")

        source_crs = self.get_crs()

        if source_crs:
            self.change_request_comments_model()
            # self.change_request_commits_model()
            # self.pull_request_events_model(pk_source_prs)
            # self.pull_request_reviews_model(pk_source_prs)
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

            # TODO: add relational table so we can include a where_clause here
            cr_comments = self.paginate_endpoint(
                comments_url, action_map=comment_action_map, table=self.change_requests_messages_table, platform="gerrit"
            )

            self.write_debug_data(cr_comments, 'pr_comments')

            # self.logger.info("CHECK")
            # pr_comments['insert'] = self.text_clean(pr_comments['insert'], 'message')
            #
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
#
# ## This is the one thing we need for sure.
#
#     def pull_request_events_model(self, pk_source_prs=[]):
#
#         if not pk_source_prs:
#             pk_source_prs = self._get_pk_source_prs()
#
#         events_url = (
#             "https://api.github.com/repos/{owner}/{repo}/issues/events?per_page=100&page={}"
#         )
#
#         # Get events that we already have stored
#         #   Set pseudo key (something other than PK) to
#         #   check dupicates with
#         event_action_map = {
#             'insert': {
#                 'source': ['url'],
#                 'augur': ['node_url']
#             }
#         }
#
#         #list to hold contributors needing insertion or update
#         pr_events = self.new_paginate_endpoint(
#             events_url, table=self.pull_request_events_table, action_map=event_action_map,
#             where_clause=self.pull_request_events_table.c.pull_request_id.in_(
#                 set(pd.DataFrame(pk_source_prs)['pull_request_id'])
#             )
#         )
#
#         self.write_debug_data(pr_events, 'pr_events')
#
#         pk_pr_events = self.enrich_data_primary_keys(pr_events['insert'],
#             self.pull_requests_table, ['issue.url'], ['pr_issue_url'])
#
#         self.write_debug_data(pk_pr_events, 'pk_pr_events')
#
#         pk_pr_events = self.enrich_cntrb_id(
#             pk_pr_events, 'actor.login', action_map_additions={
#                 'insert': {
#                     'source': ['actor.node_id'],
#                     'augur': ['gh_node_id']
#                 }
#             }, prefix='actor.'
#         )
#
#         pr_events_insert = [
#             {
#                 'pull_request_id': event['pull_request_id'],
#                 'cntrb_id': event['cntrb_id'],
#                 'action': event['event'],
#                 'action_commit_hash': None,
#                 'created_at': event['created_at'],
#                 'issue_event_src_id': event['id'],
#                 'node_id': event['node_id'],
#                 'node_url': event['url'],
#                 'tool_source': self.tool_source,
#                 'tool_version': self.tool_version,
#                 'data_source': self.data_source
#             } for event in pk_pr_events if event['actor'] is not None
#         ]
#
#         self.bulk_insert(self.pull_request_events_table, insert=pr_events_insert)
#
#
# ## May be available. I think data is there. But via API?
#     def pull_request_reviews_model(self, pk_source_prs=[]):
#
#         if not pk_source_prs:
#             pk_source_prs = self._get_pk_source_prs()
#
#         review_action_map = {
#             'insert': {
#                 'source': ['id'],
#                 'augur': ['pr_review_src_id']
#             },
#             'update': {
#                 'source': ['state'],
#                 'augur': ['pr_review_state']
#             }
#         }
#
#         reviews_urls = [
#             (
#                 "https://api.github.com/repos/{owner}/{repo}/pulls/{pr['number']}/"
#                 "reviews?per_page=100", {'pull_request_id': pr['pull_request_id']}
#             )
#             for pr in pk_source_prs
#         ]
#
#         pr_pk_source_reviews = self.multi_thread_urls(reviews_urls)
#         self.write_debug_data(pr_pk_source_reviews, 'pr_pk_source_reviews')
#
#         cols_to_query = self.get_relevant_columns(
#             self.pull_request_reviews_table, review_action_map
#         )
#
#         table_values = self.db.execute(s.sql.select(cols_to_query).where(
#             self.pull_request_reviews_table.c.pull_request_id.in_(
#                     set(pd.DataFrame(pk_source_prs)['pull_request_id'])
#                 ))).fetchall()
#
#         source_reviews_insert, source_reviews_update = self.new_organize_needed_data(
#             pr_pk_source_reviews, augur_table=self.pull_request_reviews_table,
#             action_map=review_action_map
#         )
#
#         source_reviews_insert = self.enrich_cntrb_id(
#             source_reviews_insert, 'user.login', action_map_additions={
#                 'insert': {
#                     'source': ['user.node_id'],
#                     'augur': ['gh_node_id']
#                 }
#             }, prefix='user.'
#         )
#
#         reviews_insert = [
#             {
#                 'pull_request_id': review['pull_request_id'],
#                 'cntrb_id': review['cntrb_id'],
#                 'pr_review_author_association': review['author_association'],
#                 'pr_review_state': review['state'],
#                 'pr_review_body': review['body'],
#                 'pr_review_submitted_at': review['submitted_at'] if (
#                     'submitted_at' in review
#                 ) else None,
#                 'pr_review_src_id': review['id'],
#                 'pr_review_node_id': review['node_id'],
#                 'pr_review_html_url': review['html_url'],
#                 'pr_review_pull_request_url': review['pull_request_url'],
#                 'pr_review_commit_id': review['commit_id'],
#                 'tool_source': self.tool_source,
#                 'tool_version': self.tool_version,
#                 'data_source': self.data_source
#             } for review in source_reviews_insert if review['user'] and 'login' in review['user']
#         ]
#
#         self.bulk_insert(
#             self.pull_request_reviews_table, insert=reviews_insert, update=source_reviews_update,
#             unique_columns=review_action_map['insert']['augur'],
#             update_columns=review_action_map['update']['augur']
#         )
#
#         # Merge source data to inserted data to have access to inserted primary keys
#
#         gh_merge_fields = ['id']
#         augur_merge_fields = ['pr_review_src_id']
#
#         both_pr_review_pk_source_reviews = self.enrich_data_primary_keys(
#             pr_pk_source_reviews, self.pull_request_reviews_table, gh_merge_fields,
#             augur_merge_fields
#         )
#         self.write_debug_data(both_pr_review_pk_source_reviews, 'both_pr_review_pk_source_reviews')
#
#         # Review Comments
#
#         review_msg_url = (f'https://api.github.com/repos/{self.owner}/{self.repo}/pulls' +
#             '/comments?per_page=100&page={}')
#
#         review_msg_action_map = {
#             'insert': {
#                 'source': ['created_at', 'body'],
#                 'augur': ['msg_timestamp', 'msg_text']
#             }
#         }
#
#         in_clause = [] if len(both_pr_review_pk_source_reviews) == 0 else \
#             set(pd.DataFrame(both_pr_review_pk_source_reviews)['pr_review_id'])
#
#         review_msgs = self.new_paginate_endpoint(
#             review_msg_url, action_map=review_msg_action_map, table=self.message_table,
#             where_clause=self.message_table.c.msg_id.in_(
#                 [
#                     msg_row[0] for msg_row in self.db.execute(
#                         s.sql.select([self.pull_request_review_message_ref_table.c.msg_id]).where(
#                             self.pull_request_review_message_ref_table.c.pr_review_id.in_(
#                                 in_clause
#                             )
#                         )
#                     ).fetchall()
#                 ]
#             )
#         )
#         self.write_debug_data(review_msgs, 'review_msgs')
#
#         review_msgs['insert'] = self.enrich_cntrb_id(
#             review_msgs['insert'], 'user.login', action_map_additions={
#                 'insert': {
#                     'source': ['user.node_id'],
#                     'augur': ['gh_node_id']
#                 }
#             }, prefix='user.'
#         )
#
#         review_msg_insert = [
#             {
#                 'pltfrm_id': self.platform_id,
#                 'msg_text': comment['body'],
#                 'msg_timestamp': comment['created_at'],
#                 'cntrb_id': comment['cntrb_id'],
#                 'tool_source': self.tool_source,
#                 'tool_version': self.tool_version,
#                 'data_source': self.data_source
#             } for comment in review_msgs['insert']
#             if comment['user'] and 'login' in comment['user']
#         ]
#
#         self.bulk_insert(self.message_table, insert=review_msg_insert)
#
#         # PR REVIEW MESSAGE REF TABLE
#
#         c_pk_source_comments = self.enrich_data_primary_keys(
#             review_msgs['insert'], self.message_table, ['created_at', 'body'],
#             ['msg_timestamp', 'msg_text']
#         )
#         self.write_debug_data(c_pk_source_comments, 'c_pk_source_comments')
#
#         both_pk_source_comments = self.enrich_data_primary_keys(
#             c_pk_source_comments, self.pull_request_reviews_table, ['pull_request_review_id'],
#             ['pr_review_src_id']
#         )
#         self.write_debug_data(both_pk_source_comments, 'both_pk_source_comments')
#
#         pr_review_msg_ref_insert = [
#             {
#                 'pr_review_id': comment['pr_review_id'],
#                 'msg_id': comment['msg_id'],
#                 'pr_review_msg_url': comment['url'],
#                 'pr_review_src_id': comment['pull_request_review_id'],
#                 'pr_review_msg_src_id': comment['id'],
#                 'pr_review_msg_node_id': comment['node_id'],
#                 'pr_review_msg_diff_hunk': comment['diff_hunk'],
#                 'pr_review_msg_path': comment['path'],
#                 'pr_review_msg_position': comment['position'],
#                 'pr_review_msg_original_position': comment['original_position'],
#                 'pr_review_msg_commit_id': comment['commit_id'],
#                 'pr_review_msg_original_commit_id': comment['original_commit_id'],
#                 'pr_review_msg_updated_at': comment['updated_at'],
#                 'pr_review_msg_html_url': comment['html_url'],
#                 'pr_url': comment['pull_request_url'],
#                 'pr_review_msg_author_association': comment['author_association'],
#                 'pr_review_msg_start_line': comment['start_line'],
#                 'pr_review_msg_original_start_line': comment['original_start_line'],
#                 'pr_review_msg_start_side': comment['start_side'],
#                 'pr_review_msg_line': comment['line'],
#                 'pr_review_msg_original_line': comment['original_line'],
#                 'pr_review_msg_side': comment['side'],
#                 'tool_source': self.tool_source,
#                 'tool_version': self.tool_version,
#                 'data_source': self.data_source
#             } for comment in both_pk_source_comments
#         ]
#
#         self.bulk_insert(
#             self.pull_request_review_message_ref_table,
#             insert=pr_review_msg_ref_insert
#         )
#
#
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

            # TODO: add relational table so we can include a where_clause here
            cr_labels = self.paginate_endpoint(
                labels_url, action_map=labels_action_map, table=self.change_request_labels_table, platform="gerrit"
            )

            # self.logger.info(f"Labels: {cr_labels}")

            # self.write_debug_data(pr_comments, 'pr_comments')

            # pr_comments['insert'] = self.text_clean(pr_comments['insert'], 'message')

            # cr_labels_insert = []
            # for change_request in cr_labels['insert']:
            #     for label in change_request['labels']:
            #         cr_labels_insert.append(
            #             {
            #                 'change_src_id': change_request['id'],
            #                 'change_project': change_request['id'].split('~')[0],
            #                 'change_branch': change_request['id'].split('~')[1],
            #                 'change_id': change_request['id'].split('~')[2],
            #                 'label': label,
            #                 'tool_source': self.tool_source,
            #                 'tool_version': self.tool_version,
            #                 'data_source': self.data_source
            #             }
            #         )

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

                # self.write_debug_data(pr_comments, 'pr_comments')

                # pr_comments['insert'] = self.text_clean(pr_comments['insert'], 'message')

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

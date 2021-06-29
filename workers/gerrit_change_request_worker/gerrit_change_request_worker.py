
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

### If the Gerrit API Give us Files
## Files model will work if we can get a list of files.
#     def change_request_files_model(self, task_info, repo_id):
#
#         # query existing PRs and the respective url we will append the commits url to
#         pr_number_sql = s.sql.text("""
#             SELECT DISTINCT pr_src_number as pr_src_number, change_requests.pull_request_id
#             FROM pull_requests--, pull_request_meta
#             WHERE repo_id = {}
#         """.format(self.repo_id))
#         pr_numbers = pd.read_sql(pr_number_sql, self.db, params={})
#
#         pr_file_rows = []
#
#         for index, pull_request in enumerate(pr_numbers.itertuples()):
#
#             self.logger.info(f'Querying files for pull request #{index + 1} of {len(pr_numbers)}')
#
#             query = """
#                 {{
#                   repository(owner:"%s", name:"%s"){{
#                     pullRequest (number: %s) {{
#                 """ % (self.owner, self.repo, pull_request.pr_src_number) + """
#                       files (last: 100{files}) {{
#                         pageInfo {{
#                           hasPreviousPage
#                           hasNextPage
#                           endCursor
#                           startCursor
#                         }}
#                         edges {{
#                           node {{
#                             additions
#                             deletions
#                             path
#                           }}
#                         }}
#                       }}
#                     }}
#                   }}
#                 }}
#             """
#
#             pr_file_rows += [{
#                 'pull_request_id': pull_request.pull_request_id,
#                 'pr_file_additions': pr_file['node']['additions'],
#                 'pr_file_deletions': pr_file['node']['deletions'],
#                 'pr_file_path': pr_file['node']['path'],
#                 'tool_source': self.tool_source,
#                 'tool_version': self.tool_version,
#                 'data_source': 'GitHub API',
#             } for pr_file in self.graphql_paginate(query, {'files': None})]
#
#
#         # Get current table values
#         table_values_sql = s.sql.text("""
#             SELECT pull_request_files.*
#             FROM pull_request_files, pull_requests
#             WHERE pull_request_files.pull_request_id = pull_requests.pull_request_id
#             AND repo_id = :repo_id
#         """)
#         self.logger.info(
#             f'Getting table values with the following PSQL query: \n{table_values_sql}\n'
#         )
#         table_values = pd.read_sql(table_values_sql, self.db, params={'repo_id': self.repo_id})
#
#         # Compare queried values against table values for dupes/updates
#         if len(pr_file_rows) > 0:
#             table_columns = pr_file_rows[0].keys()
#         else:
#             self.logger.info(f'No rows need insertion for repo {self.repo_id}\n')
#             self.register_task_completion(task_info, self.repo_id, 'pull_request_files')
#             return
#
#         # Compare queried values against table values for dupes/updates
#         pr_file_rows_df = pd.DataFrame(pr_file_rows)
#         pr_file_rows_df = pr_file_rows_df.dropna(subset=['pull_request_id'])
#
#         dupe_columns = ['pull_request_id', 'pr_file_path']
#         update_columns = ['pr_file_additions', 'pr_file_deletions']
#
#         need_insertion = pr_file_rows_df.merge(table_values, suffixes=('','_table'),
#                             how='outer', indicator=True, on=dupe_columns).loc[
#                                 lambda x : x['_merge']=='left_only'][table_columns]
#
#         need_updates = pr_file_rows_df.merge(table_values, on=dupe_columns, suffixes=('','_table'),
#                         how='inner',indicator=False)[table_columns].merge(table_values,
#                             on=update_columns, suffixes=('','_table'), how='outer',indicator=True
#                                 ).loc[lambda x : x['_merge']=='left_only'][table_columns]
#
#         need_updates['b_pull_request_id'] = need_updates['pull_request_id']
#         need_updates['b_pr_file_path'] = need_updates['pr_file_path']
#
#         pr_file_insert_rows = need_insertion.to_dict('records')
#         pr_file_update_rows = need_updates.to_dict('records')
#
#         self.logger.info(
#             f'Repo id {self.repo_id} needs {len(need_insertion)} insertions and '
#             f'{len(need_updates)} updates.\n'
#         )
#
#         if len(pr_file_update_rows) > 0:
#             success = False
#             while not success:
#                 try:
#                     self.db.execute(
#                         self.pull_request_files_table.update().where(
#                             self.pull_request_files_table.c.pull_request_id == bindparam(
#                                 'b_pull_request_id'
#                             ) and self.pull_request_files_table.c.pr_file_path == bindparam(
#                                 'b_pr_file_path'
#                             )
#                         ).values(
#                             pr_file_additions=bindparam('pr_file_additions'),
#                             pr_file_deletions=bindparam('pr_file_deletions')
#                         ), pr_file_update_rows
#                     )
#                     success = True
#                 except Exception as e:
#                     self.logger.info('error: {}'.format(e))
#                 time.sleep(5)
#
#         if len(pr_file_insert_rows) > 0:
#             success = False
#             while not success:
#                 try:
#                     self.db.execute(
#                         self.pull_request_files_table.insert(),
#                         pr_file_insert_rows
#                     )
#                     success = True
#                 except Exception as e:
#                     self.logger.info('error: {}'.format(e))
#                 time.sleep(5)
#
#         self.register_task_completion(task_info, self.repo_id, 'pull_request_files')
#
#
# ## If Commits are returned for each merge request, this will work.
    # def change_request_commits_model(self):
    #
    #     self.logger.info("Starting change request commits collection")
    #
    #     self.logger.info(f"{len(self.change_ids)} change requests to collect commits for")
    #
    #     for index, change_id in enumerate(self.change_ids, start=1):
    #
    #         self.logger.info(f"Commit collection {index} of {len(self.change_ids)}")
    #
    #         comments_url = (
    #             'https://gerrit.automotivelinux.org/gerrit/changes/{}/edit'.format(change_id)
    #         )
    #
    #         commit_action_map = {
    #             'insert': {
    #                 'source': ['id'],
    #                 'augur': ['msg_id']
    #             }
    #         }
    #
    #         # TODO: add relational table so we can include a where_clause here
    #         pr_commits = self.paginate_endpoint(
    #             comments_url, action_map=commit_action_map, table=self.change_requests_commits_table, platform="gerrit"
    #         )
    #
    #         # self.write_debug_data(pr_comments, 'pr_comments')
    #
    #         # self.logger.info("CHECK")
    #         # pr_comments['insert'] = self.text_clean(pr_comments['insert'], 'message')
    #         #
    #         pr_commits_insert = [
    #             {
    #                 'msg_id': commit['id'],
    #                 'change_id': change_id,
    #                 'msg_text': commit['message'],
    #                 'msg_updated': commit['updated'],
    #                 'author_id': commit['author']['_account_id'],
    #                 'tool_source': self.tool_source,
    #                 'tool_version': self.tool_version,
    #                 'data_source': self.data_source
    #             } for commit in pr_commits['insert']
    #         ]
    #
    #         self.bulk_insert(self.change_requests_commits_table, insert=pr_commits_insert)

## This is where teh GERRIT API Link will go
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

        self.write_debug_data(source_crs, 'source_prs')

        if len(source_crs['all']) == 0:
            self.logger.info("There are no prs for this repository.\n")
            self.register_task_completion(self.task_info, self.repo_id, 'change_requests')
            return

        # self.logger.info(f"Change requests: {source_crs}")
        # self.logger.info(f"Change Request keys: {source_crs.keys()}")

        # self.logger.info(f"Whole_id: {source_crs['insert'][0]['id']}")
        # self.logger.info(f"Id array: {source_crs['insert'][0]['id'].split('-')}")
        # sample_cr = source_crs['insert'][0]
        # self.logger.info(f"Project: {sample_cr['id'].split('-')[0]}")
        # self.logger.info(f"Branch: {sample_cr['id'].split('-')[1].split('~')[0]}")
        # self.logger.info(f"Id: {sample_cr['id'].split('-')[1].split('~')[1]}")
        # self.logger.info(f"Cr: {source_crs['insert'][0]}\n\n")

        # self.logger.info(f"Project: {sample_cr['id'].split('~')[0]}")
        # self.logger.info(f"Branch: {sample_cr['id'].split('~')[1]}")
        # self.logger.info(f"Id: {sample_cr['id'].split('~')[2]}")


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
            # self.change_request_comments_model()
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
                    'change_id': change_id,
                    'msg_text': comment['message'],
                    'msg_updated': comment['updated'],
                    'author_id': comment['author']['_account_id'],
                    'tool_source': self.tool_source,
                    'tool_version': self.tool_version,
                    'data_source': self.data_source
                } for comment in cr_comments['insert']
            ]

            self.bulk_insert(self.change_requests_messages_table, insert=cr_comments_insert)

            # PR MESSAGE REF TABLE
            # self.logger.info("CHECK")
            # self.logger.info(f'inserting messages for {pr_comments} repo')
            # self.logger.info(f'message table {self.message_table}')
            #
            #
            # pr_message_ref_insert = [
            #     {
            #         'change_request_id': change_id,
            #         'msg_id': comment['change_request_message_id'],
            #         'tool_source': self.tool_source,
            #         'tool_version': self.tool_version,
            #         'data_source': self.data_source
            #     } for comment in both_pk_source_comments
            # ]
            #
            # self.bulk_insert(self.change_request_message_ref_table, insert=pr_message_ref_insert)

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
#
#         if not pk_source_prs:
#             pk_source_prs = self._get_pk_source_prs()
#
#         labels_all = []
#         reviewers_all = []
#         assignees_all = []
#         meta_all = []
#
#         for index, pr in enumerate(pk_source_prs):
#
#             # PR Labels
#             source_labels = pd.DataFrame(pr['labels'])
#             source_labels['pull_request_id'] = pr['pull_request_id']
#             labels_all += source_labels.to_dict(orient='records')
#
#             # Reviewers
#             source_reviewers = pd.DataFrame(pr['requested_reviewers'])
#             source_reviewers['pull_request_id'] = pr['pull_request_id']
#             reviewers_all += source_reviewers.to_dict(orient='records')
#
#             # Assignees
#             source_assignees = pd.DataFrame(pr['assignees'])
#             source_assignees['pull_request_id'] = pr['pull_request_id']
#             assignees_all += source_assignees.to_dict(orient='records')
#
#             # Meta
#             pr['head'].update(
#                 {'pr_head_or_base': 'head', 'pull_request_id': pr['pull_request_id']}
#             )
#             pr['base'].update(
#                 {'pr_head_or_base': 'base', 'pull_request_id': pr['pull_request_id']}
#             )
#             meta_all += [pr['head'], pr['base']]
#
#         # PR labels insertion

            self.logger.info("Starting Labels Colletion")

            labels_url = (
                'https://gerrit.automotivelinux.org/gerrit/changes/?q=changes&o=LABELS'
            )

            labels_action_map = {
                'insert': {
                    'source': ['change_id', 'label'],
                    'augur': ['change_id', 'label']
                }
            }

            # TODO: add relational table so we can include a where_clause here
            cr_labels = self.paginate_endpoint(
                labels_url, action_map=labels_action_map, table=self.change_request_labels_table, platform="gerrit"
            )

            self.logger.info(f"Labels: {cr_labels}")

            # self.write_debug_data(pr_comments, 'pr_comments')

            # self.logger.info("CHECK")
            # pr_comments['insert'] = self.text_clean(pr_comments['insert'], 'message')
            #
            cr_labels_insert = []
            for change_request in cr_labels['insert']:
                for label in change_request['labels']:
                    cr_labels_insert.append(
                        {
                            'label': label,
                            'change_id': change_request['id'],
                            'tool_source': self.tool_source,
                            'tool_version': self.tool_version,
                            'data_source': self.data_source
                        }
                    )



            # cr_labels_insert = [
            #     {
            #         'label': for label_name in label['labels'],
            #         'change_id': label['change_id'],
            #         'tool_source': self.tool_source,
            #         'tool_version': self.tool_version,
            #         'data_source': self.data_source
            #     } for label in cr_labels['insert']
            # ]

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
                        'change_id': change_id,
                        'reviewer_name': reviewer['name'],
                        'reviewer_email': reviewer['email'] if 'email' in reviewer.keys() else None,
                        'reviewer_username': reviewer['username'],
                        'tool_source': self.tool_source,
                        'tool_version': self.tool_version,
                        'data_source': self.data_source
                    } for reviewer in cr_reviewers['insert']
                ]

                self.bulk_insert(self.change_request_reviewers_table, insert=cr_reviewers_insert)
#
#         # PR assignees insertion
#         assignee_action_map = {
#             'insert': {
#                 'source': ['pull_request_id', 'id'],
#                 'augur': ['pull_request_id', 'pr_assignee_src_id']
#             }
#         }
#         source_assignees_insert, _ = self.new_organize_needed_data(
#             assignees_all, augur_table=self.pull_request_assignees_table,
#             action_map=assignee_action_map
#         )
#         source_assignees_insert = self.enrich_cntrb_id(
#             source_assignees_insert, 'login', action_map_additions={
#                 'insert': {
#                     'source': ['node_id'],
#                     'augur': ['gh_node_id']
#                 }
#             }
#         )
#         assignees_insert = [
#             {
#                 'pull_request_id': assignee['pull_request_id'],
#                 'contrib_id': assignee['cntrb_id'],
#                 'pr_assignee_src_id': assignee['id'],
#                 'tool_source': self.tool_source,
#                 'tool_version': self.tool_version,
#                 'data_source': self.data_source
#             } for assignee in source_assignees_insert if 'login' in assignee
#         ]
#         self.bulk_insert(self.pull_request_assignees_table, insert=assignees_insert)
#
#         # PR meta insertion
#         meta_action_map = {
#             'insert': {
#                 'source': ['pull_request_id', 'sha', 'pr_head_or_base'],
#                 'augur': ['pull_request_id', 'pr_sha', 'pr_head_or_base']
#             }
#         }
#
#         source_meta_insert, _ = self.new_organize_needed_data(
#             meta_all, augur_table=self.pull_request_meta_table, action_map=meta_action_map
#         )
#         source_meta_insert = self.enrich_cntrb_id(
#             source_meta_insert, 'user.login', action_map_additions={
#                 'insert': {
#                     'source': ['user.node_id'],
#                     'augur': ['gh_node_id']
#                 }
#             }, prefix='user.'
#         )
#         meta_insert = [
#             {
#                 'pull_request_id': meta['pull_request_id'],
#                 'pr_head_or_base': meta['pr_head_or_base'],
#                 'pr_src_meta_label': meta['label'],
#                 'pr_src_meta_ref': meta['ref'],
#                 'pr_sha': meta['sha'],
#                 'cntrb_id': meta['cntrb_id'],
#                 'tool_source': self.tool_source,
#                 'tool_version': self.tool_version,
#                 'data_source': self.data_source
#             } for meta in source_meta_insert if meta['user'] and 'login' in meta['user']
#         ]
#         self.bulk_insert(self.pull_request_meta_table, insert=meta_insert)
#
#
# ## Get repos that need updating
#     def query_pr_repo(self, pr_repo, pr_repo_type, pr_meta_id):
#         """ TODO: insert this data as extra columns in the meta table """
#         self.logger.info(f'Querying PR {pr_repo_type} repo')
#
#         table = 'pull_request_repo'
#         duplicate_col_map = {'pr_src_repo_id': 'id'}
#         update_col_map = {}
#         table_pkey = 'pr_repo_id'
#
#         update_keys = list(update_col_map.keys()) if update_col_map else []
#         cols_query = list(duplicate_col_map.keys()) + update_keys + [table_pkey]
#
#         pr_repo_table_values = self.get_table_values(cols_query, [table])
#
#         new_pr_repo = self.assign_tuple_action(
#             [pr_repo], pr_repo_table_values, update_col_map, duplicate_col_map, table_pkey
#         )[0]
#
#         if new_pr_repo['owner'] and 'login' in new_pr_repo['owner']:
#             cntrb_id = self.find_id_from_login(new_pr_repo['owner']['login'])
#         else:
#             cntrb_id = 1
#
# # I would add "platform_id" to this table
#         pr_repo = {
#             'pr_repo_meta_id': pr_meta_id,
#             'pr_repo_head_or_base': pr_repo_type,
#             'pr_src_repo_id': new_pr_repo['id'],
#             # 'pr_src_node_id': new_pr_repo[0]['node_id'],
#             'pr_src_node_id': None,
#             'pr_repo_name': new_pr_repo['name'],
#             'pr_repo_full_name': new_pr_repo['full_name'],
#             'pr_repo_private_bool': new_pr_repo['private'],
#             'pr_cntrb_id': cntrb_id,
#             'tool_source': self.tool_source,
#             'tool_version': self.tool_version,
#             'data_source': self.data_source
#         }
#
#         if new_pr_repo['flag'] == 'need_insertion':
#             result = self.db.execute(self.pull_request_repo_table.insert().values(pr_repo))
#             self.logger.info(f"Added PR {pr_repo_type} repo {result.inserted_primary_key}")
#
#             self.results_counter += 1
#
#             self.logger.info(
#                 f"Finished adding PR {pr_repo_type} Repo data for PR with id {self.pr_id_inc}"
#             )

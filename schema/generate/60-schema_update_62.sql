BEGIN; 

INSERT INTO "augur_operations"."worker_history" ("history_id", "repo_id", "worker", "job_model", "oauth_id", "timestamp", "status", "total_results") VALUES (1, 1, 'seed.worker.record', 'seed', NULL, '2021-07-27 09:50:56', 'Success', 0);

DROP TABLE IF EXISTS "augur_data"."repo_deps_libyear";
CREATE TABLE "augur_data"."repo_deps_libyear" (
  "repo_deps_libyear_id" serial8 NOT NULL,
  "repo_id" int8, 
  "name" varchar COLLATE "pg_catalog"."default",
  "requirement" varchar COLLATE "pg_catalog"."default",
  "type" varchar COLLATE "pg_catalog"."default",
  "package_manager" varchar COLLATE "pg_catalog"."default",
  "current_verion" varchar COLLATE "pg_catalog"."default",
  "latest_version" varchar COLLATE "pg_catalog"."default",
  "current_release_date" varchar COLLATE "pg_catalog"."default",
  "latest_release_date" varchar COLLATE "pg_catalog"."default",
  "libyear" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."repo_deps_libyear" OWNER TO "augur";

-- ----------------------------
-- Primary Key structure for table repo_deps_scorecard
-- ----------------------------
ALTER TABLE "augur_data"."repo_deps_libyear" ADD CONSTRAINT "repo_deps_libyear_pkey" PRIMARY KEY ("repo_deps_libyear_id");

-- ----------------------------
-- Foreign Keys structure for table repo_deps_scorecard
-- ----------------------------
ALTER TABLE "augur_data"."repo_deps_libyear" ADD CONSTRAINT "repo_id" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;


update "augur_operations"."augur_settings" set value = 62 
  where setting = 'augur_data_version'; 


update "augur_operations"."augur_settings" set value = 61 where setting = 'augur_data_version';


COMMIT; 


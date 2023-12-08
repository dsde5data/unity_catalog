-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### DCL SYNTAX

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * () Optional
-- MAGIC * [|] one of the values
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### [CREATE | DROP] OBJECT_TYPE (IF [NOT|] EXISTS) OBJECT_NAME (CASCADE | ) (MANAGED LOCATION 'LOC_URI') (Comment 'Comment Text')
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### SHOW [OBJECT_TYPE] (IN PARENT_OBJECT_NAME)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### SELECT CURRENT_[OBJECT_TYPE];
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###### USE OBJECT_TYPE OBJECT_NAME

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### DESC [OBJECT_TYPE] (EXTENDED) OBJECT_NAME

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### [GRANT|REVOKE] PERMISSON_TYPE ON [OBJECT_TYPE] OBJECT_NAME [TO|FROM] `USERID`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### SHOW GRANTS ON OBJECT_TYPE OBJECT_NAME

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and Manage Catalog
-- MAGIC
-- MAGIC * Create| Drop | catalog [With | Wothout ] Location.
-- MAGIC * Show all catalogs.
-- MAGIC * Select a catalog.
-- MAGIC * describe catalog [extended].
-- MAGIC * Grant permissions on a catalog.
-- MAGIC * Show all grants on a catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC     CREATE CATALOG [ IF NOT EXISTS ] <catalog-name>
-- MAGIC         [ MANAGED LOCATION '<location-path>' ]
-- MAGIC         [ COMMENT <comment> ];

-- COMMAND ----------

-- MAGIC %md
-- MAGIC     DROP CATALOG [ IF EXISTS ] <catalog-name> [ RESTRICT | CASCADE ]

-- COMMAND ----------

drop catalog if exists uc_dev;  

-- COMMAND ----------

create catalog if  not exists uc_dev;

-- COMMAND ----------

show catalogs;

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

use catalog uc_dev;


-- COMMAND ----------

desc catalog extended uc_dev;

-- COMMAND ----------

grant use catalog,create schema on catalog uc_Dev to `ali.shahbaz00@gmail.com`;

-- COMMAND ----------

show  grant on catalog uc_Dev;---grants is acceptable as well

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and manage schemas
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC     USE CATALOG <catalog>;
-- MAGIC     CREATE { DATABASE | SCHEMA } [ IF NOT EXISTS ] <schema-name>
-- MAGIC         [ MANAGED LOCATION '<location-path>' ]
-- MAGIC         [ COMMENT <comment> ]
-- MAGIC         [ WITH DBPROPERTIES ( <property-key = property_value [ , ... ]> ) ];

-- COMMAND ----------

show schemas in uc_dev;

-- COMMAND ----------

show schemas;

-- COMMAND ----------

drop schema if exists landing;

-- COMMAND ----------

create schema if not exists landing;

-- COMMAND ----------

show schemas in uc_dev;

-- COMMAND ----------

use schema landing;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

drop catalog if exists uc_Dev cascade;

-- COMMAND ----------

show catalogs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and Manage External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION  ext_uc02
URL 'gs://uc02' 
with storage credential uc02_acc

-- COMMAND ----------

create external location if not exists ext_uc02 
URL  'gs://uc02/' 
with storage credential uc02_acc

-- COMMAND ----------

show grants  on external location  uc_dev_catalog;

-- COMMAND ----------

grant all privileges on external location uc_dev_catalog to `ali.shahbaz9999@gmail.com`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Putting All Together (external location, catalog)

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS uc_dev Managed LOCATION 'gs://uc_dev_catalog/LandingArea/'

-- COMMAND ----------

desc catalog extended uc_dev;

-- COMMAND ----------

drop schema if exists landing;

-- COMMAND ----------

create schema if not exists landing managed location 'gs://uc_dev_catalog/LandingArea/bronze';

-- COMMAND ----------

select current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and Manage table
-- MAGIC

-- COMMAND ----------

drop table if exists temp_table;

-- COMMAND ----------

create table if not exists temp_table(id int, name string,Parttion_id int) partitioned by (Parttion_id);


-- COMMAND ----------

desc table extended temp_table;

-- COMMAND ----------

insert into temp_table values(1,'ali',1200),(2,'alia',1200),(1,'alaa',1200)

-- COMMAND ----------

show tables in uc_dev.landing;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and Manage Volumes

-- COMMAND ----------

create volume if not exists landing_files

-- COMMAND ----------

create external volume if not exists landing_files_ext
location 'gs://ext_files_wk'

-- COMMAND ----------

create external location 

-- COMMAND ----------



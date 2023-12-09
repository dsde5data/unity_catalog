-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### DCL SYNTAX
-- MAGIC
-- MAGIC **user must be in ``**
-- MAGIC * () Optional
-- MAGIC * [|] one of the values
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **1. CREATE | DROP**
-- MAGIC
-- MAGIC **2. SHOW**
-- MAGIC
-- MAGIC **3. SELECT**
-- MAGIC
-- MAGIC **4. SHOW**
-- MAGIC
-- MAGIC **5. DESC**
-- MAGIC
-- MAGIC **6. GRANT | REVOKE**
-- MAGIC
-- MAGIC **7. ALTER**
-- MAGIC
-- MAGIC **8. DESC**

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### [CREATE | DROP] OBJECT_TYPE (IF [NOT|] EXISTS) OBJECT_NAME (CASCADE | ) (MANAGED LOCATION 'LOC_URI') (Comment 'Comment Text')
-- MAGIC
-- MAGIC ###### [CREATE | DROP] EXTERNAL LOCATION (IF [NOT|] EXISTS) EXTERNAL_LOCATION_NAME URL 'PATH' WITH (CREDENTIAL CREDENTIAL_NAME)
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



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### [GRANT|REVOKE] PERMISSON_TYPE ON [OBJECT_TYPE] OBJECT_NAME [TO|FROM] `USERID` 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### SHOW GRANTS ON OBJECT_TYPE OBJECT_NAME

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###### ALTER OBJECT_TYPE OBJECT_NAME OWNER TO `USER_ID`;
-- MAGIC ###### ALTER OBJECT_TYPE OBJECT_NAME RENAME TO NEW_NAME;
-- MAGIC ###### ALTER EXTERNAL LOCATION location_name SET URL '<url>' [FORCE];
-- MAGIC ###### ALTER EXTERNAL LOCATION <location-name> SET STORAGE CREDENTIAL CREDENTIAL_NAME;

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

drop external location if exists ext_uc02;

-- COMMAND ----------

show external locations;

-- COMMAND ----------

CREATE external location  ext_uc02
URL 'gs://uc02/' 
with (storage credential uc02_acc)

-- COMMAND ----------

show external locations

-- COMMAND ----------

grant all privileges on external location ext_uc02 to `ali.shahbaz9999@gmail.com`;


-- COMMAND ----------


grant read files,write files on external location ext_uc02 to `ali.shahbaz9999@gmail.com`


-- COMMAND ----------

show grants  on external location  ext_uc02;

-- COMMAND ----------

revoke all privileges on external location ext_uc02 from `ali.shahbaz9999@gmail.com`

-- COMMAND ----------

show grants on external location ext_uc02

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

select current_catalog();

-- COMMAND ----------

select current_schema();

-- COMMAND ----------

use gold_copy.crm;

-- COMMAND ----------

drop table if exists Sales_table;

-- COMMAND ----------

create table if not exists Sales_table(Sale_id int, Sales_Person string,Store String,Amount float,Parttion_id int) partitioned by (Parttion_id);


-- COMMAND ----------

desc table extended Sales_table;

-- COMMAND ----------

insert into Sales_table values(1,'Ali','United States',1200),(2,'Ahmed','Canada',100),(3,'Sameen','Germany',1200)

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

show volumes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and Manage Users and do POC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and Manage Views

-- COMMAND ----------

select current_user();

-- COMMAND ----------

show groups;

-- COMMAND ----------

select is_account_group_member('IT')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.format('csv').option('header','true').load('/Volumes/uc_dev/landing/landing_files/DimSalesTerritory.csv');
-- MAGIC
-- MAGIC df.write.mode("overwrite").saveAsTable("uc_dev.landing.DimSalesTerritory")

-- COMMAND ----------

show groups;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Row and Column level Security

-- COMMAND ----------

select * from Sales_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Dynamic views

-- COMMAND ----------

create or replace view vw_Sales_table
select *, case when is_account_group_member(Sales_Person) then Sales_Person
else '****' end as Sales_Person from Sales_table
where is_account_group_member(Store)

-- COMMAND ----------

select * from vw_Sales_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Row Filtering

-- COMMAND ----------

Create function udf_row_filter_sales(SalesCountry string)
return is_account_group_member(SalesCountry);

-- COMMAND ----------

alter table Sales_table
set row filter udf_row_filter_sales on  (Store)

-- COMMAND ----------

select * from Sales_table;

-- COMMAND ----------

drop table if exists Sales_table;
create table if not exists Sales_table(Sale_id int, Sales_Person string,Store String,Amount float,Parttion_id int) partitioned by (Parttion_id)
with row filter udf_row_filter_sales on (Store);
insert into Sales_table values(1,'Ali','United States',1200),(2,'Ahmed','Canada',100),(3,'Sameen','Germany',1200)



-- COMMAND ----------

select * from sales_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Column Masking

-- COMMAND ----------

create function udf_mask_names(Store string)
return if(is_account_group_member(Store), Store, '*****')


-- COMMAND ----------

alter table Sales_table
alter column Store set Mask udf_mask_names;

-- COMMAND ----------

select * from sales_table;

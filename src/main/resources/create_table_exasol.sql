-- drop table dwh_manage.job_instance_status;
create table dwh_manage.job_instance_status (
   job_instance_id bigint identity primary key,
   process_instance_id bigint,
   process_instance_name varchar(255),
   job_name varchar(255) not null,
   job_project varchar(512) UTF8,
   job_info varchar(512) UTF8,
   job_display_name varchar(255) UTF8,
   job_guid varchar(100) UTF8 not null,
   job_ext_id varchar(255) UTF8,
   root_job_guid varchar(100) UTF8,
   work_item varchar(1024) UTF8,
   time_range_start timestamp,
   time_range_end timestamp,
   value_range_start varchar(512) UTF8,
   value_range_end varchar(512) UTF8,
   job_started_at timestamp not null,
   job_ended_at timestamp,
   job_result varchar(1024) UTF8,
   count_input integer,
   count_output integer,
   count_updated integer,
   count_rejected integer,
   count_deleted integer,
   return_code integer,
   return_message varchar(4000) UTF8,
   host_name varchar(255) UTF8,
   host_pid integer,
   host_user varchar(128) UTF8);

create table dwh_manage.job_instance_context (
    job_instance_id bigint not null,
    attribute_key varchar(255)  UTF8 not null,
    attribute_value varchar(1024) UTF8,
    attribute_type varchar(32) UTF8 not null,
    is_output_attr boolean not null);

create table dwh_manage.job_instance_counters (
    job_instance_id bigint not null,
    counter_name varchar(128) not null,
    counter_type varchar(10),
    counter_value integer not null);

create table dwh_manage.job_instance_logs (
   job_instance_id bigint not null,
   log_ts timestamp not null,
   log_name varchar(128) not null,
   log_level varchar(128) not null,
   log_message varchar(10000));
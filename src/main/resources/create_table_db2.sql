--drop table dwh_manage.job_instance_status;
create table dwh_manage.job_instance_status (
   job_instance_id bigint not null,
   process_instance_id bigint,
   process_instance_name varchar(255),
   job_name varchar(255) not null,
   job_project varchar(128),
   job_info varchar(512),
   job_display_name varchar(255),
   job_guid varchar(100) not null,
   job_ext_id varchar(255),
   root_job_guid varchar(100),
   work_item varchar(1024),
   time_range_start timestamp,
   time_range_end timestamp,
   value_range_start varchar(512),
   value_range_end varchar(512),
   job_started_at timestamp not null,
   job_ended_at timestamp,
   job_result varchar(1024),
   count_input integer,
   count_output integer,
   count_updated integer,
   count_updated integer,
   count_rejected integer,
   count_deleted integer,
   return_code integer,
   return_message varchar(1024),
   host_name varchar(255),
   host_pid integer,
   host_user varchar(128),
   constraint job_instances_pkey primary key (job_instance_id));

create index dwh_manage.job_instances_job_guid on dwh_manage.job_instance_status(job_guid);
create index dwh_manage.job_instances_job_name on dwh_manage.job_instance_status(job_name);

--drop sequence dwh_manage.job_instance_id;
create sequence dwh_manage.seq_job_instance_id start with 1;

--drop table dwh_manage.job_instance_counters;
create table dwh_manage.job_instance_counters (
    job_instance_id bigint not null,
    counter_name varchar(128) not null,
    counter_type varchar(20),
    counter_value integer not null);
    
create index job_instance_counters_idx on dwh_manage.job_instance_counters(job_instance_id, counter_name);
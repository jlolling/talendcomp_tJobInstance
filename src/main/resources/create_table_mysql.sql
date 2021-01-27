create table job_instance_status (
  job_instance_id bigint unsigned not null auto_increment,
  process_instance_id bigint default null,
  process_instance_name varchar(255) default null,
  job_name varchar(255) not null,
  job_project varchar(128),
  job_display_name varchar(255) default null,
  job_guid varchar(100) not null,
  job_ext_id varchar(255) default null,
  job_info varchar(255) default null,
  root_job_guid varchar(100) default null,
  work_item varchar(1024) default null,
  time_range_start datetime null default null,
  time_range_end datetime null default null,
  value_range_start varchar(512) default null,
  value_range_end varchar(512) default null,
  job_started_at datetime null default null,
  job_ended_at datetime null default null,
  job_result varchar(1024) default null,
  count_input integer default null,
  count_output integer default null,
  count_updated integer default null,
  count_rejected integer default null,
  count_deleted integer default null,
  return_code integer default null,
  return_message text,
  host_name varchar(255) default null,
  host_pid integer default null,
  host_user varchar(128) default null,
  primary key (job_instance_id)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

create index job_instance_status_job_guid on job_instance_status(job_guid);
create index job_instance_status_job_name on job_instance_status(job_name);

create table job_instance_counters (
    job_instance_id bigint not null,     -- reference to the job instance
    counter_name varchar(128) not null,  -- name of the counter set in tjobinstanceend for a counter
    counter_type varchar(20),            -- type of the counter
    counter_value integer,               -- value of the counter
    constraint pk_job_instance_counters primary key (job_instance_id, counter_name)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

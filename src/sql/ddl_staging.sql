--users
drop table if exists EVGENIYAREFEVYANDEXRU__STAGING.users;

create table EVGENIYAREFEVYANDEXRU__STAGING.users (
	 id int primary key
	,chat_name varchar(200)
	,registration_dt timestamp(0)
	,country varchar(200)
	,age int
)
order by id
segmented by hash(id) all nodes
;

--groups
drop table if exists EVGENIYAREFEVYANDEXRU__STAGING.groups;

create table EVGENIYAREFEVYANDEXRU__STAGING.groups (
	 id int primary key
	,admin_id int
	,group_name varchar(100)
	,registration_dt timestamp(6)
	,is_private int
)
order by id, admin_id
segmented by hash(id) all nodes
partition by registration_dt::date 
group by calendar_hierarchy_day(registration_dt::date, 3, 2)
;

--dialogs
drop table if exists EVGENIYAREFEVYANDEXRU__STAGING.dialogs;

create table EVGENIYAREFEVYANDEXRU__STAGING.dialogs (
	 message_id int primary key
	,message_ts timestamp
	,message_from int
	,message_to int
	,message varchar(1000)
	,message_type int
)
order by message_id
segmented by HASH(message_id) all nodes
partition by message_ts::date 
group by calendar_hierarchy_day(message_ts::date, 3, 2)
;

--group_log
drop table if exists EVGENIYAREFEVYANDEXRU__STAGING.group_log;

create table EVGENIYAREFEVYANDEXRU__STAGING.group_log (
	 group_id int 
	,user_id int
	,user_id_from int
	,event varchar
	,event_datetime timestamp(0)
)
order by group_id, user_id, event
segmented by hash(group_id) all nodes
partition by event_datetime::date 
group by calendar_hierarchy_day(event_datetime::date, 3, 2)
;




/********/
/* hubs */
/********/

--h_users
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.h_users cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.h_users
(
    hk_user_id bigint primary key
    ,user_id int
    ,registration_dt datetime
    ,load_dt datetime
    ,load_src varchar(20)
)
order by load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2)
;

--h_groups
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.h_groups cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.h_groups
(
    hk_group_id bigint primary key
    ,group_id int
    ,registration_dt datetime
    ,load_dt datetime
    ,load_src varchar(20)
)
order by load_dt
segmented by hk_group_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2)
;

--h_dialogs
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.h_dialogs cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.h_dialogs
(
     hk_message_id bigint primary key
    ,message_id int
    ,message_ts datetime
    ,load_dt datetime
    ,load_src varchar(20)
)
order by load_dt
segmented by hk_message_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2)
;

/*********/
/* links */
/*********/

--l_user_message
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.l_user_message cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.l_user_message
(
	hk_l_user_message bigint primary key
	,hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES EVGENIYAREFEVYANDEXRU__DWH.h_users (hk_user_id)
	,hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES EVGENIYAREFEVYANDEXRU__DWH.h_dialogs (hk_message_id)
	,load_dt datetime
	,load_src varchar(20)
)
order by load_dt
segmented by hk_user_id all nodes
partition by  load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2)
; 

--l_admins
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.l_admins cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.l_admins
(
	hk_l_admin_id bigint primary key
	,hk_user_id bigint not null CONSTRAINT fk_l_admins_user REFERENCES EVGENIYAREFEVYANDEXRU__DWH.h_users (hk_user_id)
	,hk_group_id bigint not null CONSTRAINT fk_l_admins_group_id REFERENCES EVGENIYAREFEVYANDEXRU__DWH.h_groups (hk_group_id)
	,load_dt datetime
	,load_src varchar(20)
)
order by load_dt
segmented by hk_l_admin_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2)
; 

--l_groups_dialogs
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.l_groups_dialogs cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.l_groups_dialogs
(
	hk_l_groups_dialogs bigint primary key
	,hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message REFERENCES EVGENIYAREFEVYANDEXRU__DWH.h_dialogs (hk_message_id)
	,hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_group_id REFERENCES EVGENIYAREFEVYANDEXRU__DWH.h_groups (hk_group_id)
	,load_dt datetime
	,load_src varchar(20)
)
order by load_dt
segmented by hk_l_groups_dialogs all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2)
; 

--l_user_group_activity
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.l_user_group_activity cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.l_user_group_activity (
	 hk_l_user_group_activity bigint primary key
	 ,hk_user_id bigint not null constraint fk__l_user_group_activity__user references EVGENIYAREFEVYANDEXRU__DWH.h_users (hk_user_id)
	 ,hk_group_id bigint not null constraint fk__l_user_group_activity__group references EVGENIYAREFEVYANDEXRU__DWH.h_groups (hk_group_id)
	 ,load_dt datetime
	 ,load_src varchar(20)
)
order by load_dt
segmented by hk_l_user_group_activity all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2)
;

/**************/
/* satellites */
/**************/

--s_admins
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.s_admins cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.s_admins
(
	hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES EVGENIYAREFEVYANDEXRU__DWH.l_admins (hk_l_admin_id)
	,is_admin boolean
	,admin_from datetime
	,load_dt datetime
	,load_src varchar(20)
)
order by load_dt
segmented by hk_admin_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

--s_user_socdem
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.s_user_socdem cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.s_user_socdem (
	 hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES EVGENIYAREFEVYANDEXRU__DWH.h_users (hk_user_id)
	,country varchar(200)
	,age int
	,load_dt datetime
	,load_src varchar(20)
)
order by load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);


--s_user_chatinfo
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.s_user_chatinfo cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.s_user_chatinfo (
	 hk_user_id bigint not null CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES EVGENIYAREFEVYANDEXRU__DWH.h_users (hk_user_id)
	,chat_name varchar(200)
	,load_dt datetime
	,load_src varchar(20)
)
order by load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

--s_group_private_status
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.s_group_private_status cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.s_group_private_status (
	 hk_group_id bigint not null CONSTRAINT fk_s_group_private_status_h_groups REFERENCES EVGENIYAREFEVYANDEXRU__DWH.h_groups (hk_group_id)
	,is_private int
	,load_dt datetime
	,load_src varchar(20)
)
order by load_dt
segmented by hk_group_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

--s_group_name
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.s_group_name cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.s_group_name (
	 hk_group_id bigint not null CONSTRAINT fk_s_group_name_status_h_groups REFERENCES EVGENIYAREFEVYANDEXRU__DWH.h_groups (hk_group_id)
	,group_name varchar(100)
	,load_dt datetime
	,load_src varchar(20)
)
order by load_dt
segmented by hk_group_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

--s_dialog_info
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.s_dialog_info cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.s_dialog_info (
	 hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES EVGENIYAREFEVYANDEXRU__DWH.h_dialogs (hk_message_id)
	,message varchar(1000)
	,message_from int
	,message_to int
	,load_dt datetime
	,load_src varchar(20)
)
order by load_dt
segmented by hk_message_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

--s_auth_history
drop table if exists EVGENIYAREFEVYANDEXRU__DWH.s_auth_history cascade;

create table EVGENIYAREFEVYANDEXRU__DWH.s_auth_history (
	  hk_l_user_group_activity int not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES EVGENIYAREFEVYANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity)
	  ,user_id_from int
	  ,event varchar
	  ,event_dt timestamp(0)
	  ,load_dt datetime
	  ,load_src varchar(20)
)
order by load_dt
segmented by hk_l_user_group_activity all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2)
;





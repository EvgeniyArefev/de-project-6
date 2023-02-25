/********/
/* hubs */
/********/

--h_users
insert into EVGENIYAREFEVYANDEXRU__DWH.h_users(hk_user_id,user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id
       ,id as user_id
       ,registration_dt
       ,now() as load_dt
       ,'s3' as load_src
from EVGENIYAREFEVYANDEXRU__STAGING.users
where hash(id) not in (select hk_user_id from EVGENIYAREFEVYANDEXRU__DWH.h_users); 

select * from EVGENIYAREFEVYANDEXRU__DWH.h_users limit 10;

--h_groups
insert into EVGENIYAREFEVYANDEXRU__DWH.h_groups (hk_group_id,group_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_group_id
       ,id as group_id
       ,registration_dt
       ,now() as load_dt
       ,'s3' as load_src
from EVGENIYAREFEVYANDEXRU__STAGING.groups  
where hash(id) not in (select hk_group_id from EVGENIYAREFEVYANDEXRU__DWH.h_groups); 

select * from EVGENIYAREFEVYANDEXRU__DWH.h_groups limit 10;

--h_dialogs
insert into EVGENIYAREFEVYANDEXRU__DWH.h_dialogs(hk_message_id,message_id,message_ts,load_dt,load_src)
select
       hash(message_id) as  hk_message_id
       ,message_id
       ,message_ts
       ,now() as load_dt
       ,'s3' as load_src
from EVGENIYAREFEVYANDEXRU__STAGING.dialogs 
where hash(message_id) not in (select hk_message_id from EVGENIYAREFEVYANDEXRU__DWH.h_dialogs); 

select * from EVGENIYAREFEVYANDEXRU__DWH.h_dialogs limit 10;

/*********/
/* links */
/*********/

--l_user_message
insert into EVGENIYAREFEVYANDEXRU__DWH.l_user_message(hk_l_user_message,hk_user_id,hk_message_id,load_dt,load_src)
select
	hash(hu.hk_user_id,hd.hk_message_id),
	hu.hk_user_id,
	hd.hk_message_id,
	now() as load_dt,
	's3' as load_src
from EVGENIYAREFEVYANDEXRU__STAGING.dialogs d
left join EVGENIYAREFEVYANDEXRU__DWH.h_users hu 
	on d.message_from = hu.user_id 
left join EVGENIYAREFEVYANDEXRU__DWH.h_dialogs hd 
	on d.message_id = hd.message_id 
where hash(hu.hk_user_id,hd.hk_message_id) not in (select hk_l_user_message from EVGENIYAREFEVYANDEXRU__DWH.l_user_message);

--l_admins
insert into EVGENIYAREFEVYANDEXRU__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
	hash(hg.hk_group_id,hu.hk_user_id),
	hg.hk_group_id,
	hu.hk_user_id,
	now() as load_dt,
	's3' as load_src
from EVGENIYAREFEVYANDEXRU__STAGING.groups as g
left join EVGENIYAREFEVYANDEXRU__DWH.h_users as hu 
	on g.admin_id = hu.user_id
left join EVGENIYAREFEVYANDEXRU__DWH.h_groups as hg 
	on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from EVGENIYAREFEVYANDEXRU__DWH.l_admins); 

--l_groups_dialogs
insert into EVGENIYAREFEVYANDEXRU__DWH.l_groups_dialogs (hk_l_groups_dialogs,hk_message_id,hk_group_id,load_dt,load_src)
select 
	hash(hd.hk_message_id, hg.hk_group_id)
	,hd.hk_message_id 
	,hg.hk_group_id 
	,now() as load_dt
	,'s3' as load_src
from EVGENIYAREFEVYANDEXRU__STAGING.dialogs d
left join EVGENIYAREFEVYANDEXRU__DWH.h_groups hg 
	on d.message_type = hg.group_id 
left join EVGENIYAREFEVYANDEXRU__DWH.h_dialogs as hd 
	on d.message_id = hd.message_id 
where 
	d.message_type is not null 
	and hash(hd.hk_message_id, hg.hk_group_id) not in (select hk_l_groups_dialogs from EVGENIYAREFEVYANDEXRU__DWH.l_groups_dialogs);

select * from EVGENIYAREFEVYANDEXRU__DWH.l_groups_dialogs limit 10;

--l_user_group_activity
insert into EVGENIYAREFEVYANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity,hk_user_id,hk_group_id,load_dt,load_src)
select distinct
	hash(hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity
	,hu.hk_user_id
	,hg.hk_group_id
	,now() as load_dt
    ,'s3' as load_src
from EVGENIYAREFEVYANDEXRU__STAGING.group_log as gl
left join EVGENIYAREFEVYANDEXRU__DWH.h_users as hu
	on gl.user_id = hu.user_id 
left join EVGENIYAREFEVYANDEXRU__DWH.h_groups as hg
	on gl.group_id = hg.group_id 
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from EVGENIYAREFEVYANDEXRU__DWH.l_user_group_activity); 

select * from EVGENIYAREFEVYANDEXRU__DWH.l_user_group_activity limit 10;

/**************/
/* satellites */
/**************/

--s_admins
merge into EVGENIYAREFEVYANDEXRU__DWH.s_admins tgt
using (
		select 
			 la.hk_l_admin_id as hk_admin_id
			,True as is_admin
			,hg.registration_dt as admin_from
			,now() as load_dt
			,'s3' as load_src
		from EVGENIYAREFEVYANDEXRU__DWH.l_admins as la
		left join EVGENIYAREFEVYANDEXRU__DWH.h_groups as hg 
			on la.hk_group_id = hg.hk_group_id
	) src
    on (
	    tgt.hk_admin_id = src.hk_admin_id
	    and tgt.is_admin = src.is_admin
	    and tgt.admin_from = src.admin_from
    )
when not matched 
  then insert(hk_admin_id, is_admin, admin_from, load_dt, load_src)
  values(src.hk_admin_id, src.is_admin, src.admin_from, src.load_dt, src.load_src);
 
select * from EVGENIYAREFEVYANDEXRU__DWH.s_admins limit 10;

--s_user_socdem
merge into EVGENIYAREFEVYANDEXRU__DWH.s_user_socdem tgt
using (
		select
			 hu.hk_user_id 
			,u.country 
			,u.age 
			,now() as load_dt
			,'s3' as load_src
		from EVGENIYAREFEVYANDEXRU__DWH.h_users hu
		left join EVGENIYAREFEVYANDEXRU__STAGING.users u 
			on hu.user_id = u.id
	) src
    on (
	    tgt.hk_user_id = src.hk_user_id
	    and tgt.country = src.country
	    and tgt.age = src.age
    )
when not matched 
  then insert(hk_user_id,country,age,load_dt,load_src)
  values(src.hk_user_id,src.country,src.age,src.load_dt,src.load_src);
 
select * from EVGENIYAREFEVYANDEXRU__DWH.s_user_socdem limit 10;
 	
--s_user_chatinfo
merge into EVGENIYAREFEVYANDEXRU__DWH.s_user_chatinfo tgt
using (
		select
			 hu.hk_user_id 
			,u.chat_name 
			,now() as load_dt
			,'s3' as load_src
		from EVGENIYAREFEVYANDEXRU__DWH.h_users hu
		left join EVGENIYAREFEVYANDEXRU__STAGING.users u 
			on hu.user_id = u.id
	) src
    on (
	   tgt.hk_user_id = src.hk_user_id
	   and tgt.chat_name = src.chat_name
    )
when not matched 
  then insert(hk_user_id,chat_name,load_dt,load_src)
  values(src.hk_user_id,src.chat_name,src.load_dt,src.load_src);
 
select * from EVGENIYAREFEVYANDEXRU__DWH.s_user_chatinfo limit 10;

--s_group_private_status
merge into EVGENIYAREFEVYANDEXRU__DWH.s_group_private_status tgt
using (
		select
			 hg.hk_group_id 
			,g.is_private 
			,now() as load_dt
			,'s3' as load_src
		from EVGENIYAREFEVYANDEXRU__DWH.h_groups hg
		left join EVGENIYAREFEVYANDEXRU__STAGING.groups g
			on hg.group_id  = g.id
	) src
    on (
	   tgt.hk_group_id = src.hk_group_id
	   and tgt.is_private = src.is_private
    )
when not matched 
  then insert(hk_group_id,is_private,load_dt,load_src)
  values(src.hk_group_id,src.is_private,src.load_dt,src.load_src);
 
select * from EVGENIYAREFEVYANDEXRU__DWH.s_group_private_status limit 10;
 
--s_group_name
merge into EVGENIYAREFEVYANDEXRU__DWH.s_group_name tgt
using (
		select
			 hg.hk_group_id 
			,g.group_name 
			,now() as load_dt
			,'s3' as load_src
		from EVGENIYAREFEVYANDEXRU__DWH.h_groups hg
		left join EVGENIYAREFEVYANDEXRU__STAGING.groups g
			on hg.group_id  = g.id
	) src
    on (
	   tgt.hk_group_id = src.hk_group_id
	   and tgt.group_name = src.group_name
    )
when not matched 
  then insert(hk_group_id,group_name,load_dt,load_src)
  values(src.hk_group_id,src.group_name,src.load_dt,src.load_src);
 
select * from EVGENIYAREFEVYANDEXRU__DWH.s_group_name limit 10;

--s_dialog_info
merge into EVGENIYAREFEVYANDEXRU__DWH.s_dialog_info tgt
using (
		select
			 hd.hk_message_id 
			,d.message 
			,d.message_from 
			,d.message_to 
			,now() as load_dt
			,'s3' as load_src
		from EVGENIYAREFEVYANDEXRU__DWH.h_dialogs hd 
		left join EVGENIYAREFEVYANDEXRU__STAGING.dialogs d 
			on hd.message_id  = d.message_id
	) src
    on (
    	tgt.hk_message_id = src.hk_message_id
	   and tgt.message = src.message 
	   and tgt.message_from = src.message_from
	   and tgt.message_to = src.message_to
    )
when not matched 
  then insert(hk_message_id,message,message_from,message_to,load_dt,load_src)
  values(src.hk_message_id,src.message,src.message_from,src.message_to,src.load_dt,src.load_src);
 
select * from EVGENIYAREFEVYANDEXRU__DWH.s_dialog_info limit 10;

--s_auth_history
merge into EVGENIYAREFEVYANDEXRU__DWH.s_auth_history tgt
using (
		select distinct
			luga.hk_l_user_group_activity
			,gl.user_id_from 
			,gl.event 
			,gl.event_datetime as event_dt
			,now() as load_dt
		    ,'s3' as load_src
		from EVGENIYAREFEVYANDEXRU__STAGING.group_log as gl
		left join EVGENIYAREFEVYANDEXRU__DWH.h_groups as hg 
			on gl.group_id = hg.group_id
		left join EVGENIYAREFEVYANDEXRU__DWH.h_users as hu 
			on gl.user_id = hu.user_id
		left join EVGENIYAREFEVYANDEXRU__DWH.l_user_group_activity as luga 
			on hg.hk_group_id = luga.hk_group_id 
			and hu.hk_user_id = luga.hk_user_id
	) src
    on (
	   tgt.hk_l_user_group_activity = src.hk_l_user_group_activity
	   and coalesce(tgt.user_id_from, 0) = coalesce(src.user_id_from, 0)
	   and tgt.event = src.event
	   and tgt.event_dt = src.event_dt
    )
when not matched 
  then insert(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
  values(src.hk_l_user_group_activity, src.user_id_from, src.event, src.event_dt, src.load_dt, src.load_src);
 
select * from EVGENIYAREFEVYANDEXRU__DWH.s_auth_history limit 10;
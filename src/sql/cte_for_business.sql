with 
user_group_messages as (
	select 
		 lgd.hk_group_id
		,count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
	from EVGENIYAREFEVYANDEXRU__DWH.l_groups_dialogs lgd 
	left join EVGENIYAREFEVYANDEXRU__DWH.l_user_message lum
		on lgd.hk_message_id = lum.hk_message_id
	group by lgd.hk_group_id 
),
user_group_log as (
    select 
		 luga.hk_group_id 
		,count(distinct luga.hk_user_id) as cnt_added_users
	from EVGENIYAREFEVYANDEXRU__DWH.s_auth_history sah 
	left join EVGENIYAREFEVYANDEXRU__DWH.l_user_group_activity luga 
		on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity 
	left join EVGENIYAREFEVYANDEXRU__DWH.h_groups as hg 
		on luga.hk_group_id = hg.hk_group_id
	where 
		sah.event = 'add'
		--10 самых ранних групп
		and hg.hk_group_id in (
		        select hk_group_id
		        from EVGENIYAREFEVYANDEXRU__DWH.h_groups
		        order by registration_dt
		        limit 10)
	group by luga.hk_group_id 
)

select 
	 ugl.hk_group_id
	,ugl.cnt_added_users
	,ugs.cnt_users_in_group_with_messages
	,(ugs.cnt_users_in_group_with_messages / ugl.cnt_added_users) as group_conversion
from user_group_log ugl
left join user_group_messages ugs 
	on ugl.hk_group_id = ugs.hk_group_id
order by group_conversion desc;

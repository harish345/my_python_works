status_update="""update alpl_asset_telemetry am
set status=lst.status,
leg_id=lst.leg_id
from
(select trl.trip_id,trl.leg_id,
case when frm = 'UN' and tol='DEPT' then 'AT UNIT'
when frm='DEPT' and tol='DEPT' then 'AT UNIT'
when frm<>'DEPT' and frm<>'FR' and frm<>'UN' and tol='DEPT' then 'TOWARDS UNIT'
when frm='FR' and tol='DEPT' then 'AT FACTORY'
when frm<>'ACT' and tol='FR' then 'TOWARDS FACTORY'
when frm='ACT' and tol='FR' then 'AT FACTORY'
when tol='UN' then 'TOWARDS UNIT'
when tol='RLULD' then 'TOWARDS CONSIGNEE'
when tol='RLLD' then 'TOWARDS CONSIGNEE'
when tol='RLLP' then 'TOWARDS LOADING POINT'
when tol = 'ACT' then toLoc
when tol='BUNK' then 'TOWARDS BUNK'
when tol='CO' then 'TOWARDS CONTROL POINT'
when tol='GDN' then 'TOWARDS GODOWN'
when tol='CONDES' then 'TOWARDS CONSIGNEE'
when tol='DLR' then 'TOWARDS DEALER'
else toLoc 
end as status,atr.cr
from(
select legs.leg_id,legs.distance,legs.trip_id,lt1.location_type frm,lt2.location_type tol,
l1.location_name as fromLoc,l2.location_name as toLoc
from alpl_leg_master lm
join (select distinct t.trip_id,t.leg_id,t.distance,t.arrival_time,t.departure_time from alpl_trip_route_legs_transaction t
join(select distinct alpl_trip_id,max(sequence_num) seq,max(arrival_time) art from alpl_trip_route_legs_transaction
where trip_id in(select trip_id from alpl_trip_data where is_completed=0)
and arrival_time is not null
group by 1) tt
on t.alpl_trip_id=tt.alpl_trip_id
and t.sequence_num=tt.seq
and t.arrival_time= tt.art order by 1) legs on legs.leg_id=lm.leg_master_id
left join alpl_location_master l1 on lm.from_location=l1.alpl_location_master_id
left join alpl_location_master l2 on lm.to_location=l2.alpl_location_master_id
left join alpl_location_type_master lt1 on l1.location_type_id=lt1.alpl_location_type_id
left join alpl_location_type_master lt2 on l2.location_type_id=lt2.alpl_location_type_id order by 3) trl
left join (select trip_id,max(created_time) cr from alpl_asset_telemetry
where trip_id in(select trip_id from alpl_trip_data where is_completed=0)
group by 1) atr on atr.trip_id = trl.trip_id
) lst
where am.trip_id=lst.trip_id
and am.created_time=lst.cr;"""


resting_update="""update alpl_driver_timesheet ts
set status = 'Resting',
timesheet_end_date = l.frm
from (Select alpl_driver_id,max(from_date) frm,max(to_date) tod
from alpl_driver_leaves_and_rest 
group by 1) l
where ts.alpl_driver_id=l.alpl_driver_id
and ts.timesheet_start_date<l.frm
and ts.timesheet_end_date is null;"""

end_date_update="""update alpl_driver_timesheet ts
set reported_date  = t.rept
from (select alpl_driver_id,max(timesheet_start_date) rept from alpl_driver_timesheet
group by 1) t
where t.alpl_driver_id = ts.alpl_driver_id
and ts.timesheet_end_date is not null
and date(t.rept)>=date(ts.timesheet_end_date)
and ts.reported_date is null;"""

commit_date_update="""update alpl_driver_timesheet ts
set commitment_date = timesheet_end_date + interval '9' day
where timesheet_end_date is not null
and commitment_date is null;"""
with 
master as(
select
  date(sdk_detect_ptime,'Asia/Tokyo') as date
  , extract(hour from sdk_detect_ptime at time zone 'Asia/Tokyo') as hour,
  CASE 
  WHEN EXTRACT(DAYOFWEEK FROM date(sdk_detect_ptime,'Asia/Tokyo'))=1 THEN "休日"
  WHEN EXTRACT(DAYOFWEEK FROM date(sdk_detect_ptime,'Asia/Tokyo'))=7 THEN "休日"
  ELSE "平日"
  END as daytype,
  SUBSTRING(SAFE_CAST(mesh as STRING), 1, 9) as mesh_code,
  hashed_adid
from
  `prd-analysis.master_v.sdk_master_table_bq` as a
inner join
  (select col_125mesh from `prd-analysis.master_v.master_mesh_address_stats` where pref = "愛知県") as b
on
  a.mesh = b.col_125mesh 
where
  date(sdk_detect_ptime,'Asia/Tokyo') = "2019-11-01"
  and log_type in ('vl','location')
)
, adid_rate as (
select
  master.date,
  master.hour,
  master.daytype,
  master.hashed_adid,
  master.mesh_code,
  ifnull(1/ins.install_rate, null) as rate
from
  master
inner join
  `prd-analysis.master_v.poi_li_monthly_hashed_adid`  as poili
on
  master.hashed_adid = poili.hashed_adid
  and date_trunc(master.date, month) = poili.date
inner join
  `prd-analysis.master_v.master_mesh_address_stats` as mmas
on
  safe_cast(poili.poi_home_mesh as string) = mmas.col_125mesh
inner join
  `prd-analysis.analysis_base_v.li_toukei_poi_id` as poiid
on
  mmas.pref = poiid.pref
  and mmas.city = poiid.city
inner join
  `prd-analysis.analysis_base_v.li_city_install_rate_day_sdk` as ins
on
  poiid.city_id = ins.city_id
  and master.date = ins.day
) 

select
  adid_rate.date,
  adid_rate.hour,
  adid_rate.daytype,
  adid_rate.mesh_code,
  count(distinct adid_rate.hashed_adid) as count,
  -- sum(rate) as statics_cnt
from
  adid_rate
group by
  1,2,3,4
order by
  hour

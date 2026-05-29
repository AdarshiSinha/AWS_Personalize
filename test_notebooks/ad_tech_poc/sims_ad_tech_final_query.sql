--Delivery query (non-default and product keys)
with mpv_prd_key_mapping as (
select recs_model_region as locale, m.mpv_id, m.product_key
from VISTAPRINT.PRODUCT.PRODUCT_MPV m
join dna.personalization.recs_country_region_mapping crm
on case 
    when m.country_code = 'GB' then 'UK' 
    else m.country_code end = crm.country
where m.merchant = 'VISTAPRINT'
and m.tenant_website = 'VP'
and sysdate() between m.valid_from and m.valid_to
and crm.country in ('US','CA','UK','FR','DE')),

product_list as (select s.locale, m1.product_key as source_product_key, m2.product_key as item_list, s.rank
from sandbox.dna_personalization_dev.sims_items_ad_tech s
join mpv_prd_key_mapping m1 on s.mpvid = m1.mpv_id
and s.locale = upper(m1.locale)
join mpv_prd_key_mapping m2 on s.itemlist = m2.mpv_id
and s.locale = upper(m2.locale)
where s.default_flag = 0 --exclude default list items & insufficent list length items
order by s.locale, m1.product_key, s.rank asc)


select locale, source_product_key, array_agg(item_list) within group(order by rank asc) AS bundle_items
from  product_list
group by locale, source_product_key;
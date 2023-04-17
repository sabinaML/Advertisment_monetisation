--1. Собрать витрину данных c показами и запросами на показ рекламы с детализацией по форматам рекламы, группой рекламного формата, поверхностью (модель устройства), 
--тип устройства (b2c/b2b2c), количество запросов, количество показов, количество кликов.

--нюанс: в отчете requests_report под видом устройства "Модель X" скрываются данные, которые относятся к двум видам устройств:
--Модель Х и Модель Х02. Модель Х - относится к группе  b2с, Модель Х02 - к группе  b2b2c.
--Необходимо распределить количество запросов по строке с  "Модель X" на две строки  Модель Х и Модель Х02 пропорционально DAU этих устройтсв  (количество дневной аудитории)

SELECT * FROM  aggregated_report

Таблица с запросами на показ рекламы (факт, сырые данные) requests_report:
date - дата
pad_id - первый идентификатор рекламного формата
block_id -  второй идентификатор рекламного формата
adv_format - наименование рекламного формата
eid2 - поверхность(модель устройства)
app_id - первый идентификатор смартаппа
space_id - второй идентификатор смартаппа
requests - количество запросов рекламы

Таблица с показами рекламы (факт, сырые данные) impressions_report:
date - дата
income_source - признак оплачиваемый формат или нет 
pad_id - первый идентификатор рекламного формата
block_id -  второй идентификатор рекламного формата
adv_format - наименование рекламного формата
flight_id - идентификатор рекламной кампании
campaign -  наименование рекламной кампании
eid2 - поверхность(модель устройства)
app_id - первый идентификатор смартаппа
space_id - второй идентификатор смартаппа
impressions -  количество показов
clicks - количество кликов

Справочник рекламных форматов adv_format_mapping:
pad_id - первый идентификатор рекламного формата
block_id -  второй идентификатор рекламного формата
adv_format1 - целевое наименование рекламного формата (для вывода на дашборд)
blick_inventar - наименование блока рекламных форматов (для вывода на дашборд)

Справочник моделей устройств model_mapping:
eid2 - модель устройства
model  - модель устройства (целевое наименование для дашборда)
surface_group - группа устройства (целевое наименование для дашборда)

Аудиторные показатели dm_monetization_analytics_team.xau_b2b2c:
server_received_date - дата
model  - модель устройства (целевое наименование для дашборда)
surface_group - группа устройства (целевое наименование для дашборда)
active_users - количество активных пользователей
metric - метрика (wau / dau/ mau)



--Необходимо распределить количество запросов по строке с  "Модель X" на две строки  Модель Х и Модель Х02 пропорционально DAU этих устройтсв  (количество дневной аудитории)
--Для этой задачи сопоставим каждой строке отчета requests_report  с Модель X аудиторные данные из таблицы dm_monetization_analytics_team.xau_b2b2c 
--Тогда мы получим что для строк с Модель Х у нас будет процент от дневной аудитории, на который мы умножим количество запросов.
--А для  Модель Х02 возьмем долю (1-pct) аудитоиии и умножим на количество запросов.
-- Результат отражен в CTE req 
INSERT INTO  aggregated_report (date1, format, pad_id, block_id, block_inventar1, bp_format1, eid22, 
surface_group1, app_idn, space_idn, requests, im_imp,  clicks, b2c_type)

WITH req_preprocessed AS ( 
SELECT s.*, pct FROM ( 
select date, pad_id, block_id, coalesce(app_id,'no data') as app_idn, coalesce(space_id,'no data') as space_idn, 
coalesce(model, 'no data') as model, coalesce(surface_group, 'no data') AS surface_group, sum(requests) as requests
from requests_report req LEFT JOIN model_mapping  p  on  req.eid2=p.eid2
group by date, pad_id, block_id, app_idn,space_idn,model, surface_group) s
LEFT JOIN 
(
SELECT *, active_users/cum_sum AS pct FROM ( 
select *, sum(active_users) OVER (PARTITION BY server_received_date) AS cum_sum from dm_monetization_analytics_team.xau_b2b2c 
WHERE metric = 'dau' AND model in ('Model X', 'Model X02')
)
) m 
ON s.date=m.server_received_date AND s.model=m.model
), 
req AS (
SELECT date, pad_id, block_id, app_idn,space_idn,model, surface_group, CASE when model = 'Model X' THEN requests*COALESCE(pct,1) ELSE requests END AS requests 
FROM req_preprocessed
UNION ALL
SELECT date, pad_id, block_id, app_idn,space_idn,'Model X02' AS model, 'Devices' AS surface_group, CASE when model = 'Model X' THEN requests*(1-COALESCE(pct,1)) ELSE requests END AS requests FROM req_preprocessed
WHERE model = 'Model X'),
-- приведем к общим наименованиям моделей в справочнике, агрегируем данные по дням
im AS (select date, pad_id, block_id, coalesce(app_id,'no data') as app_idn, coalesce(space_id,'no data') as space_idn, 
coalesce(model, 'no data') as model, coalesce(surface_group, 'no data') AS surface_group, sum(impressions) as im_imp, sum(clicks) as clicks
from impressions_report im LEFT JOIN model_mapping  p  on  im.eid2=p.eid2
group by date, pad_id, block_id, app_idn,space_idn,model, surface_group
), 
-- объединим данные по запросам и показам в одну таблицу, сопоставив по дням, моделям устройств, идентификаторам рекламных форматов, 
--идентификаторам смартапов
a AS ( 
select coalesce(req.date, im.date) as date1, coalesce(req.block_id, im.block_id) block_id, coalesce(req.pad_id , im.pad_id ) pad_id, 
coalesce(req.model, im.model)  as eid2m, coalesce(req.surface_group, im.surface_group)  AS surface_group,
coalesce(req.app_idn,  im.app_idn) app_idn, coalesce(req.space_idn,  im.space_idn) space_idn,
requests,  im_imp,clicks from  req full JOIN im on req.date=im.date and req.pad_id=im.pad_id and req.block_id=im.block_id  and req.model=im.model 
and req.app_idn=im.app_idn and req.space_idn=im.space_idn)
-- получаем итоговую таблицу с разбивкой на b2c/b2b2c, c целевыми наименованиями рекламных форматов
select date1,  
coalesce(adv_format1, 'no data') as format, 
a.pad_id, a.block_id, coalesce(block_inventar, 'no data') as block_inventar1, coalesce(m.bp_format, 'no data') as bp_format1,
coalesce(a.eid2m,  'no data') as eid22,  coalesce(a.surface_group, 'no data') as surface_group1, app_idn,space_idn,  
round(requests) requests,   im_imp, clicks,
CASE WHEN  a.eid2m = 'Model X02' THEN 'b2b2c' ELSE 'b2c' END AS b2c_type FROM  a 
left join adv_format_mapping m on a.pad_id=m.pad_id and a.block_id=m.block_id





/*2. Собрать витрину данных, из которой можно было бы построить дневные/недельные/месячные графики фактической и прогнозной выручки с детализацией по 
рекламным форматам, группам рекламных форматов, моделям устройств, идентификаторам рекламных кампаний, смартапов, типам устройств, дням (разместить все фильтры на дашборд).



Таблица  payment содержит фактические финансовые данные по открученным рекламным показам за прошлый месяц. 
period1 - месяц показа
number1 - формат рекламы
id_im - идентификатор рекламной кампании
rk_name - наименование рекламной кампании
gross_revenue - сумма выручки за рекламную кампанию

Таблица показов impressions_report содержит события с показами в рамках моделей устройств и форматов по дням.
Разрабатываемая витрина данных должна быть основана на таблице показов и содержать информацию по стоимости каждой строки (кол-во показов по формату и модели устройств).

Финансовая отчетность (Таблица  payment) поступает после 20-х чисел месяца, следующего за отчетным. 
Тк информация о сумме вознаграждения поступает с опозданием, требуется расчет прогнозных значений выручки и оборота
в зависимости от открученных показов (средствами  SQL)*/



insert into dm_monetization_analytics_team.com ( id, date , pad_id , block_id , adv_format   ,
eid2 , app_id , space_id , flight_id , income_source, campaign, loads , tru_imp , viewable_impressions , complete_views , clicks , datem,
measure , impressions , comm  , app_name , count_imp , gross_revenue , revenue , format ,
 block_inventar1 ,  model2 , surface_group1 , bp_format1 , developers , gross_rk , 
selleru , gross , to_seller , paying_impressions , sel, impressions_by_format,
s , q , av , pct_seller, coef_paying_impressions, cpm , pct_seller_last, coef_paying_impressions_last, revenue_f , developers_f ,
gross_f , to_seller_f , revenue_ff , developers_ff , to_seller_ff , gross_ff,sum_app,  client, b2c_type)

/*revenue_f (forecst) - пронозная выручка
revenue - фактическая выручка
revenue_ff - факт+ прогноз (там где есть факт - указывается факт. Если факта еще нет - то берется прогнозное значение)
аналогично для оборота и для выплат селлеру (рекламное агентство)*/

( 
select com.*,   null as client, CASE WHEN  model2 = 'Model X02' THEN 'b2b2c' ELSE 'b2c' END AS b2c_type from 
(select *, 
(coef_paying_impressions_last*cpm*impressions*(1-pct_seller_last))*comm as revenue_f, (coef_paying_impressions_last*cpm*impressions*(1-pct_seller_last))*(1-comm) as developers_f, 
coef_paying_impressions_last*cpm*impressions as gross_f,  
cpm*impressions*pct_seller_last as to_seller_f, coalesce(revenue, revenue_f) as revenue_ff,
coalesce(developers, developers_f) as developers_ff,
coalesce(to_seller, to_seller_f) as to_seller_ff, coalesce(gross, gross_f) as gross_ff
from ( 
select *, case when 
date>(select distinct last_value(month_end) over(order by end_date rows between unbounded preceding and unbounded following) 
from  payment 
) then
--находим прогнозную стоимость за единицу показа соответствующего рекламного формата (берем последнюю известную стоимость показа - за последний  месяц)
last_value(av) over(partition by format order by case when av is null then 0 else 1 end asc, date rows between unbounded preceding and unbounded following) end
as cpm,
case when 
date>(select distinct last_value(month_end) over(order by end_date rows between unbounded preceding and unbounded following) 
from  payment ) then
last_value(pct_seller) over(partition by format order by case when pct_seller is null then 0 else 1 end asc, date rows between unbounded preceding and unbounded following) end
as pct_seller_last,
-- если  income_source ='Com' или 'Special projects', то реклама оплачиваемая. Если нет - то селф-промо
case when income_source IN ('Com', 'Special projects') THEN 1 ELSE 0
end as coef_paying_impressions_last 
from ( 
select *, s/q as av, sel/s as pct_seller, case when impressions_by_format>0 then q/impressions_by_format end  as coef_paying_impressions from (
select drd.*, 
case when bnm.adv_format1 in ('Rewarded Video', 'Fullscreen(COM)') then 0.2
else 1 end as comm,
coalesce( drd.app_id, 'no data') as app_name,  count_imp, gross_revenue,
gross_revenue*comm/count_imp*impressions as revenue,
 coalesce(bnm.adv_format1, 'no data') as format, 
 coalesce(block_inventar, 'no data') as block_inventar1, 
 coalesce(m.model, 'no data')  as model2,
 m.surface_group as surface_group1, 
coalesce(bnm.bp_format, 'no data') as bp_format1,
gross_revenue*(1-comm)/count_imp*impressions  as developers, gross_RK, selleru, 
(gross_RK/count_imp)*impressions as gross,
(selleru/count_imp)*impressions as to_seller,
case when gross is not null then impressions when gross is  null 
and drd.date>(select max(month_end) from  payment) 
then impressions
when gross is  null 
and drd.date<=(select max(month_end) from  payment)
then 0 end as paying_impressions,
-- для каждого месяца берем сумму выручки/оборота/количества открученных оплаченных показов по рекламному формату
sum(to_seller)
over(partition by bnm.adv_format_com, date_trunc('month', drd.date) order by drd.date rows between unbounded preceding and unbounded following) as sel,
sum(impressions)
over(partition by bnm.adv_format_com, date_trunc('month', drd.date), measure  order by drd.date rows between unbounded preceding and unbounded following) 
as impressions_by_format,
sum(gross)
over(partition by bnm.adv_format_com, date_trunc('month', drd.date), measure order by drd.date rows between unbounded preceding and unbounded following) as s,
sum(paying_impressions)
over(partition by bnm.adv_format_com, date_trunc('month', drd.date), measure order by drd.date rows between unbounded preceding and unbounded following) as q--,
from --select * from 
--к таблице с показами добавляем единицу изменения ("показы", "100% досмотры")
(select  x.id, x.date, x.pad_id, x.block_id, x.adv_format, x.eid2, x.app_id, x.space_id, x.flight_id::text,  x.income_source, x.campaign, x.impressions as tru_imp, x.viewable_impressions,
x.complete_views, x.clicks, x.datem, y.measure, case when y.measure='100% досмотр' then x.complete_views else tru_imp end as impressions
from (select *, date_trunc('month',date) datem
from impressions_report) x left join complete_views y
on x.flight_id=y.flight_id  and x.datem=y.month_ 
) drd  
--добавляем столбец с корректными наименованиями моделей устройств
left join model_mapping m
on drd.eid2=m.eid2
--добавляем столбец с корректными наименованиями рекламных форматов
left join adv_format_mapping bnm
on bnm.pad_id=drd.pad_id and bnm.block_id=drd.block_id
--добавляем столбец с общим числом показов по каждой рекламной кампании и формату
left join
(select flight_id, adv_format_com, date_trunc('month',date) as date, sum(impressions) as count_imp
from 
(select x.date, x.pad_id, x.block_id, x.adv_format,  x.eid2, x.app_id, x.space_id, x.flight_id::text, x.income_source, x.campaign, x.loads, x.impressions as tru_imp, 
x.viewable_impressions,
x.complete_views, x.clicks, x.datem, y.measure, case when y.measure='100% досмотр' then x.complete_views else tru_imp end as impressions
from (select *, date_trunc('month',date) datem
from impressions_report) x left join complete_views y
on x.flight_id=y.flight_id  and x.datem=y.month_ )
im left join adv_format_mapping bn
on bn.pad_id=im.pad_id and bn.block_id=im.block_id
group by flight_id, adv_format_com, date_trunc('month',date))
p
on p.flight_id=drd.flight_id and p.date=drd.datem and p.adv_format_com=bnm.adv_format_com
left JOIN
--добавляем столбец с суммой оборота, выручки по рекламной кампании (ИД РК+формат+месяц)
(select id_im, number1, date_trunc('month',period1) as date, sum(gross_revenue) as gross_revenue, sum(amount) as gross_RK, sum(amount-gross_revenue) as selleru
from  payment
group by id_im, number1, date_trunc('month',period1))
q
on drd.flight_id=q.id_im  and q.date=drd.datem and bnm.adv_format_com=q.number1
)
)
)) com
)

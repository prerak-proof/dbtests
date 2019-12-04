---- quotes

drop table taq_quote;
create table taq_quote ENGINE = MergeTree()
partition by (toYYYYMMDD(ts))
order by (sym, tsnano)
as
select ifNull(ts, today()) as ts, ifNull(sym, '') as sym, toUInt64(ifNull(tsnano,0)) as tsnano, * from
mysql('172.31.92.90:3306', 'trades', 'taq_quote', 'root', 'test');

select count(*) from taq_quote;

alter table taq_quote drop column ".ts";
alter table taq_quote drop column ".sym";
alter table taq_quote drop column ".tsnano";
alter table taq_quote drop column "tsnano_end";
optimize table taq_quote final;

---- trades

drop table taq_trade;
create table taq_trade ENGINE = MergeTree()
partition by (toYYYYMMDD(ts))
order by (sym, tsnano)
as
select ifNull(ts, today()) as ts, ifNull(sym, '') as sym, toUInt64(ifNull(tsnano,0)) as tsnano, * from
mysql('172.31.93.224:3306', 'trades', 'taq_trade', 'root', 'test');

select count(*) from taq_trade;
select count(*) from taq_trade where sym='SPY';

alter table taq_trade drop column ".ts";
alter table taq_trade drop column ".sym";
alter table taq_trade drop column ".tsnano";
optimize table taq_trade final;


---- quotes by symbol

select substring(sym, 1,1), count(*) from taq_quote group by substring(sym, 1,1) with totals order by count(*) desc ;

---- vwap by minute for each symbol
-- finished in 1.3s
select count(*), sum(vol), sum(vwap) from (
  select sym, toStartOfMinute(ts) as hr, sum(sz) as vol, sum(sz * px) / sum(sz) as vwap
  from taq_trade
  group by sym, hr
  order by vol desc
)
;

---- trades and quotes as-of only for SPY
-- finishes in 1.3s!
select sym, count(*), sum(sz), avg(px), avg(if(ask_px>bid_px,ask_px-bid_px,null)) from (
    select t.sym, t.ts, t.tsnano, t.sz, t.px,
           q.ts, q.tsnano, ifNull(q.bid_px, 0) as bid_px, ifNull(q.ask_px, 1e6) as ask_px
        from (select * from taq_trade where sym='SPY') t
        asof join
        (select * from taq_quote where sym='SPY' and cond in ('A','B','H','O','R','W')) q
    on t.sym = q.sym and t.ex=q.ex and q.tsnano < t.tsnano
) m group by sym
;

---- trades and quotes for all symbols starting with S
-- finishes in 12s.
select sym, count(*), sum(sz), avg(px), avg(if(ask_px>bid_px,ask_px-bid_px,null)) from (
select t.sym, t.ts, t.tsnano, t.sz, t.px,
      q.ts, q.tsnano, ifNull(q.bid_px, 0) as bid_px, ifNull(q.ask_px, 1e6) as ask_px
from (select * from taq_trade where match(sym, '^S.*')) t
   asof join
(select * from taq_quote where match(sym, '^S.*') and cond in ('A','B','H','O','R','W')) q
on t.sym = q.sym and t.ex=q.ex and q.tsnano < t.tsnano
) m group by sym;

---- as-of for all symbols
-- (does not finish - runs out of memory)
select sym, count(*) c, sum(sz), avg(px), avg(if(ask_px>bid_px,ask_px-bid_px,null)) from (
select t.sym, t.tsnano, t.sz, t.px, q.tsnano, q.bid_px, q.ask_px
from
     (select * from taq_trade) t
     asof join
     (select * from taq_quote where cond in ('A','B','H','O','R','W')) q
on t.sym = q.sym and t.ex = q.ex and q.tsnano < t.tsnano
) m  group by sym
order by c desc
;

---- quote count per symbol
-- finished in 2.9s
select sym, count(*) c from taq_quote
group by sym;

---- quote count per first letter of symbol
-- finished in 4s
select substring(sym, 1,1) l, count(*) c from taq_quote
group by l;

---- OHLC
-- finished in 1.7s!
select sym, count(t), sum(c), min(f), max(l), max(mx), min(mn), sum(v) from (
select sym,
       toStartOfMinute(ts) t,
       count(*) c,
       argMin(px, tsnano) f,
       argMax(px, tsnano) l,
       max(px) mx,
       min(px) mn,
       sum(sz) v
from taq_trade
group by sym, t
) m group by sym;

---- time-weighted spread - completes in 49s (on console)
---- (the query is wrong - we should be sorting by sym, ex, tsnano, but it would be better if we had created the table
---- with that order)
---- if we try to sort by sym,ex,tsnano for real, the query does finish, it just takes 415s (~7min)
select sym, count(*), sum(quote_life*(ask_px-bid_px))/sum(quote_life) as ts from
(
    select *,
           if(sym=neighbor(sym, 1) and ex=neighbor(ex, 1),
              toInt64(neighbor(tsnano, 1)),
              toInt64(ceil(tsnano / 86400000000000) * 86400000000000)) as tsnano_end,
           (tsnano_end-tsnano) as quote_life
    from (
          select sym, tsnano, ask_px, bid_px
          from taq_quote
          where bid_px is not null
            and ask_px is not null
            and ask_px > bid_px
            and cond in ('A','B','H','O','R','W')
            and toTime(ts) >= ('1970-01-02 09:30:00') and toTime(ts) <= ('1970-01-02 16:00:00')
          order by sym, tsnano
         )
) group by sym
;


--- check if tables are optimized

select * from system.parts where table='taq_quote' and active in (1,0);
select * from system.parts where table='taq_trade' and active=1;

-- check system settings

select * from system.settings where name like '%merge%';

set partial_merge_join = 1;
set partial_merge_join_optimizations = 1;



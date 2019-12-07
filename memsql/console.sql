CREATE DATABASE IF NOT EXISTS trades;
USE trades;

-- -- trades

drop table if exists taq_trade;
create table taq_trade (
 ts datetime(6),
 tsnano bigint,
 ex char(1),
 sym varchar(12),
 cond char(4),
 sz int,
 px decimal(18,4),
 seq bigint,
 tid bigint,
 tsrc char(1),
 trf char(1),
 pts datetime(6),
 ptsnano bigint,
 trfts datetime(6),
 trftsnano bigint,
 tte tinyint,
 key(sym, tsnano) using CLUSTERED COLUMNSTORE,
 shard key(sym),
 key(ex) using hash
);

LOAD DATA INFILE '/tmp/prerak/EQY_US_ALL_TRADE_20180730.gz'
INTO TABLE taq_trade 
FIELDS TERMINATED BY '|'
(@ts, ex, sym, cond, sz, px, @, @, seq, tid, tsrc, trf, @pts, @trfts, tte, @)
trailing nullcols
ignore 1 lines
set ts=taq_ts_dt('20180730', @ts), pts=taq_ts_dt('20180730', @pts), trfts=taq_ts_dt('20180730', @trfts),
tsnano=taq_ts_long('20180730', @ts), ptsnano=taq_ts_long('20180730', @pts), trftsnano=taq_ts_long('20180730', @trfts)
WHERE @ts<>'END';

select count(*) from taq_trade;
-- optimize
optimize table taq_trade;

delimiter //
create or replace function taq_ts_long(load_date TEXT, ts text) returns bigint
as
declare 
   dt datetime(6);
   nanos bigint;
BEGIN
   dt = to_timestamp(concat(load_date, if(ts='END', '', ts)), 'YYYYMMDDHH24MISSFF9');
   nanos = if(length(ts)=15, substring(ts, -3, 3), 0);
   return unix_timestamp(dt) * 1000000000 + nanos;
end //
delimiter ;

select taq_ts_long('20180730', '091940731349000');
select taq_ts_long('20180730', '');
select taq_ts_long('20180730', 'END');

delimiter //
create or replace function taq_ts_dt(load_date TEXT, ts text) returns datetime(6)
as
BEGIN
    return to_timestamp(concat(load_date, if(ts='END', '', ts)), 'YYYYMMDDHH24MISSFF9');
END //
delimiter ;

select taq_ts_dt('20180730', '091940731349000');
select taq_ts_dt('20180730', '');
select taq_ts_dt('20180730', 'END');

-- -- quotes

drop table if exists taq_quote;
create table taq_quote (
 ts datetime(6),
 tsnano bigint,
 tsnano_end bigint,
 ex char(1),
 sym varchar(12),
 bid_px decimal(18,4),
 bid_sz int,
 ask_px decimal(18,4),
 ask_sz int,
 cond char(1),
 seq bigint,
 nbbo char(1),
 qsrc char(1),
 ri char(1),
 ssr char(1),
 bbo_luld char(1),
 sip_gen tinyint,
 nbbo_luld char(1),
 pts datetime(6),
 ptsnano bigint,
 ss tinyint,
 key(sym, tsnano, tsnano_end) using CLUSTERED COLUMNSTORE,
 shard key(sym),
 key(ex) using hash
);

LOAD DATA INFILE '/tmp/prerak/SPLITS_US_ALL_BBO_Z_20180730.gz'
INTO TABLE taq_quote
FIELDS TERMINATED BY '|'
(@ts, ex, sym, bid_px, bid_sz, ask_px, ask_sz, cond, seq, nbbo, @, @, @, qsrc, ri, ssr, bbo_luld, sip_gen, nbbo_luld, @pts, @, @, ss)
trailing nullcols
ignore 1 lines
set ts=taq_ts_dt('20180730', @ts), pts=taq_ts_dt('20180730', @pts),
tsnano=taq_ts_long('20180730', @ts), tsnano_end=NULL, ptsnano=taq_ts_long('20180730', @pts)
WHERE @ts<>'END';

-- populate the end time for each quote
update taq_quote set tsnano_end=NULL;
update taq_quote q 
join (select sym, ex, tsnano, lead(tsnano) over (partition by sym, ex order by tsnano) as nextts from taq_quote where tsnano_end is null) v
on q.sym = v.sym and q.tsnano = v.tsnano and q.ex=v.ex
set tsnano_end = nextts
where nextts is not null;

-- populate quotes with no end time to end of day
update taq_quote set tsnano_end = ceil(tsnano/86400000000000)*86400000000000 where tsnano_end is null;

-- check that tsnano_end worked
select * from taq_quote where sym='BAC' order by tsnano desc limit 10;

-- how many quotes per sym
-- finishes in ~6s for 350M quotes, 7.5s for 720M quotes
select substring(sym,1,1), count(*) from taq_quote group by 1 order by 2 desc;

select count(*) from taq_quote;
-- optimize
optimize table taq_quote;

-- --

set session sql_select_limit = 1000000000;

-- -- -- AS-OF join attempts

-- actually invokes merge join (only if sym is in columstore key)
-- finished for all trades (35.5M) with only "A"/"B" quotes loaded (80M) in 47.05min
-- finished for all trades (35.5M) with "A"-"E" quotes loaded (196M) in 5.76hrs
select sym, count(*) from (
select t.sym, t.ts, t.sz, t.px, q.ts as qts, q.bid_px, q.ask_px from taq_trade t straight_join taq_quote q
on t.sym=q.sym and q.tsnano < t.tsnano and t.tsnano <= q.tsnano_end
) t
where qts is not null
group by sym;

-- merge using ex as well
-- finished for all trades (35.5M) for all symbols (720M) in 6.93hrs
-- (sounds like this is just proportional to what the heaviest symbol takes)
select sym, count(*) from (
select t.sym, t.ts, t.ex, t.sz, t.px, q.ts as qts, q.bid_px, q.ask_px from taq_trade t straight_join taq_quote q
on t.sym=q.sym and q.tsnano < t.tsnano and t.tsnano <= q.tsnano_end and t.ex=q.ex
) m
where qts is not null
group by sym;

-- finished for just BAC (150K of 35.5M trades) with only "A"/"B" quotes loaded (80M) in 21.76min
-- finished for just BAC (150K of 35.5M trades) with "A"-"E" quotes loaded (196M) in 21.8min
-- finished for just BAC (150K of 35.5M trades) with "A"-"Z" quotes loaded (719.5M) in 19.46min
select count(*) from (
select t.sym, t.ts, t.sz, t.px, q.ts as qts, q.bid_px, q.ask_px from taq_trade t straight_join taq_quote q
on t.sym = q.sym and q.tsnano < t.tsnano and t.tsnano <= q.tsnano_end
where t.sym='BAC' 
) m;

-- merge using ex as well
-- finished for just BAC (150K of 35.5M trades) with "A"-"Z" quotes loaded (719.5M) in 16.19min
profile
select count(*) from (
select t.sym, t.ts, t.ex, t.sz, t.px, q.ts as qts, q.bid_px, q.ask_px from taq_trade t
    straight_join taq_quote q
on t.sym=q.sym and q.ex=t.ex and q.tsnano < t.tsnano and t.tsnano <= q.tsnano_end
where t.sym = 'BAC'
) m;
show profile json;
show profile;

-- another (dumb) way to do as-of join
-- 4.2 hrs
select count(*), sum(sz), avg(ifnull(ask, 420000)-ifnull(bid, 0)) from (
     select t.sym, t.ts, t.ex, t.sz, t.px,
            (select ts from taq_quote where sym=t.sym and ex=t.ex and tsnano<t.tsnano order by tsnano desc limit 1) as qts,
            (select bid_px from taq_quote where sym=t.sym and ex=t.ex and tsnano<t.tsnano order by tsnano desc limit 1) as bid,
            (select ask_px from taq_quote where sym=t.sym and ex=t.ex and tsnano<t.tsnano order by tsnano desc limit 1) as ask
     from taq_trade t
     where t.sym='BAC') m
;

-- vol/vwap per hour for all symbols
-- finished in 2.03s
select sum(vol), avg(vwap) from (
select sym, date_trunc('minute', ts) as hr, sum(sz) as vol, sum(sz*px)/sum(sz) as vwap from taq_trade
group by 1, 2
order by 3 desc
) m;

-- optimization attempt #2 - shard by sym/ex
drop table taq_quote2;
create table taq_quote2 (
    ts datetime(6),
    tsnano bigint,
    tsnano_end bigint,
    ex char(1),
    sym varchar(12),
    bid_px decimal(18,4),
    bid_sz int,
    ask_px decimal(18,4),
    ask_sz int,
    cond char(1),
    seq bigint,
    nbbo char(1),
    qsrc char(1),
    ri char(1),
    ssr char(1),
    bbo_luld char(1),
    sip_gen tinyint,
    nbbo_luld char(1),
    pts datetime(6),
    ptsnano bigint,
    ss tinyint,
    key(sym, ex, tsnano, tsnano_end) using CLUSTERED COLUMNSTORE,
    shard key(sym, ex),
    key(ex) using hash
);
insert into taq_quote2 select * from taq_quote;
optimize table taq_quote2;

drop table taq_trade2;
create table taq_trade2 (
   ts datetime(6),
   tsnano bigint,
   ex char(1),
   sym varchar(12),
   cond char(4),
   sz int,
   px decimal(18,4),
   seq bigint,
   tid bigint,
   tsrc char(1),
   trf char(1),
   pts datetime(6),
   ptsnano bigint,
   trfts datetime(6),
   trftsnano bigint,
   tte tinyint,
   key(sym, ex, tsnano) using CLUSTERED COLUMNSTORE,
   shard key(sym, ex),
   key(ex) using hash
);
insert into taq_trade2 select * from taq_trade;
optimize table taq_trade2;

-- result = 118814 in 6.88 min with join
-- result = 118814 in 6.66 min with straight_join
-- result = 158339 in 8.51 min with left join
-- (seems to be related to better partitioning, rather than improved index lookup or scan)
select count(*) from (
select t.sym, t.ts, t.ex, t.sz, t.px, q.ts as qts, q.bid_px, q.ask_px from taq_trade t left join taq_quote2 q
on t.sym=q.sym and q.ex=t.ex and q.tsnano < t.tsnano and t.tsnano <= q.tsnano_end
where t.sym='BAC'
) m;

-- try a procedural way
-- takes an hour just for BAC
delimiter //
create or replace procedure trades_asof_quotes(sym_in text) as
declare
    q QUERY(tsnano bigint, sz int, ex char(1)) = select tsnano, sz, ex from taq_trade2 where sym=sym_in;
    a ARRAY(RECORD(tsnano bigint, sz int, ex char(1)));
    _tsnano bigint;
    _sz int;
    _ex char(1);
BEGIN
    drop table if exists r;
    create temporary table r (ex char(1), tsnano bigint, qtsnano bigint, sz int, bid_px decimal(18,4), ask_px decimal(18,4));

    a = COLLECT(q);
    -- echo select count(*) from q;
    for x in a loop
        _tsnano = x.tsnano;
        _sz = x.sz;
        _ex = x.ex;
        insert into r
            select ex, _tsnano, tsnano, _sz, bid_px, ask_px
            from taq_quote2
            where sym=sym_in and ex=_ex and tsnano < _tsnano
            order by tsnano desc limit 1;
    end loop;
    -- echo select count(*) from r;
    echo select sym_in as sym, ex, count(1) as cnt, sum(sz) as vol, avg(if(ask_px > 0 and bid_px>0, ask_px-bid_px, 0.01)) as spread from r group by 2 order by 3 desc;
    drop table r;
end //
delimiter ;
call trades_asof_quotes('BAC');

-- -- this actually does a MergeJoin (verified using profile)
-- finishes in 52s!
select ex, sum(sz), avg(if(ask_px > 0 and bid_px>0, ask_px-bid_px, 0.01)) from (
    select t.sym, t.ts, t.ex, t.sz, t.px, q.ts as qts, q.bid_px, q.ask_px from taq_trade2 t straight_join taq_quote2 q
        on t.sym=q.sym and q.ex=t.ex and q.tsnano < t.tsnano and t.tsnano <= q.tsnano_end
    where t.sym='BAC'
) m group by 1 order by 3;

-- finished for ALL symbols in 1hr
select sym, ex, sum(sz), avg(if(ask_px > 0 and bid_px>0, ask_px-bid_px, 0.01)) from (
    select t.sym, t.ts, t.ex, t.sz, t.px, q.ts as qts, q.bid_px, q.ask_px from taq_trade2 t straight_join taq_quote2 q
    on t.sym=q.sym and q.ex=t.ex and q.tsnano < t.tsnano and t.tsnano <= q.tsnano_end
) m group by 1, 2 order by 3 desc;

-- -- count per symbol
-- finished in 238 ms
select sym, count(*) c from taq_quote2
group by 1;

-- -- count per first letter of symbol (doing a direct group by on substring(sym,1,1) is quite slow)
-- finished in 265 ms
select substring(sym, 1, 1), sum(c) c from (
   select sym, count(*) c from taq_quote2
   group by 1
) m group by 1;

-- -- OHLC
-- finished in 3.88s
select sym, count(t), sum(c), min(f), max(l), max(mx), min(mn), sum(v) from (
  select sym,
         time_bucket('1m', ts) t,
         count(*) c,
         first(px, ts) f,
         last(px, ts) l,
         max(px) mx,
         min(px) mn,
         sum(sz) v
  from taq_trade
  group by 1, 2
) m group by 1;

-- -- time-weighted spread
-- completes in 45.87s
select sym, count(*), avg(ask_px-bid_px) as avgspread, sum((tsnano_end - tsnano)*(ask_px-bid_px))/sum(tsnano_end-tsnano) as twspread from taq_quote2
where bid_px is not null and ask_px is not null and ask_px>bid_px
and cond in ('A','B','H','O','R','W')
and time(ts) >= ('09:30:00') and time(ts) <= ('16:00:00')
and sym='AMD'
group by 1;


-- -- time-weighted spread without using tsnano_end
-- does not complete (runs out of memory after about a minute)
select sym, count(*), avg(spread) as avgspread, sum(quote_life*spread)/sum(quote_life) as twspread from (
select sym, (ask_px-bid_px) as spread, (LEAD(tsnano) over w)-tsnano as quote_life from taq_quote2
where sym = 'AMD'
  and bid_px is not null and ask_px is not null and ask_px>bid_px
  and cond in ('A','B','H','O','R','W')
  and time(ts) >= ('09:30:00') and time(ts) <= ('16:00:00')
window w as (partition by sym, ex order by tsnano)
) m group by 1;


-- -- time weighted spread
-- -- procedural approach to avoid running out of memory (do one letter at a time)
delimiter //
create or replace procedure calc_twspread() as
declare
begin
    drop table if exists r;
    create temporary table r(sym varchar(12), twspread decimal(18,4));

    for x in 65..90 loop
        insert into r
        select sym, sum(quote_life*spread)/sum(quote_life) as twspread from (
            select sym, (ask_px-bid_px) as spread, (LEAD(tsnano) over w)-tsnano as quote_life from taq_quote2
            where sym like concat(char(x),'%')
              and bid_px is not null and ask_px is not null and ask_px>bid_px
              and cond in ('A','B','H','O','R','W')
              and time(ts) >= ('09:30:00') and time(ts) <= ('16:00:00')
                window w as (partition by sym, ex order by tsnano)
        ) m group by 1;
    end loop;

    echo select * from r;
    drop table r;
end //
delimiter ;

-- finishes in ~2min
set sql_select_limit=1000000000;
call calc_twspread()
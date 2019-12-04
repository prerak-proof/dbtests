CREATE DATABASE IF NOT EXISTS trades;
USE trades;

DROP PIPELINE IF EXISTS companylist;
DROP TABLE IF EXISTS company;
CREATE TABLE company(
    symbol CHAR(5) NOT NULL,
    name VARCHAR(500),
    last_sale VARCHAR(10),
    market_cap VARCHAR(15),
    IPO_year FLOAT,
    sector VARCHAR(80),
    industry VARCHAR(80),
    summary_quote VARCHAR(50),
    extra VARCHAR(50)
);

CREATE or REPLACE PIPELINE companylist
AS LOAD DATA S3 'download.memsql.com/first-time/'
CONFIG '{"region": "us-east-1"}'
INTO TABLE `company`
FIELDS TERMINATED BY ',' ENCLOSED BY '"';
START PIPELINE companylist FOREGROUND;


DROP TABLE IF EXISTS trade;
CREATE TABLE trade(
    id BIGINT NOT NULL, 
    stock_symbol CHAR(5) NOT NULL,
    shares DECIMAL(18,4) NOT NULL,
    share_price DECIMAL(18,4) NOT NULL,
    trade_time DATETIME(6) NOT NULL,
    KEY(stock_symbol, trade_time) USING CLUSTERED COLUMNSTORE,
    SHARD KEY(stock_symbol)
);

DELIMITER //
CREATE OR REPLACE PROCEDURE seed_trades(num_trades INT) RETURNS INT AS
DECLARE
    q QUERY(symbol CHAR(5), _rank INT) =
        SELECT symbol, rank() OVER (ORDER BY market_cap) AS _rank
        FROM company
        WHERE LENGTH(symbol) < 5
        ORDER BY _rank DESC LIMIT 100;
    i INT = 0;
    sym CHAR(5);
    price_base DECIMAL(18,4);
    ts datetime(6);
    l ARRAY(RECORD(symbol CHAR(5), _rank INT));
    btch ARRAY(RECORD(id int, sym char(5), sz decimal(18,4), px decimal (18,4), ts datetime(6)));
    x int;
BEGIN
    l = collect(q);
    ts = date_add(current_date(), interval 570 minute);
    btch = create_array(1 + (num_trades DIV LENGTH(l)));
    FOR r IN l LOOP
        sym = r.symbol;
        price_base = FLOOR(rand() * 50) + 50;
        FOR j IN 0..(LENGTH(btch) - 1) LOOP
            i += 1;
            price_base += (RAND() - 0.5) * 0.1;  -- move price around by 5 cents
            btch[j] = row(i,sym,FLOOR(1 + RAND() * 10) * 100,price_base,date_add(ts, interval 1000000*((j + RAND() - 0.5) MOD 23400) MICROSECOND));
        END LOOP;
        x = INSERT_ALL("trade", btch);
        IF i >= num_trades THEN RETURN(i); END IF;
    END LOOP;
    RETURN(i);
END //
DELIMITER ;


call seed_trades(1000000);

describe trade;
delete from trade;
select * from trade;


----------------------------------------------------------------------------
DROP TABLE IF EXISTS quote;
CREATE TABLE quote(
    id BIGINT NOT NULL, 
    stock_symbol CHAR(5) NOT NULL,
    bid_size DECIMAL(18,4) NOT NULL,
    bid_price DECIMAL(18,4) NOT NULL,
    ask_price DECIMAL(18,4) NOT NULL,
    ask_size DECIMAL(18,4) NOT NULL,
    quote_time DATETIME(6) NOT NULL,
    KEY(stock_symbol, quote_time) USING CLUSTERED COLUMNSTORE,
    SHARD KEY(stock_symbol),
    KEY(id) using hash
);


DELIMITER //
CREATE OR REPLACE PROCEDURE seed_quotes(num_quotes INT, batch_size INT) RETURNS INT AS
DECLARE
    q QUERY(symbol CHAR(5)) =
        SELECT distinct stock_symbol
        FROM trade;
    i INT = 0;
    sym CHAR(5);
    price_base DECIMAL(18,4);
    ts datetime(6);
    l ARRAY(RECORD(symbol CHAR(5)));
    btch ARRAY(RECORD(id int, sym char(5), 
        bsz decimal(18,4), bpx decimal (18,4), asz decimal(18,4), apx decimal(18,4),
        ts datetime(6)));
    num_batches int;
    x int;
BEGIN
    l = collect(q);
    ts = date_add(current_date(), interval 570 minute);
    btch = create_array(batch_size);
    FOR r IN l LOOP
        sym = r.symbol;
        price_base = FLOOR(rand() * 50) + 50;
        num_batches = 1 + (num_quotes DIV (LENGTH(l) * batch_size));
        FOR j IN 0..(num_batches-1) LOOP
            price_base += (RAND() - 0.5) * 0.1;  -- move price around by 5 cents
            FOR k in 0..(batch_size-1) LOOP
                i += 1;
                btch[k] = row(i,sym,
                    FLOOR(1 + RAND() * 10) * 100, price_base-0.01, FLOOR(1 + RAND() * 10) * 100, price_base+0.01, 
                    date_add(ts, interval 1000000*(((j*batch_size + k) + RAND() - 0.5) MOD 23400) MICROSECOND));
            END LOOP;
            x = INSERT_ALL("quote", btch);
        END LOOP;
        IF i >= num_quotes THEN RETURN(i); END IF;
    END LOOP;
    RETURN(i);
END //
DELIMITER ;

call seed_quotes(10000000, 10000);
select count(*) from quote;
delete from quote;

----------------------------------



--- finished in 2.39 min
select count(*) from (
select et.*, q.quote_time, q.bid_price, q.ask_price from 
    (select id as tid, stock_symbol, trade_time, 
        (select id from quote q where t.stock_symbol=q.stock_symbol and q.quote_time < t.trade_time order by quote_time desc limit 1) as qid
         from trade t where stock_symbol='ORG') et 
    left join quote q on et.qid=q.id and et.stock_symbol=q.stock_symbol
    )
;

--- finished in 56.56 min
select count(*) from (
select et.*, q.bid_price, q.ask_price from 
    (select id as tid, stock_symbol, trade_time, 
        (select id from quote q where t.stock_symbol=q.stock_symbol and q.quote_time < t.trade_time order by quote_time desc limit 1) as qid
         from trade t) et 
    left join quote q on et.qid=q.id and et.stock_symbol=q.stock_symbol
    )
;

-- finished in 34.56 sec
select count(*) from (
select t.shares, t.share_price, t.trade_time, q.quote_time, q.bid_price, q.ask_price from trade t left join 
(select stock_symbol, quote_time, bid_price, ask_price, lead(quote_time) over (partition by stock_symbol order by quote_time) as next_quote_time from quote) q
on t.stock_symbol=q.stock_symbol and q.quote_time < t.trade_time and (t.trade_time <= q.next_quote_time or q.next_quote_time is null)
where t.stock_symbol='ORG'
) m;


-- finished in 9.57 min
select count(*) from (
select t.shares, t.share_price, t.trade_time, q.quote_time, q.bid_price, q.ask_price from trade t left join 
(select stock_symbol, quote_time, bid_price, ask_price, lead(quote_time) over (partition by stock_symbol order by quote_time) as next_quote_time from quote) q
on t.stock_symbol=q.stock_symbol and q.quote_time < t.trade_time and (t.trade_time <= q.next_quote_time or q.next_quote_time is null)
) m;

-- finished for 1 symbol in 45 seconds 
DELIMITER //
create or replace procedure get_quotes_asof2() 
as 
DECLARE
  q_trades QUERY(tid int, sym char(5), tts datetime(6)) = 
      select id, stock_symbol, trade_time from trade where stock_symbol='ORG';
  aa ARRAY(RECORD(tid int, sym char(5), tts datetime(6)));
  r ARRAY(RECORD(sym char(5), tid int, tts datetime(6), qid int, qts datetime(6)));
  sym char(5);
  tid int;
  tts datetime(6);
  i int = 0;
BEGIN
    drop table if exists myres;
    create temporary table myres(sym char(5), tid int, tts datetime(6), qid int, qts datetime(6));

    aa = collect(q_trades);
    r = create_array(length(aa));
    for x in aa LOOP
        sym = x.sym;
        tid = x.tid;
        tts = x.tts;
        insert into myres
            select sym, tid, tts, id, quote_time from quote where stock_symbol=sym and quote_time < tts order by quote_time desc limit 1;
    end loop;

    echo select count(*) from myres;
    drop table myres;
END //
DELIMITER ;

set session sql_select_limit = 1000000000;
call get_quotes_asof2();

------------------

-- finished in 790ms
select from_unixtime((unix_timestamp(trade_time) DIV 60)*60) as time, stock_symbol as sym,
       sum(shares) as vol, sum(shares * share_price) / sum (shares) as vwap from trade
group by stock_symbol, (unix_timestamp(trade_time) DIV 60)*60
order by vol desc, time;


----------------------

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
delete from taq_trade;
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

---------------------------

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

-- populate the end time for each quote (i know, this should probably be done either by exchange, or after calculating NBBO)
update taq_quote set tsnano_end=NULL;
update taq_quote q 
join (select sym, ex, tsnano, lead(tsnano) over (partition by sym, ex order by tsnano) as nextts from taq_quote where tsnano_end is null) v
on q.sym = v.sym and q.tsnano = v.tsnano and q.ex=v.ex
set tsnano_end = nextts
where nextts is not null;

-- check what's up
select sym, count(*), min(ts) from taq_quote where tsnano_end is null  group by rollup(sym);

-- populate quotes with no end time to end of day
update taq_quote set tsnano_end = ceil(tsnano/86400000000000)*86400000000000 where tsnano_end is null;

-- check that tsnano_end worked
select * from taq_quote where sym='BAC' order by tsnano desc limit 10;

-- how many quotes per sym
-- finishes in ~6s for 350M quotes, 7.5s for 720M quotes
select distinct substring(sym,1,1), count(*) from taq_quote group by 1 order by 2 desc;

select count(*) from taq_quote;
select * from taq_quote;
delete from taq_quote;
-- optimize
optimize table taq_quote;

------------------------------------------------

select count(*) from taq_trade where sym='BAC';
select count(*) from taq_quote where sym='BAC';

set session sql_select_limit = 1000000000;

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
profile;
select count(*) from (
select t.sym, t.ts, t.ex, t.sz, t.px, q.ts as qts, q.bid_px, q.ask_px from taq_trade t
    straight_join taq_quote q
on t.sym=q.sym and q.ex=t.ex and q.tsnano < t.tsnano and t.tsnano <= q.tsnano_end
where t.sym = 'BAC'
) m;
show profile json;
show profile;


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

-- optimization attempt #2
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

-- finishes in 59s!
select count(*) from (
    select t.sym, t.ts, t.ex, t.sz, t.px, q.ts as qts, q.bid_px, q.ask_px from taq_trade2 t straight_join taq_quote2 q
    on t.sym=q.sym and q.ex=t.ex and q.tsnano < t.tsnano and t.tsnano <= q.tsnano_end
    where t.sym='BAC'
 ) m;

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

-- finished in 238 ms
select sym, count(*) c from taq_quote2
group by 1;

-- finished in 265 ms
select substring(sym, 1, 1), sum(c) c from (
   select sym, count(*) c from taq_quote2
   group by 1
) m group by 1;

-- OHLC - finished in 3.88s
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

-- time-weighted spread - completes in 45.87s
select sym, count(*), avg(ask_px-bid_px), sum((tsnano_end - tsnano)*(ask_px-bid_px))/sum(tsnano_end-tsnano), max(tsnano_end-tsnano) as avgspread from taq_quote2
where bid_px is not null and ask_px is not null and ask_px>bid_px
and cond in ('A','B','H','O','R','W')
and time(ts) >= ('09:30:00') and time(ts) <= ('16:00:00')
and sym='AMD'
group by 1;




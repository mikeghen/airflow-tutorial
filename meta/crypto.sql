create table rates (
    id varchar(10),
    price numeric(15,2),
    last_price numeric(15,2),
    volume numeric(15,2),
    recorded_at timestamp without time zone default (now() at time zone 'utc')
);

create table markets (
    market varchar(32),
    id varchar(10),
    price numeric(15,2),
    volume_btc numeric(15,2),
    volume numeric(15,2),
    recorded_at timestamp without time zone default (now() at time zone 'utc')
);

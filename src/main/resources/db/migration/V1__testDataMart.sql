create table IF NOT EXISTS test_data_mart
(
    district_id varchar(255),
    count_trips int,
    avr float,
    deviation float,
    min_distance float,
    max_distance float
);

comment on column test_data_mart.district_id is 'ИД района';
comment on column test_data_mart.count_trips is 'количество поездок';
comment on column test_data_mart.avr is 'среднее расстояние';
comment on column test_data_mart.deviation is 'среднеквадратичное отклонение';
comment on column test_data_mart.min_distance is 'минимальная дистанция';
comment on column test_data_mart.max_distance is 'максимальная дистанция';

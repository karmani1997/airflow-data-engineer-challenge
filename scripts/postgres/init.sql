-- CREATE TABLE airflow__extra_conf(
--   conf_name  VARCHAR (255) PRIMARY KEY,
--   conf_value VARCHAR (255) NOT NULL
-- );

CREATE TABLE IF NOT EXISTS invoice_table(venue_id varchar (12), invoice_id varchar (32), datetime_created datetime, amount float);
CREATE TABLE IF NOT EXISTS invoice_aggregated_data_table(venue_id varchar (12), datetime_created datetime, amount float);
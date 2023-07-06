SELECT count(conf_name) FROM airflow__extra_conf WHERE conf_name='IS_INITIALIZED';
--ALTER TABLE dag_run ALTER COLUMN run_type DROP NOT NULL;
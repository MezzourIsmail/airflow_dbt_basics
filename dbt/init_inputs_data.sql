create table test_customer as from read_csv_auto('dbt/seed/raw_customers.csv');

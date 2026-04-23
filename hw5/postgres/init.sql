CREATE TABLE IF NOT EXISTS aggregate_results
(
    metric_date date NOT NULL,
    metric_name text NOT NULL,
    dimension text NOT NULL DEFAULT '',
    value double precision NOT NULL,
    calculated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, metric_name, dimension)
);


-- Fact Table
CREATE TABLE SnowflakeUsageFact (
    usage_id INT AUTOINCREMENT PRIMARY KEY,
    user_id INT,
    role_id INT,
    warehouse_id INT,
    runtime_seconds INT,
    num_queries INT,
    data_processed_bytes INT,
    credits_used FLOAT,
    timestamp TIMESTAMP
);

-- Dimension Tables
CREATE TABLE UserDimension (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(255)
);

CREATE TABLE RoleDimension (
    role_id INT PRIMARY KEY,
    role_name VARCHAR(255)
);

CREATE TABLE WarehouseDimension (
    warehouse_id INT PRIMARY KEY,
    warehouse_name VARCHAR(255)
);

-- Populate Dimension Tables (Sample data; Replace with your actual data)
INSERT INTO UserDimension VALUES
    (1, 'User1'),
    (2, 'User2'),
    (3, 'User3');

INSERT INTO RoleDimension VALUES
    (1, 'Role1'),
    (2, 'Role2'),
    (3, 'Role3');

INSERT INTO WarehouseDimension VALUES
    (1, 'Warehouse1'),
    (2, 'Warehouse2'),
    (3, 'Warehouse3');

-- Populate the SnowflakeUsageFact table using INFORMATION_SCHEMA views
INSERT INTO SnowflakeUsageFact (
    user_id,
    role_id,
    warehouse_id,
    runtime_seconds,
    num_queries,
    data_processed_bytes,
    credits_used,
    timestamp
)
SELECT
    I1.USER_ID,
    I1.ROLE_ID,
    I1.WAREHOUSE_ID,
    I1.ELAPSED_TIME / 1000 AS runtime_seconds,
    1 AS num_queries,
    I1.DATA_SCANNED_BYTES + I1.DATA_PROCESSED_BYTES AS data_processed_bytes,
    I1.CREDITS_USED AS credits_used,
    I1.START_TIME AS timestamp
FROM
    INFORMATION_SCHEMA.QUERY_HISTORY I1
WHERE
    I1.WAREHOUSE_ID IS NOT NULL
    AND I1.USER_ID IS NOT NULL
    AND I1.ROLE_ID IS NOT NULL;

-- Optional: You may want to run additional queries to populate the UserDimension, RoleDimension, and WarehouseDimension tables.

-- Query to monitor credit usage and runtimes
SELECT
    U.user_name,
    R.role_name,
    W.warehouse_name,
    SUM(F.runtime_seconds) AS total_runtime_seconds,
    SUM(F.num_queries) AS total_num_queries,
    SUM(F.data_processed_bytes) AS total_data_processed_bytes,
    SUM(F.credits_used) AS total_credits_used
FROM
    SnowflakeUsageFact F
JOIN
    UserDimension U ON F.user_id = U.user_id
JOIN
    RoleDimension R ON F.role_id = R.role_id
JOIN
    WarehouseDimension W ON F.warehouse_id = W.warehouse_id
GROUP BY
    U.user_name, R.role_name, W.warehouse_name;

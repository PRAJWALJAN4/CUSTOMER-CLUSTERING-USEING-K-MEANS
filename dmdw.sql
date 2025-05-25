-- Create the database
CREATE DATABASE dmdw;
USE dmdw;

-- Create the customer_data table
CREATE TABLE customer_data (
    customer_id INT PRIMARY KEY,
    total_spent DECIMAL(10,2),
    purchase_count INT,
    recency INT
);

-- Insert 50 rows into customer_data
INSERT INTO customer_data (customer_id, total_spent, purchase_count, recency) VALUES 
(1, 1903.97, 47, 145),
(2, 4756.04, 12, 136),
(3, 3673.37, 62, 3),
(4, 3013.36, 80, 144),
(5, 822.29, 88, 30),
(6, 822.17, 83, 237),
(7, 337.51, 8, 279),
(8, 4337.57, 95, 348),
(9, 3025.52, 21, 115),
(10, 3554.96, 81, 104),
(11, 151.89, 87, 118),
(12, 4851.05, 80, 5),
(13, 4170.59, 70, 360),
(14, 1101.08, 72, 248),
(15, 950.03, 25, 364),
(16, 957.85, 82, 321),
(17, 1555.99, 89, 121),
(18, 2647.54, 12, 286),
(19, 2188.12, 15, 149),
(20, 1491.58, 59, 84),
(21, 3078.67, 26, 231),
(22, 740.49, 26, 288),
(23, 1496.12, 47, 161),
(24, 1863.49, 32, 272),
(25, 2307.55, 10, 38),
(26, 3936.62, 16, 226),
(27, 1038.39, 71, 142),
(28, 2595.46, 17, 292),
(29, 2982.45, 23, 151),
(30, 279.93, 26, 114),
(31, 3057.35, 85, 237),
(32, 894.09, 86, 66),
(33, 372.01, 7, 361),
(34, 4746.98, 14, 341),
(35, 4829.88, 79, 258),
(36, 4051.57, 7, 249),
(37, 1557.84, 9, 331),
(38, 533.48, 48, 323),
(39, 3436.95, 72, 180),
(40, 2228.75, 59, 190),
(41, 654.09, 87, 154),
(42, 2501.13, 93, 196),
(43, 220.22, 82, 311),
(44, 4551.14, 95, 262),
(45, 1330.96, 94, 143),
(46, 3329.49, 39, 342),
(47, 1592.97, 99, 316),
(48, 2624.34, 18, 101),
(49, 2756.22, 59, 244),
(50, 965.03, 17, 12);

-- Normalize data and store in a new table
CREATE TABLE customer_data_normalized AS
SELECT 
    customer_id,
    (total_spent - (SELECT MIN(total_spent) FROM customer_data)) / 
    ((SELECT MAX(total_spent) FROM customer_data) - (SELECT MIN(total_spent) FROM customer_data)) AS norm_total_spent,
    
    (purchase_count - (SELECT MIN(purchase_count) FROM customer_data)) / 
    ((SELECT MAX(purchase_count) FROM customer_data) - (SELECT MIN(purchase_count) FROM customer_data)) AS norm_purchase_count,
    
    (recency - (SELECT MIN(recency) FROM customer_data)) / 
    ((SELECT MAX(recency) FROM customer_data) - (SELECT MIN(recency) FROM customer_data)) AS norm_recency
FROM customer_data;

-- Add cluster column for clustering
ALTER TABLE customer_data_normalized ADD COLUMN cluster INT;

-- Assign random clusters (1 to 4)
SET SQL_SAFE_UPDATES = 0;
UPDATE customer_data_normalized  
SET cluster = FLOOR(1 + (RAND() * 4));
SET SQL_SAFE_UPDATES = 1;

-- Create a table to store cluster centroids
CREATE TABLE cluster_centroids AS
SELECT 
    cluster,
    AVG(norm_total_spent) AS centroid_total_spent,
    AVG(norm_purchase_count) AS centroid_purchase_count,
    AVG(norm_recency) AS centroid_recency
FROM customer_data_normalized
GROUP BY cluster;

-- Assign customers to nearest cluster using distance calculation
CREATE TEMPORARY TABLE temp_clusters AS
SELECT cdn.customer_id, cc.cluster
FROM customer_data_normalized cdn
JOIN cluster_centroids cc
ON ABS(cdn.norm_total_spent - cc.centroid_total_spent) +
   ABS(cdn.norm_purchase_count - cc.centroid_purchase_count) +
   ABS(cdn.norm_recency - cc.centroid_recency) =
   (SELECT MIN(
        ABS(cdn2.norm_total_spent - cc2.centroid_total_spent) + 
        ABS(cdn2.norm_purchase_count - cc2.centroid_purchase_count) + 
        ABS(cdn2.norm_recency - cc2.centroid_recency)
    ) FROM customer_data_normalized cdn2
    JOIN cluster_centroids cc2
    WHERE cdn2.customer_id = cdn.customer_id);

-- Update cluster assignments
SET SQL_SAFE_UPDATES = 0;
UPDATE customer_data_normalized cdn
JOIN temp_clusters tc ON cdn.customer_id = tc.customer_id
SET cdn.cluster = tc.cluster;
SET SQL_SAFE_UPDATES = 1;

-- Display cluster statistics
SELECT 
    cluster,
    COUNT(*) AS num_customers,
    AVG(norm_total_spent) AS avg_spent,
    AVG(norm_purchase_count) AS avg_purchases,
    AVG(norm_recency) AS avg_recency
FROM customer_data_normalized
GROUP BY cluster;

-- Verify inserted and clustered data
SELECT * FROM customer_data_normalized LIMIT 10;

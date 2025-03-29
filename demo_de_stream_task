-- ============================================================================
-- >> Step 0: Set Context <<
-- ============================================================================
-- Create objects
USE ROLE SYSADMIN;

CREATE OR REPLACE DATABASE DEMO_RETAIL_TEST;
CREATE OR REPLACE SCHEMA DEMO_RETAIL_TEST.POS;
USE SCHEMA DEMO_RETAIL_TEST.POS;

CREATE OR REPLACE WAREHOUSE DEMO_DE_WH
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
WAREHOUSE_SIZE = 'XSMALL';

USE WAREHOUSE DEMO_DE_WH;

-- Variable for relative timestamps
SET current_ts = CURRENT_TIMESTAMP();

-- ============================================================================
-- >> Step 1: Create Landing Tables <<
-- ============================================================================

-- Landing table for Order Headers
CREATE OR REPLACE TABLE landing_orders (
    order_id VARCHAR(50) NOT NULL,       -- Natural key for the order
    customer_id VARCHAR(50),           -- Who placed the order
    order_timestamp TIMESTAMP_NTZ,       -- When the customer placed the order
    source_system VARCHAR(30),           -- Originating system
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() -- When the record landed
);

-- Landing table for Order Line Items
CREATE OR REPLACE TABLE landing_order_items (
    order_item_id VARCHAR(100) NOT NULL, -- Unique ID for the line item (use for dedupe)
    order_id VARCHAR(50) NOT NULL,        -- Foreign key to landing_orders
    product_sku VARCHAR(50),            -- Product identifier
    quantity INT,                       -- Number of units
    unit_price DECIMAL(10, 2),          -- Price per unit
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() -- When the record landed
);

-- ============================================================================
-- >> Step 2: Create the Final Target Table <<
-- ============================================================================

CREATE OR REPLACE TABLE fact_order_details (
    order_item_id VARCHAR(100) PRIMARY KEY, -- Unique line item ID is the PK
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_timestamp TIMESTAMP_NTZ,
    product_sku VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10, 2),
    total_price DECIMAL(12, 2),          -- Calculated field
    processed_timestamp TIMESTAMP_NTZ      -- When the record was merged
);

-- ============================================================================
-- >> Step 3: Create the Join View <<
-- ============================================================================

CREATE OR REPLACE VIEW vw_joined_order_data AS
SELECT
    oi.order_item_id,
    o.order_id,
    o.customer_id,
    o.order_timestamp,
    oi.product_sku,
    oi.quantity,
    oi.unit_price,
    (oi.quantity * oi.unit_price) AS calculated_total_price, -- Calculate total price
    o.load_timestamp AS order_load_ts,     -- Keep both timestamps if needed for complex logic
    oi.load_timestamp AS item_load_ts,     -- Or just use GREATEST below
    GREATEST(o.load_timestamp, oi.load_timestamp) AS latest_load_ts -- Used for ordering duplicates
FROM landing_orders AS o
INNER JOIN landing_order_items AS oi
    ON o.order_id = oi.order_id;

-- ============================================================================
-- >> Step 4: Create the Stream on the View <<
-- ============================================================================

CREATE OR REPLACE STREAM stream_on_vw_joined_order_data
    ON VIEW vw_joined_order_data
    APPEND_ONLY = TRUE; -- Assuming landing tables mostly receive inserts

-- ============================================================================
-- >> Step 5: Insert Sample Data into Landing Tables <<
-- ============================================================================
-- Scenario 1: Clean new order 'ORD101' with two items arrives together
INSERT INTO landing_orders (order_id, customer_id, order_timestamp, source_system, load_timestamp) VALUES
    ('ORD101', 'CUST-A', DATEADD(hour, -2, $current_ts), 'Web', DATEADD(minute, 1, $current_ts));
INSERT INTO landing_order_items (order_item_id, order_id, product_sku, quantity, unit_price, load_timestamp) VALUES
    ('ITEM-A1', 'ORD101', 'SKU-BLUE-WIDGET', 2, 10.50, DATEADD(minute, 2, $current_ts)),
    ('ITEM-A2', 'ORD101', 'SKU-RED-GADGET', 1, 25.00, DATEADD(minute, 2, $current_ts));

-- Scenario 2: Staggered arrival for order 'ORD102'
INSERT INTO landing_orders (order_id, customer_id, order_timestamp, source_system, load_timestamp) VALUES
    ('ORD102', 'CUST-B', DATEADD(hour, -1, $current_ts), 'MobileApp', DATEADD(minute, 2, $current_ts));
INSERT INTO landing_order_items (order_item_id, order_id, product_sku, quantity, unit_price, load_timestamp) VALUES
    ('ITEM-B1', 'ORD102', 'SKU-GREEN-THING', 5, 5.00, DATEADD(minute, 5, $current_ts)),
    ('ITEM-B2', 'ORD102', 'SKU-BLUE-WIDGET', 1, 10.50, DATEADD(minute, 5, 15, $current_ts));

-- Scenario 3: Duplicate Item Message for 'ITEM-A1'
INSERT INTO landing_order_items (order_item_id, order_id, product_sku, quantity, unit_price, load_timestamp) VALUES
    ('ITEM-A1', 'ORD101', 'SKU-BLUE-WIDGET', 2, 10.50, DATEADD(minute, 6, $current_ts)); -- Duplicate arrival

-- Scenario 4: Clean new order 'ORD103'
INSERT INTO landing_orders (order_id, customer_id, order_timestamp, source_system, load_timestamp) VALUES
    ('ORD103', 'CUST-A', DATEADD(minute, -30, $current_ts), 'Web', DATEADD(minute, 7, $current_ts));
INSERT INTO landing_order_items (order_item_id, order_id, product_sku, quantity, unit_price, load_timestamp) VALUES
    ('ITEM-C1', 'ORD103', 'SKU-YELLOW-DOODAD', 10, 1.99, DATEADD(minute, 7, 10, $current_ts));

-- Scenario 5: Orphaned item (no matching order header)
INSERT INTO landing_order_items (order_item_id, order_id, product_sku, quantity, unit_price, load_timestamp) VALUES
    ('ITEM-X1', 'ORD999-MISSING', 'SKU-BLACK-BOX', 1, 99.99, DATEADD(minute, 8, $current_ts));

-- ============================================================================
-- >> Step 6: Verify Initial State (Optional) <<
-- ============================================================================
SELECT 'Landing Orders' as tbl, * FROM landing_orders ORDER BY load_timestamp;
SELECT 'Landing Order Items' as tbl, * FROM landing_order_items ORDER BY load_timestamp;
SELECT 'View Output (Full)' as vw, * FROM vw_joined_order_data ORDER BY order_id, order_item_id;
SELECT 'Stream Output (Initial)' as strm, * FROM stream_on_vw_joined_order_data; -- Should show joined rows based on inserts
SELECT 'Final Table (Initial)' as fin, * FROM fact_order_details; -- Should be empty

-- ============================================================================
-- >> Step 7: Ready to Test MERGE using the Stream <<
-- ============================================================================
/*
-- Example MERGE statement consuming the stream:

MERGE INTO fact_order_details AS T
USING (
    SELECT
       src.order_item_id,
       src.order_id,
       src.customer_id,
       src.order_timestamp,
       src.product_sku,
       src.quantity,
       src.unit_price,
       src.calculated_total_price,
       src.latest_load_ts -- Need this for ordering duplicates
       -- You might need METADATA$ACTION, METADATA$ISUPDATE depending on stream config and logic
    FROM stream_on_vw_joined_order_data AS src
    -- Deduplicate based on unique item ID within this batch from the stream
    QUALIFY ROW_NUMBER() OVER (
                  PARTITION BY src.order_item_id
                  ORDER BY src.latest_load_ts DESC -- Keep the latest instance if duplicates in stream batch
                                                    -- (or ASC to keep first seen)
              ) = 1
) AS S
ON T.order_item_id = S.order_item_id -- Match based on the unique line item ID
WHEN NOT MATCHED THEN
    INSERT (
        order_item_id, order_id, customer_id, order_timestamp, product_sku,
        quantity, unit_price, total_price, processed_timestamp
    )
    VALUES (
        S.order_item_id, S.order_id, S.customer_id, S.order_timestamp, S.product_sku,
        S.quantity, S.unit_price, S.calculated_total_price, CURRENT_TIMESTAMP() -- Use S.total_price if calculated in view
    )
WHEN MATCHED THEN -- Example: Update if the source record is somehow newer (less common for append-only)
    UPDATE SET
        T.customer_id = S.customer_id,
        T.order_timestamp = S.order_timestamp,
        T.product_sku = S.product_sku,
        T.quantity = S.quantity,
        T.unit_price = S.unit_price,
        T.total_price = S.calculated_total_price,
        T.processed_timestamp = CURRENT_TIMESTAMP();


-- After running the MERGE, check the stream (should be empty) and the final table
SELECT 'Stream Output (After MERGE)' as strm, * FROM stream_on_vw_joined_order_data;
SELECT 'Final Table (After MERGE)' as fin, * FROM fact_order_details ORDER BY order_id, order_item_id;

*/

-- ============================================================================
-- >> Step 8: Reset source tables (Optional) <<
-- ============================================================================
TRUNCATE TABLE landing_order_items; -- Reset source orders table
TRUNCATE TABLE landing_orders; -- Reset source order items table

-- ============================================================================
-- >> Step 9: Create Stored Procedure for the MERGE Logic <<
-- ============================================================================
-- This procedure encapsulates the logic to process data from the stream
-- into the final table, including deduplication.

CREATE OR REPLACE PROCEDURE sp_merge_order_details()
RETURNS VARCHAR -- Return status message
LANGUAGE SQL
EXECUTE AS OWNER -- Or EXECUTE AS CALLER depending on your permission model
AS
$$
DECLARE
  merge_result VARIANT; -- To potentially capture MERGE output if needed
  status_message VARCHAR DEFAULT 'No data in stream to process.';
BEGIN
  -- Check if the stream actually has data before attempting MERGE
  -- Note: The WHEN clause in the TASK handles this, but double-checking
  -- or performing actions ONLY when data exists can be done here too.
  -- For simplicity, we rely on the TASK's WHEN clause and proceed.

  MERGE INTO fact_order_details AS T
  USING (
      SELECT
         src.order_item_id,
         src.order_id,
         src.customer_id,
         src.order_timestamp,
         src.product_sku,
         src.quantity,
         src.unit_price,
         src.calculated_total_price,
         src.latest_load_ts -- Need this for ordering duplicates
         -- METADATA$ACTION, METADATA$ISUPDATE columns are available from the stream if needed
      FROM stream_on_vw_joined_order_data AS src -- Read from the stream
      -- Deduplicate based on unique item ID within this batch from the stream
      QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY src.order_item_id
                    ORDER BY src.latest_load_ts ASC -- Keep first seen for duplicates (ASC), or DESC for latest update
                ) = 1
  ) AS S
  ON T.order_item_id = S.order_item_id -- Match based on the unique line item ID

  -- Action when a stream record represents a new order item
  WHEN NOT MATCHED THEN
      INSERT (
          order_item_id, order_id, customer_id, order_timestamp, product_sku,
          quantity, unit_price, total_price, processed_timestamp
      )
      VALUES (
          S.order_item_id, S.order_id, S.customer_id, S.order_timestamp, S.product_sku,
          S.quantity, S.unit_price, S.calculated_total_price, CURRENT_TIMESTAMP()
      )

  -- Action when a stream record matches an existing order item (e.g., for updates if stream isn't append-only)
  WHEN MATCHED -- Add conditions like AND S.METADATA$ACTION = 'UPDATE' if needed
  THEN
      UPDATE SET
          T.customer_id = S.customer_id,
          T.order_timestamp = S.order_timestamp,
          T.product_sku = S.product_sku,
          T.quantity = S.quantity,
          T.unit_price = S.unit_price,
          T.total_price = S.calculated_total_price,
          T.processed_timestamp = CURRENT_TIMESTAMP();

  -- Optional: Capture results if needed
  -- SELECT SYSTEM$LAST_MERGE_RESULT() INTO :merge_result;

  status_message := 'Successfully merged order details from stream.'; -- Update status if MERGE ran
  RETURN status_message;

END;
$$;

-- ============================================================================
-- >> Step 10: Create the Task to Run the Stored Procedure <<
-- ============================================================================
-- This task calls the stored procedure, but ONLY runs if the stream has data.

-- Make sure the role creating the task has necessary privileges:
-- USAGE on DB/SCHEMA, CREATE TASK, USAGE on WAREHOUSE, EXECUTE on PROCEDURE
-- Role executing the task (usually owner or via EXECUTE TASK priv) needs warehouse usage & SP execute rights.
-- The SP (via EXECUTE AS CALLER/OWNER) needs SELECT on stream and MERGE on fact_order_details.

CREATE OR REPLACE TASK task_process_order_stream
    WAREHOUSE = DEMO_DE_WH -- Specify the warehouse for the task
    SCHEDULE = '1 HOUR'          -- Example: Check every 5 minutes
    -- Condition: Only run the task if the stream contains data
    WHEN SYSTEM$STREAM_HAS_DATA('stream_on_vw_joined_order_data')
AS
    -- Action: Call the stored procedure
    CALL sp_merge_order_details();

-- Tasks are created in a SUSPENDED state by default. You need to resume it.
-- ALTER TASK task_process_order_stream RESUME;
-- To suspend later:
-- ALTER TASK task_process_order_stream SUSPEND;

-- ============================================================================
-- >> Step 11: Insert Sample Data into Landing Tables <<
-- ============================================================================
-- (Same data insertion logic as before)

-- Scenario 1: Clean new order 'ORD101' with two items arrives together
INSERT INTO landing_orders (order_id, customer_id, order_timestamp, source_system, load_timestamp) VALUES
    ('ORD101', 'CUST-A', DATEADD(hour, -2, $current_ts), 'Web', DATEADD(minute, 1, $current_ts));
INSERT INTO landing_order_items (order_item_id, order_id, product_sku, quantity, unit_price, load_timestamp) VALUES
    ('ITEM-A1', 'ORD101', 'SKU-BLUE-WIDGET', 2, 10.50, DATEADD(minute, 2, $current_ts)),
    ('ITEM-A2', 'ORD101', 'SKU-RED-GADGET', 1, 25.00, DATEADD(minute, 2, $current_ts));

-- Scenario 2: Staggered arrival for order 'ORD102'
INSERT INTO landing_orders (order_id, customer_id, order_timestamp, source_system, load_timestamp) VALUES
    ('ORD102', 'CUST-B', DATEADD(hour, -1, $current_ts), 'MobileApp', DATEADD(minute, 2, $current_ts));
INSERT INTO landing_order_items (order_item_id, order_id, product_sku, quantity, unit_price, load_timestamp) VALUES
    ('ITEM-B1', 'ORD102', 'SKU-GREEN-THING', 5, 5.00, DATEADD(minute, 5, $current_ts)),
    ('ITEM-B2', 'ORD102', 'SKU-BLUE-WIDGET', 1, 10.50, DATEADD(minute, 6, $current_ts));

-- Scenario 3: Duplicate Item Message for 'ITEM-A1'
INSERT INTO landing_order_items (order_item_id, order_id, product_sku, quantity, unit_price, load_timestamp) VALUES
    ('ITEM-A1', 'ORD101', 'SKU-BLUE-WIDGET', 2, 10.50, DATEADD(minute, 6, $current_ts)); -- Duplicate arrival

-- Scenario 4: Clean new order 'ORD103'
INSERT INTO landing_orders (order_id, customer_id, order_timestamp, source_system, load_timestamp) VALUES
    ('ORD103', 'CUST-A', DATEADD(minute, -30, $current_ts), 'Web', DATEADD(minute, 7, $current_ts));
INSERT INTO landing_order_items (order_item_id, order_id, product_sku, quantity, unit_price, load_timestamp) VALUES
    ('ITEM-C1', 'ORD103', 'SKU-YELLOW-DOODAD', 10, 1.99, DATEADD(minute, 7, $current_ts));

-- Scenario 5: Orphaned item (no matching order header)
INSERT INTO landing_order_items (order_item_id, order_id, product_sku, quantity, unit_price, load_timestamp) VALUES
    ('ITEM-X1', 'ORD999-MISSING', 'SKU-BLACK-BOX', 1, 99.99, DATEADD(minute, 8, $current_ts));

-- ============================================================================
-- >> Step 12: Verify State Before Task Run (Optional) <<
-- ============================================================================
SELECT 'Landing Orders' as tbl, * FROM landing_orders ORDER BY load_timestamp;
SELECT 'Landing Order Items' as tbl, * FROM landing_order_items ORDER BY load_timestamp;
SELECT 'View Output (Full)' as vw, * FROM vw_joined_order_data ORDER BY order_id, order_item_id;
SELECT 'Stream Output (Before Task Run)' as strm, * FROM stream_on_vw_joined_order_data; -- Should show data ready for processing
SELECT 'Final Table (Before Task Run)' as fin, * FROM fact_order_details; -- Should be empty

-- ============================================================================
-- >> Step 13: Reset tables (Optional) <<
-- ============================================================================
TRUNCATE TABLE landing_order_items; -- Reset source orders table
TRUNCATE TABLE landing_orders; -- Reset source order items table
TRUNCATE TABLE fact_order_details; -- Reset target table

-- ============================================================================
-- >> Step 14: Execute Task <<
-- ============================================================================
USE ROLE SECURITYADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
EXECUTE TASK task_process_order_stream;

-- ============================================================================
-- >> Step 15: Verify State Before Task Run (Optional) <<
-- ============================================================================
SELECT 'Stream Output (Before Task Run)' as strm, * FROM stream_on_vw_joined_order_data; -- Should be empty
SELECT 'Final Table (Before Task Run)' as fin, * FROM fact_order_details; -- Should should show ingested data without duplicates and orphan items


-- DISMANTLE DEMO
/*

DROP DATABASE DEMO_RETAIL_TEST;
DROP WAREHOUSE DEMO_DE_WH;

*/

-- =========================================
-- 1. Detail Items View
--    Shows detail items with “English readable” data
--    (contact info, PO number, project info, file link, etc.)
-- =========================================
DROP VIEW IF EXISTS vw_detail_items_extended;
CREATE OR REPLACE VIEW vw_detail_items_extended AS
SELECT
    di.id AS detail_item_id,
    di.state AS detail_item_state,
    di.description AS detail_description,
    di.sub_total AS detail_subtotal,
    -- Pull the associated Purchase Order
    po.id AS po_id,
    po.po_number AS po_number,
    -- The contact on that PO
    c.id AS contact_id,
    c.name AS contact_name,
    -- The associated project info
    p.id AS project_id,
    p.project_number AS project_number,
    p.name AS project_name,
    -- Example of linking file; attempts to grab from a receipt or invoice if present
    COALESCE(r.file_link, i.file_link) AS file_link
FROM detail_item di
    JOIN purchase_order po       ON di.po_id = po.id
    JOIN project p               ON po.project_id = p.id
    LEFT JOIN contact c          ON po.contact_id = c.id
    LEFT JOIN receipt r          ON di.receipt_id = r.id
    LEFT JOIN invoice i          ON di.invoice_id = i.id;

-- =========================================
-- 2. Project Overview & Stats
--    Shows each project's totals:
--      - total sum of all detail items
--      - how many POs
--      - how many detail items
--      - how many items in each status, etc.
-- =========================================
DROP VIEW IF EXISTS vw_project_stats;
CREATE OR REPLACE VIEW vw_project_stats AS
SELECT
    p.id AS project_id,
    p.project_number,
    p.name AS project_name,
    COUNT(DISTINCT po.id) AS total_pos,
    COUNT(DISTINCT di.id) AS total_detail_items,
    IFNULL(SUM(di.sub_total), 0) AS total_sum_of_detail_items,

    -- Example breakdown of how many detail items are in each status
    SUM(CASE WHEN di.state = 'PENDING'      THEN 1 ELSE 0 END) AS cnt_pending,
    SUM(CASE WHEN di.state = 'OVERDUE'      THEN 1 ELSE 0 END) AS cnt_overdue,
    SUM(CASE WHEN di.state = 'REVIEWED'     THEN 1 ELSE 0 END) AS cnt_reviewed,
    SUM(CASE WHEN di.state = 'ISSUE'        THEN 1 ELSE 0 END) AS cnt_issue,
    SUM(CASE WHEN di.state = 'RTP'          THEN 1 ELSE 0 END) AS cnt_rtp,
    SUM(CASE WHEN di.state = 'RECONCILED'   THEN 1 ELSE 0 END) AS cnt_reconciled,
    SUM(CASE WHEN di.state = 'PAID'         THEN 1 ELSE 0 END) AS cnt_paid,
    SUM(CASE WHEN di.state = 'APPROVED'     THEN 1 ELSE 0 END) AS cnt_approved,
    SUM(CASE WHEN di.state = 'SUBMITTED'    THEN 1 ELSE 0 END) AS cnt_submitted,
    SUM(CASE WHEN di.state = 'PO MISMATCH'  THEN 1 ELSE 0 END) AS cnt_po_mismatch

FROM project p
    LEFT JOIN purchase_order po ON p.id = po.project_id
    LEFT JOIN detail_item di    ON po.id = di.po_id
GROUP BY
    p.id, p.project_number, p.name;

-- =========================================
-- 3. Compare Xero Bills and Their Associated POs
--    This view pairs each Xero Bill with its Purchase Order
--    (if any) for quick comparison.
-- =========================================
DROP VIEW IF EXISTS vw_xero_bills_vs_pos;
CREATE OR REPLACE VIEW vw_xero_bills_vs_pos AS
SELECT
    xb.id AS xero_bill_id,
    xb.state AS xero_bill_state,
    xb.xero_reference_number,
    xb.created_at AS xero_bill_created_at,
    xb.updated_at AS xero_bill_updated_at,
    po.id AS purchase_order_id,
    po.po_number AS purchase_order_number,
    po.state AS purchase_order_state,
    p.id AS project_id,
    p.project_number,
    p.name AS project_name
FROM xero_bill xb
    LEFT JOIN purchase_order po ON xb.po_id = po.id
    LEFT JOIN project p         ON po.project_id = p.id;

-- =========================================
-- 4. Compare Xero Bills, Their Line Items, and Associated Detail Items
--    This shows each Xero Bill, each BillLineItem, and the matching DetailItem.
-- =========================================
DROP VIEW IF EXISTS vw_xero_bills_line_items;
CREATE OR REPLACE VIEW vw_xero_bills_line_items AS
SELECT
    xb.id AS xero_bill_id,
    xb.state AS xero_bill_state,
    xb.xero_reference_number,
    bli.id AS bill_line_item_id,
    bli.description AS bill_line_desc,
    bli.line_amount AS bill_line_amount,
    bli.account_code AS bill_line_account_code,
    di.id AS detail_item_id,
    di.state AS detail_item_state,
    di.sub_total AS detail_item_subtotal,
    po.id AS purchase_order_id,
    po.po_number AS purchase_order_number,
    p.name AS project_name
FROM xero_bill xb
    JOIN bill_line_item bli  ON xb.id = bli.parent_id
    LEFT JOIN detail_item di ON bli.detail_item_id = di.id
    LEFT JOIN purchase_order po ON di.po_id = po.id
    LEFT JOIN project p         ON po.project_id = p.id;
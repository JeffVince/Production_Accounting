create
    definer = root@localhost procedure sp_create_investor_views()
BEGIN
    -- 1. Vendor Spend Summary: now join contact <-> purchase_order
    CREATE OR REPLACE VIEW vw_vendor_spend_summary AS
    SELECT
        c.id AS contact_id,
        c.name AS vendor_name,
        COUNT(po.id) AS total_po_count,
        SUM(po.amount_total) AS total_spent,
        AVG(po.amount_total) AS avg_po_amount
    FROM contact c
    JOIN purchase_order po ON po.contact_id = c.id
    GROUP BY c.id, c.name;

    -- 2. Monthly Spending Trends
    CREATE OR REPLACE VIEW vw_monthly_spend_trends AS
    SELECT
        YEAR(po.created_at) AS year,
        MONTH(po.created_at) AS month,
        SUM(po.amount_total) AS total_spent_this_month,
        COUNT(po.id) AS total_po_count
    FROM purchase_order po
    GROUP BY YEAR(po.created_at), MONTH(po.created_at)
    ORDER BY YEAR(po.created_at), MONTH(po.created_at);

    -- 3. Invoice Turnaround Time
    CREATE OR REPLACE VIEW vw_invoice_turnaround AS
    SELECT
        i.id AS invoice_id,
        i.invoice_number,
        p.name AS project_name,
        po.po_number,
        po.created_at AS po_created_at,
        i.transaction_date AS invoice_date,
        DATEDIFF(i.transaction_date, po.created_at) AS days_to_invoice
    FROM invoice i
    JOIN purchase_order po ON i.po_id = po.id
    JOIN project p ON p.id = po.project_id;

    -- 4. Project Vendor Distribution
    --   Using purchase_order.contact_id instead of contact_po
    CREATE OR REPLACE VIEW vw_project_vendor_distribution AS
    SELECT
        p.id AS project_id,
        p.name AS project_name,
        COUNT(DISTINCT c.id) AS distinct_vendors,
        SUM(po.amount_total) AS total_spent
    FROM project p
    JOIN purchase_order po ON p.id = po.project_id
    LEFT JOIN contact c ON c.id = po.contact_id
    GROUP BY p.id, p.name;

    -- 5. Vendor Popularity Across Projects
    CREATE OR REPLACE VIEW vw_vendor_popularity AS
    SELECT
        c.id AS contact_id,
        c.name AS vendor_name,
        COUNT(DISTINCT po.project_id) AS distinct_projects,
        SUM(po.amount_total) AS total_earnings
    FROM contact c
    JOIN purchase_order po ON po.contact_id = c.id
    GROUP BY c.id, c.name;

    -- 6. Average Purchase Order Value Per Project
    CREATE OR REPLACE VIEW vw_average_po_value_per_project AS
    SELECT
        p.id AS project_id,
        p.name AS project_name,
        AVG(po.amount_total) AS avg_po_value
    FROM project p
    JOIN purchase_order po ON p.id = po.project_id
    GROUP BY p.id, p.name;
END;


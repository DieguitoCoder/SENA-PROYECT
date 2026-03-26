-- ==========================================================
--  Star Schema & Analytical Queries — Riwi Analytics
--  Author: Dieguito Coder
--  Database: PostgreSQL 13+
--  Description: Data warehouse schema (Star Schema) with
--  dimension tables, fact table, indexes, and a suite of
--  analytical SQL queries from basic to advanced.
-- ==========================================================


-- ════════════════════════════════════════════════════════
-- 1. DIMENSION TABLES
-- ════════════════════════════════════════════════════════

-- DIMENSION: FECHA (Time Intelligence)
-- Stores date attributes for time-based analysis
CREATE TABLE dim_fecha (
    id_fecha SERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    year_month VARCHAR(7) NOT NULL,
    CONSTRAINT uq_dim_fecha_fecha UNIQUE (fecha)
);

-- DIMENSION: REGION (Geographic)
-- Stores geographic hierarchy: city → country
CREATE TABLE dim_region (
    id_region SERIAL PRIMARY KEY,
    ciudad VARCHAR(100) NOT NULL,
    pais VARCHAR(100) NOT NULL,
    CONSTRAINT uq_dim_region_ciudad_pais UNIQUE (ciudad, pais)
);

-- DIMENSION: CLIENTE (Customer Segmentation)
-- Stores customer types, segments, and sales channels
CREATE TABLE dim_cliente (
    id_cliente SERIAL PRIMARY KEY,
    tipo_cliente VARCHAR(50) NOT NULL,
    segmento_cliente VARCHAR(50) NOT NULL,
    tipo_venta VARCHAR(50) NOT NULL,
    id_region INT NOT NULL,
    CONSTRAINT uq_dim_cliente UNIQUE (
        tipo_cliente,
        segmento_cliente,
        tipo_venta,
        id_region
    ),
    CONSTRAINT fk_cliente_region
        FOREIGN KEY (id_region)
        REFERENCES dim_region(id_region)
);

-- DIMENSION: PRODUCTO (Product Catalog)
-- Stores product names and their categories
CREATE TABLE dim_producto (
    id_producto SERIAL PRIMARY KEY,
    producto VARCHAR(100) NOT NULL,
    tipo_producto VARCHAR(100) NOT NULL,
    CONSTRAINT uq_dim_producto UNIQUE (producto, tipo_producto)
);


-- ════════════════════════════════════════════════════════
-- 2. FACT TABLE
-- ════════════════════════════════════════════════════════

-- FACT: VENTAS (Sales Transactions)
-- Central fact table linking all dimensions
CREATE TABLE fact_ventas (
    id_venta SERIAL PRIMARY KEY,
    id_fecha INT NOT NULL,
    id_producto INT NOT NULL,
    id_cliente INT NOT NULL,
    cantidad NUMERIC(10,2) NOT NULL,
    precio_unitario NUMERIC(12,2) NOT NULL,
    descuento NUMERIC(5,2) DEFAULT 0,
    costo_envio NUMERIC(12,2) DEFAULT 0,
    total NUMERIC(14,2) NOT NULL,

    CONSTRAINT fk_fact_fecha
        FOREIGN KEY (id_fecha)
        REFERENCES dim_fecha(id_fecha),

    CONSTRAINT fk_fact_producto
        FOREIGN KEY (id_producto)
        REFERENCES dim_producto(id_producto),

    CONSTRAINT fk_fact_cliente
        FOREIGN KEY (id_cliente)
        REFERENCES dim_cliente(id_cliente)
);


-- ════════════════════════════════════════════════════════
-- 3. PERFORMANCE INDEXES
-- ════════════════════════════════════════════════════════

-- These indexes optimize JOIN operations on the fact table
-- when querying by foreign key dimensions
CREATE INDEX idx_fact_fecha ON fact_ventas(id_fecha);
CREATE INDEX idx_fact_producto ON fact_ventas(id_producto);
CREATE INDEX idx_fact_cliente ON fact_ventas(id_cliente);

-- Composite index for common region-based queries
CREATE INDEX idx_region_pais_ciudad ON dim_region(pais, ciudad);


-- ════════════════════════════════════════════════════════
-- 4. BASIC ANALYTICAL QUERIES
-- ════════════════════════════════════════════════════════

-- ── Q1: Total Sales by Region ──────────────────────────
-- Identifies the most profitable geographic markets
-- Technique: GROUP BY + aggregate + multi-table JOIN
SELECT
    r.pais,
    r.ciudad,
    COUNT(f.id_venta) AS total_transactions,
    SUM(f.total) AS total_sales,
    ROUND(AVG(f.total), 2) AS avg_sale
FROM fact_ventas f
JOIN dim_cliente c ON f.id_cliente = c.id_cliente
JOIN dim_region r ON c.id_region = r.id_region
GROUP BY r.pais, r.ciudad
ORDER BY total_sales DESC;


-- ── Q2: Top 5 Products by Revenue ─────────────────────
-- Classifies highest-revenue products for commercial focus
-- Technique: ORDER BY DESC + LIMIT
SELECT
    p.producto,
    p.tipo_producto AS category,
    SUM(f.total) AS total_sales,
    SUM(f.cantidad) AS units_sold,
    ROUND(AVG(f.precio_unitario), 2) AS avg_unit_price
FROM fact_ventas f
JOIN dim_producto p ON f.id_producto = p.id_producto
GROUP BY p.producto, p.tipo_producto
ORDER BY total_sales DESC
LIMIT 5;


-- ── Q3: Average Ticket by Customer Type ───────────────
-- Calculates average spending per transaction per segment
-- Technique: AVG aggregate + GROUP BY
SELECT
    c.tipo_cliente,
    c.segmento_cliente,
    COUNT(f.id_venta) AS total_purchases,
    ROUND(AVG(f.total), 2) AS avg_ticket,
    SUM(f.total) AS total_revenue
FROM fact_ventas f
JOIN dim_cliente c ON f.id_cliente = c.id_cliente
GROUP BY c.tipo_cliente, c.segmento_cliente
ORDER BY avg_ticket DESC;


-- ── Q4: Inactive Customers (No Sales) ─────────────────
-- Detects registered customers without purchases
-- Technique: LEFT JOIN + IS NULL filter
SELECT
    c.id_cliente,
    c.tipo_cliente,
    c.segmento_cliente,
    r.pais,
    r.ciudad
FROM dim_cliente c
LEFT JOIN fact_ventas f ON c.id_cliente = f.id_cliente
JOIN dim_region r ON c.id_region = r.id_region
WHERE f.id_cliente IS NULL;


-- ════════════════════════════════════════════════════════
-- 5. INTERMEDIATE ANALYTICAL QUERIES
-- ════════════════════════════════════════════════════════

-- ── Q5: Sales by Category and Region ──────────────────
-- Multi-dimensional segmentation: product × geography
-- Technique: Multi-table JOIN + GROUP BY on multiple fields
SELECT
    r.pais,
    p.tipo_producto,
    SUM(f.total) AS total_sales,
    COUNT(f.id_venta) AS num_transactions,
    ROUND(AVG(f.descuento), 2) AS avg_discount
FROM fact_ventas f
JOIN dim_producto p ON f.id_producto = p.id_producto
JOIN dim_cliente c ON f.id_cliente = c.id_cliente
JOIN dim_region r ON c.id_region = r.id_region
GROUP BY r.pais, p.tipo_producto
ORDER BY r.pais, total_sales DESC;


-- ── Q6: Customer Ranking by Sales (DENSE_RANK) ────────
-- Ranks customers by total sales without skipping numbers
-- Technique: Window function DENSE_RANK()
SELECT
    c.tipo_cliente,
    c.segmento_cliente,
    SUM(f.total) AS total_sales,
    DENSE_RANK() OVER (ORDER BY SUM(f.total) DESC) AS sales_rank
FROM fact_ventas f
JOIN dim_cliente c ON f.id_cliente = c.id_cliente
GROUP BY c.tipo_cliente, c.segmento_cliente;


-- ── Q7: Year-over-Year Growth (LAG) ──────────────────
-- Compares annual sales to detect growth trends
-- Technique: LAG() window function + percentage calculation
SELECT
    d.year,
    SUM(f.total) AS total_sales,
    LAG(SUM(f.total)) OVER (ORDER BY d.year) AS prev_year_sales,
    ROUND(
        (SUM(f.total) - LAG(SUM(f.total)) OVER (ORDER BY d.year))
        / NULLIF(LAG(SUM(f.total)) OVER (ORDER BY d.year), 0) * 100, 2
    ) AS yoy_growth_pct
FROM fact_ventas f
JOIN dim_fecha d ON f.id_fecha = d.id_fecha
GROUP BY d.year
ORDER BY d.year;


-- ── Q8: Market Participation by Category ──────────────
-- Percentage of total revenue per product category
-- Technique: Window function SUM() OVER()
SELECT
    p.tipo_producto,
    SUM(f.total) AS total_sales,
    ROUND(
        SUM(f.total) * 100.0 / SUM(SUM(f.total)) OVER (), 2
    ) AS sales_percentage
FROM fact_ventas f
JOIN dim_producto p ON f.id_producto = p.id_producto
GROUP BY p.tipo_producto
ORDER BY sales_percentage DESC;


-- ════════════════════════════════════════════════════════
-- 6. ADVANCED ANALYTICAL QUERIES
-- ════════════════════════════════════════════════════════

-- ── Q9: Monthly Cumulative Sales (Running Total) ──────
-- Tracks revenue accumulation over time using window SUM
-- Technique: SUM() OVER(ORDER BY) — running aggregate
SELECT
    d.year,
    d.month,
    d.year_month,
    SUM(f.total) AS monthly_sales,
    SUM(SUM(f.total)) OVER (
        PARTITION BY d.year
        ORDER BY d.month
    ) AS cumulative_sales
FROM fact_ventas f
JOIN dim_fecha d ON f.id_fecha = d.id_fecha
GROUP BY d.year, d.month, d.year_month
ORDER BY d.year, d.month;


-- ── Q10: 3-Month Moving Average ──────────────────────
-- Smooths monthly fluctuations for trend detection
-- Technique: AVG() OVER(ROWS BETWEEN) — sliding window
SELECT
    year_month,
    monthly_sales,
    ROUND(
        AVG(monthly_sales) OVER (
            ORDER BY year_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    ) AS moving_avg_3m
FROM (
    SELECT
        d.year_month,
        SUM(f.total) AS monthly_sales
    FROM fact_ventas f
    JOIN dim_fecha d ON f.id_fecha = d.id_fecha
    GROUP BY d.year_month
) AS monthly_data
ORDER BY year_month;


-- ── Q11: Revenue Summary with ROLLUP ─────────────────
-- Multi-level aggregation: country → category → grand total
-- Technique: GROUP BY ROLLUP for hierarchical totals
SELECT
    COALESCE(r.pais, '*** GRAND TOTAL ***') AS pais,
    COALESCE(p.tipo_producto, '--- Subtotal ---') AS categoria,
    SUM(f.total) AS total_sales,
    COUNT(f.id_venta) AS num_transactions
FROM fact_ventas f
JOIN dim_producto p ON f.id_producto = p.id_producto
JOIN dim_cliente c ON f.id_cliente = c.id_cliente
JOIN dim_region r ON c.id_region = r.id_region
GROUP BY ROLLUP(r.pais, p.tipo_producto)
ORDER BY r.pais NULLS LAST, total_sales DESC;


-- ── Q12: Top Product per Country (CTE + ROW_NUMBER) ──
-- Finds the single best-selling product in each country
-- Technique: CTE + ROW_NUMBER for partitioned ranking
WITH ranked_products AS (
    SELECT
        r.pais,
        p.producto,
        p.tipo_producto,
        SUM(f.total) AS total_sales,
        ROW_NUMBER() OVER (
            PARTITION BY r.pais
            ORDER BY SUM(f.total) DESC
        ) AS rn
    FROM fact_ventas f
    JOIN dim_producto p ON f.id_producto = p.id_producto
    JOIN dim_cliente c ON f.id_cliente = c.id_cliente
    JOIN dim_region r ON c.id_region = r.id_region
    GROUP BY r.pais, p.producto, p.tipo_producto
)
SELECT pais, producto, tipo_producto, total_sales
FROM ranked_products
WHERE rn = 1
ORDER BY total_sales DESC;


-- ── Q13: Discount Impact Analysis ────────────────────
-- Compares revenue of discounted vs non-discounted sales
-- Technique: CASE WHEN + conditional aggregation
SELECT
    CASE
        WHEN f.descuento > 0 THEN 'Discounted'
        ELSE 'Full Price'
    END AS sale_type,
    COUNT(f.id_venta) AS num_transactions,
    SUM(f.total) AS total_revenue,
    ROUND(AVG(f.total), 2) AS avg_ticket,
    ROUND(AVG(f.descuento), 2) AS avg_discount
FROM fact_ventas f
GROUP BY
    CASE
        WHEN f.descuento > 0 THEN 'Discounted'
        ELSE 'Full Price'
    END;


-- ── Q14: Shipping Cost Efficiency by Region ──────────
-- Shipping cost as percentage of total revenue per country
-- Technique: Percentage calculation with GROUP BY
SELECT
    r.pais,
    SUM(f.costo_envio) AS total_shipping,
    SUM(f.total) AS total_revenue,
    ROUND(
        SUM(f.costo_envio) * 100.0 / NULLIF(SUM(f.total), 0), 2
    ) AS shipping_pct_of_revenue
FROM fact_ventas f
JOIN dim_cliente c ON f.id_cliente = c.id_cliente
JOIN dim_region r ON c.id_region = r.id_region
GROUP BY r.pais
ORDER BY shipping_pct_of_revenue DESC;

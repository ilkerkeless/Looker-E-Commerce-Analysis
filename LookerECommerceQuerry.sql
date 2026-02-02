# AVG SELLING TIME BY DEPARTMENT AND BRAND
CREATE OR REPLACE VIEW `lookerecommerceproject.Looker_E_Commerce_Project.avg_selling_time_by_dpt_brand` AS
WITH sold_items AS (
  SELECT
    created_at,
    sold_at,
    product_brand,
    product_department
  FROM `lookerecommerceproject.Looker_E_Commerce_Project.inventory_items`
  WHERE sold_at IS NOT NULL
    AND created_at IS NOT NULL
    AND sold_at >= created_at
),
item_metric AS (
  SELECT
     product_brand,
    product_department,
  DATE_DIFF(DATE(sold_at), DATE(created_at), DAY) AS days_to_sell
  FROM sold_items
)
SELECT
    product_brand,
    product_department,
  AVG(days_to_sell) AS avg_selling_time_days,
  COUNT(*) AS items_sold_count
FROM item_metric
GROUP BY 
   product_brand,
  product_department
 ;

# avg selling time by category
 CREATE OR REPLACE VIEW `lookerecommerceproject.Looker_E_Commerce_Project.avg_selling_time_by_category` AS
WITH sold_items AS (
  SELECT
    product_category,
    created_at,
    sold_at
  FROM `lookerecommerceproject.Looker_E_Commerce_Project.inventory_items`
  WHERE sold_at IS NOT NULL
    AND created_at IS NOT NULL
    AND sold_at >= created_at
),
item_metric AS (
  SELECT
    product_category,
    DATE_DIFF(DATE(sold_at), DATE(created_at), DAY) AS days_to_sell
  FROM sold_items
)
SELECT
  product_category,
  AVG(days_to_sell) AS avg_selling_time_days,
  COUNT(*) AS items_sold_count
FROM item_metric
GROUP BY product_category;



#PROFIT BY CATEGORY 

CREATE OR REPLACE VIEW `lookerecommerceproject.Looker_E_Commerce_Project.profit_by_category` AS
SELECT
  COALESCE(product_category, 'Unknown') AS product_category,
  SUM(product_retail_price) AS total_revenue,
  SUM(cost) AS total_cost,
  SUM(product_retail_price - cost) AS total_profit,
  COUNT(*) AS items_sold_count
FROM `lookerecommerceproject.Looker_E_Commerce_Project.inventory_items`
WHERE sold_at IS NOT NULL
  AND created_at IS NOT NULL
  AND sold_at >= created_at
GROUP BY product_category;

#PROFIT BY DEPARTMENT AND BRAND

CREATE OR REPLACE VIEW `lookerecommerceproject.Looker_E_Commerce_Project.profit_by_dpt_brand` AS
SELECT 
  COALESCE(product_department,'Unknown') as product_department,
  COALESCE(product_brand,'Unknown') as product_brand,
  SUM(product_retail_price) AS total_revenue,
  SUM(cost) AS total_cost,
  SUM(product_retail_price - cost) AS total_profit,
  COUNT(*) AS items_sold_count
 FROM `lookerecommerceproject.Looker_E_Commerce_Project.inventory_items` 
 WHERE sold_at IS NOT NULL
  AND created_at IS NOT NULL
  AND sold_at >= created_at
GROUP BY product_department,product_brand;


#EFFIENCY BY CATEGORY

CREATE OR REPLACE VIEW `lookerecommerceproject.Looker_E_Commerce_Project.efficiency_by_category` AS
SELECT
  st.product_category,
  st.avg_selling_time_days,
  st.items_sold_count AS selling_time_item_count,

  p.total_revenue,
  p.total_cost,
  p.total_profit,
  p.ites_sold_count AS profit_item_count,

  SAFE_DIVIDE(p.total_profit, p.items_sold_count) AS avg_profit_per_item,
  SAFE_DIVIDE(p.total_profit, NULLIF(st.avg_selling_time_days, 0)) AS profit_per_day
FROM `lookerecommerceproject.Looker_E_Commerce_Project.avg_selling_time_by_category` st
LEFT JOIN `lookerecommerceproject.Looker_E_Commerce_Project.profit_by_category` p
  ON st.product_category = p.product_category;


#EFFIENCY BY DEPARTMENT AND BRAND
CREATE OR REPLACE VIEW `lookerecommerceproject.Looker_E_Commerce_Project.efficiency_by_dpt_brand` AS
SELECT
  st.product_department,
  st.product_brand,
  st.avg_selling_time_days,
  st.items_sold_count AS selling_time_item_count,

  p.total_revenue,
  p.total_cost,
  p.total_profit,
  p.items_sold_count AS profit_item_count,

  SAFE_DIVIDE(p.total_profit, p.items_sold_count) AS avg_profit_per_item,
  SAFE_DIVIDE(p.total_profit, NULLIF(st.avg_selling_time_days, 0)) AS profit_per_day
FROM `lookerecommerceproject.Looker_E_Commerce_Project.avg_selling_time_by_dpt_brand` st
LEFT JOIN `lookerecommerceproject.Looker_E_Commerce_Project.profit_by_dpt_brand` p
  ON st.product_department = p.product_department
 AND st.product_brand = p.product_brand;

###########################################################################################################
 
# Brand Profit 

CREATE OR REPLACE VIEW `lookerecommerceproject.Looker_E_Commerce_Project.brand_profit` AS
SELECT
  COALESCE(product_brand, 'Unknown') AS product_brand,

  COUNT(*) AS items_sold_count,

  ROUND(SUM(product_retail_price),3) AS total_revenue,
  ROUND(SUM(cost),3) AS total_cost,
  ROUND(SUM(product_retail_price - cost),3) AS total_profit,

ROUND(
  SAFE_DIVIDE(
    SUM(product_retail_price - cost),
    SUM(product_retail_price)
  ),
  4
) AS profit_margin

FROM `lookerecommerceproject.Looker_E_Commerce_Project.inventory_items`
WHERE sold_at IS NOT NULL
  AND created_at IS NOT NULL
  AND sold_at >= created_at
GROUP BY product_brand;


# Product Cost 

CREATE OR REPLACE VIEW `lookerecommerceproject.Looker_E_Commerce_Project.dim_product_cost` AS
SELECT
  product_id,
  AVG(cost) AS avg_cost
FROM `lookerecommerceproject.Looker_E_Commerce_Project.inventory_items`
WHERE cost IS NOT NULL
GROUP BY product_id;

# Traffic Summary 

CREATE OR REPLACE VIEW `lookerecommerceproject.Looker_E_Commerce_Project.traffic_source_summary` AS
WITH valid_orders AS (
  SELECT
    order_id,
    user_id
  FROM `lookerecommerceproject.Looker_E_Commerce_Project.orders`
  WHERE status IN ('Complete', 'Shipped')
),
users_dim AS (
  SELECT
    id AS user_id,
    COALESCE(traffic_source, 'Unknown') AS traffic_source
  FROM `lookerecommerceproject.Looker_E_Commerce_Project.users`
),
order_items_enriched AS (
  SELECT
    oi.order_id,
    oi.product_id,
    oi.sale_price AS revenue  -- <-- burada fiyat kolonu adını senin tabloya göre değiştir
  FROM `lookerecommerceproject.Looker_E_Commerce_Project.order_items` oi
)
SELECT
  u.traffic_source,

  COUNT(DISTINCT o.order_id) AS total_orders,
  COUNT(DISTINCT o.user_id) AS total_customers,

  COUNT(*) AS items_count,
  ROUND(SUM(oi.revenue), 3) AS total_revenue,
  ROUND(SUM(pc.avg_cost), 3) AS total_cost,
  ROUND(SUM(oi.revenue - pc.avg_cost), 3) AS total_profit,

  SAFE_DIVIDE(SUM(oi.revenue - pc.avg_cost), SUM(oi.revenue)) AS profit_margin,
  SAFE_DIVIDE(COUNT(DISTINCT o.order_id), COUNT(DISTINCT o.user_id)) AS orders_per_customer
FROM valid_orders o
LEFT JOIN users_dim u
  ON o.user_id = u.user_id
LEFT JOIN order_items_enriched oi
  ON o.order_id = oi.order_id
LEFT JOIN `lookerecommerceproject.Looker_E_Commerce_Project.dim_product_cost` pc
  ON oi.product_id = pc.product_id
GROUP BY u.traffic_source;




# Sales Daily
 
CREATE OR REPLACE VIEW `lookerecommerceproject.Looker_E_Commerce_Project.sales_daily` AS
WITH valid_orders AS (
  SELECT
    order_id,
    user_id,
    DATE(created_at) AS order_date
  FROM `lookerecommerceproject.Looker_E_Commerce_Project.orders`
  WHERE status IN ('Complete', 'Shipped')
),
order_items_enriched AS (
  SELECT
    oi.order_id,
    oi.product_id,
    oi.sale_price AS revenue
  FROM `lookerecommerceproject.Looker_E_Commerce_Project.order_items` oi
)
SELECT
  o.order_date,

  COUNT(DISTINCT o.order_id) AS total_orders,
  COUNT(DISTINCT o.user_id) AS total_customers,
  COUNT(*) AS items_sold_count,

  ROUND(SUM(oi.revenue), 3) AS total_revenue,
  ROUND(SUM(COALESCE(pc.avg_cost, 0)), 3) AS total_cost,
  ROUND(SUM(oi.revenue - COALESCE(pc.avg_cost, 0)), 3) AS total_profit,

  SAFE_DIVIDE(
    SUM(oi.revenue - COALESCE(pc.avg_cost, 0)),
    SUM(oi.revenue)
  ) AS profit_margin,

  -- Zaman boyutları
  FORMAT_DATE('%A', o.order_date) AS day_of_week,
  EXTRACT(MONTH FROM o.order_date) AS month,
  EXTRACT(YEAR FROM o.order_date) AS year,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM o.order_date) IN (1,7) THEN 1
    ELSE 0
  END AS is_weekend

FROM valid_orders o
LEFT JOIN order_items_enriched oi
  ON o.order_id = oi.order_id
LEFT JOIN `lookerecommerceproject.Looker_E_Commerce_Project.dim_product_cost` pc
  ON oi.product_id = pc.product_id
GROUP BY o.order_date;















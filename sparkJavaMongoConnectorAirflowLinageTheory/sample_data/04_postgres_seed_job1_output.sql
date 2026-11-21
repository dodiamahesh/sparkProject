-- =============================================================================
--  04_postgres_seed_job1_output.sql
--  TEMPORARY: Job-1 (J1_PRODUCT_INVENTORY) nu expected output, jethi Job-2 ekla test thai shake.
--
--  Kem? Job-2 ne order line mate dim_product joie (J2-F02). Job-1 haju code ma nathi.
--  Job-1 ready thay pachhi aa file ni JARUR NATHI (Job-1 j aa tables bharshe).
--
--  Data design doc Section 5.5 mujab: brand 10, category 15, supplier 4, warehouse 3, product 20.
--  Re-run safe (WHERE NOT EXISTS / ON CONFLICT DO NOTHING).
--
--  Kram:
--    1. 03_postgres_create_tables.sql
--    2. aa file (PART A)
--    3. Job-2 FULL LOAD (window end 2026-10-21T00:00)
--    4. 02 Mongo incremental script + aa file no PART B (P024)
--    5. Job-2 INCREMENTAL (window end 2026-10-21T04:00)
-- =============================================================================

-- ============================ PART A: full load pehla ============================

INSERT INTO curated.dim_brand (brand_code, brand_name)
SELECT v.code, v.name FROM (VALUES
    ('B01','Volt'), ('B02','SoundMax'), ('B03','TechBook'), ('B04','UrbanWear'), ('B05','Ethnika'),
    ('B06','StepUp'), ('B07','KitchenPro'), ('B08','CookEasy'), ('B09','FitLife'), ('B10','PlayStrong')
) AS v(code, name)
WHERE NOT EXISTS (SELECT 1 FROM curated.dim_brand b WHERE b.brand_code = v.code);

-- Parent categories pehla (level 1), pachhi sub (level 2) parent key sathe
INSERT INTO curated.dim_category (category_code, category_name, level, parent_category_key)
SELECT v.code, v.name, 1, NULL FROM (VALUES
    ('C-ELE','Electronics'), ('C-FAS','Fashion'), ('C-HOM','Home & Kitchen'), ('C-SPO','Sports')
) AS v(code, name)
WHERE NOT EXISTS (SELECT 1 FROM curated.dim_category c WHERE c.category_code = v.code);

INSERT INTO curated.dim_category (category_code, category_name, level, parent_category_key)
SELECT v.code, v.name, 2, p.category_key FROM (VALUES
    ('C-ELE-MOB','Mobiles','C-ELE'), ('C-ELE-AUD','Audio','C-ELE'), ('C-ELE-LAP','Laptops','C-ELE'),
    ('C-ELE-WEA','Wearables','C-ELE'), ('C-FAS-MEN','Men Clothing','C-FAS'), ('C-FAS-WOM','Women Clothing','C-FAS'),
    ('C-FAS-FOO','Footwear','C-FAS'), ('C-HOM-KIT','Kitchen Appliances','C-HOM'), ('C-HOM-COO','Cookware','C-HOM'),
    ('C-SPO-FIT','Fitness','C-SPO'), ('C-SPO-CRI','Cricket','C-SPO')
) AS v(code, name, parent)
JOIN curated.dim_category p ON p.category_code = v.parent
WHERE NOT EXISTS (SELECT 1 FROM curated.dim_category c WHERE c.category_code = v.code);

INSERT INTO curated.dim_supplier (supplier_code, supplier_name, city)
SELECT v.code, v.name, v.city FROM (VALUES
    ('SUP-01','Shree Electronics Pvt Ltd','Ahmedabad'), ('SUP-02','Fashion Hub Traders','Surat'),
    ('SUP-03','HomeNeeds Distributors','Mumbai'), ('SUP-04','SportZone Wholesale','Jalandhar')
) AS v(code, name, city)
WHERE NOT EXISTS (SELECT 1 FROM curated.dim_supplier s WHERE s.supplier_code = v.code);

INSERT INTO curated.dim_warehouse (warehouse_code, city, state)
SELECT v.code, v.city, v.state FROM (VALUES
    ('WH-AHM','Ahmedabad','Gujarat'), ('WH-MUM','Mumbai','Maharashtra'), ('WH-DEL','Delhi','Delhi')
) AS v(code, city, state)
WHERE NOT EXISTS (SELECT 1 FROM curated.dim_warehouse w WHERE w.warehouse_code = v.code);

-- 20 sacha products (P021, P022, P023 ane P003 nu duplicate Job-1 ma reject thay, etle nathi)
INSERT INTO curated.dim_product (product_code, product_name, brand_key, category_key, supplier_key, current_price, is_active)
SELECT v.code, v.name, b.brand_key, c.category_key, s.supplier_key, v.price, 'Y'
FROM (VALUES
    ('P001','Smartphone Pro 5G','B01','C-ELE-MOB','SUP-01',25000),
    ('P002','Smartphone Lite','B01','C-ELE-MOB','SUP-01',12000),
    ('P003','Wireless Earbuds','B02','C-ELE-AUD','SUP-01',2000),
    ('P004','Bluetooth Speaker','B02','C-ELE-AUD','SUP-01',3500),
    ('P005','Laptop 14 inch','B03','C-ELE-LAP','SUP-01',55000),
    ('P006','Cotton T-Shirt','B04','C-FAS-MEN','SUP-02',500),
    ('P007','Denim Jeans','B04','C-FAS-MEN','SUP-02',1500),
    ('P008','Printed Kurti','B05','C-FAS-WOM','SUP-02',800),
    ('P009','Silk Saree','B05','C-FAS-WOM','SUP-02',4500),
    ('P010','Running Shoes','B06','C-FAS-FOO','SUP-02',2800),
    ('P011','Mixer Grinder','B07','C-HOM-KIT','SUP-03',3500),
    ('P012','Electric Kettle','B07','C-HOM-KIT','SUP-03',1200),
    ('P013','Non-stick Tawa','B08','C-HOM-COO','SUP-03',700),
    ('P014','Pressure Cooker 5L','B08','C-HOM-COO','SUP-03',1800),
    ('P015','Yoga Mat','B09','C-SPO-FIT','SUP-04',600),
    ('P016','Dumbbell Set 10kg','B09','C-SPO-FIT','SUP-04',2200),
    ('P017','Cricket Bat','B10','C-SPO-CRI','SUP-04',1800),
    ('P018','Cricket Ball (Pack of 6)','B10','C-SPO-CRI','SUP-04',900),
    ('P019','Smart Watch','B01','C-ELE-WEA','SUP-01',4000),
    ('P020','Sports Water Bottle','B09','C-SPO-FIT','SUP-04',350)
) AS v(code, name, brand, cat, sup, price)
JOIN curated.dim_brand b    ON b.brand_code = v.brand
JOIN curated.dim_category c ON c.category_code = v.cat
JOIN curated.dim_supplier s ON s.supplier_code = v.sup
WHERE NOT EXISTS (SELECT 1 FROM curated.dim_product p WHERE p.product_code = v.code);

-- Expected: 20
SELECT count(*) AS dim_product_rows FROM curated.dim_product;


-- ============== PART B: incremental (04:00) run PEHLA chalavo ==============
-- Job-1 na 04:00 run e P024 umeryo ane P003 ni price 1800 kari (doc Section 11.2).
-- O1024 ma P024 che: aa na hoy to e line FK_MISSING thi reject thashe.
--
-- INSERT INTO curated.dim_product (product_code, product_name, brand_key, category_key, supplier_key, current_price, is_active)
-- SELECT 'P024', 'Wireless Mouse', b.brand_key, c.category_key, s.supplier_key, 800, 'Y'
-- FROM curated.dim_brand b, curated.dim_category c, curated.dim_supplier s
-- WHERE b.brand_code = 'B03' AND c.category_code = 'C-ELE-LAP' AND s.supplier_code = 'SUP-01'
--   AND NOT EXISTS (SELECT 1 FROM curated.dim_product p WHERE p.product_code = 'P024');
-- UPDATE curated.dim_product SET current_price = 1800 WHERE product_code = 'P003';

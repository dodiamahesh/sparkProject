# 5 ETL Jobs – Raw (MongoDB) → Curated (PostgreSQL) : Data Design Guide

> Aa document **code lakhya pehla data samajva mate** che.
> Ahiya **koi Java / Spark code nathi**. Fakt data, rules, ane transformation pehla ane pachhi data kevu dekhay, e j che.
>
> - Source: **MongoDB** (database `shopdb`) – 9 collections. Job-5 (Customer 360) Mongo nahi, **curated tables** mathi vanche che
> - Target: **PostgreSQL / EPAS** – schema `curated` (dim + fact tables), ane schema `meta` (watermark, rejected, audit)
> - Layer: fakt **Raw → Curated**
> - Load: **pehli vaar Full Load**, pachhi **darek 4 kalake Incremental** (`updated_at` watermark thi)
> - Staging: **staging_* table nathi**. Direct insert / upsert dim ane fact ma (staging kyare jaruri pade e Section 12 ma)
> - Badha kharab records (duplicate, corrupt, validation fail) → ek j table **`meta.meta_rejected`**

---

## Table of Contents

1. [Big Picture – 5 Jobs, Flow ane Order](#1-big-picture--5-jobs-flow-ane-order)
2. [Source – MongoDB Collections ane Sample Data](#2-source--mongodb-collections-ane-sample-data)
3. [Common Framework – badha jobs ma same](#3-common-framework--badha-jobs-ma-same)
4. [Shared Table – dim_date](#4-shared-table--dim_date)
5. [Job-1: Single Collection → Multiple Dim + Fact (Snowflake)](#5-job-1-single-collection--multiple-dim--fact-snowflake)
6. [Job-2: Multiple Collections → Multiple Dim + Fact (Star)](#6-job-2-multiple-collections--multiple-dim--fact-star)
7. [Job-3: Multiple Collections → Single Table](#7-job-3-multiple-collections--single-table)
8. [Job-4: Shared + New Collections → Multiple Dim + Fact](#8-job-4-shared--new-collections--multiple-dim--fact)
9. [Job-5: Curated Tables → Customer 360 Summary](#9-job-5-curated-tables--customer-360-summary)
10. [Full Load pachhi – meta_rejected, watermark, audit](#10-full-load-pachhi--meta_rejected-watermark-audit)
11. [Incremental Run (04:00) – shu badlay che?](#11-incremental-run-0400--shu-badlay-che)
12. [Staging table vagar – shakya che? Kyare staging jaruri?](#12-staging-table-vagar--shakya-che-kyare-staging-jaruri)
13. [Star vs Snowflake – aapda jobs ma](#13-star-vs-snowflake--aapda-jobs-ma)
14. [Sawal-Jawab](#14-sawal-jawab)
15. [Next Step – Code Development mate Checklist](#15-next-step--code-development-mate-checklist)
16. [Concept Notes – SCD1 vs SCD2, Explode](#16-concept-notes--scd1-vs-scd2-explode)

---

## 1. Big Picture – 5 Jobs, Flow ane Order

### 1.1 Kayo job shu kare che?

| Job | Naam | Source Collections | Target Tables | Pattern |
|---|---|---|---|---|
| **Job-1** | `J1_PRODUCT_INVENTORY` | `products` (1 j) | `dim_brand`, `dim_category`, `dim_supplier`, `dim_warehouse`, `dim_product`, `fact_inventory_snapshot` | 1 collection → ghana dim + fact, **Snowflake** |
| **Job-2** | `J2_SALES` | `customers`, `orders`, `payments` | `dim_customer` (SCD2), `dim_payment_method`, `dim_order_status`, `fact_sales`, `fact_payment` | Ghani collections → ghana dim + fact, **Star** |
| **Job-3** | `J3_SUPPORT_TICKETS` | `email_tickets`, `chat_tickets`, `call_logs` | `support_ticket` (1 j table) | Ghani collections → **1 table** (UNION merge) |
| **Job-4** | `J4_FULFILMENT_REVIEWS` | `orders` (Job-2 pan vapare che), `shipments`, `reviews` | `dim_courier`, `fact_shipment`, `fact_product_review` (+ Job-1/Job-2 na dims reuse) | Shared + navi collections → ghana dim + fact |
| **Job-5** | `J5_CUSTOMER_360` | **Curated tables** (Mongo nahi): `dim_customer`, `fact_sales`, `fact_payment`, `support_ticket`, `fact_shipment`, `fact_product_review` | `customer_360_summary` (1 j table) | Ghana tables → **1 summary table** (JOIN merge + aggregate) |

### 1.2 Jobs no order (Airflow DAG)

```
                 ┌───────────────────────────┐
                 │ Job-1  J1_PRODUCT_INVENTORY│   dim_product banave
                 └─────────────┬─────────────┘
                               │ (dim_product joie)
                 ┌─────────────▼────────────┐
                 │ Job-2  J2_SALES           │   dim_customer banave
                 └──────┬──────────────┬─────┘
       (dim_customer joie)            (dim_customer, dim_product joie)
          ┌─────────────▼───┐   ┌──────▼───────────────────┐
          │ Job-3 SUPPORT   │   │ Job-4 FULFILMENT_REVIEWS │
          └────────┬────────┘   └────────────┬─────────────┘
                   │                         │
                   └────────────┬────────────┘
                 ┌──────────────▼────────────┐
                 │ Job-5  J5_CUSTOMER_360    │   customer_360_summary banave
                 └───────────────────────────┘
       (Job-3 ane Job-4 parallel chali shake; Job-5 banne puro thay pachhi j)
```

**Kem aa order?**
- Job-2 na `fact_sales` ne `product_key` joie che, je `dim_product` mathi aave che, ane `dim_product` Job-1 banave che.
- Job-3 ne email ke phone thi customer shodhvo che, ane Job-4 ne `customer_key` joie che. Banne `dim_customer` mathi aave che, je Job-2 banave che.
- Job-5 Job-2, Job-3 ane Job-4 na facts mathi summary banave che, etle e sauthi chhelle chale che.

### 1.3 Timeline

| Run | Samay (IST) | Prakar | Shu vanchay |
|---|---|---|---|
| Run-0 | **2026-10-21 00:00** | **Full Load** | Watermark khali che, etle badha documents |
| Run-1 | **2026-10-21 04:00** | Incremental | `updated_at` > 00:00 ane ≤ 04:00 vala j |
| Run-2 | 2026-10-21 08:00 | Incremental | `updated_at` > 04:00 ane ≤ 08:00 |
| … | darek 4 kalake | Incremental | … |

---

## 2. Source – MongoDB Collections ane Sample Data

### 2.1 Collections List

| # | Collection | Shu che? | Docs | Kayo job vapare? |
|---|---|---|---|---|
| 1 | `products` | Product master: brand, category, supplier ane warehouse stock **embedded** | 24 | Job-1 |
| 2 | `customers` | Customer master | 23 | Job-2 |
| 3 | `orders` | Order: items **array** sathe | 24 | Job-2, **Job-4** |
| 4 | `payments` | Payment transactions | 23 | Job-2 |
| 5 | `shipments` | Courier shipments (ek order na 2 package hoi sake) | 24 | Job-4 |
| 6 | `reviews` | Product reviews | 24 | Job-4 |
| 7 | `email_tickets` | Email thi aavela complaints | 21 | Job-3 |
| 8 | `chat_tickets` | Chat thi aavela complaints | 21 | Job-3 |
| 9 | `call_logs` | Call center na calls | 20 | Job-3 |

> Darek collection ma **jaanibujhi ne kharab data** mukyo che (⚠️ Note column ma), jethi rules samjay.

### 2.2 Collections vacche relation

```
 products ──(product_code)──┬────────── orders.items[] ──────────┐
                            │                                    │
                            └────────── reviews                  │
                                          │                      │
 customers ─(customer_code)─┬── orders ───┼── payments (order_no)│
                            │      │      │                      │
                            │      ├──────┴── shipments (order_no)
                            │      │
                            ├── reviews (customer_code, order_no)
                            │
                            ├── chat_tickets (customerCode)
                            ├── email_tickets (from_email = customers.email)
                            └── call_logs     (phone = customers.phone)
```

---

### 2.3 Collection: `products` (24 docs) → Job-1

**Ek full document:**
```json
{
  "_id": "6710a1f0c2a1b3d4e5f60001",
  "product_code": "P001",
  "name": "Smartphone Pro 5G",
  "brand":    { "code": "B01", "name": "Volt" },
  "category": { "code": "C-ELE-MOB", "name": "Mobiles",
                "parent": { "code": "C-ELE", "name": "Electronics" } },
  "supplier": { "code": "SUP-01", "name": "Shree Electronics Pvt Ltd", "city": "Ahmedabad" },
  "price": 25000,
  "stock": [ { "warehouse": "WH-AHM", "qty": 40 },
             { "warehouse": "WH-MUM", "qty": 25 } ],
  "is_active": true,
  "updated_at": "2026-10-01T09:00:00+05:30"
}
```

**Embedded master values (documents ni andar je values aave che):**

| Brand | Category (sub ← parent) | Supplier | Warehouse |
|---|---|---|---|
| B01 Volt | C-ELE-MOB Mobiles ← C-ELE Electronics | SUP-01 Shree Electronics Pvt Ltd, Ahmedabad | WH-AHM Ahmedabad |
| B02 SoundMax | C-ELE-AUD Audio ← Electronics | SUP-02 Fashion Hub Traders, Surat | WH-MUM Mumbai |
| B03 TechBook | C-ELE-LAP Laptops ← Electronics | SUP-03 HomeNeeds Distributors, Mumbai | WH-DEL Delhi |
| B04 UrbanWear | C-ELE-WEA Wearables ← Electronics | SUP-04 SportZone Wholesale, Jalandhar | |
| B05 Ethnika | C-FAS-MEN Men Clothing ← C-FAS Fashion | | |
| B06 StepUp | C-FAS-WOM Women Clothing ← Fashion | | |
| B07 KitchenPro | C-FAS-FOO Footwear ← Fashion | | |
| B08 CookEasy | C-HOM-KIT Kitchen Appliances ← C-HOM Home & Kitchen | | |
| B09 FitLife | C-HOM-COO Cookware ← Home & Kitchen | | |
| B10 PlayStrong | C-SPO-FIT Fitness ← C-SPO Sports | | |
| | C-SPO-CRI Cricket ← Sports | | |

**Badha 24 documents:**

| # | product_code | name (raw) | brand | category (sub) | supplier | price (raw) | stock[] (warehouse:qty) | updated_at | ⚠️ Note |
|---|---|---|---|---|---|---|---|---|---|
| 1 | P001 | Smartphone Pro 5G | B01 Volt | C-ELE-MOB | SUP-01 | 25000 | AHM:40, MUM:25 | 10-01 | |
| 2 | P002 | Smartphone Lite | B01 Volt | C-ELE-MOB | SUP-01 | 12000 | AHM:60, DEL:30 | 10-01 | |
| 3 | P003 | Wireless Earbuds | B02 SoundMax | C-ELE-AUD | SUP-01 | 2000 | AHM:100, MUM:80, DEL:50 | **10-20** | latest version |
| 4 | P004 | Bluetooth Speaker | B02 SoundMax | C-ELE-AUD | SUP-01 | 3500 | MUM:45 | 10-02 | |
| 5 | P005 | Laptop 14 inch | B03 TechBook | C-ELE-LAP | SUP-01 | 55000 | AHM:10, DEL:8 | 10-02 | |
| 6 | P006 | Cotton T-Shirt | B04 UrbanWear | C-FAS-MEN | SUP-02 | 500 | AHM:200, MUM:150 | 10-03 | |
| 7 | P007 | Denim Jeans | B04 **"urbanwear"** | C-FAS-MEN | SUP-02 | 1500 | AHM:90 | 10-03 | 🧹 brand name lowercase |
| 8 | P008 | Printed Kurti | B05 Ethnika | C-FAS-WOM | SUP-02 | 800 | AHM:120, DEL:60 | 10-04 | |
| 9 | P009 | Silk Saree | B05 Ethnika | C-FAS-WOM | SUP-02 | 4500 | MUM:20 | 10-04 | |
| 10 | P010 | Running Shoes | B06 StepUp | C-FAS-FOO | SUP-02 | 2800 | **"wh-ahm ":35**, MUM:40, DEL:25 | 10-05 | 🧹 warehouse code lowercase + space |
| 11 | P011 | Mixer Grinder | B07 KitchenPro | C-HOM-KIT | SUP-03 | 3500 | MUM:30 | 10-05 | |
| 12 | P012 | Electric Kettle | B07 KitchenPro | C-HOM-KIT | SUP-03 | **"₹1,200"** | AHM:50, MUM:50 | 10-06 | 🧹 price string |
| 13 | P013 | **"  non-stick tawa "** | B08 CookEasy | C-HOM-COO | SUP-03 | 700 | AHM:80 | 10-06 | 🧹 name spaces + lowercase |
| 14 | P014 | Pressure Cooker 5L | B08 CookEasy | C-HOM-COO | SUP-03 | 1800 | DEL:40 | 10-07 | |
| 15 | P015 | Yoga Mat | B09 FitLife | C-SPO-FIT | SUP-04 | 600 | AHM:70, DEL:70 | 10-07 | |
| 16 | P016 | Dumbbell Set 10kg | B09 FitLife | C-SPO-FIT | SUP-04 | 2200 | AHM:15, **MUM:-5** | 10-08 | ⚠️ ek stock line negative |
| 17 | P017 | Cricket Bat | B10 PlayStrong | C-SPO-CRI | SUP-04 | 1800 | AHM:25, DEL:30 | 10-08 | |
| 18 | P018 | Cricket Ball (Pack of 6) | B10 PlayStrong | C-SPO-CRI | SUP-04 | 900 | AHM:60 | 10-09 | |
| 19 | P019 | Smart Watch | B01 Volt | C-ELE-WEA | SUP-01 | 4000 | AHM:30, DEL:20 | 10-09 | |
| 20 | P020 | Sports Water Bottle | B09 FitLife | C-SPO-FIT | SUP-04 | 350 | DEL:90 | 10-10 | |
| 21 | **P003** | Wireless Earbuds | B02 SoundMax | C-ELE-AUD | SUP-01 | 1900 | AHM:120 | **10-05** | ⚠️ **DUPLICATE** (juno version) |
| 22 | P021 | Gym Gloves | B09 FitLife | **null** | SUP-04 | 450 | AHM:40 | 10-11 | ⚠️ category missing |
| 23 | P022 | Steel Lunch Box | B08 CookEasy | C-HOM-COO | SUP-03 | **"abc"** | AHM:30 | 10-11 | ⚠️ corrupt price |
| 24 | P023 | Travel Bag | B04 UrbanWear | C-FAS-MEN | SUP-02 | **-150** | MUM:10 | 10-12 | ⚠️ negative price |

---

### 2.4 Collection: `customers` (23 docs) → Job-2

```json
{
  "_id": "6710b2a0c2a1b3d4e5f70001",
  "customer_code": "C001",
  "name": "Rahul Patel",
  "email": "rahul.patel@mail.com",
  "phone": "9876500001",
  "address": { "city": "Ahmedabad", "state": "Gujarat", "pincode": "380001" },
  "tier": "Gold",
  "signup_date": "2025-01-10",
  "updated_at": "2026-10-01T10:00:00+05:30"
}
```

| # | customer_code | name | email (raw) | phone (raw) | city (raw) | state (raw) | tier | signup_date | updated_at | ⚠️ Note |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | C001 | Rahul Patel | rahul.patel@mail.com | 9876500001 | Ahmedabad | Gujarat | Gold | 2025-01-10 | 10-01 | |
| 2 | C002 | Priya Shah | **"  PRIYA.SHAH@MAIL.COM "** | 9876500002 | Surat | Gujarat | Silver | 2025-02-14 | 10-01 | 🧹 email uppercase + space |
| 3 | C003 | Amit Mehta | amit.mehta@mail.com | 9876500003 | Mumbai | Maharashtra | Gold | 2025-03-05 | 10-01 | |
| 4 | C004 | Neha Joshi | neha.joshi@mail.com | **"+91-98765 00004"** | Pune | Maharashtra | Bronze | 2025-04-20 | 10-01 | 🧹 phone format |
| 5 | C005 | Karan Desai | karan.desai@mail.com | 9876500005 | Vadodara | Gujarat | Silver | 2025-05-11 | **10-02** | latest version |
| 6 | C006 | Sneha Iyer | sneha.iyer@mail.com | 9876500006 | Bengaluru | Karnataka | Gold | 2025-06-01 | 10-02 | |
| 7 | C007 | Vikram Singh | vikram.singh@mail.com | 9876500007 | Jaipur | **"RJ"** | Bronze | 2025-07-19 | 10-02 | 🧹 state code |
| 8 | C008 | Pooja Trivedi | pooja.trivedi@mail.com | 9876500008 | Rajkot | Gujarat | Silver | 2025-08-08 | 10-03 | |
| 9 | C009 | Arjun Nair | arjun.nair@mail.com | 9876500009 | Kochi | Kerala | Bronze | 2025-09-25 | 10-03 | (koi order nathi) |
| 10 | C010 | Riya Kapoor | riya.kapoor@mail.com | 9876500010 | Delhi | Delhi | Gold | 2025-10-30 | 10-03 | |
| 11 | C011 | Mehul Shah | mehul.shah@mail.com | 9876500011 | Ahmedabad | Gujarat | **null** | 2025-11-12 | 10-04 | 🧹 tier missing → default |
| 12 | C012 | Kavya Reddy | kavya.reddy@mail.com | 9876500012 | Hyderabad | Telangana | Silver | 2025-12-03 | 10-04 | |
| 13 | C013 | Rohan Gupta | rohan.gupta@mail.com | 9876500013 | **"lucknow "** | Uttar Pradesh | Bronze | 2026-01-15 | 10-04 | 🧹 city case + space |
| 14 | C014 | Isha Bhatt | isha.bhatt@mail.com | 9876500014 | Bhavnagar | Gujarat | Gold | 2026-02-08 | 10-05 | |
| 15 | C015 | Siddharth Rao | siddharth.rao@mail.com | 9876500015 | Chennai | Tamil Nadu | Silver | 2026-03-21 | 10-05 | |
| 16 | C016 | Ananya Das | ananya.das@mail.com | 9876500016 | Kolkata | West Bengal | Bronze | 2026-04-02 | 10-05 | |
| 17 | C017 | Harsh Vora | harsh.vora@mail.com | 9876500017 | Surat | Gujarat | Silver | 2026-05-17 | 10-06 | |
| 18 | C018 | Nidhi Agarwal | nidhi.agarwal@mail.com | 9876500018 | Indore | Madhya Pradesh | Bronze | 2026-06-09 | 10-06 | |
| 19 | C019 | Yash Thakkar | yash.thakkar@mail.com | 9876500019 | Ahmedabad | Gujarat | Gold | 2026-07-23 | 10-06 | |
| 20 | C020 | Diya Menon | diya.menon@mail.com | 9876500020 | Mumbai | Maharashtra | Silver | 2026-08-30 | 10-07 | |
| 21 | **C005** | Karan Desai | karan.desai@mail.com | 9876500005 | Vadodara | Gujarat | Bronze | 2025-05-11 | **09-01** | ⚠️ **DUPLICATE** (juno) |
| 22 | C021 | Test User | **"test@@mail"** | 9876500021 | Surat | Gujarat | Bronze | 2026-09-10 | 10-08 | ⚠️ invalid email |
| 23 | C022 | **null** | no.name@mail.com | 9876500022 | Pune | Maharashtra | Bronze | 2026-09-15 | 10-08 | ⚠️ name missing |

---

### 2.5 Collection: `orders` (24 docs) → Job-2 ane Job-4

```json
{
  "_id": "6710c3b0c2a1b3d4e5f80001",
  "order_no": "O1001",
  "customer_code": "C001",
  "order_ts": "2026-10-01T10:15:00+05:30",
  "status": "DELIVERED",
  "payment_method": "UPI",
  "items": [
    { "product_code": "P001", "qty": 1, "unit_price": 25000 },
    { "product_code": "P003", "qty": 2, "unit_price": 2000 }
  ],
  "shipping": { "city": "Ahmedabad", "state": "Gujarat", "pincode": "380001" },
  "order_total": 29000,
  "updated_at": "2026-10-03T14:00:00+05:30"
}
```

| # | order_no | customer | order_ts | status (raw) | payment (raw) | items (product × qty @ price) | order_total | updated_at | ⚠️ Note |
|---|---|---|---|---|---|---|---|---|---|
| 1 | O1001 | C001 | 10-01 10:15 | DELIVERED | UPI | P001×1@25000, P003×2@2000 | 29000 | 10-03 | |
| 2 | O1002 | C002 | 10-01 12:40 | **"delivered"** | COD | P008×3@800 | 2400 | 10-04 | 🧹 status lowercase |
| 3 | O1003 | C003 | 10-02 09:05 | DELIVERED | CARD | P005×1@55000 | 55000 | 10-06 | |
| 4 | O1004 | C001 | 10-03 18:20 | CANCELLED | UPI | P017×1@1800, P015×1@600 | 2400 | 10-03 | |
| 5 | O1005 | C004 | 10-03 20:10 | DELIVERED | UPI | P006×4@500, P007×1@1500 | 3500 | **10-06** | latest version |
| 6 | O1006 | C005 | 10-04 11:00 | DELIVERED | CARD | P011×1@3500, P013×2@700 | 4900 | 10-07 | |
| 7 | O1007 | C006 | 10-05 15:30 | DELIVERED | **"upi "** | P003×1@2000 | 2000 | 10-07 | 🧹 payment lowercase + space |
| 8 | O1008 | C007 | 10-05 16:45 | RETURNED | COD | P017×2@1800, **P018×0**@900 | 3600 | 10-09 | ⚠️ line-2 qty 0 |
| 9 | O1009 | C008 | 10-06 10:00 | DELIVERED | UPI | P008×2@800, P006×2@500 | 2600 | 10-08 | |
| 10 | O1010 | C010 | 10-07 13:25 | PLACED | CARD | P001×1@25000, P005×1@55000 | 80000 | 10-07 | |
| 11 | O1011 | C011 | 10-08 09:40 | DELIVERED | UPI | P012×1@1200, P014×1@1800 | 3000 | 10-10 | |
| 12 | O1012 | C012 | 10-08 19:15 | DELIVERED | **"Net Banking"** | P002×1@12000, **P022**×1@999 | 12999 | 10-13 | 🧹 payment naam / ⚠️ line-2 product dim ma nathi |
| 13 | O1013 | C013 | 10-09 14:00 | DELIVERED | **"Cash on Delivery"** | P010×1@2800 | 2800 | 10-12 | 🧹 payment naam |
| 14 | O1014 | C014 | 10-10 11:30 | DELIVERED | CARD | P009×1@4500, P008×1@800 | 5300 | 10-16 | |
| 15 | O1015 | C015 | 10-11 17:50 | SHIPPED | UPI | P019×1@4000 | 4000 | 10-12 | (haju raste che) |
| 16 | O1016 | C016 | 10-12 08:20 | DELIVERED | UPI | P016×1@2200, P015×2@600 | 3400 | 10-17 | |
| 17 | O1017 | C017 | 10-13 21:05 | DELIVERED | NETBANKING | P004×1@3500 | 3500 | 10-16 | |
| 18 | O1018 | C018 | 10-14 12:10 | CANCELLED | COD | P020×2@350 | 700 | 10-14 | |
| 19 | O1019 | C019 | 10-15 10:45 | DELIVERED | CARD | P001×1@25000, P003×1@2000 | 27000 | 10-18 | |
| 20 | O1020 | C020 | 10-16 16:30 | DELIVERED | UPI | P012×2@1200 | 2400 | 10-19 | |
| 21 | **O1005** | C004 | 10-03 20:10 | PLACED | UPI | P006×4@500, P007×1@1500 | 3500 | **10-03** | ⚠️ **DUPLICATE** (juno) |
| 22 | O1021 | **C099** | 10-17 11:00 | PLACED | UPI | P003×1@2000 | 2000 | 10-17 | ⚠️ customer exist nathi |
| 23 | O1022 | C009 | **"2026-13-45"** | PLACED | CARD | P015×1@600 | 600 | 10-18 | ⚠️ corrupt date |
| 24 | O1023 | C011 | 10-19 15:00 | PLACED | UPI | **[] (khali)** | 0 | 10-19 | ⚠️ items nathi |

---

### 2.6 Collection: `payments` (23 docs) → Job-2

```json
{
  "_id": "6710d4c0c2a1b3d4e5f90001",
  "payment_id": "PAY01",
  "order_no": "O1001",
  "method": "UPI",
  "amount": "₹29,000",
  "status": "SUCCESS",
  "paid_at": "2026-10-01T10:16:00+05:30",
  "updated_at": "2026-10-01T10:16:00+05:30"
}
```

| # | payment_id | order_no | method (raw) | amount (raw) | status (raw) | paid_at | updated_at | ⚠️ Note |
|---|---|---|---|---|---|---|---|---|
| 1 | PAY01 | O1001 | UPI | **"₹29,000"** | SUCCESS | 10-01 | 10-01 | 🧹 amount string |
| 2 | PAY02 | O1002 | COD | 2400 | SUCCESS | 10-04 | 10-04 | (COD: delivery vakhte) |
| 3 | PAY03 | O1003 | CARD | 55000 | SUCCESS | 10-02 | 10-02 | |
| 4 | PAY04 | O1004 | UPI | 2400 | REFUNDED | 10-03 | 10-03 | |
| 5 | PAY05 | O1005 | UPI | 3500 | SUCCESS | 10-03 | **10-03 20:12** | latest |
| 6 | PAY06 | O1006 | CARD | 4900 | SUCCESS | 10-04 | 10-04 | |
| 7 | PAY07 | O1007 | UPI | 2000 | **"success"** | 10-05 | 10-05 | 🧹 status lowercase |
| 8 | PAY08 | O1008 | COD | 3600 | REFUNDED | 10-07 | 10-09 | |
| 9 | PAY09 | O1009 | UPI | 2600 | SUCCESS | 10-06 | 10-06 | |
| 10 | PAY10 | O1010 | CARD | 80000 | PENDING | 10-07 | 10-07 | |
| 11 | PAY11 | O1011 | UPI | 3000 | SUCCESS | 10-08 | 10-08 | |
| 12 | PAY12 | O1012 | **"Net Banking"** | 12999 | SUCCESS | 10-08 | 10-08 | 🧹 method naam |
| 13 | PAY13 | O1013 | COD | 2800 | SUCCESS | 10-12 | 10-12 | |
| 14 | PAY14 | O1014 | CARD | 5300 | SUCCESS | 10-10 | 10-10 | |
| 15 | PAY15 | O1015 | UPI | 4000 | SUCCESS | 10-11 | 10-11 | |
| 16 | PAY16 | O1016 | UPI | 3400 | SUCCESS | 10-12 | 10-12 | |
| 17 | PAY17 | O1017 | NETBANKING | 3500 | SUCCESS | 10-13 | 10-13 | |
| 18 | PAY18 | O1018 | COD | 700 | FAILED | 10-14 | 10-14 | (cancelled COD) |
| 19 | PAY19 | O1019 | CARD | 27000 | SUCCESS | 10-15 | 10-15 | |
| 20 | PAY20 | O1020 | UPI | 2400 | SUCCESS | 10-16 | 10-16 | |
| 21 | **PAY05** | O1005 | UPI | 3500 | SUCCESS | 10-03 | **10-03 20:11** | ⚠️ **DUPLICATE** (gateway callback 2 vaar) |
| 22 | PAY21 | **O1021** | UPI | 2000 | SUCCESS | 10-17 | 10-17 | ⚠️ order reject thayo che |
| 23 | PAY22 | O1020 | UPI | **"abc"** | FAILED | 10-16 | 10-16 | ⚠️ corrupt amount |

---

### 2.7 Collection: `shipments` (24 docs) → Job-4

```json
{
  "_id": "6710e5d0c2a1b3d4e5fa0001",
  "shipment_id": "SH-001A",
  "order_no": "O1001",
  "courier": "SpeedEx",
  "dispatched_at": "2026-10-01T18:00:00+05:30",
  "promised_date": "2026-10-04",
  "delivered_at": "2026-10-03T14:00:00+05:30",
  "status": "DELIVERED",
  "updated_at": "2026-10-03T14:00:00+05:30"
}
```

> Ek order ma 2 package hoi sake, etle ek order na 2 shipments bane (jem ke O1001 → SH-001A, SH-001B).

| # | shipment_id | order_no | courier (raw) | dispatched | promised | delivered | status | updated_at | ⚠️ Note |
|---|---|---|---|---|---|---|---|---|---|
| 1 | SH-001A | O1001 | SpeedEx | 10-01 | 10-04 | 10-03 | DELIVERED | 10-03 | |
| 2 | SH-001B | O1001 | SpeedEx | 10-02 | 10-05 | 10-04 | DELIVERED | 10-04 | (2nd package) |
| 3 | SH-002 | O1002 | QuickMove | 10-02 | 10-05 | 10-04 | DELIVERED | 10-04 | |
| 4 | SH-003 | O1003 | **"speedex"** | 10-02 | 10-05 | 10-06 | DELIVERED | 10-06 | 🧹 courier lowercase |
| 5 | SH-005 | O1005 | EcoDeliver | 10-04 | 10-07 | 10-06 | DELIVERED | **10-06** | latest |
| 6 | SH-006 | O1006 | QuickMove | 10-05 | 10-08 | 10-07 | DELIVERED | 10-07 | |
| 7 | SH-007 | O1007 | SpeedEx | 10-05 | 10-08 | 10-07 | DELIVERED | 10-07 | |
| 8 | SH-008 | O1008 | EcoDeliver | 10-06 | 10-09 | 10-08 | RETURNED | 10-09 | |
| 9 | SH-009 | O1009 | QuickMove | 10-06 | 10-09 | 10-08 | DELIVERED | 10-08 | |
| 10 | SH-011 | O1011 | SpeedEx | 10-08 | 10-11 | 10-10 | DELIVERED | 10-10 | |
| 11 | SH-012 | O1012 | SpeedEx | 10-09 | 10-12 | 10-13 | DELIVERED | 10-13 | (late) |
| 12 | SH-013 | O1013 | EcoDeliver | 10-09 | 10-12 | 10-12 | DELIVERED | 10-12 | |
| 13 | SH-014A | O1014 | QuickMove | 10-10 | 10-13 | 10-12 | DELIVERED | 10-12 | |
| 14 | SH-014B | O1014 | **"QUICKMOVE "** | 10-11 | 10-14 | 10-16 | DELIVERED | 10-16 | 🧹 courier case + space |
| 15 | SH-015 | O1015 | SpeedEx | 10-12 | 10-15 | null | IN_TRANSIT | 10-12 | (promise date vitavi gai) |
| 16 | SH-016 | O1016 | EcoDeliver | 10-12 | 10-15 | 10-17 | DELIVERED | 10-17 | (late) |
| 17 | SH-017 | O1017 | QuickMove | 10-14 | 10-17 | 10-16 | DELIVERED | 10-16 | |
| 18 | SH-019A | O1019 | SpeedEx | 10-15 | 10-18 | 10-17 | DELIVERED | 10-17 | |
| 19 | SH-019B | O1019 | SpeedEx | 10-16 | 10-19 | 10-18 | DELIVERED | 10-18 | |
| 20 | SH-020 | O1020 | EcoDeliver | 10-17 | 10-20 | 10-19 | DELIVERED | 10-19 | |
| 21 | **SH-005** | O1005 | EcoDeliver | 10-04 | 10-07 | null | IN_TRANSIT | **10-04** | ⚠️ **DUPLICATE** (juno) |
| 22 | SH-004 | **O1004** | SpeedEx | 10-04 | 10-07 | null | IN_TRANSIT | 10-04 | ⚠️ order CANCELLED che |
| 23 | SH-021 | **O1099** | QuickMove | 10-18 | 10-21 | null | IN_TRANSIT | 10-18 | ⚠️ order exist nathi |
| 24 | SH-013B | O1013 | EcoDeliver | **10-12** | 10-15 | **10-10** | DELIVERED | 10-12 | ⚠️ delivery dispatch pehla! |

---

### 2.8 Collection: `reviews` (24 docs) → Job-4

```json
{
  "_id": "6710f6e0c2a1b3d4e5fb0001",
  "review_id": "RV01",
  "product_code": "P001",
  "customer_code": "C001",
  "order_no": "O1001",
  "rating": 5,
  "comment": "Superb phone",
  "review_date": "2026-10-05",
  "updated_at": "2026-10-05T19:00:00+05:30"
}
```

| # | review_id | product | customer | order_no | rating (raw) | comment (raw) | review_date | updated_at | ⚠️ Note |
|---|---|---|---|---|---|---|---|---|---|
| 1 | RV01 | P001 | C001 | O1001 | 5 | Superb phone | 10-05 | 10-05 | |
| 2 | RV02 | P003 | C001 | O1001 | 4 | Good sound | 10-05 | 10-05 | |
| 3 | RV03 | P008 | C002 | O1002 | 4 | **"  nice fabric!!  "** | 10-06 | 10-06 | 🧹 comment trim |
| 4 | RV04 | P005 | C003 | O1003 | 3 | Good but heats up | 10-08 | 10-08 | |
| 5 | RV05 | P006 | C004 | O1005 | 5 | Comfortable | 10-08 | 10-08 | |
| 6 | RV06 | P007 | C004 | O1005 | 4 | Good fit | 10-08 | 10-08 | |
| 7 | RV07 | P003 | C006 | O1007 | 3 | Left earbud weak, replaced now ok | 10-09 | **10-12** | latest (edit karyu) |
| 8 | RV08 | P008 | C008 | O1009 | 5 | Loved it | 10-10 | 10-10 | |
| 9 | RV09 | P006 | C008 | O1009 | 4 | Value for money | 10-10 | 10-10 | |
| 10 | RV10 | P012 | C011 | O1011 | 1 | Arrived damaged | 10-11 | 10-11 | |
| 11 | RV11 | P014 | C011 | O1011 | **"4.0"** | Cooks fast | 10-12 | 10-12 | 🧹 rating string |
| 12 | RV12 | P002 | C012 | O1012 | 4 | Good budget phone | 10-14 | 10-14 | |
| 13 | RV13 | P010 | C013 | O1013 | 3 | Size slightly small | 10-13 | 10-13 | |
| 14 | RV14 | P009 | C014 | O1014 | 3 | Colour different | 10-17 | 10-17 | |
| 15 | RV15 | P008 | C014 | O1014 | 5 | Beautiful | 10-17 | 10-17 | |
| 16 | RV16 | P016 | C016 | O1016 | 4 | Solid build | 10-18 | 10-18 | |
| 17 | RV17 | P004 | C017 | O1017 | 2 | Bass is low | 10-17 | 10-17 | |
| 18 | RV18 | P001 | C019 | O1019 | 5 | Fast and smooth | 10-18 | 10-18 | |
| 19 | RV19 | P003 | C019 | O1019 | 4 | Good | 10-19 | 10-19 | |
| 20 | RV20 | P012 | C020 | O1020 | 4 | Boils quickly | 10-20 | 10-20 | |
| 21 | **RV07** | P003 | C006 | O1007 | 2 | Left earbud weak | 10-09 | **10-09** | ⚠️ **DUPLICATE** (juno) |
| 22 | RV21 | **P005** | C005 | **O1006** | 1 | Not good | 10-12 | 10-12 | ⚠️ O1006 ma P005 nathi |
| 23 | RV22 | P015 | C016 | O1016 | **7** | Great mat | 10-18 | 10-18 | ⚠️ rating 1–5 bahar |
| 24 | RV24 | P017 | C007 | O1008 | **"five"** | Nice bat | 10-10 | 10-10 | ⚠️ corrupt rating |

---

### 2.9 Collection: `email_tickets` (21 docs) → Job-3

```json
{
  "_id": "6711a7f0c2a1b3d4e5fc0001",
  "ticket_ref": "EM-001",
  "from_email": "rahul.patel@mail.com",
  "subject": "Order O1001 delivery late",
  "received_at": "2026-10-02T09:10:00+05:30",
  "status": "closed",
  "priority": "High",
  "order_no": "O1001",
  "updated_at": "2026-10-02T15:00:00+05:30"
}
```

| # | ticket_ref | from_email (raw) | subject | received_at (IST) | status | priority | order_no | ⚠️ Note |
|---|---|---|---|---|---|---|---|---|
| 1 | EM-001 | rahul.patel@mail.com | Order O1001 delivery late | 10-02 09:10 | closed | High | O1001 | |
| 2 | EM-002 | priya.shah@mail.com | Kurti size issue - want exchange | 10-05 11:00 | closed | Medium | O1002 | |
| 3 | EM-003 | amit.mehta@mail.com | Laptop not working after update | 10-06 14:20 | open | High | O1003 | |
| 4 | EM-004 | neha.joshi@mail.com | Payment debited twice | 10-04 08:30 | closed | High | O1005 | |
| 5 | EM-005 | karan.desai@mail.com | When will my order be delivered? | 10-06 10:00 | pending | Low | O1006 | |
| 6 | EM-006 | sneha.iyer@mail.com | Earbuds left side not working | 10-08 16:45 | open | Medium | O1007 | |
| 7 | EM-007 | vikram.singh@mail.com | Refund for returned bat | 10-09 12:00 | open | High | O1008 | |
| 8 | EM-008 | pooja.trivedi@mail.com | Change delivery address | 10-06 09:00 | closed | Low | O1009 | |
| 9 | EM-009 | riya.kapoor@mail.com | Payment pending for card order | 10-08 10:30 | pending | High | O1010 | |
| 10 | EM-010 | mehul.shah@mail.com | Kettle damaged in box | 10-10 18:00 | closed | High | O1011 | |
| 11 | EM-011 | kavya.reddy@mail.com | Invoice copy needed | 10-11 11:20 | closed | Low | O1012 | |
| 12 | EM-012 | rohan.gupta@mail.com | Shoes size exchange | 10-12 13:00 | open | Medium | O1013 | |
| 13 | EM-013 | isha.bhatt@mail.com | Saree colour different, quality issue | 10-13 15:10 | closed | Medium | O1014 | |
| 14 | EM-014 | unknown.person@mail.com | Forgot password | 10-14 09:30 | closed | Low | – | (customer list ma nathi) |
| 15 | EM-015 | **noreply@spam.xyz** | WIN PRIZE NOW | 10-14 03:00 | open | Low | – | ⚠️ spam |
| 16 | EM-016 | ananya.das@mail.com | Dumbbell delivery not received | 10-15 10:40 | open | High | O1016 | |
| 17 | EM-017 | harsh.vora@mail.com | Speaker not working | 10-16 17:30 | pending | Medium | O1017 | |
| 18 | EM-018 | **" NIDHI.AGARWAL@MAIL.COM "** | Refund for cancelled order | 10-15 12:00 | closed | Medium | O1018 | 🧹 email case + space |
| 19 | EM-019 | yash.thakkar@mail.com | Delivery late | **null** | open | High | O1019 | ⚠️ time missing |
| 20 | EM-020 | diya.menon@mail.com | Return kettle | 10-18 11:15 | open | Low | O1020 | |
| 21 | **EM-007** | vikram.singh@mail.com | Refund for returned bat | 10-09 12:00 | open | High | O1008 | ⚠️ **DUPLICATE** (email 2 vaar process) |

---

### 2.10 Collection: `chat_tickets` (21 docs) → Job-3

> Dhyan: aa collection na **field naam alag** che (`chatId`, `customerCode`, `startedAt`), ane time **UTC** ma che (IST = UTC + 5:30).

```json
{
  "_id": "6711b8a0c2a1b3d4e5fd0001",
  "chatId": "CH-001",
  "customerCode": "C001",
  "topic": "upi payment failed but money deducted",
  "startedAt": "2026-10-03T12:55:00Z",
  "state": "RESOLVED",
  "agent": "Meena",
  "updatedAt": "2026-10-03T13:20:00Z"
}
```

| # | chatId | customerCode | topic | startedAt (**UTC**) | state | agent | ⚠️ Note |
|---|---|---|---|---|---|---|---|
| 1 | CH-001 | C001 | upi payment failed but money deducted | 10-03 12:55 | RESOLVED | Meena | |
| 2 | CH-002 | C003 | laptop delivery status | 10-02 04:30 | RESOLVED | Ravi | |
| 3 | CH-003 | C004 | jeans size exchange | 10-07 07:00 | PENDING | Meena | |
| 4 | CH-004 | C005 | mixer courier late | 10-07 13:30 | TRANSFERRED | Ravi | |
| 5 | CH-005 | C006 | earbuds defective replace | 10-09 05:40 | RESOLVED | Sonal | |
| 6 | CH-006 | C008 | kurti return pickup | 10-10 09:15 | PENDING | Sonal | |
| 7 | CH-007 | C009 | account login problem | 10-11 03:50 | RESOLVED | Ravi | |
| 8 | CH-008 | C010 | card payment pending | 10-08 08:20 | TRANSFERRED | Meena | |
| 9 | CH-009 | C011 | pressure cooker quality | 10-12 10:30 | RESOLVED | Sonal | |
| 10 | CH-010 | C012 | phone delivery tracking | 10-09 05:00 | RESOLVED | Ravi | |
| 11 | CH-011 | C013 | shoes size issue | **"abc"** | PENDING | Meena | ⚠️ corrupt time |
| 12 | CH-012 | C014 | saree return | 10-14 06:10 | PENDING | Meena | |
| 13 | CH-013 | C015 | smart watch delivery late | 10-14 12:40 | TRANSFERRED | Ravi | |
| 14 | CH-014 | C016 | yoga mat damaged | 10-15 03:25 | RESOLVED | Sonal | |
| 15 | CH-015 | C017 | netbanking refund | 10-16 14:30 | PENDING | Meena | |
| 16 | CH-016 | C018 | order cancelled refund status | 10-15 08:00 | RESOLVED | Ravi | |
| 17 | CH-017 | C019 | thanks for quick service | 10-17 06:30 | RESOLVED | Sonal | |
| 18 | CH-018 | **C099** | where is my order | 10-18 04:40 | PENDING | Meena | (customer list ma nathi) |
| 19 | CH-019 | C020 | kettle return pickup date | 10-19 04:15 | PENDING | Ravi | |
| 20 | CH-020 | C002 | address change | 10-19 11:50 | RESOLVED | Sonal | |
| 21 | **CH-005** | C006 | earbuds defective replace | 10-09 05:40 | RESOLVED | Sonal | ⚠️ **DUPLICATE** |

---

### 2.11 Collection: `call_logs` (20 docs) → Job-3

> Aa collection ma time **"dd/MM/yyyy HH:mm"** string ma che (IST), ane customer ne **phone** thi olakhvo pade che.

```json
{
  "_id": "6711c9b0c2a1b3d4e5fe0001",
  "call_id": "CL-001",
  "phone": "+91 98765 00001",
  "reason": "delivery late",
  "call_time": "02/10/2026 11:30",
  "outcome": "Resolved",
  "duration_sec": 240,
  "agent": "Kiran",
  "logged_at": "2026-10-02T11:35:00+05:30"
}
```

| # | call_id | phone (raw) | reason | call_time (raw) | outcome | duration_sec | agent | ⚠️ Note |
|---|---|---|---|---|---|---|---|---|
| 1 | CL-001 | **"+91 98765 00001"** | delivery late | 02/10/2026 11:30 | Resolved | 240 | Kiran | 🧹 phone format |
| 2 | CL-002 | 9876500003 | laptop defective | 07/10/2026 10:05 | Escalated | 600 | Asha | |
| 3 | CL-003 | **"09876500004"** | payment debited twice | 04/10/2026 09:15 | Resolved | 180 | Kiran | 🧹 aagal 0 |
| 4 | CL-004 | 9876500005 | courier not reached | 08/10/2026 17:40 | Callback | 120 | Asha | |
| 5 | CL-005 | 9876500006 | return earbuds | 10/10/2026 12:20 | Resolved | 300 | Kiran | |
| 6 | CL-006 | 9876500007 | refund status | 10/10/2026 15:00 | Escalated | 420 | Asha | |
| 7 | CL-007 | 9876500008 | address change | 06/10/2026 08:45 | Resolved | 150 | Kiran | |
| 8 | CL-008 | 9876500010 | card payment pending | 09/10/2026 11:00 | Callback | 200 | Asha | |
| 9 | CL-009 | 9876500011 | kettle damaged | 11/10/2026 10:00 | Resolved | **-30** | Kiran | ⚠️ negative duration |
| 10 | CL-010 | 9876500012 | invoice needed | 11/10/2026 16:30 | Resolved | 90 | Kiran | |
| 11 | CL-011 | 9876500013 | shoes size exchange | 12/10/2026 14:10 | Resolved | 210 | Asha | |
| 12 | CL-012 | 9876500014 | saree colour issue | **"2026/10/12 25:61"** | Resolved | 180 | Kiran | ⚠️ corrupt time |
| 13 | CL-013 | 9876500015 | watch delivery late | 15/10/2026 10:20 | Escalated | 360 | Kiran | |
| 14 | CL-014 | 9876500016 | dumbbell not received | 15/10/2026 18:05 | Callback | 270 | Asha | |
| 15 | CL-015 | 9876500017 | speaker not working | 16/10/2026 19:30 | Resolved | 330 | Kiran | |
| 16 | CL-016 | **null** | **null** | 16/10/2026 20:00 | Resolved | 45 | Asha | ⚠️ phone ane reason banne nathi |
| 17 | CL-017 | 9876500019 | account login issue | 17/10/2026 09:10 | Resolved | 120 | Asha | |
| 18 | CL-018 | 9876500020 | kettle return pickup | 19/10/2026 10:50 | Callback | 160 | Kiran | |
| 19 | CL-019 | **9999999999** | product enquiry | 19/10/2026 13:15 | Resolved | 60 | Asha | (customer list ma nathi) |
| 20 | CL-020 | 9876500002 | kurti exchange status | 20/10/2026 11:40 | Resolved | 140 | Kiran | |

---

## 3. Common Framework – badha jobs ma same

### 3.1 Darek job no step-by-step flow

Badha jobs **aa j kram ma** chale che:

```
 ① WATERMARK VANCHO    meta.etl_watermark mathi "chhelli vaar kya sudhi vanchyu"
        │
        ▼
 ② READ (Mongo)        Full: badha docs  |  Incremental: updated_at > watermark AND <= run_start
        │
        ▼
 ③ SANITIZE (sudharo)  trim, case, format, mapping, default value  → data SUDHARAY, reject nahi
        │
        ▼
 ④ CORRUPT CHECK       type convert (number/date) thay che? na thay to → meta_rejected
        │
        ▼
 ⑤ DEDUPLICATE         same business key 2 vaar? latest updated_at rakho, baki → meta_rejected
        │
        ▼
 ⑥ VALIDATE            business rules (range, required, allowed values)  fail → meta_rejected
        │
        ▼
 ⑦ LOOKUP / FK CHECK   customer, product, order exist kare che? na hoy to → meta_rejected
        │
        ▼
 ⑧ TRANSFORM           explode array, flatten nested, derive columns (amount, days, sentiment)
        │
        ▼
 ⑨ LOAD DIMS           pehla dimensions (insert / upsert / SCD2)
        │
        ▼
 ⑩ LOAD FACTS          pachhi facts (dim keys lai ne upsert)
        │
        ▼
 ⑪ WRITE REJECTS       badha rejected records → meta.meta_rejected
        │
        ▼
 ⑫ UPDATE WATERMARK    fakt job SUCCESS thay to j  (fail thay to juno watermark j rahe → re-run safe)
```

**Kem aa kram?**

| Kram | Kem pehla? |
|---|---|
| Sanitize → Corrupt check | `"₹1,200"` ne pehla `1200` banavo, pachhi number check karo. Nahi to saru data pan corrupt ganay. |
| Corrupt → Dedupe | Corrupt date vala record ma `updated_at` sachu nathi, etle "latest" kayo e nakki na thay. |
| Dedupe → Validate | Juno duplicate fail thay to khota rejects bane. Pehla latest rakho, pachhi ene check karo. |
| Validate → FK | FK check mate Postgres ma lookup karvo pade (moghu). Pehla sasta checks kari ne rows ochha karo. |
| Dims → Facts | Fact ne dim ni key (`customer_key`) joie, etle dim pehla taiyar hovu joie. |
| Watermark chhelle | Vacche fail thay to aagla run ma e j window farithi vanchay, ane data gumay nahi. |

### 3.2 Rule na prakar

| Prakar | Code | Shu thay? | Example |
|---|---|---|---|
| **Sanitize** | S | Data **sudhari** ne aagal mokalo (reject nahi) | `" PRIYA.SHAH@MAIL.COM "` → `priya.shah@mail.com` |
| **Corrupt** | C | Type convert na thay to **reject** | price `"abc"` |
| **Duplicate** | D | Same key 2 vaar: **latest rakho**, juno reject | P003 2 vaar |
| **Validation** | V | Business rule fail to **reject** | rating 7 (1–5 hovu joie) |
| **FK / Lookup** | F | Reference data na male to **reject** | order ma customer C099 |
| **Soft rule** | SR | Reject **nahi**. Default value muko ane aagal mokalo. | ticket no customer na male to `UNKNOWN` |

**Reject level:**
- **Document level:** aakho document reject (jem ke corrupt order date).
- **Line level:** fakt array ni ek line reject, baki document load thay (jem ke O1008 ni qty 0 vali line).

### 3.3 Watermark technique

**Table: `meta.etl_watermark`** – darek job ane darek collection ni ek row.

| Column | Matlab |
|---|---|
| `job_name` | Kayo job |
| `source_collection` | Kai collection |
| `watermark_column` | Kaya field thi navo data olakhvo (`updated_at`, `updatedAt`, `logged_at`) |
| `last_watermark` | Chhelle success run e kya sudhi vanchyu |
| `last_run_id` | Chhello run |
| `last_status` | SUCCESS / FAILED |

**Niyam:**
1. `last_watermark` **NULL** hoy to **Full Load**: badha documents vancho.
2. Bije vakhte: `watermark_column > last_watermark` **AND** `<= run_start_time`.
3. Upper bound `run_start_time` fix rakho, jethi job chale tyare aavto data aagla run ma jay.
4. Job **success** thay pachhi j `last_watermark = run_start_time` karo.
5. **Darek job no potano watermark** hoy che. `orders` Job-2 ane Job-4 banne vapare che, pan banne na watermark alag che.
6. Badha time **ek j timezone** ma compare karo. `chat_tickets` UTC ma che, etle compare karta pehla convert karo (Section 11 ma example che).

> **`created_at` nahi, `updated_at` kem?** Order no status badle (SHIPPED → DELIVERED) tyare `created_at` badlatu nathi, etle e change pakdai j nahi. `updated_at` darek change par badlay che.

### 3.4 Common meta tables

**`meta.meta_rejected`** – badha jobs na rejects ek j jagyae.

| Column | Matlab |
|---|---|
| `reject_id` | Serial number |
| `job_name` | Kayo job |
| `run_id` | Kayo run (`J1_20261021_0000`) |
| `source_collection` | Kai collection |
| `source_key` | Business key (`P022`, `O1008/line-2`) |
| `reject_type` | DUPLICATE / CORRUPT / VALIDATION / FK_MISSING / BUSINESS_RULE |
| `rule_code` | Kayo rule (`J1-C01`) |
| `reason` | Manas samji shake evo message |
| `bad_value` | Kayi value kharab hati |
| `rejected_at` | Kyare |

**`meta.etl_run_audit`** – darek run no hisab: read, loaded, rejected, status.

### 3.5 Staging vagar load kevi rite? (concept)

| Target table prakar | Kevi rite lakhay | Example |
|---|---|---|
| Nana lookup dims | **Insert if not exists**: hoy to kai na karo, na hoy to insert | `dim_brand`, `dim_courier` |
| SCD Type 1 dims | **Upsert**: hoy to update, na hoy to insert | `dim_product`, `dim_category` |
| SCD Type 2 dims | Badlay to **juni row bandh** (`valid_to`, `is_current = N`) ane **navi row insert** | `dim_customer` |
| Facts | **Upsert** fact na business key par | `fact_sales` key = (order_no, line_no) |
| Single table | **Upsert** (channel + source id) par | `support_ticket` |
| meta_rejected | **Append** (re-run hoy to e run_id na juna rejects pehla delete) | |

> Upsert ne lidhe **re-run safe** che: same window 2 vaar chale to pan duplicate nahi bane.

---

## 4. Shared Table – dim_date

> `dim_date` **Mongo mathi nathi banti**. Ek vaar script thi 2020–2035 na badha divas generate kari didha che. Badha jobs ene vapare che (**conformed dimension**).

**`curated.dim_date`** (Oct-2026 na 21 divas)

| date_key | full_date | day_name | month | year | is_weekend | is_holiday |
|---|---|---|---|---|---|---|
| 20261001 | 2026-10-01 | Thursday | October | 2026 | N | N |
| 20261002 | 2026-10-02 | Friday | October | 2026 | N | **Y** (Gandhi Jayanti) |
| 20261003 | 2026-10-03 | Saturday | October | 2026 | Y | N |
| 20261004 | 2026-10-04 | Sunday | October | 2026 | Y | N |
| 20261005 | 2026-10-05 | Monday | October | 2026 | N | N |
| 20261006 | 2026-10-06 | Tuesday | October | 2026 | N | N |
| 20261007 | 2026-10-07 | Wednesday | October | 2026 | N | N |
| 20261008 | 2026-10-08 | Thursday | October | 2026 | N | N |
| 20261009 | 2026-10-09 | Friday | October | 2026 | N | N |
| 20261010 | 2026-10-10 | Saturday | October | 2026 | Y | N |
| 20261011 | 2026-10-11 | Sunday | October | 2026 | Y | N |
| 20261012 | 2026-10-12 | Monday | October | 2026 | N | N |
| 20261013 | 2026-10-13 | Tuesday | October | 2026 | N | N |
| 20261014 | 2026-10-14 | Wednesday | October | 2026 | N | N |
| 20261015 | 2026-10-15 | Thursday | October | 2026 | N | N |
| 20261016 | 2026-10-16 | Friday | October | 2026 | N | N |
| 20261017 | 2026-10-17 | Saturday | October | 2026 | Y | N |
| 20261018 | 2026-10-18 | Sunday | October | 2026 | Y | N |
| 20261019 | 2026-10-19 | Monday | October | 2026 | N | N |
| 20261020 | 2026-10-20 | Tuesday | October | 2026 | N | N |
| 20261021 | 2026-10-21 | Wednesday | October | 2026 | N | N |

---

## 5. Job-1: Single Collection → Multiple Dim + Fact (Snowflake)

### 5.1 Purpose

`products` collection ma **ek document ni andar ghanu chhupayelu che**: brand, category (ane eno parent), supplier, ane warehouse wise stock (array).
Job-1 aa ek collection ne todi ne **5 dimensions + 1 fact** banave che.

**Business question je answer thashe:**
- "Kaya warehouse ma kaya category no ketlo stock che?"
- "Kaya supplier na product no stock value ketlo che?"
- "Electronics (parent) ma total stock value ketli?"

### 5.2 Input → Output

```
                               products (1 collection)
                                         │
   ┌───────────────┬───────────────┬─────┴─────────┬────────────────┬──────────────────────┐
   ▼               ▼               ▼               ▼                ▼                      ▼
 brand{}        category{}      supplier{}      stock[].warehouse  product (main)       stock[] explode
   │            + parent{}         │                 │                  │                      │
   ▼               ▼               ▼                 ▼                  ▼                      ▼
 dim_brand     dim_category    dim_supplier     dim_warehouse       dim_product      fact_inventory_snapshot
              (self parent)
```

### 5.3 Schema – Snowflake

```
                          dim_date
                             │
 dim_warehouse ──── fact_inventory_snapshot ──── dim_product ──┬── dim_brand
                                                              ├── dim_supplier
                                                              └── dim_category (sub: Mobiles)
                                                                        │ parent_category_key
                                                                        ▼
                                                                  dim_category (parent: Electronics)
```

**Snowflake kem?** `dim_product` sidhu fact sathe che, pan category ni details ek **biji level** (`dim_category`) ma che, ane eno parent pan e j table ma che. Etle dimensions ni pan dimension bane che, ane e "snowflake" jevo aakar le che.

### 5.4 Rules – Job-1

**Sanitization (sudharo, reject nahi):**

| Code | Rule | Before | After |
|---|---|---|---|
| J1-S01 | Product name: trim + Title Case | `"  non-stick tawa "` (P013) | `Non-stick Tawa` |
| J1-S02 | Price string mathi `₹` ane `,` kadhi ne number | `"₹1,200"` (P012) | `1200` |
| J1-S03 | Brand name brand_code mujab standard | `"urbanwear"` (P007, B04) | `UrbanWear` |
| J1-S04 | Warehouse code: trim + UPPERCASE | `"wh-ahm "` (P010) | `WH-AHM` |
| J1-S05 | Category ane supplier code: trim + UPPERCASE | `"c-ele-mob"` | `C-ELE-MOB` |
| J1-S06 | `is_active` na hoy to default `true` | null | true |

**Corrupt / Duplicate / Validation:**

| Code | Prakar | Rule | Level | Aa data ma kone lagu? |
|---|---|---|---|---|
| J1-C01 | Corrupt | Sanitize pachhi pan price number na bane to reject | Document | P022 (`"abc"`) |
| J1-D01 | Duplicate | Same `product_code` 2 vaar: latest `updated_at` rakho | Document | P003 (10-05 vado juno) |
| J1-V01 | Validation | Category required (code null na hovo joie) | Document | P021 |
| J1-V02 | Validation | Price > 0 | Document | P023 (-150) |
| J1-V03 | Validation | Stock qty >= 0 | **Line** | P016 / WH-MUM (-5) |
| J1-V04 | Validation | `product_code` format `P` + 3 digit | Document | (koi nahi) |
| J1-V05 | Validation | Product name required | Document | (koi nahi) |
| J1-V06 | Validation | Warehouse code allowed list ma (WH-AHM, WH-MUM, WH-DEL) | Line | (koi nahi) |

### 5.5 Step-by-step (Full Load, Run-0 = 2026-10-21 00:00)

**Step ① – Watermark:** `products` no watermark NULL che, etle **Full Load**: 24 docs vanchya.

**Step ③ – Sanitize:**

| product_code | Field | Before | After |
|---|---|---|---|
| P007 | brand.name | urbanwear | UrbanWear |
| P010 | stock[0].warehouse | "wh-ahm " | WH-AHM |
| P012 | price | "₹1,200" | 1200 |
| P013 | name | "  non-stick tawa " | Non-stick Tawa |

**Step ④ – Corrupt check:** P022 price `"abc"` number nathi banto, etle **reject (J1-C01)**. Have 23 docs.

**Step ⑤ – Deduplicate:** P003 2 vaar che:

| product_code | price | updated_at | Result |
|---|---|---|---|
| P003 | 2000 | 2026-10-20 | ✅ Rakho (latest) |
| P003 | 1900 | 2026-10-05 | ❌ Reject (J1-D01) |

Have 22 docs.

**Step ⑥ – Validate:**
- P021 ni category null che, etle **reject (J1-V01)**.
- P023 no price -150 che, etle **reject (J1-V02)**.
- Have **20 products** bachya.

**Step ⑧ – Transform: `stock[]` explode (array ni darek line ek row bane):**

```
 P003 { stock: [ {WH-AHM,100}, {WH-MUM,80}, {WH-DEL,50} ] }
                       │ explode
                       ▼
 P003 | WH-AHM | 100
 P003 | WH-MUM |  80
 P003 | WH-DEL |  50
```

Aam 20 products mathi **34 stock lines** bani. Line level validation (J1-V03) ma P016 / WH-MUM (-5) **reject** thai, etle **33 lines** bachi.

Darek line par `stock_value = stock_qty × unit_price` ganyu.

**Step ⑨ – Load Dims** (distinct values kadhi ne):

**`curated.dim_brand`** (10 rows) – Insert if not exists

| brand_key | brand_code | brand_name |
|---|---|---|
| 1 | B01 | Volt |
| 2 | B02 | SoundMax |
| 3 | B03 | TechBook |
| 4 | B04 | UrbanWear |
| 5 | B05 | Ethnika |
| 6 | B06 | StepUp |
| 7 | B07 | KitchenPro |
| 8 | B08 | CookEasy |
| 9 | B09 | FitLife |
| 10 | B10 | PlayStrong |

**`curated.dim_category`** (15 rows) – Snowflake: parent pan e j table ma

| category_key | category_code | category_name | level | parent_category_key |
|---|---|---|---|---|
| 1 | C-ELE | Electronics | 1 | null |
| 2 | C-FAS | Fashion | 1 | null |
| 3 | C-HOM | Home & Kitchen | 1 | null |
| 4 | C-SPO | Sports | 1 | null |
| 5 | C-ELE-MOB | Mobiles | 2 | 1 (Electronics) |
| 6 | C-ELE-AUD | Audio | 2 | 1 (Electronics) |
| 7 | C-ELE-LAP | Laptops | 2 | 1 (Electronics) |
| 8 | C-ELE-WEA | Wearables | 2 | 1 (Electronics) |
| 9 | C-FAS-MEN | Men Clothing | 2 | 2 (Fashion) |
| 10 | C-FAS-WOM | Women Clothing | 2 | 2 (Fashion) |
| 11 | C-FAS-FOO | Footwear | 2 | 2 (Fashion) |
| 12 | C-HOM-KIT | Kitchen Appliances | 2 | 3 (Home & Kitchen) |
| 13 | C-HOM-COO | Cookware | 2 | 3 (Home & Kitchen) |
| 14 | C-SPO-FIT | Fitness | 2 | 4 (Sports) |
| 15 | C-SPO-CRI | Cricket | 2 | 4 (Sports) |

**`curated.dim_supplier`** (4 rows)

| supplier_key | supplier_code | supplier_name | city |
|---|---|---|---|
| 1 | SUP-01 | Shree Electronics Pvt Ltd | Ahmedabad |
| 2 | SUP-02 | Fashion Hub Traders | Surat |
| 3 | SUP-03 | HomeNeeds Distributors | Mumbai |
| 4 | SUP-04 | SportZone Wholesale | Jalandhar |

**`curated.dim_warehouse`** (3 rows)

| warehouse_key | warehouse_code | city | state |
|---|---|---|---|
| 1 | WH-AHM | Ahmedabad | Gujarat |
| 2 | WH-MUM | Mumbai | Maharashtra |
| 3 | WH-DEL | Delhi | Delhi |

> **Nana dims ma 20 rows kem nathi?** Brand, category, supplier ane warehouse ni sankhya j eti che. 20 banavva khota rows umerva pade. Real data ma lookup dims nana j hoy che. Collections ane mukhya dim/fact ma 20+ rows che.

**`curated.dim_product`** (20 rows) – SCD Type 1 (badlay to overwrite)

| product_key | product_code | product_name | brand_key | category_key | supplier_key | current_price | is_active |
|---|---|---|---|---|---|---|---|
| 1 | P001 | Smartphone Pro 5G | 1 (Volt) | 5 (Mobiles) | 1 | 25000 | Y |
| 2 | P002 | Smartphone Lite | 1 (Volt) | 5 (Mobiles) | 1 | 12000 | Y |
| 3 | P003 | Wireless Earbuds | 2 (SoundMax) | 6 (Audio) | 1 | 2000 | Y |
| 4 | P004 | Bluetooth Speaker | 2 (SoundMax) | 6 (Audio) | 1 | 3500 | Y |
| 5 | P005 | Laptop 14 inch | 3 (TechBook) | 7 (Laptops) | 1 | 55000 | Y |
| 6 | P006 | Cotton T-Shirt | 4 (UrbanWear) | 9 (Men Clothing) | 2 | 500 | Y |
| 7 | P007 | Denim Jeans | 4 (UrbanWear) | 9 (Men Clothing) | 2 | 1500 | Y |
| 8 | P008 | Printed Kurti | 5 (Ethnika) | 10 (Women Clothing) | 2 | 800 | Y |
| 9 | P009 | Silk Saree | 5 (Ethnika) | 10 (Women Clothing) | 2 | 4500 | Y |
| 10 | P010 | Running Shoes | 6 (StepUp) | 11 (Footwear) | 2 | 2800 | Y |
| 11 | P011 | Mixer Grinder | 7 (KitchenPro) | 12 (Kitchen Appl.) | 3 | 3500 | Y |
| 12 | P012 | Electric Kettle | 7 (KitchenPro) | 12 (Kitchen Appl.) | 3 | 1200 | Y |
| 13 | P013 | Non-stick Tawa | 8 (CookEasy) | 13 (Cookware) | 3 | 700 | Y |
| 14 | P014 | Pressure Cooker 5L | 8 (CookEasy) | 13 (Cookware) | 3 | 1800 | Y |
| 15 | P015 | Yoga Mat | 9 (FitLife) | 14 (Fitness) | 4 | 600 | Y |
| 16 | P016 | Dumbbell Set 10kg | 9 (FitLife) | 14 (Fitness) | 4 | 2200 | Y |
| 17 | P017 | Cricket Bat | 10 (PlayStrong) | 15 (Cricket) | 4 | 1800 | Y |
| 18 | P018 | Cricket Ball (Pack of 6) | 10 (PlayStrong) | 15 (Cricket) | 4 | 900 | Y |
| 19 | P019 | Smart Watch | 1 (Volt) | 8 (Wearables) | 1 | 4000 | Y |
| 20 | P020 | Sports Water Bottle | 9 (FitLife) | 14 (Fitness) | 4 | 350 | Y |

> ( ) ma lakhelu naam fakt samajva mate che. Table ma fakt number hoy.

**Step ⑩ – Load Fact:**

**`curated.fact_inventory_snapshot`** (33 rows)
**Grain:** 1 row = 1 product × 1 warehouse × 1 snapshot divas. **Upsert key:** (snapshot_date_key, product_key, warehouse_key)

| # | snapshot_date_key | product_key | warehouse_key | stock_qty | unit_price | stock_value |
|---|---|---|---|---|---|---|
| 1 | 20261021 | 1 (P001) | 1 (AHM) | 40 | 25000 | 1,000,000 |
| 2 | 20261021 | 1 (P001) | 2 (MUM) | 25 | 25000 | 625,000 |
| 3 | 20261021 | 2 (P002) | 1 (AHM) | 60 | 12000 | 720,000 |
| 4 | 20261021 | 2 (P002) | 3 (DEL) | 30 | 12000 | 360,000 |
| 5 | 20261021 | 3 (P003) | 1 (AHM) | 100 | 2000 | 200,000 |
| 6 | 20261021 | 3 (P003) | 2 (MUM) | 80 | 2000 | 160,000 |
| 7 | 20261021 | 3 (P003) | 3 (DEL) | 50 | 2000 | 100,000 |
| 8 | 20261021 | 4 (P004) | 2 (MUM) | 45 | 3500 | 157,500 |
| 9 | 20261021 | 5 (P005) | 1 (AHM) | 10 | 55000 | 550,000 |
| 10 | 20261021 | 5 (P005) | 3 (DEL) | 8 | 55000 | 440,000 |
| 11 | 20261021 | 6 (P006) | 1 (AHM) | 200 | 500 | 100,000 |
| 12 | 20261021 | 6 (P006) | 2 (MUM) | 150 | 500 | 75,000 |
| 13 | 20261021 | 7 (P007) | 1 (AHM) | 90 | 1500 | 135,000 |
| 14 | 20261021 | 8 (P008) | 1 (AHM) | 120 | 800 | 96,000 |
| 15 | 20261021 | 8 (P008) | 3 (DEL) | 60 | 800 | 48,000 |
| 16 | 20261021 | 9 (P009) | 2 (MUM) | 20 | 4500 | 90,000 |
| 17 | 20261021 | 10 (P010) | 1 (AHM) | 35 | 2800 | 98,000 |
| 18 | 20261021 | 10 (P010) | 2 (MUM) | 40 | 2800 | 112,000 |
| 19 | 20261021 | 10 (P010) | 3 (DEL) | 25 | 2800 | 70,000 |
| 20 | 20261021 | 11 (P011) | 2 (MUM) | 30 | 3500 | 105,000 |
| 21 | 20261021 | 12 (P012) | 1 (AHM) | 50 | 1200 | 60,000 |
| 22 | 20261021 | 12 (P012) | 2 (MUM) | 50 | 1200 | 60,000 |
| 23 | 20261021 | 13 (P013) | 1 (AHM) | 80 | 700 | 56,000 |
| 24 | 20261021 | 14 (P014) | 3 (DEL) | 40 | 1800 | 72,000 |
| 25 | 20261021 | 15 (P015) | 1 (AHM) | 70 | 600 | 42,000 |
| 26 | 20261021 | 15 (P015) | 3 (DEL) | 70 | 600 | 42,000 |
| 27 | 20261021 | 16 (P016) | 1 (AHM) | 15 | 2200 | 33,000 |
| 28 | 20261021 | 17 (P017) | 1 (AHM) | 25 | 1800 | 45,000 |
| 29 | 20261021 | 17 (P017) | 3 (DEL) | 30 | 1800 | 54,000 |
| 30 | 20261021 | 18 (P018) | 1 (AHM) | 60 | 900 | 54,000 |
| 31 | 20261021 | 19 (P019) | 1 (AHM) | 30 | 4000 | 120,000 |
| 32 | 20261021 | 19 (P019) | 3 (DEL) | 20 | 4000 | 80,000 |
| 33 | 20261021 | 20 (P020) | 3 (DEL) | 90 | 350 | 31,500 |

> P016 ni WH-MUM line (-5) ahiya **nathi**. E `meta_rejected` ma gai, pan P016 ni AHM line ane P016 product banne load thaya. Aa **line level reject** che.

**Step ⑪–⑫:** 5 rejects `meta_rejected` ma gaya. Watermark `products = 2026-10-21 00:00` set thayo.

### 5.6 Job-1 Summary

| | Count |
|---|---|
| Docs read | 24 |
| Rejected docs | 4 (P003-dup, P021, P022, P023) |
| Rejected lines | 1 (P016 / WH-MUM) |
| dim_product | 20 |
| fact_inventory_snapshot | 33 |

---

## 6. Job-2: Multiple Collections → Multiple Dim + Fact (Star)

### 6.1 Purpose

Sales ni puri story **3 collections ma vahenchayeli che**: customer kon che (`customers`), shu kharidyu (`orders`), ane paisa aavya ke nahi (`payments`).
Job-2 aa 3 ne jodi ne ek **star schema** banave che.

**Business questions:**
- "Gujarat ma Gold customers e kai category ma ketlu kharidyu?"
- "UPI vs COD ma ketla payments fail/refund thaya?"
- "Customer e city badli, to juna orders juni city ma j dekhay?" (SCD2)

### 6.2 Input → Output

```
 customers ──────────────────────────────────────────► dim_customer (SCD2)
 orders ──┬── status ───────────────────────────────► dim_order_status
          ├── payment_method ───────────────────────► dim_payment_method
          └── items[] explode ──┬─ lookup dim_customer ─┐
                                ├─ lookup dim_product (Job-1) ─► fact_sales
                                └─ lookup dim_date ─────┘
 payments ──── lookup order (fact_sales) + dim_customer ─────► fact_payment
```

**Job ni andar no kram:** `customers` pehla, pachhi `orders`, pachhi `payments`.
Kem ke order ne customer key joie, ane payment ne order loaded hovo joie.

### 6.3 Schema – Star

```
                       dim_date
                          │
   dim_customer ───── fact_sales ───── dim_product  (Job-1 mathi)
        │                 │    └────── dim_order_status
        │                 └─────────── dim_payment_method
        │                                    │
        └──────────── fact_payment ──────────┘
                          │
                       dim_date
```

**Star kem?** Darek dim **sidhu fact sathe** jodayelu che, ane dim ni dim nathi. Query saral ane fast thay che.

### 6.4 Rules – Job-2

**customers:**

| Code | Prakar | Rule | Example (before → after / result) |
|---|---|---|---|
| J2-S01 | Sanitize | Email: trim + lowercase | `"  PRIYA.SHAH@MAIL.COM "` → `priya.shah@mail.com` |
| J2-S02 | Sanitize | Phone: fakt digits, chhella 10 | `"+91-98765 00004"` → `9876500004` |
| J2-S03 | Sanitize | City: trim + Title Case | `"lucknow "` → `Lucknow` |
| J2-S04 | Sanitize | State code → full naam (GJ, MH, RJ…) | `RJ` → `Rajasthan` |
| J2-S05 | Sanitize | Tier null → `Bronze` (default) | C011: null → `Bronze` |
| J2-D01 | Duplicate | Same `customer_code`: latest rakho | C005 (09-01 vado reject) |
| J2-V01 | Validation | Name required | C022 → reject |
| J2-V02 | Validation | Email format sacho (`x@y.z`) | C021 `test@@mail` → reject |
| J2-V03 | Validation | Phone 10 digit | (koi nahi) |
| J2-V04 | Validation | Tier allowed: Bronze/Silver/Gold/Platinum | (koi nahi) |

**orders:**

| Code | Prakar | Rule | Example |
|---|---|---|---|
| J2-S06 | Sanitize | Status: trim + UPPERCASE | `"delivered"` → `DELIVERED` |
| J2-S07 | Sanitize | Payment method mapping | `"upi "`→`UPI`, `"Net Banking"`→`NETBANKING`, `"Cash on Delivery"`→`COD` |
| J2-C01 | Corrupt | `order_ts` valid date hovi joie | O1022 `"2026-13-45"` → reject |
| J2-D02 | Duplicate | Same `order_no`: latest `updated_at` | O1005 PLACED (juno) → reject |
| J2-V05 | Validation | Items khali na hova joie | O1023 → reject |
| J2-V06 | Validation (line) | qty > 0 | O1008 line-2 (P018, qty 0) → line reject |
| J2-V07 | Validation (line) | unit_price > 0 | (koi nahi) |
| J2-V08 | Validation | Status allowed list ma | (koi nahi) |
| J2-F01 | FK | Customer `dim_customer` ma hovo joie | O1021 (C099) → reject |
| J2-F02 | FK (line) | Product `dim_product` ma hovo joie | O1012 line-2 (P022) → line reject |

**payments:**

| Code | Prakar | Rule | Example |
|---|---|---|---|
| J2-S08 | Sanitize | Amount string → number | `"₹29,000"` → `29000` |
| J2-S09 | Sanitize | Status UPPERCASE, method mapping | `"success"` → `SUCCESS` |
| J2-C02 | Corrupt | Amount number banvo joie | PAY22 `"abc"` → reject |
| J2-D03 | Duplicate | Same `payment_id`: latest | PAY05 (20:11 vado) → reject |
| J2-V09 | Validation | Amount > 0 | (koi nahi) |
| J2-V10 | Validation | Status allowed: SUCCESS/FAILED/REFUNDED/PENDING | (koi nahi) |
| J2-F03 | FK | Order curated ma loaded hovo joie | PAY21 (O1021 reject thayo hato) → reject |

> **FK check fakt aa batch ma nahi, aakha curated table ma karvo.** Incremental ma koi payment juna order no hoi sake, je aa batch ma nathi pan `fact_sales` ma pehlethi che.

### 6.5 Step-by-step (Full Load)

**① Watermark:** customers, orders, payments – trane NULL che, etle Full Load.

**② Read:** customers 23, orders 24, payments 23.

#### Part A – customers → dim_customer

**③ Sanitize:**

| customer_code | Field | Before | After |
|---|---|---|---|
| C002 | email | `"  PRIYA.SHAH@MAIL.COM "` | priya.shah@mail.com |
| C004 | phone | `"+91-98765 00004"` | 9876500004 |
| C007 | state | RJ | Rajasthan |
| C011 | tier | null | Bronze |
| C013 | city | `"lucknow "` | Lucknow |

**⑤ Dedupe:** C005 2 vaar che. 10-02 vado (Silver) rakhyo, ane 09-01 vado (Bronze) reject.
**⑥ Validate:** C021 (email kharab) ane C022 (name nathi) reject thaya. **20 customers** bachya.

**⑨ Load `curated.dim_customer`** (20 rows) – **SCD Type 2**

- **Track thata columns (SCD2):** `city`, `state`, `tier` – badlay to navi row bane.
- **Overwrite thata columns (SCD1):** `email`, `phone` – badlay to e j row update thay.

| customer_key | customer_code | customer_name | email | phone | city | state | tier | valid_from | valid_to | is_current |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | C001 | Rahul Patel | rahul.patel@mail.com | 9876500001 | Ahmedabad | Gujarat | Gold | 2025-01-10 | 9999-12-31 | Y |
| 2 | C002 | Priya Shah | priya.shah@mail.com | 9876500002 | Surat | Gujarat | Silver | 2025-02-14 | 9999-12-31 | Y |
| 3 | C003 | Amit Mehta | amit.mehta@mail.com | 9876500003 | Mumbai | Maharashtra | Gold | 2025-03-05 | 9999-12-31 | Y |
| 4 | C004 | Neha Joshi | neha.joshi@mail.com | 9876500004 | Pune | Maharashtra | Bronze | 2025-04-20 | 9999-12-31 | Y |
| 5 | C005 | Karan Desai | karan.desai@mail.com | 9876500005 | Vadodara | Gujarat | Silver | 2025-05-11 | 9999-12-31 | Y |
| 6 | C006 | Sneha Iyer | sneha.iyer@mail.com | 9876500006 | Bengaluru | Karnataka | Gold | 2025-06-01 | 9999-12-31 | Y |
| 7 | C007 | Vikram Singh | vikram.singh@mail.com | 9876500007 | Jaipur | Rajasthan | Bronze | 2025-07-19 | 9999-12-31 | Y |
| 8 | C008 | Pooja Trivedi | pooja.trivedi@mail.com | 9876500008 | Rajkot | Gujarat | Silver | 2025-08-08 | 9999-12-31 | Y |
| 9 | C009 | Arjun Nair | arjun.nair@mail.com | 9876500009 | Kochi | Kerala | Bronze | 2025-09-25 | 9999-12-31 | Y |
| 10 | C010 | Riya Kapoor | riya.kapoor@mail.com | 9876500010 | Delhi | Delhi | Gold | 2025-10-30 | 9999-12-31 | Y |
| 11 | C011 | Mehul Shah | mehul.shah@mail.com | 9876500011 | Ahmedabad | Gujarat | Bronze | 2025-11-12 | 9999-12-31 | Y |
| 12 | C012 | Kavya Reddy | kavya.reddy@mail.com | 9876500012 | Hyderabad | Telangana | Silver | 2025-12-03 | 9999-12-31 | Y |
| 13 | C013 | Rohan Gupta | rohan.gupta@mail.com | 9876500013 | Lucknow | Uttar Pradesh | Bronze | 2026-01-15 | 9999-12-31 | Y |
| 14 | C014 | Isha Bhatt | isha.bhatt@mail.com | 9876500014 | Bhavnagar | Gujarat | Gold | 2026-02-08 | 9999-12-31 | Y |
| 15 | C015 | Siddharth Rao | siddharth.rao@mail.com | 9876500015 | Chennai | Tamil Nadu | Silver | 2026-03-21 | 9999-12-31 | Y |
| 16 | C016 | Ananya Das | ananya.das@mail.com | 9876500016 | Kolkata | West Bengal | Bronze | 2026-04-02 | 9999-12-31 | Y |
| 17 | C017 | Harsh Vora | harsh.vora@mail.com | 9876500017 | Surat | Gujarat | Silver | 2026-05-17 | 9999-12-31 | Y |
| 18 | C018 | Nidhi Agarwal | nidhi.agarwal@mail.com | 9876500018 | Indore | Madhya Pradesh | Bronze | 2026-06-09 | 9999-12-31 | Y |
| 19 | C019 | Yash Thakkar | yash.thakkar@mail.com | 9876500019 | Ahmedabad | Gujarat | Gold | 2026-07-23 | 9999-12-31 | Y |
| 20 | C020 | Diya Menon | diya.menon@mail.com | 9876500020 | Mumbai | Maharashtra | Silver | 2026-08-30 | 9999-12-31 | Y |

> `valid_to = 9999-12-31` no matlab "haju chalu che". Pehli version ma `valid_from = signup_date` rakhyu, jethi juna orders ne pan sachi version male.

#### Part B – orders → dim_order_status, dim_payment_method, fact_sales

**③ Sanitize:**

| order_no | Field | Before | After |
|---|---|---|---|
| O1002 | status | delivered | DELIVERED |
| O1007 | payment_method | `"upi "` | UPI |
| O1012 | payment_method | Net Banking | NETBANKING |
| O1013 | payment_method | Cash on Delivery | COD |

**④ Corrupt:** O1022 (`"2026-13-45"`) → reject.
**⑤ Dedupe:** O1005 2 vaar che. DELIVERED (10-06) rakhyo, PLACED (10-03) reject.
**⑥ Validate:** O1023 (items khali) → reject.
**⑦ FK:** O1021 no customer C099 `dim_customer` ma nathi → reject.
Have **20 orders** bachya.

**⑧ Transform – items explode:**

```
 O1008 { items: [ {P017, qty 2, 1800}, {P018, qty 0, 900} ] }
                         │ explode
                         ▼
 O1008 | line 1 | P017 | 2 | 1800   ✅
 O1008 | line 2 | P018 | 0 |  900   ❌ J2-V06 (qty 0) → meta_rejected

 O1012 { items: [ {P002, 1, 12000}, {P022, 1, 999} ] }
                         │ explode + product lookup
                         ▼
 O1012 | line 1 | P002 → product_key 2   ✅
 O1012 | line 2 | P022 → dim_product ma nathi   ❌ J2-F02 → meta_rejected
```

20 orders mathi **32 lines** bani. 2 line reject thai, etle **30 lines** bachi.
Darek line par `line_amount = quantity × unit_price` ganyu.

**⑨ Load small dims:**

**`curated.dim_order_status`** (5 rows)

| order_status_key | status_code | description | is_final |
|---|---|---|---|
| 1 | PLACED | Order mukayo | N |
| 2 | SHIPPED | Courier ne apayo | N |
| 3 | DELIVERED | Customer ne malyo | Y |
| 4 | CANCELLED | Radd thayo | Y |
| 5 | RETURNED | Pachho aavyo | Y |

**`curated.dim_payment_method`** (4 rows)

| payment_method_key | method_code | method_name | is_online |
|---|---|---|---|
| 1 | UPI | Unified Payments Interface | Y |
| 2 | CARD | Credit / Debit Card | Y |
| 3 | NETBANKING | Net Banking | Y |
| 4 | COD | Cash on Delivery | N |

**⑩ Load `curated.fact_sales`** (30 rows)
**Grain:** 1 row = 1 order ni 1 product line. **Upsert key:** (order_no, line_no)

| sales_key | order_no | line_no | order_date_key | customer_key | product_key | payment_method_key | order_status_key | quantity | unit_price | line_amount |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | O1001 | 1 | 20261001 | 1 (Rahul) | 1 (P001) | 1 (UPI) | 3 (DELIVERED) | 1 | 25000 | 25000 |
| 2 | O1001 | 2 | 20261001 | 1 (Rahul) | 3 (P003) | 1 (UPI) | 3 (DELIVERED) | 2 | 2000 | 4000 |
| 3 | O1002 | 1 | 20261001 | 2 (Priya) | 8 (P008) | 4 (COD) | 3 (DELIVERED) | 3 | 800 | 2400 |
| 4 | O1003 | 1 | 20261002 | 3 (Amit) | 5 (P005) | 2 (CARD) | 3 (DELIVERED) | 1 | 55000 | 55000 |
| 5 | O1004 | 1 | 20261003 | 1 (Rahul) | 17 (P017) | 1 (UPI) | 4 (CANCELLED) | 1 | 1800 | 1800 |
| 6 | O1004 | 2 | 20261003 | 1 (Rahul) | 15 (P015) | 1 (UPI) | 4 (CANCELLED) | 1 | 600 | 600 |
| 7 | O1005 | 1 | 20261003 | 4 (Neha) | 6 (P006) | 1 (UPI) | 3 (DELIVERED) | 4 | 500 | 2000 |
| 8 | O1005 | 2 | 20261003 | 4 (Neha) | 7 (P007) | 1 (UPI) | 3 (DELIVERED) | 1 | 1500 | 1500 |
| 9 | O1006 | 1 | 20261004 | 5 (Karan) | 11 (P011) | 2 (CARD) | 3 (DELIVERED) | 1 | 3500 | 3500 |
| 10 | O1006 | 2 | 20261004 | 5 (Karan) | 13 (P013) | 2 (CARD) | 3 (DELIVERED) | 2 | 700 | 1400 |
| 11 | O1007 | 1 | 20261005 | 6 (Sneha) | 3 (P003) | 1 (UPI) | 3 (DELIVERED) | 1 | 2000 | 2000 |
| 12 | O1008 | 1 | 20261005 | 7 (Vikram) | 17 (P017) | 4 (COD) | 5 (RETURNED) | 2 | 1800 | 3600 |
| 13 | O1009 | 1 | 20261006 | 8 (Pooja) | 8 (P008) | 1 (UPI) | 3 (DELIVERED) | 2 | 800 | 1600 |
| 14 | O1009 | 2 | 20261006 | 8 (Pooja) | 6 (P006) | 1 (UPI) | 3 (DELIVERED) | 2 | 500 | 1000 |
| 15 | O1010 | 1 | 20261007 | 10 (Riya) | 1 (P001) | 2 (CARD) | 1 (PLACED) | 1 | 25000 | 25000 |
| 16 | O1010 | 2 | 20261007 | 10 (Riya) | 5 (P005) | 2 (CARD) | 1 (PLACED) | 1 | 55000 | 55000 |
| 17 | O1011 | 1 | 20261008 | 11 (Mehul) | 12 (P012) | 1 (UPI) | 3 (DELIVERED) | 1 | 1200 | 1200 |
| 18 | O1011 | 2 | 20261008 | 11 (Mehul) | 14 (P014) | 1 (UPI) | 3 (DELIVERED) | 1 | 1800 | 1800 |
| 19 | O1012 | 1 | 20261008 | 12 (Kavya) | 2 (P002) | 3 (NETBANKING) | 3 (DELIVERED) | 1 | 12000 | 12000 |
| 20 | O1013 | 1 | 20261009 | 13 (Rohan) | 10 (P010) | 4 (COD) | 3 (DELIVERED) | 1 | 2800 | 2800 |
| 21 | O1014 | 1 | 20261010 | 14 (Isha) | 9 (P009) | 2 (CARD) | 3 (DELIVERED) | 1 | 4500 | 4500 |
| 22 | O1014 | 2 | 20261010 | 14 (Isha) | 8 (P008) | 2 (CARD) | 3 (DELIVERED) | 1 | 800 | 800 |
| 23 | O1015 | 1 | 20261011 | 15 (Siddharth) | 19 (P019) | 1 (UPI) | 2 (SHIPPED) | 1 | 4000 | 4000 |
| 24 | O1016 | 1 | 20261012 | 16 (Ananya) | 16 (P016) | 1 (UPI) | 3 (DELIVERED) | 1 | 2200 | 2200 |
| 25 | O1016 | 2 | 20261012 | 16 (Ananya) | 15 (P015) | 1 (UPI) | 3 (DELIVERED) | 2 | 600 | 1200 |
| 26 | O1017 | 1 | 20261013 | 17 (Harsh) | 4 (P004) | 3 (NETBANKING) | 3 (DELIVERED) | 1 | 3500 | 3500 |
| 27 | O1018 | 1 | 20261014 | 18 (Nidhi) | 20 (P020) | 4 (COD) | 4 (CANCELLED) | 2 | 350 | 700 |
| 28 | O1019 | 1 | 20261015 | 19 (Yash) | 1 (P001) | 2 (CARD) | 3 (DELIVERED) | 1 | 25000 | 25000 |
| 29 | O1019 | 2 | 20261015 | 19 (Yash) | 3 (P003) | 2 (CARD) | 3 (DELIVERED) | 1 | 2000 | 2000 |
| 30 | O1020 | 1 | 20261016 | 20 (Diya) | 12 (P012) | 1 (UPI) | 3 (DELIVERED) | 2 | 1200 | 2400 |
| | | | | | | | | | **TOTAL** | **249,500** |

#### Part C – payments → fact_payment

**③ Sanitize:** PAY01 `"₹29,000"` → 29000; PAY07 `success` → SUCCESS; PAY12 `Net Banking` → NETBANKING.
**④ Corrupt:** PAY22 amount `"abc"` → reject.
**⑤ Dedupe:** PAY05 2 vaar che. 20:12 vado rakhyo, 20:11 vado reject.
**⑦ FK:** PAY21 no order O1021 curated ma nathi (reject thayo hato) → reject.
Have **20 payments**.

**⑩ Load `curated.fact_payment`** (20 rows)
**Grain:** 1 row = 1 payment. **Upsert key:** payment_id. `customer_key` order mathi lidho.

| payment_key | payment_id | order_no | payment_date_key | customer_key | payment_method_key | payment_status | amount |
|---|---|---|---|---|---|---|---|
| 1 | PAY01 | O1001 | 20261001 | 1 (Rahul) | 1 (UPI) | SUCCESS | 29000 |
| 2 | PAY02 | O1002 | 20261004 | 2 (Priya) | 4 (COD) | SUCCESS | 2400 |
| 3 | PAY03 | O1003 | 20261002 | 3 (Amit) | 2 (CARD) | SUCCESS | 55000 |
| 4 | PAY04 | O1004 | 20261003 | 1 (Rahul) | 1 (UPI) | REFUNDED | 2400 |
| 5 | PAY05 | O1005 | 20261003 | 4 (Neha) | 1 (UPI) | SUCCESS | 3500 |
| 6 | PAY06 | O1006 | 20261004 | 5 (Karan) | 2 (CARD) | SUCCESS | 4900 |
| 7 | PAY07 | O1007 | 20261005 | 6 (Sneha) | 1 (UPI) | SUCCESS | 2000 |
| 8 | PAY08 | O1008 | 20261007 | 7 (Vikram) | 4 (COD) | REFUNDED | 3600 |
| 9 | PAY09 | O1009 | 20261006 | 8 (Pooja) | 1 (UPI) | SUCCESS | 2600 |
| 10 | PAY10 | O1010 | 20261007 | 10 (Riya) | 2 (CARD) | PENDING | 80000 |
| 11 | PAY11 | O1011 | 20261008 | 11 (Mehul) | 1 (UPI) | SUCCESS | 3000 |
| 12 | PAY12 | O1012 | 20261008 | 12 (Kavya) | 3 (NETBANKING) | SUCCESS | 12999 |
| 13 | PAY13 | O1013 | 20261012 | 13 (Rohan) | 4 (COD) | SUCCESS | 2800 |
| 14 | PAY14 | O1014 | 20261010 | 14 (Isha) | 2 (CARD) | SUCCESS | 5300 |
| 15 | PAY15 | O1015 | 20261011 | 15 (Siddharth) | 1 (UPI) | SUCCESS | 4000 |
| 16 | PAY16 | O1016 | 20261012 | 16 (Ananya) | 1 (UPI) | SUCCESS | 3400 |
| 17 | PAY17 | O1017 | 20261013 | 17 (Harsh) | 3 (NETBANKING) | SUCCESS | 3500 |
| 18 | PAY18 | O1018 | 20261014 | 18 (Nidhi) | 4 (COD) | FAILED | 700 |
| 19 | PAY19 | O1019 | 20261015 | 19 (Yash) | 2 (CARD) | SUCCESS | 27000 |
| 20 | PAY20 | O1020 | 20261016 | 20 (Diya) | 1 (UPI) | SUCCESS | 2400 |

> **Reconciliation point:** O1012 ma payment 12999 che, pan `fact_sales` ma 12000 j che. Fark 999 no che, kem ke P022 vali line reject thai hati. Aavo fark **DQ report** ma dekhavo joie, chhupavvo nahi.

### 6.6 Job-2 Summary

| | customers | orders | payments |
|---|---|---|---|
| Read | 23 | 24 | 23 |
| Doc rejects | 3 | 4 | 3 |
| Line rejects | – | 2 | – |
| Loaded | dim_customer 20 | fact_sales 30 | fact_payment 20 |

---

## 7. Job-3: Multiple Collections → Single Table

### 7.1 Purpose

Customer complaints **3 alag rasta** thi aave che: email, chat ane call. Darek ni collection alag che, ane field naam ane format pan alag che.
Support team ne **ek j list** joie che: "aaje ketla complaints aavya, kaya prakar na, ane ketla pending che".

Etle Job-3 trane collections ne **standard banavi ne ek j table** `curated.support_ticket` ma bhega kare che (**UNION merge**).

### 7.2 Real production ma aavu thay?

**Ha, bahu common che.** Aa prakar ne **UNION merge** kahevay: **same prakar no data (ticket), alag-alag source**.

| Real example | Sources | Single table |
|---|---|---|
| Support tickets (aapdu) | Email, Chat, Call, WhatsApp | `support_ticket` |
| Orders | Website, Mobile App, Store POS | `all_orders` |
| Leads | Facebook ads, Google ads, Website form | `marketing_lead` |
| Bank transactions | UPI, Card, NEFT systems | `transaction` |

**JOIN merge** (alag entity ne ek wide table ma jodvi, jem ke Customer 360) pan real che, pan e mostly **Gold / reporting layer** ma bane che. Curated ma UNION merge vadhare jova male che.

> Single table ma dim/fact nathi, pan ema **standard columns, standard values ane cleaned data** hoy che. Aa pan curated j che.

### 7.3 Field Mapping – 3 alag structure → 1 standard structure

| Standard column | email_tickets | chat_tickets | call_logs |
|---|---|---|---|
| `source_channel` | `'EMAIL'` (fix) | `'CHAT'` (fix) | `'CALL'` (fix) |
| `source_ticket_id` | `ticket_ref` | `chatId` | `call_id` |
| `customer_code` | `from_email` → dim_customer.email | `customerCode` → dim_customer | `phone` → dim_customer.phone |
| `order_no` | `order_no` | – | – |
| `issue_text` | `subject` | `topic` | `reason` |
| `issue_category` | subject mathi keyword | topic mathi keyword | reason mathi keyword |
| `status` | `status` (mapping) | `state` (mapping) | `outcome` (mapping) |
| `priority` | `priority` | state mujab | outcome mujab |
| `created_at` (IST) | `received_at` (IST) | `startedAt` (**UTC → IST**) | `call_time` (**dd/MM/yyyy → timestamp**) |
| `handled_by` | `'Email Desk'` | `agent` | `agent` |
| `call_duration_sec` | null | null | `duration_sec` |

### 7.4 Value Mapping

**Status:**

| Email | Chat | Call | → Standard |
|---|---|---|---|
| open | – | – | OPEN |
| pending | PENDING | Callback | IN_PROGRESS |
| closed | RESOLVED | Resolved | RESOLVED |
| – | TRANSFERRED | Escalated | ESCALATED |

**Priority:**
- Email: source ni value UPPERCASE (High → HIGH)
- Chat ane Call: ESCALATED hoy to **HIGH**, nahi to **MEDIUM**

**Issue category (keyword thi, aa j kram ma check thay; pehlo match jite):**

| Kram | Category | Keywords |
|---|---|---|
| 1 | PAYMENT | payment, refund, debited, deducted, charged |
| 2 | RETURN | return, exchange, size, pickup |
| 3 | PRODUCT_QUALITY | damaged, defective, not working, quality, colour, weak |
| 4 | ACCOUNT | login, password, account, address |
| 5 | DELIVERY | deliver, courier, late, not received, tracking, where is my order, dispatch |
| 6 | OTHER | koi match nahi |

> **Kram kem jaruri?** "Change delivery address" ma `delivery` ane `address` banne che. ACCOUNT pehla check thay che, etle sachu ACCOUNT male. "Refund for returned bat" ma PAYMENT pehla aave che, etle sachu PAYMENT male.

### 7.5 Rules – Job-3

| Code | Prakar | Rule | Example |
|---|---|---|---|
| J3-S01 | Sanitize | Email trim + lowercase | `" NIDHI.AGARWAL@MAIL.COM "` → `nidhi.agarwal@mail.com` |
| J3-S02 | Sanitize | Phone: digits, `+91` ane aagal no `0` kadho, chhella 10 | `"+91 98765 00001"` → `9876500001`, `"09876500004"` → `9876500004` |
| J3-S03 | Sanitize | Chat time UTC → IST (+5:30) | `10-03 12:55Z` → `10-03 18:25 IST` |
| J3-S04 | Sanitize | Call time `dd/MM/yyyy HH:mm` → timestamp | `02/10/2026 11:30` → `2026-10-02 11:30` |
| J3-S05 | Sanitize | Status mapping (7.4) | `closed` → RESOLVED |
| J3-S06 | Sanitize | Priority mapping + default | chat TRANSFERRED → HIGH |
| J3-S07 | Sanitize | Issue category keyword thi | `kettle damaged in box` → PRODUCT_QUALITY |
| J3-C01 | Corrupt | Time parse thavo joie | CH-011 `"abc"`, CL-012 `"2026/10/12 25:61"` → reject |
| J3-D01 | Duplicate | Same (channel + source id): latest rakho | EM-007, CH-005 → copy reject |
| J3-V01 | Validation | created_at required | EM-019 (null) → reject |
| J3-V02 | Validation | Spam / test sender block list | EM-015 `noreply@spam.xyz` → reject |
| J3-V03 | Validation | Call duration >= 0 | CL-009 (-30) → reject |
| J3-V04 | Validation | Customer olakhva kai pan (email/phone/code) ane issue text joie | CL-016 (banne null) → reject |
| J3-SR01 | **Soft rule** | Customer na male to `customer_code = 'UNKNOWN'` (reject **nahi**) | EM-014, CH-018, CL-019 |

> **Soft rule kem?** Ticket sacho che. Customer system ma nathi, to pan complaint to aavi j che. Ene reject karo to support team ne ticket j na dekhay. Etle load karo ane `UNKNOWN` thi mark karo.

### 7.6 Step-by-step (Full Load)

**② Read:** email 21, chat 21, call 20 = **62 docs**.

**③ Sanitize – ek-ek example darek collection mathi:**

```
 EMAIL  EM-018  from_email " NIDHI.AGARWAL@MAIL.COM "  → nidhi.agarwal@mail.com → lookup → C018
 CHAT   CH-001  startedAt 2026-10-03T12:55Z (UTC)      → 2026-10-03 18:25 (IST)
 CALL   CL-001  phone "+91 98765 00001"                 → 9876500001 → lookup → C001
        CL-001  call_time "02/10/2026 11:30"            → 2026-10-02 11:30
```

**④ Corrupt:** CH-011, CL-012 → reject.
**⑤ Dedupe:** EM-007 ni copy ane CH-005 ni copy → reject.
**⑥ Validate:** EM-015 (spam), EM-019 (time nathi), CL-009 (duration -30), CL-016 (info nathi) → reject.
**⑦ Lookup (soft):** EM-014, CH-018, CL-019 → `UNKNOWN`.

**⑧ Standardize + UNION → ⑩ Upsert `curated.support_ticket`** (54 rows)
**Upsert key:** (source_channel, source_ticket_id)

| ticket_key | channel | source_id | customer_code | order_no | issue_category | status | priority | created_at (IST) | handled_by | call_sec |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | EMAIL | EM-001 | C001 | O1001 | DELIVERY | RESOLVED | HIGH | 2026-10-02 09:10 | Email Desk | – |
| 2 | EMAIL | EM-002 | C002 | O1002 | RETURN | RESOLVED | MEDIUM | 2026-10-05 11:00 | Email Desk | – |
| 3 | EMAIL | EM-003 | C003 | O1003 | PRODUCT_QUALITY | OPEN | HIGH | 2026-10-06 14:20 | Email Desk | – |
| 4 | EMAIL | EM-004 | C004 | O1005 | PAYMENT | RESOLVED | HIGH | 2026-10-04 08:30 | Email Desk | – |
| 5 | EMAIL | EM-005 | C005 | O1006 | DELIVERY | IN_PROGRESS | LOW | 2026-10-06 10:00 | Email Desk | – |
| 6 | EMAIL | EM-006 | C006 | O1007 | PRODUCT_QUALITY | OPEN | MEDIUM | 2026-10-08 16:45 | Email Desk | – |
| 7 | EMAIL | EM-007 | C007 | O1008 | PAYMENT | OPEN | HIGH | 2026-10-09 12:00 | Email Desk | – |
| 8 | EMAIL | EM-008 | C008 | O1009 | ACCOUNT | RESOLVED | LOW | 2026-10-06 09:00 | Email Desk | – |
| 9 | EMAIL | EM-009 | C010 | O1010 | PAYMENT | IN_PROGRESS | HIGH | 2026-10-08 10:30 | Email Desk | – |
| 10 | EMAIL | EM-010 | C011 | O1011 | PRODUCT_QUALITY | RESOLVED | HIGH | 2026-10-10 18:00 | Email Desk | – |
| 11 | EMAIL | EM-011 | C012 | O1012 | OTHER | RESOLVED | LOW | 2026-10-11 11:20 | Email Desk | – |
| 12 | EMAIL | EM-012 | C013 | O1013 | RETURN | OPEN | MEDIUM | 2026-10-12 13:00 | Email Desk | – |
| 13 | EMAIL | EM-013 | C014 | O1014 | PRODUCT_QUALITY | RESOLVED | MEDIUM | 2026-10-13 15:10 | Email Desk | – |
| 14 | EMAIL | EM-014 | **UNKNOWN** | – | ACCOUNT | RESOLVED | LOW | 2026-10-14 09:30 | Email Desk | – |
| 15 | EMAIL | EM-016 | C016 | O1016 | DELIVERY | OPEN | HIGH | 2026-10-15 10:40 | Email Desk | – |
| 16 | EMAIL | EM-017 | C017 | O1017 | PRODUCT_QUALITY | IN_PROGRESS | MEDIUM | 2026-10-16 17:30 | Email Desk | – |
| 17 | EMAIL | EM-018 | C018 | O1018 | PAYMENT | RESOLVED | MEDIUM | 2026-10-15 12:00 | Email Desk | – |
| 18 | EMAIL | EM-020 | C020 | O1020 | RETURN | OPEN | LOW | 2026-10-18 11:15 | Email Desk | – |
| 19 | CHAT | CH-001 | C001 | – | PAYMENT | RESOLVED | MEDIUM | 2026-10-03 18:25 | Meena | – |
| 20 | CHAT | CH-002 | C003 | – | DELIVERY | RESOLVED | MEDIUM | 2026-10-02 10:00 | Ravi | – |
| 21 | CHAT | CH-003 | C004 | – | RETURN | IN_PROGRESS | MEDIUM | 2026-10-07 12:30 | Meena | – |
| 22 | CHAT | CH-004 | C005 | – | DELIVERY | ESCALATED | HIGH | 2026-10-07 19:00 | Ravi | – |
| 23 | CHAT | CH-005 | C006 | – | PRODUCT_QUALITY | RESOLVED | MEDIUM | 2026-10-09 11:10 | Sonal | – |
| 24 | CHAT | CH-006 | C008 | – | RETURN | IN_PROGRESS | MEDIUM | 2026-10-10 14:45 | Sonal | – |
| 25 | CHAT | CH-007 | C009 | – | ACCOUNT | RESOLVED | MEDIUM | 2026-10-11 09:20 | Ravi | – |
| 26 | CHAT | CH-008 | C010 | – | PAYMENT | ESCALATED | HIGH | 2026-10-08 13:50 | Meena | – |
| 27 | CHAT | CH-009 | C011 | – | PRODUCT_QUALITY | RESOLVED | MEDIUM | 2026-10-12 16:00 | Sonal | – |
| 28 | CHAT | CH-010 | C012 | – | DELIVERY | RESOLVED | MEDIUM | 2026-10-09 10:30 | Ravi | – |
| 29 | CHAT | CH-012 | C014 | – | RETURN | IN_PROGRESS | MEDIUM | 2026-10-14 11:40 | Meena | – |
| 30 | CHAT | CH-013 | C015 | – | DELIVERY | ESCALATED | HIGH | 2026-10-14 18:10 | Ravi | – |
| 31 | CHAT | CH-014 | C016 | – | PRODUCT_QUALITY | RESOLVED | MEDIUM | 2026-10-15 08:55 | Sonal | – |
| 32 | CHAT | CH-015 | C017 | – | PAYMENT | IN_PROGRESS | MEDIUM | 2026-10-16 20:00 | Meena | – |
| 33 | CHAT | CH-016 | C018 | – | PAYMENT | RESOLVED | MEDIUM | 2026-10-15 13:30 | Ravi | – |
| 34 | CHAT | CH-017 | C019 | – | OTHER | RESOLVED | MEDIUM | 2026-10-17 12:00 | Sonal | – |
| 35 | CHAT | CH-018 | **UNKNOWN** | – | DELIVERY | IN_PROGRESS | MEDIUM | 2026-10-18 10:10 | Meena | – |
| 36 | CHAT | CH-019 | C020 | – | RETURN | IN_PROGRESS | MEDIUM | 2026-10-19 09:45 | Ravi | – |
| 37 | CHAT | CH-020 | C002 | – | ACCOUNT | RESOLVED | MEDIUM | 2026-10-19 17:20 | Sonal | – |
| 38 | CALL | CL-001 | C001 | – | DELIVERY | RESOLVED | MEDIUM | 2026-10-02 11:30 | Kiran | 240 |
| 39 | CALL | CL-002 | C003 | – | PRODUCT_QUALITY | ESCALATED | HIGH | 2026-10-07 10:05 | Asha | 600 |
| 40 | CALL | CL-003 | C004 | – | PAYMENT | RESOLVED | MEDIUM | 2026-10-04 09:15 | Kiran | 180 |
| 41 | CALL | CL-004 | C005 | – | DELIVERY | IN_PROGRESS | MEDIUM | 2026-10-08 17:40 | Asha | 120 |
| 42 | CALL | CL-005 | C006 | – | RETURN | RESOLVED | MEDIUM | 2026-10-10 12:20 | Kiran | 300 |
| 43 | CALL | CL-006 | C007 | – | PAYMENT | ESCALATED | HIGH | 2026-10-10 15:00 | Asha | 420 |
| 44 | CALL | CL-007 | C008 | – | ACCOUNT | RESOLVED | MEDIUM | 2026-10-06 08:45 | Kiran | 150 |
| 45 | CALL | CL-008 | C010 | – | PAYMENT | IN_PROGRESS | MEDIUM | 2026-10-09 11:00 | Asha | 200 |
| 46 | CALL | CL-010 | C012 | – | OTHER | RESOLVED | MEDIUM | 2026-10-11 16:30 | Kiran | 90 |
| 47 | CALL | CL-011 | C013 | – | RETURN | RESOLVED | MEDIUM | 2026-10-12 14:10 | Asha | 210 |
| 48 | CALL | CL-013 | C015 | – | DELIVERY | ESCALATED | HIGH | 2026-10-15 10:20 | Kiran | 360 |
| 49 | CALL | CL-014 | C016 | – | DELIVERY | IN_PROGRESS | MEDIUM | 2026-10-15 18:05 | Asha | 270 |
| 50 | CALL | CL-015 | C017 | – | PRODUCT_QUALITY | RESOLVED | MEDIUM | 2026-10-16 19:30 | Kiran | 330 |
| 51 | CALL | CL-017 | C019 | – | ACCOUNT | RESOLVED | MEDIUM | 2026-10-17 09:10 | Asha | 120 |
| 52 | CALL | CL-018 | C020 | – | RETURN | IN_PROGRESS | MEDIUM | 2026-10-19 10:50 | Kiran | 160 |
| 53 | CALL | CL-019 | **UNKNOWN** | – | OTHER | RESOLVED | MEDIUM | 2026-10-19 13:15 | Asha | 60 |
| 54 | CALL | CL-020 | C002 | – | RETURN | RESOLVED | MEDIUM | 2026-10-20 11:40 | Kiran | 140 |

**Aa ek table thi have saral answer male che:**

| Sawal | Jawab (54 tickets mathi) |
|---|---|
| Channel wise tickets | EMAIL 18, CHAT 19, CALL 17 |
| Haju khula (OPEN + IN_PROGRESS) | 19 |
| ESCALATED | 6 |
| Category wise | DELIVERY 12, PAYMENT 11, RETURN 11, PRODUCT_QUALITY 10, ACCOUNT 6, OTHER 4 |

### 7.7 Job-3 Summary

| | email | chat | call | Total |
|---|---|---|---|---|
| Read | 21 | 21 | 20 | 62 |
| Rejected | 3 | 2 | 3 | 8 |
| Loaded | 18 | 19 | 17 | **54** |
| UNKNOWN customer (soft) | 1 | 1 | 1 | 3 |

---

## 8. Job-4: Shared + New Collections → Multiple Dim + Fact

### 8.1 Purpose

Order pachhi shu thayu? **Delivery samay par thai ke nahi** (`shipments`), ane **customer ne product kevo laagyo** (`reviews`).
Aa mate `orders` ni pan jarur che, je Job-2 pan vapare che. Aa ek **shared collection** che.

**Business questions:**
- "Kayo courier sauthi vadhare late kare che?"
- "Kaya product nu average rating ketlu che?"
- "Late delivery vala customers e kharab rating aapyu?"

### 8.2 `orders` collection Job-4 ma kevi rite vaparay? (important)

| | Job-2 ma `orders` | Job-4 ma `orders` |
|---|---|---|
| Role | **Driving collection** – ena navi/badlayela orders thi j fact bane che | **Lookup collection** – shipment ane review ne verify karva mate |
| Kevi rite vanchay? | **Watermark** thi (updated_at window) | **Key thi**: fakt e order_no jeno shipment/review aa batch ma che |
| Watermark row? | Ha (`J2_SALES / orders`) | **Na** |

**Kem watermark thi nahi?** Aaje navo review aave che je 2 athvadiya juna order no che. E order aa 4 kalak ni window ma nathi, to pan joie che.
Etle Job-4 `orders` ne **"je order_no joie e j"** vanche che.

### 8.3 Input → Output

```
 shipments (watermark) ──┬── courier ───────────────────────► dim_courier
                         └── lookup orders (key) → customer, order_date, status
                                     └─ lookup dim_customer, dim_date ──► fact_shipment

 reviews (watermark) ────── lookup orders (key) → verify purchase
                                     └─ lookup dim_product, dim_customer ──► fact_product_review
```

### 8.4 Schema – Star (shared dims sathe = Fact Constellation)

```
                 dim_courier
                      │
   dim_customer ── fact_shipment ── dim_date (4 vaar: order, dispatch, promised, delivered)
        │
        └────── fact_product_review ── dim_product (Job-1)
                         │
                      dim_date
```

- `dim_customer` (Job-2) ane `dim_product` (Job-1) **pharithi nathi banavta. Reuse** karya. Aane **conformed dimension** kahe che.
- `fact_shipment` ma `dim_date` **4 alag roles** ma vaparay che. Aane **role-playing dimension** kahe che.

### 8.5 Rules – Job-4

**shipments:**

| Code | Prakar | Rule | Example |
|---|---|---|---|
| J4-S01 | Sanitize | Courier naam standard (case + trim) | `"speedex"` → SpeedEx, `"QUICKMOVE "` → QuickMove |
| J4-S02 | Sanitize | Status UPPERCASE | |
| J4-D01 | Duplicate | Same shipment_id: latest | SH-005 IN_TRANSIT (juno) → reject |
| J4-F01 | FK | Order `orders` collection ma hovo joie | SH-021 (O1099) → reject |
| J4-V01 | Validation | Status allowed: IN_TRANSIT / DELIVERED / RETURNED | |
| J4-V02 | Business rule | CANCELLED order no shipment na hoy | SH-004 (O1004) → reject |
| J4-V03 | Validation | delivered_at >= dispatched_at | SH-013B → reject |
| J4-V04 | Validation | DELIVERED hoy to delivered_at required | |

**Derived columns:**
- `days_to_deliver` = delivered date − dispatched date
- `delay_days` = max(0, delivered date − promised date)
- `is_on_time` = delay_days = 0 ? Y : N (IN_TRANSIT hoy to khali)

**reviews:**

| Code | Prakar | Rule | Example |
|---|---|---|---|
| J4-S03 | Sanitize | Comment trim | `"  nice fabric!!  "` → `nice fabric!!` |
| J4-S04 | Sanitize | Rating `"4.0"` → 4 | RV11 |
| J4-C01 | Corrupt | Rating number banvo joie | RV24 `"five"` → reject |
| J4-D02 | Duplicate | Same review_id: latest | RV07 (rating 2 vado juno) → reject |
| J4-V05 | Validation | Rating 1 thi 5 | RV22 (7) → reject |
| J4-V06 | Business rule | **Verified purchase**: e order ma e product hovo joie, ane order DELIVERED ke RETURNED hovo joie | RV21 (O1006 ma P005 nathi) → reject |
| J4-F02 | FK | Product ane customer dims ma hova joie | |

**Derived:** `sentiment` = rating 4–5 → Positive, 3 → Neutral, 1–2 → Negative

### 8.6 Step-by-step (Full Load)

**② Read:** shipments 24 (watermark), reviews 24 (watermark). `orders` ma thi fakt jaruri order_no (key lookup).

**Shipments:**
- ③ Sanitize: SH-003 `speedex` → SpeedEx, SH-014B `"QUICKMOVE "` → QuickMove
- ⑤ Dedupe: SH-005 juno → reject
- ⑦ Lookup order: SH-021 (O1099 nathi) → reject; SH-004 (O1004 CANCELLED) → reject
- ⑥ Validate: SH-013B (delivered 10-10 < dispatched 10-12) → reject
- Have **20 shipments**

**Derive example:**
```
 SH-014B  dispatched 10-11, promised 10-14, delivered 10-16
          days_to_deliver = 16 - 11 = 5
          delay_days      = 16 - 14 = 2
          is_on_time      = N
```

**`curated.dim_courier`** (3 rows)

| courier_key | courier_name |
|---|---|
| 1 | SpeedEx |
| 2 | QuickMove |
| 3 | EcoDeliver |

**`curated.fact_shipment`** (20 rows)
**Grain:** 1 row = 1 package (shipment). **Upsert key:** shipment_id

| key | shipment_id | order_no | customer_key | courier_key | order_date | dispatch_date | promised_date | delivered_date | status | days | delay | on_time |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | SH-001A | O1001 | 1 (Rahul) | 1 (SpeedEx) | 20261001 | 20261001 | 20261004 | 20261003 | DELIVERED | 2 | 0 | Y |
| 2 | SH-001B | O1001 | 1 (Rahul) | 1 (SpeedEx) | 20261001 | 20261002 | 20261005 | 20261004 | DELIVERED | 2 | 0 | Y |
| 3 | SH-002 | O1002 | 2 (Priya) | 2 (QuickMove) | 20261001 | 20261002 | 20261005 | 20261004 | DELIVERED | 2 | 0 | Y |
| 4 | SH-003 | O1003 | 3 (Amit) | 1 (SpeedEx) | 20261002 | 20261002 | 20261005 | 20261006 | DELIVERED | 4 | 1 | N |
| 5 | SH-005 | O1005 | 4 (Neha) | 3 (EcoDeliver) | 20261003 | 20261004 | 20261007 | 20261006 | DELIVERED | 2 | 0 | Y |
| 6 | SH-006 | O1006 | 5 (Karan) | 2 (QuickMove) | 20261004 | 20261005 | 20261008 | 20261007 | DELIVERED | 2 | 0 | Y |
| 7 | SH-007 | O1007 | 6 (Sneha) | 1 (SpeedEx) | 20261005 | 20261005 | 20261008 | 20261007 | DELIVERED | 2 | 0 | Y |
| 8 | SH-008 | O1008 | 7 (Vikram) | 3 (EcoDeliver) | 20261005 | 20261006 | 20261009 | 20261008 | RETURNED | 2 | 0 | Y |
| 9 | SH-009 | O1009 | 8 (Pooja) | 2 (QuickMove) | 20261006 | 20261006 | 20261009 | 20261008 | DELIVERED | 2 | 0 | Y |
| 10 | SH-011 | O1011 | 11 (Mehul) | 1 (SpeedEx) | 20261008 | 20261008 | 20261011 | 20261010 | DELIVERED | 2 | 0 | Y |
| 11 | SH-012 | O1012 | 12 (Kavya) | 1 (SpeedEx) | 20261008 | 20261009 | 20261012 | 20261013 | DELIVERED | 4 | 1 | N |
| 12 | SH-013 | O1013 | 13 (Rohan) | 3 (EcoDeliver) | 20261009 | 20261009 | 20261012 | 20261012 | DELIVERED | 3 | 0 | Y |
| 13 | SH-014A | O1014 | 14 (Isha) | 2 (QuickMove) | 20261010 | 20261010 | 20261013 | 20261012 | DELIVERED | 2 | 0 | Y |
| 14 | SH-014B | O1014 | 14 (Isha) | 2 (QuickMove) | 20261010 | 20261011 | 20261014 | 20261016 | DELIVERED | 5 | 2 | N |
| 15 | SH-015 | O1015 | 15 (Siddharth) | 1 (SpeedEx) | 20261011 | 20261012 | 20261015 | – | IN_TRANSIT | – | – | – |
| 16 | SH-016 | O1016 | 16 (Ananya) | 3 (EcoDeliver) | 20261012 | 20261012 | 20261015 | 20261017 | DELIVERED | 5 | 2 | N |
| 17 | SH-017 | O1017 | 17 (Harsh) | 2 (QuickMove) | 20261013 | 20261014 | 20261017 | 20261016 | DELIVERED | 2 | 0 | Y |
| 18 | SH-019A | O1019 | 19 (Yash) | 1 (SpeedEx) | 20261015 | 20261015 | 20261018 | 20261017 | DELIVERED | 2 | 0 | Y |
| 19 | SH-019B | O1019 | 19 (Yash) | 1 (SpeedEx) | 20261015 | 20261016 | 20261019 | 20261018 | DELIVERED | 2 | 0 | Y |
| 20 | SH-020 | O1020 | 20 (Diya) | 3 (EcoDeliver) | 20261016 | 20261017 | 20261020 | 20261019 | DELIVERED | 2 | 0 | Y |

**Insight:** 19 delivered packages mathi **4 late** che: SpeedEx 2, QuickMove 1, EcoDeliver 1.

**Reviews:**
- ③ Sanitize: RV03 comment trim, RV11 `"4.0"` → 4
- ④ Corrupt: RV24 `"five"` → reject
- ⑤ Dedupe: RV07 – 10-12 vado (rating 3) rakhyo, 10-09 vado (rating 2) reject
- ⑥ Validate: RV22 rating 7 → reject
- ⑦ Verify purchase (orders lookup): RV21 – O1006 ma P005 nathi → reject
- Have **20 reviews**

**Verified purchase check kevi rite?**
```
 RV21: product P005, order O1006
       orders lookup → O1006 items = [P011, P013], status DELIVERED
       P005 items ma nathi → ❌ NOT VERIFIED

 RV01: product P001, order O1001
       orders lookup → O1001 items = [P001, P003], status DELIVERED
       P001 che ane DELIVERED → ✅ VERIFIED
```

**`curated.fact_product_review`** (20 rows)
**Grain:** 1 row = 1 review. **Upsert key:** review_id

| review_key | review_id | product_key | customer_key | order_no | review_date_key | rating | sentiment |
|---|---|---|---|---|---|---|---|
| 1 | RV01 | 1 (P001) | 1 (Rahul) | O1001 | 20261005 | 5 | Positive |
| 2 | RV02 | 3 (P003) | 1 (Rahul) | O1001 | 20261005 | 4 | Positive |
| 3 | RV03 | 8 (P008) | 2 (Priya) | O1002 | 20261006 | 4 | Positive |
| 4 | RV04 | 5 (P005) | 3 (Amit) | O1003 | 20261008 | 3 | Neutral |
| 5 | RV05 | 6 (P006) | 4 (Neha) | O1005 | 20261008 | 5 | Positive |
| 6 | RV06 | 7 (P007) | 4 (Neha) | O1005 | 20261008 | 4 | Positive |
| 7 | RV07 | 3 (P003) | 6 (Sneha) | O1007 | 20261009 | 3 | Neutral |
| 8 | RV08 | 8 (P008) | 8 (Pooja) | O1009 | 20261010 | 5 | Positive |
| 9 | RV09 | 6 (P006) | 8 (Pooja) | O1009 | 20261010 | 4 | Positive |
| 10 | RV10 | 12 (P012) | 11 (Mehul) | O1011 | 20261011 | 1 | Negative |
| 11 | RV11 | 14 (P014) | 11 (Mehul) | O1011 | 20261012 | 4 | Positive |
| 12 | RV12 | 2 (P002) | 12 (Kavya) | O1012 | 20261014 | 4 | Positive |
| 13 | RV13 | 10 (P010) | 13 (Rohan) | O1013 | 20261013 | 3 | Neutral |
| 14 | RV14 | 9 (P009) | 14 (Isha) | O1014 | 20261017 | 3 | Neutral |
| 15 | RV15 | 8 (P008) | 14 (Isha) | O1014 | 20261017 | 5 | Positive |
| 16 | RV16 | 16 (P016) | 16 (Ananya) | O1016 | 20261018 | 4 | Positive |
| 17 | RV17 | 4 (P004) | 17 (Harsh) | O1017 | 20261017 | 2 | Negative |
| 18 | RV18 | 1 (P001) | 19 (Yash) | O1019 | 20261018 | 5 | Positive |
| 19 | RV19 | 3 (P003) | 19 (Yash) | O1019 | 20261019 | 4 | Positive |
| 20 | RV20 | 12 (P012) | 20 (Diya) | O1020 | 20261020 | 4 | Positive |

**Insight:** P008 Printed Kurti nu average rating (4+5+5)/3 = **4.67** che. P012 Electric Kettle nu (1+4)/2 = **2.5** che, ane EM-010 (email ticket) ma pan kettle damage ni complaint che. Job-3 ane Job-4 no data saathe joie to aavi insight male che.

### 8.7 Job-4 Summary

| | shipments | reviews |
|---|---|---|
| Read | 24 | 24 |
| Rejected | 4 | 4 |
| Loaded | fact_shipment 20 | fact_product_review 20 |
| dims | dim_courier 3 (navu), dim_customer + dim_product reuse | |

---

## 9. Job-5: Curated Tables → Customer 360 Summary

### 9.1 Purpose

Sales, support ane management team ne **ek customer ni aakhi kundli ek j row ma** joie: ketla orders, ketla paisa, payment haju baki che ke nahi, ketli complaints khuli che, rating kevu aape che, ane delivery late thai ke nahi.

Aa data **6 alag curated tables** ma vahenchayelo che. Job-5 ene **customer na hisabe bhego (aggregate)** kari ne ek j table `gold.customer_360_summary` banave che.

**Business questions:**
- "Kaya mota (VIP) customers naraj che, etle ke complaints khuli che?"
- "Kona payment haju baki che?"
- "Spend mujab top 5 customers kon che?"

> Section 7.2 ma kahyu tem, aa **JOIN merge** che (alag entities ne jodi ne wide table). Real project ma e mostly **Gold / reporting layer** ma hoy che. Aapda project ma Gold layer nathi, etle ene `curated` schema ma j rakhyu che.

### 9.2 Sauthi motu farak: source MongoDB nahi, **Curated tables**

| | Job-1 thi Job-4 | Job-5 |
|---|---|---|
| Source | MongoDB collections | **PostgreSQL curated tables** (Job 1–4 nu output) |
| Sanitize / Dedupe / Validate | Ha (darek collection mate) | **Na**. Data pehlethi clean che |
| Watermark column | Mongo nu `updated_at` | Curated tables nu **`etl_updated_at`** (row kyare load/update thai) |
| Kaam | Clean karo ane load karo | **Aggregate** karo (sum, count, avg) ane load karo |
| Kya chale? | Spark | **EPAS ma SQL** (Section 9.7) |

**Mongo mathi farithi kem na vanchvu?**
1. Duplicate, corrupt, SCD2 jeva **badha rules farithi lakhva pade**. Ek j logic 2 jagyae thay, ane kyarek ne kyarek numbers alag aave.
2. Curated ma rejected records **pehlethi bahar** che. Mongo mathi vanchiye to O1021 (C099) ke PAY22 ("abc") pharithi aavi jay.
3. Customer 360 na numbers ane fact tables na numbers **match thava j joie** (reconciliation). Source same hoy to j e shakya che.

### 9.3 Input → Output

```
 dim_customer (is_current = Y) ───────────────┐  name, city, tier (aaj ni value)
 dim_customer (badhi versions) ── key → code ──┤  (SCD2 trap, Section 9.8)
 fact_sales + dim_order_status ───────────────┤  total_orders, net_spend, last_order_date
 fact_payment ────────────────────────────────┼──► customer_360_summary  (1 row = 1 customer)
 support_ticket (UNKNOWN sivay) ──────────────┤  total_tickets, open_tickets
 fact_product_review ─────────────────────────┤  avg_rating
 fact_shipment ───────────────────────────────┘  late_deliveries
                                                  + customer_segment, spend_rank (derived)
```

**DAG ma jagya:** J1 → J2 → (J3, J4) → **J5**. Job-5 ne Job-2, Job-3 ane Job-4 tranena facts joie che, etle e **sauthi chhelle** chale che. Koi pan ek job fail thay to Job-5 na chale, jethi adhuro 360 na bane.

### 9.4 Columns – darek column no business rule

**Grain:** 1 row = 1 customer (`customer_code`). **Upsert key:** `customer_code`

| Column | Kyathi | Rule |
|---|---|---|
| `customer_code` | dim_customer | Business key |
| `customer_name`, `city`, `tier` | dim_customer (`is_current = Y`) | Customer ni **aaj ni** value |
| `total_orders` | fact_sales | Order ketla (distinct `order_no`), badha status |
| `net_spend` | fact_sales | `line_amount` no sum, **CANCELLED ane RETURNED sivay** |
| `last_order_date` | fact_sales | Chhello order kyare |
| `paid_amount` | fact_payment | `SUCCESS` payments no sum |
| `refund_amount` | fact_payment | `REFUNDED` payments no sum |
| `total_tickets` | support_ticket | Badhi complaints |
| `open_tickets` | support_ticket | `RESOLVED` na hoy evi (OPEN, IN_PROGRESS, ESCALATED) |
| `avg_rating` | fact_product_review | Average rating |
| `late_deliveries` | fact_shipment | `is_on_time = N` vala packages |
| `customer_segment` | derived | Section 9.5 na rules |
| `spend_rank` | derived | `net_spend` mujab rank, **badha customers vacche** |
| `last_refreshed_run` | – | Kaya run e aa row banavi |

> **0 ane null no farak:** Arjun (C009) e koi order nathi karyo, to `total_orders = 0` ane `net_spend = 0` (null nahi). Pan Karan e review nathi aapyo, to `avg_rating = null` rakhvu, **0 nahi**. 0 lakhiye to average ma "0 star" ganay, je khotu che.

### 9.5 Rules – Job-5

Source clean che, etle ahiya rules no **prakar alag** che. Mostly mapping, default ane **reconciliation** che.

| Code | Prakar | Rule | Example |
|---|---|---|---|
| J5-S01 | Mapping | SCD2: customer ni **badhi `customer_key`** ne `customer_code` par map kari ne group karo | Amit: key 3 ane key 21 → C003 |
| J5-S02 | Default | Data na hoy to count ane amount = 0 | C009: orders 0, net_spend 0 |
| J5-S03 | Default | Review na hoy to `avg_rating` = null (0 nahi) | C005, C007 |
| J5-SR01 | Soft rule | `UNKNOWN` tickets 360 ma na ganvi (reject nahi, fakt bahar) | EM-014, CH-018, CL-019 |
| J5-R01 | Reconciliation (hard) | `sum(net_spend)` = fact_sales nu total (CANCELLED/RETURNED sivay) | 242,800 = 242,800 |
| J5-R02 | Reconciliation (hard) | `sum(total_tickets)` + UNKNOWN = support_ticket rows | 51 + 3 = 54 |
| J5-R03 | Reconciliation (hard) | Rows = current customers | 20 = 20 |
| J5-W01 | Warning (DQ report) | `net_spend` ane `paid_amount` alag hoy to DQ report ma dekhado | C010: 80,000 pending, C012: 999 vadhare |

- **Hard rule fail thay** to job **FAIL** thay. Table ma kai lakhvanu nahi, ane watermark same rahe.
- **Warning** thi job rokatu nathi. Fakt DQ report ma dekhay.

**Segment rules (aa j kram ma check thay; pehlo match jite):**

| Kram | Segment | Rule |
|---|---|---|
| 1 | `NO_ORDER` | `total_orders = 0` |
| 2 | `AT_RISK` | `open_tickets >= 2` **ke** `avg_rating <= 2.5` |
| 3 | `VIP` | `net_spend >= 25,000` |
| 4 | `ZERO_SPEND` | `net_spend = 0` (badha orders cancel ke return) |
| 5 | `REGULAR` | Baki badha |

> **AT_RISK pehla, VIP pachhi kem?** Naraj VIP customer (jem ke Amit, Riya) ne **pehla** pakadvo che. VIP pehla check karta e "VIP" j dekhat ane complaint chhupai jat.

> **`meta_rejected` ma Job-5 na 0 rows kem?** Data clean che, etle row level par reject karvanu kai nathi. Problem aave (jem ke reconciliation fail) to e **aakha run** no problem che, ek row no nahi. Etle row reject nahi, **job fail** thay.

### 9.6 Step-by-step (Full Load)

**① Watermark:** `J5_CUSTOMER_360` ni row NULL che, etle Full Load: **badha customers** recompute.

**② Read (Postgres):** dim_customer 20, fact_sales 30, fact_payment 20, support_ticket 54, fact_product_review 20, fact_shipment 20.

**③ SCD2 map:** `customer_key → customer_code`. Full load ma 20 keys ane 20 codes che (1:1). Incremental ma aa important bane che (Section 9.8).

**④ Darek table ne ALAG aggregate karo (customer na hisabe):**

Rahul (C001) nu example:
```
 fact_sales:     O1001 L1 25000 DELIVERED, O1001 L2 4000 DELIVERED,
                 O1004 L1 1800 CANCELLED, O1004 L2 600 CANCELLED
                 → total_orders 2, net_spend 29000, last_order_date 2026-10-03
 fact_payment:   PAY01 SUCCESS 29000, PAY04 REFUNDED 2400
                 → paid_amount 29000, refund_amount 2400
 support_ticket: EM-001 RESOLVED, CH-001 RESOLVED, CL-001 RESOLVED
                 → total_tickets 3, open_tickets 0
 reviews:        RV01 5, RV02 4            → avg_rating 4.5
 shipments:      SH-001A on time, SH-001B on time → late_deliveries 0
```

**Fan-out trap (bahu common bug):** pehla badha tables JOIN karo ane pachhi SUM karo, to rows **guna** thai jay.
```
 ❌ Khoti rit: fact_sales (Rahul 4 lines) JOIN support_ticket (Rahul 3 tickets)
      → 4 × 3 = 12 rows. Darek sales line 3 vaar aave.
      → net_spend = (25000 + 4000) × 3 = 87,000 ❌   (sachu 29,000)

 ✅ Sachi rit: pehla darek table ne customer par aggregate karo (1 customer = 1 row),
      pachhi aggregate thayela results ne customer_code par JOIN karo.
```

**⑤ JOIN:** `dim_customer` (current) ne **base** rakhi ne baki 5 aggregate results ne **LEFT JOIN** karo. LEFT JOIN etle Arjun (C009) jeva customers, jemne order nathi karyo, e pan 360 ma aave.

**⑥ Derive:** segment (9.5 na rules) ane spend_rank.

**⑦ Reconciliation check:** R01 (242,800 ✅), R02 (51 + 3 = 54 ✅), R03 (20 ✅). Badha pass.

**⑧ Upsert `gold.customer_360_summary`** (20 rows)

Table pahodi che, etle ahiya **2 bhag ma** batavi che. Kharekhar aa **ek j table** che.

**Bhag A – Profile, Sales, Payment:**

| customer_code | customer_name | city | tier | total_orders | net_spend | last_order_date | paid_amount | refund_amount |
|---|---|---|---|---|---|---|---|---|
| C001 | Rahul Patel | Ahmedabad | Gold | 2 | 29000 | 2026-10-03 | 29000 | 2400 |
| C002 | Priya Shah | Surat | Silver | 1 | 2400 | 2026-10-01 | 2400 | 0 |
| C003 | Amit Mehta | Mumbai | Gold | 1 | 55000 | 2026-10-02 | 55000 | 0 |
| C004 | Neha Joshi | Pune | Bronze | 1 | 3500 | 2026-10-03 | 3500 | 0 |
| C005 | Karan Desai | Vadodara | Silver | 1 | 4900 | 2026-10-04 | 4900 | 0 |
| C006 | Sneha Iyer | Bengaluru | Gold | 1 | 2000 | 2026-10-05 | 2000 | 0 |
| C007 | Vikram Singh | Jaipur | Bronze | 1 | **0** (RETURNED) | 2026-10-05 | 0 | 3600 |
| C008 | Pooja Trivedi | Rajkot | Silver | 1 | 2600 | 2026-10-06 | 2600 | 0 |
| C009 | Arjun Nair | Kochi | Bronze | **0** | 0 | – | 0 | 0 |
| C010 | Riya Kapoor | Delhi | Gold | 1 | 80000 | 2026-10-07 | **0** (PENDING) | 0 |
| C011 | Mehul Shah | Ahmedabad | Bronze | 1 | 3000 | 2026-10-08 | 3000 | 0 |
| C012 | Kavya Reddy | Hyderabad | Silver | 1 | 12000 | 2026-10-08 | **12999** | 0 |
| C013 | Rohan Gupta | Lucknow | Bronze | 1 | 2800 | 2026-10-09 | 2800 | 0 |
| C014 | Isha Bhatt | Bhavnagar | Gold | 1 | 5300 | 2026-10-10 | 5300 | 0 |
| C015 | Siddharth Rao | Chennai | Silver | 1 | 4000 | 2026-10-11 | 4000 | 0 |
| C016 | Ananya Das | Kolkata | Bronze | 1 | 3400 | 2026-10-12 | 3400 | 0 |
| C017 | Harsh Vora | Surat | Silver | 1 | 3500 | 2026-10-13 | 3500 | 0 |
| C018 | Nidhi Agarwal | Indore | Bronze | 1 | **0** (CANCELLED) | 2026-10-14 | 0 (FAILED) | 0 |
| C019 | Yash Thakkar | Ahmedabad | Gold | 1 | 27000 | 2026-10-15 | 27000 | 0 |
| C020 | Diya Menon | Mumbai | Silver | 1 | 2400 | 2026-10-16 | 2400 | 0 |
| | | | **TOTAL** | **20** | **242,800** | | **163,799** | **6,000** |

**Bhag B – Service, Experience, Segment:**

| customer_code | total_tickets | open_tickets | avg_rating | late_deliveries | customer_segment | spend_rank |
|---|---|---|---|---|---|---|
| C001 | 3 | 0 | 4.5 | 0 | **VIP** | 3 |
| C002 | 3 | 0 | 4.0 | 0 | REGULAR | 15 |
| C003 | 3 | 2 | 3.0 | 1 | **AT_RISK** | 2 |
| C004 | 3 | 1 | 4.5 | 0 | REGULAR | 9 |
| C005 | 3 | 3 | – | 0 | **AT_RISK** | 7 |
| C006 | 3 | 1 | 3.0 | 0 | REGULAR | 17 |
| C007 | 2 | 2 | – | 0 | **AT_RISK** | 18 |
| C008 | 3 | 1 | 4.5 | 0 | REGULAR | 14 |
| C009 | 1 | 0 | – | 0 | NO_ORDER | 18 |
| C010 | 3 | 3 | – | 0 | **AT_RISK** | 1 |
| C011 | 2 | 0 | **2.5** | 0 | **AT_RISK** | 12 |
| C012 | 3 | 0 | 4.0 | 1 | REGULAR | 5 |
| C013 | 2 | 1 | 3.0 | 0 | REGULAR | 13 |
| C014 | 2 | 1 | 4.0 | 1 | REGULAR | 6 |
| C015 | 2 | 2 | – | 0 | **AT_RISK** | 8 |
| C016 | 3 | 2 | 4.0 | 1 | **AT_RISK** | 11 |
| C017 | 3 | 2 | 2.0 | 0 | **AT_RISK** | 9 |
| C018 | 2 | 0 | – | 0 | ZERO_SPEND | 18 |
| C019 | 2 | 0 | 4.5 | 0 | **VIP** | 4 |
| C020 | 3 | 3 | 4.0 | 0 | **AT_RISK** | 15 |
| **TOTAL** | **51** | **24** | | **4** | | |

> **Rank ma tie:** Neha ane Harsh banne 3500 che, etle banne rank 9, ane pachhi no rank 11. Vikram, Arjun ane Nidhi 0 che, etle traney rank 18.

**Segment wise:** AT_RISK 9, REGULAR 7, VIP 2, NO_ORDER 1, ZERO_SPEND 1 = 20.

**Aa ek table thi male evi insights:**
- **Riya (C010):** spend ma rank 1 (80,000), pan payment PENDING che ane 3 complaints khuli che. Sales team mate top priority.
- **Amit (C003):** rank 2. Laptop ni complaint (EM-003 OPEN, CL-002 ESCALATED), delivery late, ane rating 3.
- **Mehul (C011):** ek pan complaint khuli nathi, chhata AT_RISK che, kem ke avg rating 2.5 che (kettle, RV10). Job-4 ni insight sathe match thay che.
- **Kavya (C012):** paid 12,999 pan net_spend 12,000. Aa e j 999 no fark che je Job-2 ma P022 vali line reject thai tyare aavyo hato. Have e **customer level** par pan dekhay che.

**⑨ Watermark update:** `J5_CUSTOMER_360` → `2026-10-21 00:13` (Job-5 no run start).

### 9.7 Staging table vaprvu ke direct insert/update? (main sawal)

**Jawab: aapda case ma direct j saru che. EPAS ni andar ek SQL upsert vaparo, staging ni jarur nathi.**

Customer 360 mate 3 rasta che:

| Rit | Kevi rite | Aapda case ma |
|---|---|---|
| **A. Direct SQL upsert (EPAS ma)** | Ek j statement: `INSERT … SELECT (aggregate) … ON CONFLICT (customer_code) DO UPDATE`. Aggregate SELECT (CTE) ma j thay. | ✅ **Best** |
| B. Spark + `foreachPartition` (row by row upsert) | Spark Postgres mathi data vanche, aggregate kare, ane row by row pacho Postgres ma lakhe | ❌ Data DB ni bahar jay ane pacho aave. Koi fayado nathi, ane vacche fail thay to adhuro 360 dekhay. |
| C. Spark + staging table + SQL MERGE | Spark aggregate kari ne `stg_customer_360` ma bulk write kare. Pachhi ek transaction ma MERGE. | ✅ Fakt **moto scale** par (niche joie) |

**Direct (A) kem chale che?**
1. **Badha source tables pehlethi EPAS ma che.** Data ne DB ni bahar lai javani jarur j nathi. DB pote aggregate kari shake che.
2. **Ek SQL statement = ek transaction.** Badhi rows ek sathe update thay ke ek pan nahi. Dashboard ne adhuro 360 kyarey na dekhay. Staging no sauthi motto fayado (all-or-nothing, Section 12.2 reason 2) ahiya **mafat ma** male che.
3. **Upsert key `customer_code` che.** Same run 2 vaar chale to pan duplicate na bane, etle re-run safe che.
4. **Volume nanu che.** Full load ma 20 customers, ane incremental ma fakt 5 affected customers.

**Staging (C) kyare jaruri bane?**

| Situation | Kem staging? |
|---|---|
| 360 **Spark thi** banavvu pade (source Mongo ke data lake ma hoy, ke karodo fact rows nu aggregation DB mate bahu bharu hoy) | Spark ma thi 20M rows row by row upsert bahu dheemu che. Bulk write staging ma, pachhi **ek MERGE** fast ane atomic che. |
| **Roj raatre aakhu rebuild** karvu hoy | `customer_360_summary_new` banavo, pachhi **rename swap** karo. Dashboard ne ek pan second khali table na dekhay. (Aa pan staging no ek prakar che.) |
| Lakho customers no **rank** farithi ganvo hoy | Staging ma thi ek set-based UPDATE thay, row by row nahi. |

```
 Aapdo case (sources EPAS ma, nanu / madhyam volume):
     ✅ Direct: EPAS ma SQL upsert (fakt affected customers) + rank update
     ❌ Staging nahi, foreachPartition nahi

 Moto scale (20M customers, Spark thi):
     ✅ Spark → stg_customer_360 (bulk) → ek transaction ma MERGE + rank update
     ❌ foreachPartition row by row: dheemu, ane fail thay to adhuro 360

 Kyarey nahi:
     ❌ Spark JDBC mode("overwrite"): table drop/truncate thay, ane fail thay to dashboard khali
```

### 9.8 Incremental Run (04:00)

**Sauthi important niyam: "Recompute karo, umero (+=) nahi."**

```
 ❌ Khoti rit (delta umervo):
    PAY10 PENDING → SUCCESS aavyu → Riya.paid_amount += 80000
    Run fail thai ne farithi chale → += 80000 pharithi → 160,000 ❌
    O1015 SHIPPED → DELIVERED aavyu → total_orders += 1 → 2 ❌ (order to ek j che)

 ✅ Sachi rit (recompute):
    Kaya customers badlaya e shodho → e customers ni AAKHI row
    curated tables mathi farithi gano → upsert.
    2 vaar chale to pan same answer aave ✅
```

**Step 1 – Affected customers shodho** (curated tables ma `etl_updated_at` > 00:13 AND <= 04:12):

| Curated table | Badlayeli rows (Job 1–4 na 04:00 run ma) | customer_code |
|---|---|---|
| dim_customer | C003 (SCD2 navi key 21), C002 (phone), C023 (navo) | C003, C002, C023 |
| fact_sales | O1010 status, O1015 status, O1024 (navo) | C010, C015, C023 |
| fact_payment | PAY10, PAY23 (navo) | C010, C023 |
| support_ticket | EM-003, EM-021, CH-021, CL-021 | C003, C015, C023, C010 |
| fact_shipment | SH-015, SH-010 | C015, C010 |
| fact_product_review | RV25 | C015 |

→ **Affected: C002, C003, C010, C015, C023 = 5 customers.** Baki 15 customers ne adakvanu nahi.

> **Aa mate shart:** Job 1–4 na **badha curated tables ma `etl_updated_at` column** hovi joie, ane darek insert/update vakhte e set thavi joie. Ena vagar Job-5 ne khabar na pade ke kono data badlayo.

**Step 2 – SCD2 trap (Amit, C003):**
```
 dim_customer ma Amit ni have 2 rows che:
     key 3  → Mumbai, Gold,     is_current = N   (O1003 aa key par che)
     key 21 → Pune,  Platinum,  is_current = Y

 ❌ Khoti rit: fakt current key (21) na orders gano
      key 21 par koi order nathi → total_orders 0, net_spend 0 ❌

 ✅ Sachi rit: badhi keys (3, 21) → C003 par map, pachhi customer_code par group
      total_orders 1, net_spend 55000 ✅
      city / tier = current row mathi → Pune, Platinum ✅
```

**Step 3 – 5 affected rows recompute → upsert:**

| customer_code | Column | Pehla (00:13) | Pachhi (04:12) | Kem? |
|---|---|---|---|---|
| C003 | city / tier | Mumbai / Gold | **Pune / Platinum** | SCD2 current row |
| C003 | open_tickets | 2 | **1** | EM-003 RESOLVED thayu |
| C003 | customer_segment | AT_RISK | **VIP** | Open 1 thayu, ane net_spend 55,000 ≥ 25,000 |
| C010 | paid_amount | 0 | **80000** | PAY10 SUCCESS |
| C010 | total_tickets | 3 | **4** | CL-021 navi (RESOLVED, etle open 3 j rahi) |
| C015 | total_tickets | 2 | **3** | EM-021 navi (RESOLVED) |
| C015 | avg_rating | – | **4.0** | RV25 (rating 4) |
| C015 | late_deliveries | 0 | **1** | SH-015 delivered, 6 divas late |
| C002 | – | – | – | Phone badlayo, pan 360 ma phone column nathi. Values same rahi. |
| **C023** | **navi row** | – | Jay Parmar, Gandhinagar, Bronze, orders 1, net_spend 2600, last order 2026-10-21, paid 2600, tickets 1, open 1, rating –, late 0, **REGULAR** | **INSERT** |

- C010 nu segment AT_RISK j rahyu (open 3), ane C015 nu pan AT_RISK (open 2).
- O1010 SHIPPED ane O1015 DELIVERED thaya, pan banne pehla pan `net_spend` ma ganata hata. Etle net_spend badlayu nathi.

**Step 4 – Rank: affected na hoy eva customers pan badlay!**

Jay (2600) aavyo etle **6 bija customers** no rank badlayo, je affected list ma nathi:

| customer_code | net_spend | Rank pehla | Rank pachhi |
|---|---|---|---|
| C008 Pooja | 2600 | 14 | 14 (Jay sathe tie) |
| **C023 Jay** | 2600 | – | **14** |
| C002 Priya | 2400 | 15 | **16** |
| C020 Diya | 2400 | 15 | **16** |
| C006 Sneha | 2000 | 17 | **18** |
| C007, C009, C018 | 0 | 18 | **19** |

> **Shikhaman:** Rank, percentile, "top 10" jevi values **ek customer par nahi, badha customers par** depend kare che. Etle affected customers nu upsert thay pachhi, **badha rows no rank ek set-based UPDATE thi** farithi gano (sasto che, ek j column). Bijo rasto: rank table ma store j na karo, query ke view ma gano.

**Step 5 – Reconciliation (pachhi):**

| Check | Expected | 360 ma | Result |
|---|---|---|---|
| R01 net_spend | fact_sales 252,100 − cancelled/returned 6,700 = 245,400 | 245,400 | ✅ |
| R02 tickets | support_ticket 57 = 54 + UNKNOWN 3 | 54 | ✅ |
| R03 rows | current customers 21 | 21 | ✅ |

- **Paid total:** 163,799 + 80,000 + 2,600 = **246,399**.
- **Fark (net_spend − paid):** pehla **79,001** hato (Riya 80,000 pending − Kavya 999). Have fakt **−999** (Kavya) rahyo, kem ke Riya nu payment aavi gayu.

**Segment wise (pachhi):** AT_RISK 8, REGULAR 8, VIP 3, NO_ORDER 1, ZERO_SPEND 1 = 21.

**Step 6 – Watermark:** `J5_CUSTOMER_360` → `2026-10-21 04:12`.

### 9.9 Job-5 Summary

| | Full Load (00:00 run) | Incremental (04:00 run) |
|---|---|---|
| Source | 6 curated tables | 6 curated tables (`etl_updated_at` window) |
| Customers recompute | 20 (badha) | 5 (affected) |
| Inserted | 20 | 1 (C023) |
| Updated | – | 4 (C002, C003, C010, C015) + rank 6 rows |
| Rejected (`meta_rejected`) | 0 | 0 |
| Reconciliation | 242,800 / 51 + 3 = 54 / 20 | 245,400 / 54 + 3 = 57 / 21 |
| Load rit | Direct SQL upsert (EPAS), **staging nahi** | Same |

---

## 10. Full Load pachhi – meta_rejected, watermark, audit

### 10.1 `meta.meta_rejected` (33 rows – badha jobs)

| id | job | collection | source_key | reject_type | rule | reason | bad_value |
|---|---|---|---|---|---|---|---|
| 1 | J1 | products | P003 (10-05) | DUPLICATE | J1-D01 | Same product_code 2 vaar. Juno version chhodyo. | updated_at=2026-10-05, price=1900 |
| 2 | J1 | products | P021 | VALIDATION | J1-V01 | Category missing | category=null |
| 3 | J1 | products | P022 | CORRUPT | J1-C01 | Price number nathi | price="abc" |
| 4 | J1 | products | P023 | VALIDATION | J1-V02 | Price 0 thi moto hovo joie | price=-150 |
| 5 | J1 | products | P016 / WH-MUM | VALIDATION | J1-V03 | Stock qty negative (line) | qty=-5 |
| 6 | J2 | customers | C005 (09-01) | DUPLICATE | J2-D01 | Same customer_code 2 vaar. Juno chhodyo. | updated_at=2026-09-01 |
| 7 | J2 | customers | C021 | VALIDATION | J2-V02 | Email format khoto | email="test@@mail" |
| 8 | J2 | customers | C022 | VALIDATION | J2-V01 | Name missing | name=null |
| 9 | J2 | orders | O1005 (10-03) | DUPLICATE | J2-D02 | Same order_no 2 vaar. Juno chhodyo. | status=PLACED |
| 10 | J2 | orders | O1021 | FK_MISSING | J2-F01 | Customer dim_customer ma nathi | customer=C099 |
| 11 | J2 | orders | O1022 | CORRUPT | J2-C01 | Order date valid nathi | order_ts="2026-13-45" |
| 12 | J2 | orders | O1023 | VALIDATION | J2-V05 | Order ma items nathi | items=[] |
| 13 | J2 | orders | O1008 / line-2 | VALIDATION | J2-V06 | Quantity 0 (line) | P018, qty=0 |
| 14 | J2 | orders | O1012 / line-2 | FK_MISSING | J2-F02 | Product dim_product ma nathi (line) | product=P022 |
| 15 | J2 | payments | PAY05 (20:11) | DUPLICATE | J2-D03 | Gateway callback 2 vaar | updated_at=20:11 |
| 16 | J2 | payments | PAY21 | FK_MISSING | J2-F03 | Order curated ma nathi | order=O1021 |
| 17 | J2 | payments | PAY22 | CORRUPT | J2-C02 | Amount number nathi | amount="abc" |
| 18 | J3 | email_tickets | EM-007 (copy) | DUPLICATE | J3-D01 | Email 2 vaar process thayo | – |
| 19 | J3 | email_tickets | EM-015 | VALIDATION | J3-V02 | Spam sender | noreply@spam.xyz |
| 20 | J3 | email_tickets | EM-019 | VALIDATION | J3-V01 | Ticket time missing | received_at=null |
| 21 | J3 | chat_tickets | CH-005 (copy) | DUPLICATE | J3-D01 | Chat 2 vaar aavyo | – |
| 22 | J3 | chat_tickets | CH-011 | CORRUPT | J3-C01 | Time parse na thayo | startedAt="abc" |
| 23 | J3 | call_logs | CL-009 | VALIDATION | J3-V03 | Duration negative | duration=-30 |
| 24 | J3 | call_logs | CL-012 | CORRUPT | J3-C01 | Time parse na thayo | "2026/10/12 25:61" |
| 25 | J3 | call_logs | CL-016 | VALIDATION | J3-V04 | Phone ane reason banne nathi | phone=null, reason=null |
| 26 | J4 | shipments | SH-005 (10-04) | DUPLICATE | J4-D01 | Juno status version | status=IN_TRANSIT |
| 27 | J4 | shipments | SH-004 | BUSINESS_RULE | J4-V02 | Cancelled order no shipment | order=O1004 (CANCELLED) |
| 28 | J4 | shipments | SH-021 | FK_MISSING | J4-F01 | Order exist nathi | order=O1099 |
| 29 | J4 | shipments | SH-013B | VALIDATION | J4-V03 | Delivery dispatch pehla | dispatched 10-12, delivered 10-10 |
| 30 | J4 | reviews | RV07 (10-09) | DUPLICATE | J4-D02 | Review edit thayo, juno version | rating=2 |
| 31 | J4 | reviews | RV21 | BUSINESS_RULE | J4-V06 | Verified purchase nathi | O1006 ma P005 nathi |
| 32 | J4 | reviews | RV22 | VALIDATION | J4-V05 | Rating 1–5 bahar | rating=7 |
| 33 | J4 | reviews | RV24 | CORRUPT | J4-C01 | Rating number nathi | rating="five" |

(Badha rows ma `run_id` = `<JOB>_20261021_0000` ane `rejected_at` = 2026-10-21 00:0x che.)

> Job-5 na **0 rows** che, kem ke e clean curated data vanche che (Section 9.5).

**Reject type wise:**

| reject_type | Count |
|---|---|
| DUPLICATE | 8 |
| CORRUPT | 6 |
| VALIDATION | 13 |
| FK_MISSING | 4 |
| BUSINESS_RULE | 2 |
| **Total** | **33** |

> **FK_MISSING ne pharithi try (retry):** Kyarek customer moda aave che ane order pehla aave che (late arriving data). Aava rejects ne aagla run ma farithi check kari shakay. Customer aavi jay to load thay.

### 10.2 `meta.etl_watermark` (Full Load pachhi)

| job_name | source_collection | watermark_column | last_watermark | last_run_id | last_status |
|---|---|---|---|---|---|
| J1_PRODUCT_INVENTORY | products | updated_at | 2026-10-21 00:00 | J1_20261021_0000 | SUCCESS |
| J2_SALES | customers | updated_at | 2026-10-21 00:00 | J2_20261021_0000 | SUCCESS |
| J2_SALES | orders | updated_at | 2026-10-21 00:00 | J2_20261021_0000 | SUCCESS |
| J2_SALES | payments | updated_at | 2026-10-21 00:00 | J2_20261021_0000 | SUCCESS |
| J3_SUPPORT_TICKETS | email_tickets | updated_at | 2026-10-21 00:00 | J3_20261021_0000 | SUCCESS |
| J3_SUPPORT_TICKETS | chat_tickets | **updatedAt** (UTC) | 2026-10-21 00:00 IST | J3_20261021_0000 | SUCCESS |
| J3_SUPPORT_TICKETS | call_logs | **logged_at** | 2026-10-21 00:00 | J3_20261021_0000 | SUCCESS |
| J4_FULFILMENT_REVIEWS | shipments | updated_at | 2026-10-21 00:00 | J4_20261021_0000 | SUCCESS |
| J4_FULFILMENT_REVIEWS | reviews | updated_at | 2026-10-21 00:00 | J4_20261021_0000 | SUCCESS |
| J5_CUSTOMER_360 | curated (6 tables) | **etl_updated_at** | 2026-10-21 00:13 | J5_20261021_0000 | SUCCESS |

> `orders` mate `J4` ni row **nathi**, kem ke Job-4 ma e lookup che (Section 8.2).

### 10.3 `meta.etl_run_audit` (Full Load)

| run_id | job | type | start | end | read | loaded | rejected | status |
|---|---|---|---|---|---|---|---|---|
| J1_20261021_0000 | J1 | FULL | 00:00 | 00:03 | 24 docs | dim_product 20, fact 33 | 5 | SUCCESS |
| J2_20261021_0000 | J2 | FULL | 00:03 | 00:09 | 70 docs | dim_customer 20, fact_sales 30, fact_payment 20 | 12 | SUCCESS |
| J3_20261021_0000 | J3 | FULL | 00:09 | 00:12 | 62 docs | support_ticket 54 | 8 | SUCCESS |
| J4_20261021_0000 | J4 | FULL | 00:09 | 00:13 | 48 docs | fact_shipment 20, fact_review 20 | 8 | SUCCESS |
| J5_20261021_0000 | J5 | FULL | 00:13 | 00:14 | 20 customers (6 curated tables) | customer_360_summary 20 | 0 | SUCCESS |

---

## 11. Incremental Run (04:00) – shu badlay che?

**Run-1:** 2026-10-21 04:00. **Window:** `updated_at > 2026-10-21 00:00 AND <= 2026-10-21 04:00`

### 11.1 Mongo ma 00:00 thi 04:00 vacche shu badlayu?

| Collection | Document | Shu thayu | updated_at |
|---|---|---|---|
| products | P003 | Price 2000 → **1800**, WH-AHM stock 100 → **95** | 01:10 |
| products | **P024** (navo) | Wireless Mouse, TechBook, Laptops, SUP-01, 800, AHM:50 | 00:45 |
| customers | C003 | City Mumbai → **Pune**, tier Gold → **Platinum** | 01:20 |
| customers | C002 | Phone → **9876511122** | 02:15 |
| customers | **C023** (navo) | Jay Parmar, jay.parmar@mail.com, 9876500023, Gandhinagar, Gujarat, Bronze | 00:30 |
| orders | **O1024** (navo) | C023, 01:30, PLACED, UPI, P024×1@800 + P003×1@1800 = 2600 | 01:30 |
| orders | O1015 | SHIPPED → **DELIVERED** | 02:05 |
| orders | O1010 | PLACED → **SHIPPED** | 02:40 |
| payments | PAY10 | PENDING → **SUCCESS** (paid 10-21) | 01:00 |
| payments | **PAY23** (navo) | O1024, UPI, 2600, SUCCESS | 01:31 |
| shipments | SH-015 | IN_TRANSIT → **DELIVERED** (10-21 01:50) | 02:00 |
| shipments | **SH-010** (navo) | O1010, SpeedEx, dispatched 10-21, promised 10-24, IN_TRANSIT | 02:30 |
| reviews | **RV25** (navo) | P019, C015, O1015, rating 4, "Nice watch, late delivery" | 03:10 |
| email_tickets | EM-003 | open → **closed** | 02:30 |
| email_tickets | **EM-021** (navo) | siddharth.rao@mail.com, "Watch finally delivered, thanks", O1015, closed, Low | 03:30 |
| chat_tickets | **CH-021** (navo) | C023, "first order tracking", startedAt **2026-10-20T21:20Z**, PENDING, Meena | 21:20Z |
| call_logs | **CL-021** (navo) | 9876500010, "laptop dispatch confirmation", 21/10/2026 03:00, Resolved, 100 sec, Asha | 03:00 |

### 11.2 Job-1 (Incremental)

**Read:** 2 docs (P003, P024)

**`dim_product` – SCD1 (upsert):**

| product_key | product_code | product_name | current_price | Shu thayu |
|---|---|---|---|---|
| 3 | P003 | Wireless Earbuds | 2000 → **1800** | **UPDATE** (e j row) |
| **21** | **P024** | Wireless Mouse | 800 | **INSERT** (brand 3 TechBook, category 7 Laptops, supplier 1) |

**`fact_inventory_snapshot` – upsert (snapshot_date_key = 20261021 same che, etle e j rows update):**

| product | warehouse | stock_qty | unit_price | stock_value | Shu thayu |
|---|---|---|---|---|---|
| 3 (P003) | 1 (AHM) | 100 → **95** | 2000 → **1800** | 200,000 → **171,000** | UPDATE |
| 3 (P003) | 2 (MUM) | 80 | 2000 → **1800** | 160,000 → **144,000** | UPDATE |
| 3 (P003) | 3 (DEL) | 50 | 2000 → **1800** | 100,000 → **90,000** | UPDATE |
| **21 (P024)** | 1 (AHM) | 50 | 800 | 40,000 | INSERT |

> Kale (20261022) no pehlo run navi snapshot rows banavshe, ane aajni rows **history** tarike rahi jashe.

### 11.3 Job-2 (Incremental)

**Read:** customers 3, orders 3, payments 2

**`dim_customer` – SCD2 ane SCD1:**

| customer_key | code | name | phone | city | tier | valid_from | valid_to | is_current | Shu thayu |
|---|---|---|---|---|---|---|---|---|---|
| 3 | C003 | Amit Mehta | 9876500003 | Mumbai | Gold | 2025-03-05 | **2026-10-21 01:20** | **N** | **juni version bandh** |
| **21** | C003 | Amit Mehta | 9876500003 | **Pune** | **Platinum** | **2026-10-21 01:20** | 9999-12-31 | Y | **navi version** (SCD2) |
| 2 | C002 | Priya Shah | **9876511122** | Surat | Silver | 2025-02-14 | 9999-12-31 | Y | phone **overwrite** (SCD1) |
| **22** | C023 | Jay Parmar | 9876500023 | Gandhinagar | Bronze | 2026-10-21 | 9999-12-31 | Y | **navo customer** |

**SCD2 no faydo:**
- Amit na **juna order (O1003)** `fact_sales` ma `customer_key = 3` sathe j rahe che, je **Mumbai, Gold** version che. Etle juni report sachi rahe che.
- Amit no **koi navo order** aavshe to ene `customer_key = 21` (Pune, Platinum) malshe.

**`fact_sales` – upsert:**

| sales_key | order_no | line | customer_key | product_key | order_status_key | qty | unit_price | amount | Shu thayu |
|---|---|---|---|---|---|---|---|---|---|
| 15 | O1010 | 1 | 10 | 1 | 1 → **2 (SHIPPED)** | 1 | 25000 | 25000 | UPDATE |
| 16 | O1010 | 2 | 10 | 5 | 1 → **2 (SHIPPED)** | 1 | 55000 | 55000 | UPDATE |
| 23 | O1015 | 1 | 15 | 19 | 2 → **3 (DELIVERED)** | 1 | 4000 | 4000 | UPDATE |
| **31** | **O1024** | 1 | **22 (Jay)** | **21 (P024)** | 1 (PLACED) | 1 | 800 | 800 | INSERT |
| **32** | **O1024** | 2 | **22 (Jay)** | 3 (P003) | 1 (PLACED) | 1 | 1800 | 1800 | INSERT |

> **Dependency nu example:** O1024 ma P024 che, je **Job-1 e aaj na run ma j** banavyo. Job-1 pehla na chale to aa line `FK_MISSING` thi reject thai jat. Ane C023 pan **Job-2 na customers part ma pehla** load thayo, pachhi orders part chalyo.

**`fact_payment` – upsert:**

| payment_key | payment_id | order_no | payment_date_key | customer_key | status | amount | Shu thayu |
|---|---|---|---|---|---|---|---|
| 10 | PAY10 | O1010 | 20261007 → **20261021** | 10 | PENDING → **SUCCESS** | 80000 | UPDATE |
| **21** | **PAY23** | O1024 | 20261021 | 22 (Jay) | SUCCESS | 2600 | INSERT |

### 11.4 Job-3 (Incremental)

**Timezone trap (important):**

```
 CH-021  updatedAt = 2026-10-20T21:20Z (UTC)

 ❌ Khoti rit: UTC value ne sidhi IST watermark sathe compare kari
    "2026-10-20 21:20"  >  "2026-10-21 00:00" ?  → NA → record CHHUTI JAY ❌

 ✅ Sachi rit: pehla IST ma convert kari ne compare karo
    2026-10-20T21:20Z = 2026-10-21 02:50 IST  >  00:00 ?  → HA → record aave ✅
```

**`support_ticket` – upsert:**

| ticket_key | channel | source_id | customer_code | issue_category | status | priority | created_at | Shu thayu |
|---|---|---|---|---|---|---|---|---|
| 3 | EMAIL | EM-003 | C003 | PRODUCT_QUALITY | OPEN → **RESOLVED** | HIGH | 2026-10-06 14:20 | UPDATE |
| **55** | EMAIL | EM-021 | C015 | DELIVERY | RESOLVED | LOW | 2026-10-21 03:30 | INSERT |
| **56** | CHAT | CH-021 | C023 | DELIVERY | IN_PROGRESS | MEDIUM | 2026-10-21 02:50 | INSERT |
| **57** | CALL | CL-021 | C010 | DELIVERY | RESOLVED | MEDIUM | 2026-10-21 03:00 | INSERT |

> CH-021 no customer C023 **Job-2 e aaj na run ma** load karyo, etle ahiya `UNKNOWN` nathi thayo. Aa pan dependency nu example che.

### 11.5 Job-4 (Incremental)

**Read:** shipments 2 (SH-015, SH-010), reviews 1 (RV25). Orders lookup: O1015, O1010.

**`fact_shipment` – upsert:**

| key | shipment_id | order_no | customer | courier | dispatch | promised | delivered | status | days | delay | on_time | Shu thayu |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 15 | SH-015 | O1015 | 15 | 1 (SpeedEx) | 20261012 | 20261015 | **20261021** | **DELIVERED** | **9** | **6** | **N** | UPDATE |
| **21** | SH-010 | O1010 | 10 | 1 (SpeedEx) | 20261021 | 20261024 | – | IN_TRANSIT | – | – | – | INSERT |

**`fact_product_review` – insert:**

| review_key | review_id | product_key | customer_key | order_no | review_date_key | rating | sentiment |
|---|---|---|---|---|---|---|---|
| **21** | RV25 | 19 (P019) | 15 (Siddharth) | O1015 | 20261021 | 4 | Positive |

> **Verified purchase:** RV25 aavyo tyare O1015 Mongo ma **DELIVERED** thai gayo hato (02:05). Lookup **current** order status jove che, etle verify thayo ✅.
> Job-4 e O1015 ne key thi vanchyo. Jo watermark thi vanchat ane O1015 window ma na hot, to review verify na that.

### 11.6 Incremental pachhi watermark ane audit

Job 1–4 ni 9 watermark rows: `last_watermark = 2026-10-21 04:00`, `last_run_id = <JOB>_20261021_0400`, `SUCCESS`.
Job-5 ni row: `last_watermark = 2026-10-21 04:12` (Job-5 no run start). Job-5 nu incremental detail Section 9.8 ma che.

| run_id | job | read | inserted | updated | rejected |
|---|---|---|---|---|---|
| J1_20261021_0400 | J1 | 2 | dim 1, fact 1 | dim 1, fact 3 | 0 |
| J2_20261021_0400 | J2 | 8 | dim 2, fact_sales 2, fact_payment 1 | dim 2 (C003 close, C002), fact_sales 3, fact_payment 1 | 0 |
| J3_20261021_0400 | J3 | 4 | 3 | 1 | 0 |
| J4_20261021_0400 | J4 | 3 | fact_shipment 1, fact_review 1 | fact_shipment 1 | 0 |
| J5_20261021_0400 | J5 | 5 customers | 360: 1 (C023) | 360: 4 + rank 6 rows | 0 |

### 11.7 Job fail thay to?

```
 04:00 Job-2 start → 04:03 customers loaded → 04:04 ❌ orders part ma error → job FAIL
        watermark update NATHI thayo (haju 00:00)

 08:00 aaglo run → window = 00:00 thi 08:00   (4 kalak nahi, 8 kalak)
        customers farithi aavshe → UPSERT thashe → duplicate NAHI ✅
        orders pehli vaar sacha load thashe ✅
```

Aa j kaaran che ke **watermark chhelle** update thay che ane **badhe upsert** che.

---

## 12. Staging table vagar – shakya che? Kyare staging jaruri?

### 12.1 Aapda 5 jobs mate: **staging vagar chale che ✅**

| Job | Direct kevi rite? | Staging vagar OK? |
|---|---|---|
| Job-1 | Dims: upsert. Fact: (date, product, warehouse) par upsert | ✅ Ha |
| Job-2 | dim_customer: SCD2 (juni row close + navi insert). Facts: business key par upsert | ✅ Ha (volume nanu hoy tyare) |
| Job-3 | (channel + source_id) par upsert | ✅ Ha |
| Job-4 | Facts: shipment_id / review_id par upsert | ✅ Ha |
| Job-5 | EPAS ma ek SQL upsert (`customer_code` par, fakt affected customers) + rank update | ✅ Ha (detail Section 9.7) |

**Shart (condition):**
1. Darek target table par **unique business key** (constraint) hovi joie: `product_code`, `(order_no, line_no)`, `shipment_id`…
2. Badhu **upsert** hovu joie, jethi re-run safe rahe.
3. Watermark **chhelle** update thavo joie.
4. Dims **pehla**, facts **pachhi**.

### 12.2 Staging kyare **jaruri** bane? – Reasons

| # | Situation | Staging vagar shu problem? | Aapda jobs ma kya laagu? |
|---|---|---|---|
| 1 | **Moto fact load** (karodo rows) | Row-by-row upsert bahu dheemo che. Staging ma bulk `COPY` kari ne ek SQL thi merge karvu ghanu fast che. | Job-2 `fact_sales`: jo 5M orders/hour vala scale par jav to |
| 2 | **All-or-nothing joie** (atomic) | Job vacche fail thay to adhuro data curated ma dekhay (jem ke 30 mathi 18 lines). Staging hoy to curated ma ek transaction thi j jay. | Live dashboard vala facts |
| 3 | **Mota SCD2** | Hajaro customers badlay to darek mate "close + insert" row by row thay. Staging thi set-based (ek vaar ma) thay. | `dim_customer` jo lakho changes aave |
| 4 | **Hard delete detect karvu** | Mongo ma document delete thay to `updated_at` thi khabar j na pade. Aakhi key list sathe compare karva staging snapshot joie. | Jo source delete kare (soft delete flag na hoy) |
| 5 | **Cross-batch dedupe** | Same key ghana Spark partitions ma aave to direct upsert ma lock / conflict thai shake. | Moto volume |
| 6 | **Audit / debug** | "Aa run ma kharekhar shu aavyu hatu?" e joi na shakay. | Strict audit jaruri hoy tyare |

### 12.3 Recommendation

```
 Shuruat (aapda 5 jobs, nanu/madhyam volume):
     ✅ Staging NAHI – direct upsert / SCD2
     ✅ meta_rejected + watermark + run_audit

 Pachhi jaruri lage tyare (fakt e table mate):
     ➕ Job-2 fact_sales mate staging   ← volume moto thay ke dashboard ne adhuro data na dekhavo joie
     ➕ dim_customer mate staging        ← SCD2 changes hajaro ma jay
```

> Staging **aakha project ma farajiyat nathi**. Je table ma upar na 6 mathi koi problem aave, fakt e table mate umero.

---

## 13. Star vs Snowflake – aapda jobs ma

| | Star (Job-2, Job-4) | Snowflake (Job-1) |
|---|---|---|
| Aakar | Fact vacche, dims sidha fact sathe | Dims ni pan dims (category → parent category) |
| Join | Ochha (fact → dim) | Vadhare (fact → product → category → parent) |
| Duplicate data dim ma | Thodo vadhare | Ochho (normalized) |
| Query speed | Vadhare fast | Thodi dheemi |
| Kyare saru? | BI dashboards, saral reports | Hierarchy vala data (category tree, location tree), master data ni maintenance |

**Aapda project ma:**

```
 Job-1 (Snowflake):  fact_inventory_snapshot → dim_product → dim_category → dim_category(parent)
                                                          → dim_brand
                                                          → dim_supplier

 Job-2 (Star):       fact_sales, fact_payment → dim_customer, dim_payment_method,
                                                dim_order_status, dim_date, dim_product

 Job-4 (Star + shared dims):  fact_shipment, fact_product_review → dim_courier,
                                                dim_customer (Job-2), dim_product (Job-1), dim_date

 Job-5 (Flat summary):  customer_360_summary ← dim_customer + badha facts
                         (aggregate kareli wide table, ema dim/fact nathi)

 Aakhu project:  ghana facts + shared dims = "Fact Constellation" (Galaxy schema)
```

**Important concepts je aapde vapariya:**

| Concept | Kya? |
|---|---|
| Conformed dimension | `dim_date`, `dim_customer`, `dim_product` – ghana jobs ane facts share kare |
| Role-playing dimension | `dim_date` fact_shipment ma 4 roles: order, dispatch, promised, delivered |
| Degenerate dimension | `order_no`, `shipment_id`, `review_id` – fact ma che, pan eni alag dim nathi |
| SCD Type 1 | `dim_product` (price overwrite), `dim_customer` (email, phone) |
| SCD Type 2 | `dim_customer` (city, tier history) |
| Periodic snapshot fact | `fact_inventory_snapshot` (darek divas no stock) |
| Transaction fact | `fact_sales`, `fact_payment`, `fact_product_review` |
| Accumulating-ish fact | `fact_shipment` (status ane dates samay sathe update thay) |

---

## 14. Sawal-Jawab

**Q1. Job-3 jevu "ghani collections → ek table" real production ma hoy?**
→ **Ha.** Same entity alag-alag source mathi aave (email/chat/call, website/app/store orders) tyare **UNION merge** curated ma bahu common che. Alag entities ne jodi ne wide table (Customer 360) banavvu pan real che, pan e mostly Gold layer ma thay che.

**Q2. Duplicate ne `meta_rejected` ma kem mukvu? E to fakt juni copy che.**
→ Duplicate vaaramvaar aave to **source system ma bug** che (jem ke gateway callback 2 vaar, email 2 vaar process). Count dekhay to source team ne fix karva kahi shakay.

**Q3. Sanitize ane Validate ma farak?**
→ **Sanitize** = data **sudharo** ane aagal mokalo (`" UPI "` → `UPI`). **Validate** = rule fail thay to **reject** karo (rating 7). Je sudhari shakay e sudharo, ane je na sudhare e reject karo.

**Q4. FK check fakt aa batch ma karvo ke aakha table ma?**
→ **Aakha curated table ma.** Incremental ma navo payment juna order no hoi sake, je aa batch ma nathi pan `fact_sales` ma pehlethi che.

**Q5. Ek collection 2 jobs vapare (`orders`) to watermark kevi rite?**
→ **Darek job no potano watermark** hoy che. Ane jo e job mate collection fakt lookup hoy (Job-4), to watermark nahi, **key thi** vancho.

**Q6. Line reject thay to aakho order reject karvo?**
→ Business par depend che. Aapde **fakt e line** reject kari (O1008, O1012) ane baki order load karyo. Pan e fark (O1012 ma 999) DQ report ma dekhavo joie. Jo business kahe ke "adhuro order na joie", to aakho order reject karo.

**Q7. Mongo ma document delete thay to?**
→ `updated_at` watermark thi delete **pakdatu nathi**. Ena 2 upay che:
- Source ma **soft delete** flag (`is_deleted = true` + `updated_at`)
- Kyarek kyarek **full key compare** (ema staging joie, Section 12)

**Q8. Full load ane incremental na rules alag che?**
→ **Na. Rules same che.** Fakt read nu filter alag che: full ma badhu, ane incremental ma window.

**Q9. Customer 360 Mongo mathi kem nathi banavta?**
→ Curated data **pehlethi clean** che (duplicate, corrupt, reject bahar). Mongo mathi banaviye to badha rules farithi lakhva pade, ane 360 na numbers fact tables sathe match na thay (Section 9.2).

**Q10. Incremental ma summary ma value umervi (+=) ke farithi ganvi?**
→ **Farithi ganvi (recompute).** Affected customers ni aakhi row curated tables mathi farithi gano. `+=` karo to re-run ma double thay, ane status badlay (PENDING → SUCCESS) tyare khotu thay (Section 9.8).

---

## 15. Next Step – Code Development mate Checklist

Aa document samjay pachhi code mate aa kram rakhishu:

```
 PHASE 1 – Setup
 [ ] MongoDB ma 9 collections + aa sample data insert (JSON files)
 [ ] PostgreSQL ma schema: curated, meta
 [ ] Tables + primary key + unique business key constraints
 [ ] dim_date generate (2020–2035)
 [ ] meta.etl_watermark, meta.meta_rejected, meta.etl_run_audit
 [ ] Badha curated tables ma `etl_updated_at` column (Job-5 mate jaruri)

 PHASE 2 – Common framework (badha jobs mate ek j vaar)
 [ ] Watermark read / update
 [ ] Mongo read (full / incremental window)
 [ ] Rule engine: Sanitize → Corrupt → Dedupe → Validate → FK
 [ ] Reject writer → meta_rejected
 [ ] Upsert writer (insert / upsert / SCD2)
 [ ] Run audit writer

 PHASE 3 – Jobs (aa kram ma)
 [ ] Job-1  J1_PRODUCT_INVENTORY   (snowflake, explode, line reject)
 [ ] Job-2  J2_SALES               (SCD2, multi-collection, FK lookups)
 [ ] Job-3  J3_SUPPORT_TICKETS     (union, mapping, timezone, soft rule)
 [ ] Job-4  J4_FULFILMENT_REVIEWS  (shared collection lookup, role-playing date)
 [ ] Job-5  J5_CUSTOMER_360        (curated → aggregate, SQL upsert, recompute, rank)

 PHASE 4 – Test (aa document na numbers sathe match)
 [ ] Full load: J1 = 20/33/5, J2 = 20/30/20/12, J3 = 54/8, J4 = 20/20/8, J5 = 20/0
 [ ] Incremental 04:00: section 11 ane 9.8 na badha before/after match
 [ ] Job-5 reconciliation: net_spend 242,800 → 245,400, tickets 54 → 57
 [ ] Re-run same window → koi duplicate nahi
 [ ] Fail vacche → aaglo run sachu recover kare

 PHASE 5 – Airflow DAG
 [ ] J1 → J2 → (J3, J4) → J5 dependency, darek 4 kalake
```

> **Test mate golden rule:** aa document na **expected numbers** (20 products, 30 sales lines, 33 rejects, 54 tickets…) j aapdo **test case** che. Code na output ane aa numbers match thay, to logic sacho che.

---

## 16. Concept Notes – SCD1 vs SCD2, Explode

### 16.1 SCD1 ane SCD2 etle shu?

**SCD** = Slowly Changing Dimension. Dimension table ma koi attribute (city, tier, phone) badlay, tyare juna data nu shu karvu e nakki karvani rit.

#### SCD1: juni value par lakhi devu (overwrite)
- Juni value **bhunsai jay** ane history **nathi rehti**.
- Jyare juni value no koi business arth na hoy tyare vaparay: phone, email, spelling sudharo.

Udaharan: C002 no phone badlayo.

| customer_key | customer_id | phone |
|---|---|---|
| 2 | C002 | ~~juno number~~ → **navo number** |

Row 1 j rahe che, ane e j row update thay che.

#### SCD2: navi row umervi (history rakhvi)
- Juni row **bandh** thay (`valid_to` ane `is_current = N`), ane **navi row** nava surrogate key sathe aave.
- Jyare "e vakhte shu hatu" puchhvanu hoy tyare vaparay: city, state, tier.

Udaharan: C003 Mumbai/Gold mathi Pune/Platinum thayo (04:00 vala run ma).

| customer_key | customer_id | city | tier | valid_from | valid_to | is_current |
|---|---|---|---|---|---|---|
| 3 | C003 | Mumbai | Gold | 2025-03-05 | 2026-10-21 01:20 | N |
| **21** | C003 | **Pune** | **Platinum** | 2026-10-21 01:20 | 9999-12-31 | Y |

#### SCD2 no fayado
- O1003 (10-02 no order) `fact_sales` ma **customer_key 3** sathe j rahe che.
- Etle report ma e sale **Mumbai/Gold** ma ganay, kem ke order vakhte customer tya hato.
- Have pachhi na nava orders key **21** (Pune/Platinum) sathe jodashe.
- SCD1 vaprya hot to juni sale pan Pune ma dekhat, je khotu che.

#### Tunk ma

| | SCD1 | SCD2 |
|---|---|---|
| Change par | UPDATE (overwrite) | Juni row close + navi row INSERT |
| History | Nathi rehti | Rahe che |
| Ek customer ni rows | 1 | Ghani (1 current) |
| Aapda `dim_customer` ma | email, phone | city, state, tier |

Ek j table ma banne bhega hoy e normal che. Kayo column SCD1 ane kayo SCD2, e business nakki kare che.

---

### 16.2 Transform – items explode etle shu?

**Explode** etle **ek document ni andar no array kholi ne, array ni darek item ne alag row banavvi**.

#### Kem jaruri che?
Mongo ma ek order nu ek j document hoy, ane ena badha products `items[]` array ma hoy. Pan `fact_sales` no **grain** "ek order ni ek product line" che, "ek order" nahi. Etle array ne rows ma todvo pade.

#### Udaharan: O1001 (Rahul, 2 products)

**Pehla (Mongo document, 1 row):**
```
{ order_id: "O1001", customer_id: "C001", payment: "UPI",
  items: [ {product_id: "P001", qty: 1, price: 25000},
           {product_id: "P003", qty: 2, price: 2000} ] }
```

**Explode pachhi (2 rows):**

| order_id | customer_id | payment | line_no | product_id | qty | price |
|---|---|---|---|---|---|---|
| O1001 | C001 | UPI | 1 | P001 | 1 | 25000 |
| O1001 | C001 | UPI | 2 | P003 | 2 | 2000 |

- Order level na fields (order_id, customer, payment) **darek row ma copy** thay che.
- `line_no` = array ma position (`posexplode` thi male). Aa `(order_id, line_no)` ne unique key banave che.

**Pachhi lookup + amount, etle `fact_sales` ma:**

| sales_key | order_id | line_no | customer_key | product_key | qty | unit_price | amount |
|---|---|---|---|---|---|---|---|
| 1 | O1001 | 1 | 1 (Rahul) | 1 (P001) | 1 | 25000 | 25000 |
| 2 | O1001 | 2 | 1 (Rahul) | 3 (P003) | 2 | 2000 | 4000 |

#### Yaad rakhvana muddao
- **1 document → N rows.** Aapda data ma 20 valid orders hata, explode pachhi `fact_sales` ma 30 rows thai.
- **Line level reject explode pachhi j thay.** Jem ke O1008 ni line-2 ma qty 0 che. Fakt e line `meta_rejected` ma jay, ane order ni baki lines load thay.
- **Khali array:** `explode` khali `items[]` vala order (O1023) ne chupchap kadhi nakhe che. Etle "items empty" no rule **explode pehla** check karvo, jethi ene reject reason sathe `meta_rejected` ma mukay.
- Job-1 ma pan aa j vaat che: `products.stock[]` explode thay, ane darek warehouse ni ek row `fact_inventory_snapshot` ma bane.

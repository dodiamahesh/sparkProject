# Raw → Curated ETL: MongoDB collections thi Dim & Fact tables (Spark)

> Aa document ma aapde samjishu ke **ETL job raw layer thi curated layer par data kem split kare che**,
> **Dim / Fact shu che**, ane **3 common cases** step-by-step example sathe.
>
> Example domain: **Online Shopping (E-commerce)** – badha ne samjay evo common scenario.

---

## Table of Contents

1. [Big Picture – Layers shu che?](#1-big-picture--layers-shu-che)
2. [Collection vs Table – farak shu?](#2-collection-vs-table--farak-shu)
3. [Fact ane Dimension – simple bhasha ma](#3-fact-ane-dimension--simple-bhasha-ma)
4. [Split kem kariye chiye? (Purpose & Benefits)](#4-split-kem-kariye-chiye-purpose--benefits)
5. [Sample Source Data – 4 MongoDB Collections](#5-sample-source-data--4-mongodb-collections)
6. [Case 1 – Single Collection → Multiple Dim + Fact](#6-case-1--single-collection--multiple-dim--fact)
7. [Case 2 – Multiple Collections → Multiple Dim + Fact](#7-case-2--multiple-collections--multiple-dim--fact)
8. [Case 3 – Multiple Collections → Single Table (Merge)](#8-case-3--multiple-collections--single-table-merge)
9. [3 Cases ni Comparison](#9-3-cases-ni-comparison)
10. [Source → Target Mapping Sheet](#10-source--target-mapping-sheet)
11. [Tips & Common Mistakes](#11-tips--common-mistakes)

---

## 1. Big Picture – Layers shu che?

```
 ┌──────────────┐      ┌──────────────────┐      ┌───────────────────┐      ┌──────────────────┐
 │   MongoDB    │      │    RAW LAYER     │      │  CURATED LAYER    │      │ CONSUMPTION/GOLD │
 │ (App no DB)  │ ───► │ (as-is copy)     │ ───► │ (clean + split)   │ ───► │ (reports, BI,    │
 │              │      │ raw_orders       │      │ dim_customer      │      │  ML, API)        │
 │ customers    │      │ raw_customers    │      │ dim_product       │      │ customer_360     │
 │ products     │      │ raw_products     │      │ dim_date          │      │ sales dashboard  │
 │ orders       │      │ raw_payments     │      │ fact_sales        │      │                  │
 │ payments     │      │                  │      │ fact_payment      │      │                  │
 └──────────────┘      └──────────────────┘      └───────────────────┘      └──────────────────┘
       Source              Bronze                     Silver                      Gold
```

| Layer | Shu hoy che? | Kem joie? |
|---|---|---|
| **Source (MongoDB)** | Application no live database. Nested JSON documents. | App mate fast che, pan analytics mate nahi. |
| **Raw (Bronze)** | MongoDB no data **jevo che tevo** copy (JSON / Parquet). Koi change nahi. | Backup, re-process kari sakay, audit. "Original proof". |
| **Curated (Silver)** | Clean, flatten, dedupe, type-cast, ane **Dim + Fact** ma split. | Analytics, BI tools, SQL queries mate ready. |
| **Gold / Consumption** | Business-specific aggregated / merged tables. | Dashboard, report, ML features. |

> **Aapdu ETL job** = Raw layer mathi data vanchi ne, Spark thi transform kari ne, Curated layer ma Dim/Fact tables lakhe che.

---

## 2. Collection vs Table – farak shu?

| | MongoDB **Collection** | Curated **Table** |
|---|---|---|
| Structure | Document (JSON), nested object + array | Rows & columns (flat) |
| Schema | Flexible – darek document alag hoi sake | Fixed schema |
| Design kona mate? | **Application** mate (ek screen = ek document) | **Analytics** mate (sum, count, group by) |
| Data repeat | Ha – customer nu naam darek order ma repeat thay | Na – customer ek j vaar `dim_customer` ma |
| Example | `orders` document ma customer, items[], address badhu ek saathe | `fact_sales` + `dim_customer` + `dim_product` alag |

**Simple example:**

MongoDB ma ek order document aavu dekhay:
```json
{ "order_id": "O1001", "customer": { "name": "Rahul Patel", "city": "Ahmedabad" },
  "items": [ { "product": "Smartphone Pro 5G", "qty": 1 }, { "product": "Wireless Earbuds", "qty": 2 } ] }
```
App mate perfect che – ek j read ma aakho order screen par aavi jay.
Pan jo puchiye **"Electronics category ma Gujarat ma ketla rupiya nu vechan thayu?"** – to darek document kholvo pade, array explode karvo pade, naam match karva pade… **slow ane error-prone**.

Etle aapde aa document ne **tables ma todi nakhiye chiye (split)**.

---

## 3. Fact ane Dimension – simple bhasha ma

Ek **dukan nu bill (receipt)** vicharo:

```
 ┌───────────────────────────────────────────┐
 │  Bill No: O1001        Date: 01-Oct-2026   │  ← KYARE (Date Dim)
 │  Customer: Rahul Patel, Ahmedabad          │  ← KON   (Customer Dim)
 │  Payment: UPI                              │  ← KEVI RITE (Payment Mode Dim)
 │-------------------------------------------│
 │  Item               Qty   Price   Amount   │  ← SHU   (Product Dim)
 │  Smartphone Pro 5G   1    25000   25000    │  ← Qty, Amount = NUMBERS (Fact)
 │  Wireless Earbuds    2     2000    4000    │
 │-------------------------------------------│
 │  TOTAL                            29000    │
 └───────────────────────────────────────────┘
```

| | **Dimension (Dim)** | **Fact** |
|---|---|---|
| Shu che? | **Description / context** – Kon? Shu? Kyare? Kya? Kevi rite? | **Event / Transaction + numbers** – Shu thayu ane ketlu? |
| Example columns | customer_name, city, product_name, category, day_name | quantity, unit_price, amount |
| Size | Nani (hajaro rows) | Moti (karodo rows) – roj vadhe |
| Change | Kyarek j badlay (customer city badle) | Navi rows add j thay (navo order) |
| Naming | `dim_customer`, `dim_product`, `dim_date` | `fact_sales`, `fact_payment` |

### Important words

| Word | Matlab | Example |
|---|---|---|
| **Natural / Business key** | Source system ni ID | `customer_id = C001` |
| **Surrogate key** | Curated layer ma aapde banavelo number (integer) | `customer_key = 1` |
| **Grain** | Fact table ni **ek row shu represent kare che** | `fact_sales` = 1 row per order per product |
| **Measure** | Fact ma numeric value je sum/avg thay | `line_amount`, `quantity` |
| **Degenerate dimension** | Fact ma rakheli ID jeno alag dim nathi | `order_id` in `fact_sales` |
| **Star schema** | Vacche fact, aaju-baaju dims (star jevu) | niche diagram |

```
                 dim_date
                    │
   dim_customer ── fact_sales ── dim_product
                    │
             dim_payment_mode
```

---

## 4. Split kem kariye chiye? (Purpose & Benefits)

| # | Benefit | Explanation (example sathe) |
|---|---|---|
| 1 | **Duplicate data ochho** | "Rahul Patel, Ahmedabad" 1000 orders ma repeat thay to 1000 vaar store. Dim ma **ek j vaar**. |
| 2 | **Update ek j jagyae** | Rahul Ahmedabad thi Surat shift thay → fakt `dim_customer` ni 1 row update. Fact ne touch nathi karvu. |
| 3 | **Fast queries** | Fact table ma fakt keys (int) + numbers → narrow table → Spark/SQL `GROUP BY` khub fast. |
| 4 | **BI tools friendly** | Power BI / Tableau / Superset star schema ne directly samje che (drag-drop). |
| 5 | **Reusability** | Ek `dim_customer` ne `fact_sales`, `fact_payment`, `fact_returns` badha use kare. |
| 6 | **Clear grain = sacha numbers** | Nested array ma double counting thay; flat fact ma ek row = ek line item, so sum sacho. |
| 7 | **History tracking (SCD)** | Customer nu tier Silver → Gold thayu, to juni history pan rakhi sakay (SCD Type 2). |
| 8 | **Data quality** | Darek table par alag check: `dim_product` ma product_id unique? fact ma amount > 0? |
| 9 | **Source change thi bachav** | Mongo document structure badle to fakt ETL badlay, reports nahi. |
| 10 | **Security** | Customer PII (email, phone) `dim_customer` ma – fakt e table par access control. |

> **Purpose ek line ma:** *Application mate banavelo data ne Analytics mate ready karvo – sacho, fast ane samjay evo.*

---

## 5. Sample Source Data – 4 MongoDB Collections

Database: `shop` → Raw layer ma `shop_raw`

### Relation Diagram

```
 ┌───────────────┐         ┌────────────────────┐         ┌────────────────┐
 │  customers    │ 1     N │      orders        │ N     N │   products     │
 │ customer_id ◄─┼─────────┤ customer.customer_id│        │ product_id     │
 │ name, email   │         │ order_id           │─────────►│ name, category │
 │ address{}     │         │ items[] ───────────┼─────────┤ (items[].product_id)
 │ tier          │         │ payment_mode       │         │ brand, price   │
 └───────────────┘         │ status             │         └────────────────┘
                           └─────────┬──────────┘
                                     │ 1
                                     │
                                     │ 1
                           ┌─────────▼──────────┐
                           │     payments       │
                           │ payment_id         │
                           │ order_id           │
                           │ amount, status     │
                           └────────────────────┘
```

- Ek **customer** na ghana **orders** (1 : N)
- Ek **order** ma ghana **products** (items array) (N : N)
- Ek **order** nu ek **payment** (1 : 1)

---

### 5.1 Collection: `customers` (10 documents)

Ek full document:
```json
{
  "_id": "C001",
  "customer_id": "C001",
  "name": "Rahul Patel",
  "email": "rahul.patel@mail.com",
  "phones": ["9876500001", "9876500011"],
  "address": { "city": "Ahmedabad", "state": "Gujarat", "pincode": "380001" },
  "tier": "Gold",
  "signup_date": "2025-01-10"
}
```

Badha 10 (readable table form):

| customer_id | name | email | city | state | tier | signup_date |
|---|---|---|---|---|---|---|
| C001 | Rahul Patel | rahul.patel@mail.com | Ahmedabad | Gujarat | Gold | 2025-01-10 |
| C002 | Priya Shah | priya.shah@mail.com | Surat | Gujarat | Silver | 2025-02-14 |
| C003 | Amit Mehta | amit.mehta@mail.com | Mumbai | Maharashtra | Gold | 2025-03-05 |
| C004 | Neha Joshi | neha.joshi@mail.com | Pune | Maharashtra | Bronze | 2025-04-20 |
| C005 | Karan Desai | karan.desai@mail.com | Vadodara | Gujarat | Silver | 2025-05-11 |
| C006 | Sneha Iyer | sneha.iyer@mail.com | Bengaluru | Karnataka | Gold | 2025-06-01 |
| C007 | Vikram Singh | vikram.singh@mail.com | Jaipur | Rajasthan | Bronze | 2025-07-19 |
| C008 | Pooja Trivedi | pooja.trivedi@mail.com | Rajkot | Gujarat | Silver | 2025-08-08 |
| C009 | Arjun Nair | arjun.nair@mail.com | Kochi | Kerala | Bronze | 2025-09-25 |
| C010 | Riya Kapoor | riya.kapoor@mail.com | Delhi | Delhi | Gold | 2025-10-30 |

> Note: **C009 Arjun e haju sudhi koi order nathi karyo** – aa point Case 1 vs Case 2 ma important che.

---

### 5.2 Collection: `products` (10 documents)

Ek full document:
```json
{
  "_id": "P001",
  "product_id": "P001",
  "name": "Smartphone Pro 5G",
  "category": { "main": "Electronics", "sub": "Mobiles" },
  "brand": "Volt",
  "price": 25000,
  "is_active": true
}
```

| product_id | name | category.main | category.sub | brand | price |
|---|---|---|---|---|---|
| P001 | Smartphone Pro 5G | Electronics | Mobiles | Volt | 25000 |
| P002 | Wireless Earbuds | Electronics | Audio | SoundMax | 2000 |
| P003 | Laptop 14 inch | Electronics | Computers | TechBook | 55000 |
| P004 | Cotton T-Shirt | Fashion | Men | UrbanWear | 500 |
| P005 | Denim Jeans | Fashion | Men | DenimCo | 1500 |
| P006 | Printed Kurti | Fashion | Women | Ethnika | 800 |
| P007 | Mixer Grinder | Home & Kitchen | Appliances | KitchenPro | 3500 |
| P008 | Non-stick Tawa | Home & Kitchen | Cookware | CookEasy | 700 |
| P009 | Cricket Bat | Sports | Cricket | PlayStrong | 1800 |
| P010 | Yoga Mat | Sports | Fitness | FitLife | 600 |

---

### 5.3 Collection: `orders` (10 documents) – nested + array

Ek full document (aa sauthi important che):
```json
{
  "_id": "O1001",
  "order_id": "O1001",
  "order_date": "2026-10-01T10:12:00Z",
  "status": "DELIVERED",
  "payment_mode": "UPI",
  "customer": {                       // ← customer no SNAPSHOT (embedded)
    "customer_id": "C001",
    "name": "Rahul Patel",
    "city": "Ahmedabad"
  },
  "shipping_address": { "city": "Ahmedabad", "state": "Gujarat", "pincode": "380001" },
  "items": [                          // ← ARRAY: ek order ma ghana products
    { "product_id": "P001", "product_name": "Smartphone Pro 5G", "category": "Electronics", "qty": 1, "unit_price": 25000 },
    { "product_id": "P002", "product_name": "Wireless Earbuds",  "category": "Electronics", "qty": 2, "unit_price": 2000 }
  ],
  "order_total": 29000
}
```

Badha 10 orders (summary):

| order_id | order_date | customer | items (product × qty @ price) | order_total | status | payment_mode |
|---|---|---|---|---|---|---|
| O1001 | 2026-10-01 | C001 Rahul | P001×1 @25000, P002×2 @2000 | 29000 | DELIVERED | UPI |
| O1002 | 2026-10-01 | C002 Priya | P006×3 @800 | 2400 | DELIVERED | COD |
| O1003 | 2026-10-02 | C003 Amit | P003×1 @55000 | 55000 | DELIVERED | CARD |
| O1004 | 2026-10-03 | C001 Rahul | P009×1 @1800, P010×1 @600 | 2400 | CANCELLED | UPI |
| O1005 | 2026-10-03 | C004 Neha | P004×4 @500, P005×1 @1500 | 3500 | DELIVERED | UPI |
| O1006 | 2026-10-04 | C005 Karan | P007×1 @3500, P008×2 @700 | 4900 | SHIPPED | CARD |
| O1007 | 2026-10-05 | C006 Sneha | P002×1 @2000 | 2000 | DELIVERED | UPI |
| O1008 | 2026-10-05 | C007 Vikram | P009×2 @1800 | 3600 | RETURNED | COD |
| O1009 | 2026-10-06 | C008 Pooja | P006×2 @800, P004×2 @500 | 2600 | DELIVERED | UPI |
| O1010 | 2026-10-07 | C010 Riya | P001×1 @25000, P003×1 @55000 | 80000 | PLACED | CARD |

> 10 orders, pan **16 item lines** (array explode karya pachhi).

---

### 5.4 Collection: `payments` (10 documents)

```json
{
  "_id": "PAY5001",
  "payment_id": "PAY5001",
  "order_id": "O1001",
  "mode": "UPI",
  "amount": 29000,
  "status": "SUCCESS",
  "payment_date": "2026-10-01T10:15:00Z",
  "gateway_ref": "TXN88990011"
}
```

| payment_id | order_id | mode | amount | status | payment_date |
|---|---|---|---|---|---|
| PAY5001 | O1001 | UPI | 29000 | SUCCESS | 2026-10-01 |
| PAY5002 | O1002 | COD | 2400 | SUCCESS | 2026-10-04 |
| PAY5003 | O1003 | CARD | 55000 | SUCCESS | 2026-10-02 |
| PAY5004 | O1004 | UPI | 2400 | REFUNDED | 2026-10-03 |
| PAY5005 | O1005 | UPI | 3500 | SUCCESS | 2026-10-03 |
| PAY5006 | O1006 | CARD | 4900 | SUCCESS | 2026-10-04 |
| PAY5007 | O1007 | UPI | 2000 | SUCCESS | 2026-10-05 |
| PAY5008 | O1008 | COD | 3600 | REFUNDED | 2026-10-06 |
| PAY5009 | O1009 | UPI | 2600 | SUCCESS | 2026-10-06 |
| PAY5010 | O1010 | CARD | 80000 | PENDING | 2026-10-07 |

---

## 6. Case 1 – Single Collection → Multiple Dim + Fact

### Kyare aavu thay?
- Source ma **ek j collection** che jema **badhu embedded** che (Mongo ma bahu common).
- Example: `orders` document ma customer snapshot, items array, payment mode – badhu ek saathe.
- Biji collection ni access nathi / jarur nathi.

### Idea
```
                         raw_orders (1 collection)
                                   │
        ┌──────────────┬───────────┼─────────────┬────────────────┐
        ▼              ▼           ▼             ▼                ▼
  dim_customer    dim_product   dim_date   dim_payment_mode   fact_order_item
  (customer{})    (items[])     (order_date) (payment_mode)   (items[] explode)
                                                                   +
                                                              fact_order
                                                              (order level)
```

### Step-by-step

#### Step 1 – Raw collection read karo
```java
Dataset<Row> rawOrders = spark.read().format("mongodb")
        .option("connection.uri", mongoUri)
        .option("database", "shop_raw")
        .option("collection", "orders")
        .load();
```

#### Step 2 – Nested `customer{}` mathi `dim_customer` banavo
```java
Dataset<Row> dimCustomer = rawOrders
        .select(col("customer.customer_id").as("customer_id"),
                col("customer.name").as("customer_name"),
                col("customer.city").as("city"))
        .dropDuplicates("customer_id")                       // C001 2 vaar che → 1 j rakho
        .withColumn("customer_key", row_number().over(Window.orderBy("customer_id")));
```

**Output: `dim_customer` (9 rows)** – C009 nathi kem ke eno koi order nathi!

| customer_key | customer_id | customer_name | city |
|---|---|---|---|
| 1 | C001 | Rahul Patel | Ahmedabad |
| 2 | C002 | Priya Shah | Surat |
| 3 | C003 | Amit Mehta | Mumbai |
| 4 | C004 | Neha Joshi | Pune |
| 5 | C005 | Karan Desai | Vadodara |
| 6 | C006 | Sneha Iyer | Bengaluru |
| 7 | C007 | Vikram Singh | Jaipur |
| 8 | C008 | Pooja Trivedi | Rajkot |
| 9 | C010 | Riya Kapoor | Delhi |

> ⚠️ Limitation: email, state, tier – aa info `orders` ma nathi, etle Case 1 ma dim adhuro (incomplete) che.

#### Step 3 – `items[]` array explode karo (sauthi important step)
```java
Dataset<Row> orderItems = rawOrders
        .select(col("order_id"),
                to_date(col("order_date")).as("order_date"),
                col("customer.customer_id").as("customer_id"),
                col("payment_mode"),
                explode(col("items")).as("item"))            // 1 order → N rows
        .select(col("order_id"), col("order_date"), col("customer_id"), col("payment_mode"),
                col("item.product_id"), col("item.product_name"), col("item.category"),
                col("item.qty").as("quantity"), col("item.unit_price"));
```
O1001 (2 items) → 2 rows, O1002 (1 item) → 1 row … total **16 rows**.

#### Step 4 – `dim_product` banavo (exploded items mathi)
```java
Dataset<Row> dimProduct = orderItems
        .select("product_id", "product_name", "category")
        .dropDuplicates("product_id")
        .withColumn("product_key", row_number().over(Window.orderBy("product_id")));
```

**Output: `dim_product` (10 rows)**

| product_key | product_id | product_name | category |
|---|---|---|---|
| 1 | P001 | Smartphone Pro 5G | Electronics |
| 2 | P002 | Wireless Earbuds | Electronics |
| 3 | P003 | Laptop 14 inch | Electronics |
| 4 | P004 | Cotton T-Shirt | Fashion |
| 5 | P005 | Denim Jeans | Fashion |
| 6 | P006 | Printed Kurti | Fashion |
| 7 | P007 | Mixer Grinder | Home & Kitchen |
| 8 | P008 | Non-stick Tawa | Home & Kitchen |
| 9 | P009 | Cricket Bat | Sports |
| 10 | P010 | Yoga Mat | Sports |

> ⚠️ Limitation: brand, sub_category nathi (e `products` collection ma che).

#### Step 5 – `dim_date` banavo
```java
Dataset<Row> dimDate = orderItems.select("order_date").distinct()
        .withColumn("date_key", date_format(col("order_date"), "yyyyMMdd").cast("int"))
        .withColumn("day_name", date_format(col("order_date"), "EEEE"))
        .withColumn("month_name", date_format(col("order_date"), "MMMM"))
        .withColumn("is_weekend", dayofweek(col("order_date")).isin(1, 7));
```

**Output: `dim_date`**

| date_key | full_date | day_name | month_name | year | is_weekend |
|---|---|---|---|---|---|
| 20261001 | 2026-10-01 | Thursday | October | 2026 | false |
| 20261002 | 2026-10-02 | Friday | October | 2026 | false |
| 20261003 | 2026-10-03 | Saturday | October | 2026 | true |
| 20261004 | 2026-10-04 | Sunday | October | 2026 | true |
| 20261005 | 2026-10-05 | Monday | October | 2026 | false |
| 20261006 | 2026-10-06 | Tuesday | October | 2026 | false |
| 20261007 | 2026-10-07 | Wednesday | October | 2026 | false |

> Real project ma `dim_date` sauthi pehla ek vaar 10-20 varsh mate generate kari devay che.

#### Step 6 – `dim_payment_mode` (small static dim)

| payment_mode_key | payment_mode | payment_mode_desc | is_online |
|---|---|---|---|
| 1 | UPI | Unified Payments Interface | true |
| 2 | CARD | Credit / Debit Card | true |
| 3 | COD | Cash on Delivery | false |

#### Step 7 – `fact_order_item` banavo (natural key → surrogate key join)
```java
Dataset<Row> factOrderItem = orderItems
        .join(dimCustomer.select("customer_id", "customer_key"), "customer_id")
        .join(dimProduct.select("product_id", "product_key"), "product_id")
        .join(dimPaymentMode.select("payment_mode", "payment_mode_key"), "payment_mode")
        .withColumn("date_key", date_format(col("order_date"), "yyyyMMdd").cast("int"))
        .withColumn("line_amount", col("quantity").multiply(col("unit_price")))
        .select("order_id", "date_key", "customer_key", "product_key", "payment_mode_key",
                "quantity", "unit_price", "line_amount");
```

**Output: `fact_order_item`** — **Grain: 1 row = 1 product in 1 order** (16 rows)

| order_item_key | order_id | date_key | customer_key | product_key | payment_mode_key | quantity | unit_price | line_amount |
|---|---|---|---|---|---|---|---|---|
| 1 | O1001 | 20261001 | 1 (Rahul) | 1 (Smartphone) | 1 (UPI) | 1 | 25000 | 25000 |
| 2 | O1001 | 20261001 | 1 (Rahul) | 2 (Earbuds) | 1 (UPI) | 2 | 2000 | 4000 |
| 3 | O1002 | 20261001 | 2 (Priya) | 6 (Kurti) | 3 (COD) | 3 | 800 | 2400 |
| 4 | O1003 | 20261002 | 3 (Amit) | 3 (Laptop) | 2 (CARD) | 1 | 55000 | 55000 |
| 5 | O1004 | 20261003 | 1 (Rahul) | 9 (Cricket Bat) | 1 (UPI) | 1 | 1800 | 1800 |
| 6 | O1004 | 20261003 | 1 (Rahul) | 10 (Yoga Mat) | 1 (UPI) | 1 | 600 | 600 |
| 7 | O1005 | 20261003 | 4 (Neha) | 4 (T-Shirt) | 1 (UPI) | 4 | 500 | 2000 |
| 8 | O1005 | 20261003 | 4 (Neha) | 5 (Jeans) | 1 (UPI) | 1 | 1500 | 1500 |
| 9 | O1006 | 20261004 | 5 (Karan) | 7 (Mixer) | 2 (CARD) | 1 | 3500 | 3500 |
| 10 | O1006 | 20261004 | 5 (Karan) | 8 (Tawa) | 2 (CARD) | 2 | 700 | 1400 |
| 11 | O1007 | 20261005 | 6 (Sneha) | 2 (Earbuds) | 1 (UPI) | 1 | 2000 | 2000 |
| 12 | O1008 | 20261005 | 7 (Vikram) | 9 (Cricket Bat) | 3 (COD) | 2 | 1800 | 3600 |
| 13 | O1009 | 20261006 | 8 (Pooja) | 6 (Kurti) | 1 (UPI) | 2 | 800 | 1600 |
| 14 | O1009 | 20261006 | 8 (Pooja) | 4 (T-Shirt) | 1 (UPI) | 2 | 500 | 1000 |
| 15 | O1010 | 20261007 | 9 (Riya) | 1 (Smartphone) | 2 (CARD) | 1 | 25000 | 25000 |
| 16 | O1010 | 20261007 | 9 (Riya) | 3 (Laptop) | 2 (CARD) | 1 | 55000 | 55000 |

> ( ) ma lakhelu naam fakt samajva mate che – real table ma fakt number hoy.

#### Step 8 – `fact_order` (order header level)

**Grain: 1 row = 1 order** (10 rows)

| order_id | date_key | customer_key | payment_mode_key | order_status | item_count | total_qty | order_amount |
|---|---|---|---|---|---|---|---|
| O1001 | 20261001 | 1 | 1 | DELIVERED | 2 | 3 | 29000 |
| O1002 | 20261001 | 2 | 3 | DELIVERED | 1 | 3 | 2400 |
| O1003 | 20261002 | 3 | 2 | DELIVERED | 1 | 1 | 55000 |
| O1004 | 20261003 | 1 | 1 | CANCELLED | 2 | 2 | 2400 |
| O1005 | 20261003 | 4 | 1 | DELIVERED | 2 | 5 | 3500 |
| O1006 | 20261004 | 5 | 2 | SHIPPED | 2 | 3 | 4900 |
| O1007 | 20261005 | 6 | 1 | DELIVERED | 1 | 1 | 2000 |
| O1008 | 20261005 | 7 | 3 | RETURNED | 1 | 2 | 3600 |
| O1009 | 20261006 | 8 | 1 | DELIVERED | 2 | 4 | 2600 |
| O1010 | 20261007 | 9 | 2 | PLACED | 2 | 2 | 80000 |

#### Step 9 – Curated layer ma write karo
```java
dimCustomer.write().mode("overwrite").parquet(curatedPath + "/dim_customer");
factOrderItem.write().mode("append").partitionBy("date_key").parquet(curatedPath + "/fact_order_item");
```

### Case 1 – Summary

| ✅ Faydo | ❌ Limitation |
|---|---|
| Simple – ek j source, join ochha | Dim **adhuro** (email, state, tier, brand nathi) |
| Fast job | Je customer e order nathi karyo (C009) e dim ma nahi aave |
| Mongo ma embedded design hoy tyare perfect | Snapshot data – customer e city badli hoy to order ma juni city j hoy |

---

## 7. Case 2 – Multiple Collections → Multiple Dim + Fact

### Kyare aavu thay?
- Data **ghani collections ma vahechayelo** che (normalized Mongo design / microservices).
- **Master data** (customers, products) alag collection ma, **transaction data** (orders, payments) alag.
- Business ne complete info joie che: "**State-wise**, **Brand-wise**, **successful payment** wala sales".

### Idea
```
  raw_customers ─────────────────────────────► dim_customer   (10 rows, full detail)
  raw_products  ─────────────────────────────► dim_product    (10 rows, brand + sub_category)
  raw_orders    ──┬── order_date ────────────► dim_date
                  ├── payment_mode ──────────► dim_payment_mode
                  │
                  └── items[] explode ──┐
                                        ├── JOIN ─► fact_sales    (16 rows, + payment_status)
  raw_payments  ────────────────────────┘
                └────────────────────── JOIN ─► fact_payment  (10 rows)
```

**Rule:**
- **Dim** = master collection mathi (customers → dim_customer, products → dim_product)
- **Fact** = transaction collections mathi (orders + payments), ane dims sathe join kari ne **keys** muko

### Step-by-step

#### Step 1 – Badhi collections read karo
```java
Dataset<Row> rawCustomers = readMongo("customers");
Dataset<Row> rawProducts  = readMongo("products");
Dataset<Row> rawOrders    = readMongo("orders");
Dataset<Row> rawPayments  = readMongo("payments");
```

#### Step 2 – `customers` → `dim_customer` (nested `address{}` flatten)
```java
Dataset<Row> dimCustomer = rawCustomers
        .select(col("customer_id"),
                col("name").as("customer_name"),
                lower(trim(col("email"))).as("email"),
                col("phones").getItem(0).as("primary_phone"),   // array → first value
                col("address.city").as("city"),
                col("address.state").as("state"),
                col("tier").as("customer_tier"),
                to_date(col("signup_date")).as("signup_date"))
        .withColumn("customer_key", row_number().over(Window.orderBy("customer_id")));
```

**Output: `dim_customer` (10 rows – C009 pan aavi gayo ✅)**

| customer_key | customer_id | customer_name | email | city | state | customer_tier | signup_date |
|---|---|---|---|---|---|---|---|
| 1 | C001 | Rahul Patel | rahul.patel@mail.com | Ahmedabad | Gujarat | Gold | 2025-01-10 |
| 2 | C002 | Priya Shah | priya.shah@mail.com | Surat | Gujarat | Silver | 2025-02-14 |
| 3 | C003 | Amit Mehta | amit.mehta@mail.com | Mumbai | Maharashtra | Gold | 2025-03-05 |
| 4 | C004 | Neha Joshi | neha.joshi@mail.com | Pune | Maharashtra | Bronze | 2025-04-20 |
| 5 | C005 | Karan Desai | karan.desai@mail.com | Vadodara | Gujarat | Silver | 2025-05-11 |
| 6 | C006 | Sneha Iyer | sneha.iyer@mail.com | Bengaluru | Karnataka | Gold | 2025-06-01 |
| 7 | C007 | Vikram Singh | vikram.singh@mail.com | Jaipur | Rajasthan | Bronze | 2025-07-19 |
| 8 | C008 | Pooja Trivedi | pooja.trivedi@mail.com | Rajkot | Gujarat | Silver | 2025-08-08 |
| 9 | C009 | Arjun Nair | arjun.nair@mail.com | Kochi | Kerala | Bronze | 2025-09-25 |
| 10 | C010 | Riya Kapoor | riya.kapoor@mail.com | Delhi | Delhi | Gold | 2025-10-30 |

> Note: Case 1 ma Riya = key 9 hati, ahiya key 10 che. Etle j **fact ma hamesha dim sathe join kari ne key levi** – hard-code nahi.

#### Step 3 – `products` → `dim_product` (nested `category{}` flatten)

| product_key | product_id | product_name | category | sub_category | brand | current_price | is_active |
|---|---|---|---|---|---|---|---|
| 1 | P001 | Smartphone Pro 5G | Electronics | Mobiles | Volt | 25000 | true |
| 2 | P002 | Wireless Earbuds | Electronics | Audio | SoundMax | 2000 | true |
| 3 | P003 | Laptop 14 inch | Electronics | Computers | TechBook | 55000 | true |
| 4 | P004 | Cotton T-Shirt | Fashion | Men | UrbanWear | 500 | true |
| 5 | P005 | Denim Jeans | Fashion | Men | DenimCo | 1500 | true |
| 6 | P006 | Printed Kurti | Fashion | Women | Ethnika | 800 | true |
| 7 | P007 | Mixer Grinder | Home & Kitchen | Appliances | KitchenPro | 3500 | true |
| 8 | P008 | Non-stick Tawa | Home & Kitchen | Cookware | CookEasy | 700 | true |
| 9 | P009 | Cricket Bat | Sports | Cricket | PlayStrong | 1800 | true |
| 10 | P010 | Yoga Mat | Sports | Fitness | FitLife | 600 | true |

> `current_price` (dim) ≠ `unit_price` (fact). Dim ma **aaj no price**, fact ma **order vakhte no price**. Price badle to juna orders na numbers kharab na thay.

#### Step 4 – `dim_date`, `dim_payment_mode` – Case 1 jevu j (same tables reuse).

#### Step 5 – `orders` explode + `payments` join → `fact_sales`
```java
Dataset<Row> paymentStatus = rawPayments.select(col("order_id"), col("status").as("payment_status"));

Dataset<Row> factSales = orderItems                                // Case 1 Step 3 jevu explode
        .join(paymentStatus, "order_id")                          // ← biji collection sathe join
        .join(dimCustomer.select("customer_id", "customer_key"), "customer_id")
        .join(dimProduct.select("product_id", "product_key"), "product_id")
        .join(dimPaymentMode.select("payment_mode", "payment_mode_key"), "payment_mode")
        .withColumn("date_key", date_format(col("order_date"), "yyyyMMdd").cast("int"))
        .withColumn("gross_amount", col("quantity").multiply(col("unit_price")))
        .withColumn("net_amount", when(col("payment_status").equalTo("SUCCESS"), col("gross_amount")).otherwise(0))
        .select("order_id", "date_key", "customer_key", "product_key", "payment_mode_key",
                "payment_status", "quantity", "unit_price", "gross_amount", "net_amount");
```

**Output: `fact_sales`** — **Grain: 1 row = 1 product in 1 order** (16 rows)

| sales_key | order_id | date_key | customer_key | product_key | payment_mode_key | payment_status | quantity | unit_price | gross_amount | net_amount |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | O1001 | 20261001 | 1 | 1 | 1 | SUCCESS | 1 | 25000 | 25000 | 25000 |
| 2 | O1001 | 20261001 | 1 | 2 | 1 | SUCCESS | 2 | 2000 | 4000 | 4000 |
| 3 | O1002 | 20261001 | 2 | 6 | 3 | SUCCESS | 3 | 800 | 2400 | 2400 |
| 4 | O1003 | 20261002 | 3 | 3 | 2 | SUCCESS | 1 | 55000 | 55000 | 55000 |
| 5 | O1004 | 20261003 | 1 | 9 | 1 | REFUNDED | 1 | 1800 | 1800 | 0 |
| 6 | O1004 | 20261003 | 1 | 10 | 1 | REFUNDED | 1 | 600 | 600 | 0 |
| 7 | O1005 | 20261003 | 4 | 4 | 1 | SUCCESS | 4 | 500 | 2000 | 2000 |
| 8 | O1005 | 20261003 | 4 | 5 | 1 | SUCCESS | 1 | 1500 | 1500 | 1500 |
| 9 | O1006 | 20261004 | 5 | 7 | 2 | SUCCESS | 1 | 3500 | 3500 | 3500 |
| 10 | O1006 | 20261004 | 5 | 8 | 2 | SUCCESS | 2 | 700 | 1400 | 1400 |
| 11 | O1007 | 20261005 | 6 | 2 | 1 | SUCCESS | 1 | 2000 | 2000 | 2000 |
| 12 | O1008 | 20261005 | 7 | 9 | 3 | REFUNDED | 2 | 1800 | 3600 | 0 |
| 13 | O1009 | 20261006 | 8 | 6 | 1 | SUCCESS | 2 | 800 | 1600 | 1600 |
| 14 | O1009 | 20261006 | 8 | 4 | 1 | SUCCESS | 2 | 500 | 1000 | 1000 |
| 15 | O1010 | 20261007 | 10 | 1 | 2 | PENDING | 1 | 25000 | 25000 | 0 |
| 16 | O1010 | 20261007 | 10 | 3 | 2 | PENDING | 1 | 55000 | 55000 | 0 |
| | | | | | | **TOTAL** | | | **185400** | **99400** |

#### Step 6 – `payments` → `fact_payment`

**Grain: 1 row = 1 payment transaction** (10 rows)

| payment_id | order_id | date_key | customer_key | payment_mode_key | payment_status | amount |
|---|---|---|---|---|---|---|
| PAY5001 | O1001 | 20261001 | 1 | 1 | SUCCESS | 29000 |
| PAY5002 | O1002 | 20261004 | 2 | 3 | SUCCESS | 2400 |
| PAY5003 | O1003 | 20261002 | 3 | 2 | SUCCESS | 55000 |
| PAY5004 | O1004 | 20261003 | 1 | 1 | REFUNDED | 2400 |
| PAY5005 | O1005 | 20261003 | 4 | 1 | SUCCESS | 3500 |
| PAY5006 | O1006 | 20261004 | 5 | 2 | SUCCESS | 4900 |
| PAY5007 | O1007 | 20261005 | 6 | 1 | SUCCESS | 2000 |
| PAY5008 | O1008 | 20261006 | 7 | 3 | REFUNDED | 3600 |
| PAY5009 | O1009 | 20261006 | 8 | 1 | SUCCESS | 2600 |
| PAY5010 | O1010 | 20261007 | 10 | 2 | PENDING | 80000 |

> `customer_key` payments collection ma nathi – `order_id` thi orders sathe join kari ne lavya.

### Final Star Schema (Case 2)

```
                          dim_date
                             │
                             │
   dim_customer ─────── fact_sales ─────── dim_product
        │                    │
        │                    │
        └────────────── fact_payment ───── dim_payment_mode
                                                 │
                         (fact_sales pan) ───────┘

   → 2 facts SHARE same dims (conformed dimensions)
```

### Have business questions easily answer thay 🎯

**Q1. Category-wise net sales?**
```sql
SELECT p.category, SUM(f.net_amount) AS net_sales
FROM fact_sales f JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category;
```
| category | net_sales |
|---|---|
| Electronics | 86000 |
| Fashion | 8500 |
| Home & Kitchen | 4900 |
| Sports | 0 |

**Q2. State-wise net sales?** (👉 Case 1 ma **possible j nathi**, kem ke state `orders` ma customer snapshot ma nathi)
```sql
SELECT c.state, SUM(f.net_amount) AS net_sales
FROM fact_sales f JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.state;
```
| state | net_sales |
|---|---|
| Maharashtra | 58500 |
| Gujarat | 38900 |
| Karnataka | 2000 |
| Rajasthan | 0 |
| Delhi | 0 |

**Q3. Weekend vs Weekday sales?**
```sql
SELECT d.is_weekend, SUM(f.net_amount) FROM fact_sales f JOIN dim_date d USING (date_key) GROUP BY d.is_weekend;
```

### Case 2 – Summary

| ✅ Faydo | ❌ Dhyan rakhvanu |
|---|---|
| Complete dim (badhi details) | Ghana joins → job vadhare complex |
| Order na karyo hoy evo customer pan dim ma | Collections ni load timing alag hoy (late arriving data) |
| Ghana facts same dims share kare | Join key mismatch (`C001` vs `c001`) → clean karvu pade |
| Business na badha questions answer thay | Join na thay to row drop thay – `left join` + "Unknown" key vaparvi |

---

## 8. Case 3 – Multiple Collections → Single Table (Merge)

### Shu aa possible che? → **Ha, 100% possible ane bahu common che.** ✅

Pan dhyan raho: aa **star schema nathi**. Aa **denormalized / wide / "One Big Table (OBT)"** che.
Aa mostly **Gold / Consumption layer** ma banavay che (Curated na dim/fact mathi athva directly raw mathi).

### Merge 2 prakar na hoy

| Type | Shu kare? | Example |
|---|---|---|
| **A. JOIN merge (wide / horizontal)** | Alag-alag collections na **columns** ek row ma bhega kare | customers + orders + payments → `customer_360_summary` |
| **B. UNION merge (vertical)** | **Same structure** wali collections ni **rows** ek table ma bhegi kare | `orders_website` + `orders_mobile_app` + `orders_store` → `all_orders` |

### Kya scenario ma use thay?

| # | Scenario | Single table kem? |
|---|---|---|
| 1 | **Customer 360 view** | CRM / support team ne ek j row ma customer ni aakhi info joie |
| 2 | **ML feature table** | ML model ne ek flat table joie (1 row = 1 customer, ghana feature columns) |
| 3 | **Dashboard speed** | Dashboard par join vagar direct read → fast |
| 4 | **Export / API / Elasticsearch** | External system ne flat file / document joie |
| 5 | **Multi-source same entity** | Website, App, Store na orders alag collection ma – ek j `all_orders` ma |
| 6 | **Regulatory / Excel report** | Business user ne Excel ma ek j sheet joie |

---

### 8A. JOIN merge → `customer_360_summary`

**Sources:** `customers` + `orders` + `payments` (+ `products` for favourite category)
**Grain:** 1 row = 1 customer

```
 raw_customers ──┐
 raw_orders ─────┼──► JOIN + GROUP BY customer ──► customer_360_summary (1 table)
 raw_payments ───┤
 raw_products ───┘
```

```java
Dataset<Row> orderPay = rawOrders.select(col("order_id"), col("customer.customer_id").as("customer_id"),
                                         to_date(col("order_date")).as("order_date"))
        .join(rawPayments.select("order_id", "amount", "status"), "order_id");

Dataset<Row> agg = orderPay.groupBy("customer_id").agg(
        countDistinct("order_id").as("total_orders"),
        sum(when(col("status").equalTo("SUCCESS"), col("amount")).otherwise(0)).as("net_spent"),
        max("order_date").as("last_order_date"));

Dataset<Row> customer360 = dimCustomer                    // LEFT join → order vagar na customer pan aave
        .join(agg, "customer_id", "left")
        .join(favCategory, "customer_id", "left")
        .na().fill(0, new String[]{"total_orders", "net_spent"});
```

**Output: `customer_360_summary` (10 rows)**

| customer_id | customer_name | city | state | customer_tier | total_orders | net_spent | last_order_date | favourite_category | customer_status |
|---|---|---|---|---|---|---|---|---|---|
| C001 | Rahul Patel | Ahmedabad | Gujarat | Gold | 2 | 29000 | 2026-10-03 | Electronics | Active |
| C002 | Priya Shah | Surat | Gujarat | Silver | 1 | 2400 | 2026-10-01 | Fashion | Active |
| C003 | Amit Mehta | Mumbai | Maharashtra | Gold | 1 | 55000 | 2026-10-02 | Electronics | Active |
| C004 | Neha Joshi | Pune | Maharashtra | Bronze | 1 | 3500 | 2026-10-03 | Fashion | Active |
| C005 | Karan Desai | Vadodara | Gujarat | Silver | 1 | 4900 | 2026-10-04 | Home & Kitchen | Active |
| C006 | Sneha Iyer | Bengaluru | Karnataka | Gold | 1 | 2000 | 2026-10-05 | Electronics | Active |
| C007 | Vikram Singh | Jaipur | Rajasthan | Bronze | 1 | 0 | 2026-10-05 | Sports | Refunded Only |
| C008 | Pooja Trivedi | Rajkot | Gujarat | Silver | 1 | 2600 | 2026-10-06 | Fashion | Active |
| C009 | Arjun Nair | Kochi | Kerala | Bronze | 0 | 0 | null | null | Never Ordered |
| C010 | Riya Kapoor | Delhi | Delhi | Gold | 1 | 0 | 2026-10-07 | Electronics | Payment Pending |

> Have support team ek j `SELECT * FROM customer_360_summary WHERE customer_id='C001'` thi badhu joi sake – **koi join nahi**.
> Marketing team: "**Never Ordered** customers ne coupon moklo" → C009 Arjun.

---

### 8B. JOIN merge → `sales_order_details_flat` (wide table)

**Grain:** 1 row = 1 product in 1 order (16 rows) – pan badhi details ek j row ma.

| order_id | order_date | day_name | customer_name | city | state | customer_tier | product_name | category | brand | quantity | unit_price | gross_amount | payment_mode | payment_status | net_amount | order_status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| O1001 | 2026-10-01 | Thursday | Rahul Patel | Ahmedabad | Gujarat | Gold | Smartphone Pro 5G | Electronics | Volt | 1 | 25000 | 25000 | UPI | SUCCESS | 25000 | DELIVERED |
| O1001 | 2026-10-01 | Thursday | Rahul Patel | Ahmedabad | Gujarat | Gold | Wireless Earbuds | Electronics | SoundMax | 2 | 2000 | 4000 | UPI | SUCCESS | 4000 | DELIVERED |
| O1004 | 2026-10-03 | Saturday | Rahul Patel | Ahmedabad | Gujarat | Gold | Cricket Bat | Sports | PlayStrong | 1 | 1800 | 1800 | UPI | REFUNDED | 0 | CANCELLED |
| O1006 | 2026-10-04 | Sunday | Karan Desai | Vadodara | Gujarat | Silver | Mixer Grinder | Home & Kitchen | KitchenPro | 1 | 3500 | 3500 | CARD | SUCCESS | 3500 | SHIPPED |
| … | … | … | … | … | … | … | … | … | … | … | … | … | … | … | … | … |

> Joi shako cho – **"Rahul Patel, Ahmedabad, Gujarat, Gold" repeat thay che**. Aa j denormalization no trade-off che: **read fast, pan storage vadhare ane update muskel**.

---

### 8C. UNION merge → `all_orders` (same structure, alag source)

Ghani vaar company ma alag channel ni alag collection hoy:

**`orders_website`**
| order_id | customer_id | order_date | amount |
|---|---|---|---|
| W-501 | C001 | 2026-10-01 | 29000 |
| W-502 | C003 | 2026-10-02 | 55000 |

**`orders_mobile_app`**
| orderNo | custId | createdAt | totalAmt |
|---|---|---|---|
| A-901 | C006 | 2026-10-05 | 2000 |
| A-902 | C008 | 2026-10-06 | 2600 |

**`orders_store_pos`**
| bill_no | cust_code | bill_date | bill_amount |
|---|---|---|---|
| S-301 | C002 | 2026-10-01 | 2400 |

> Column naam alag che! Etle pehla **standard naam ma rename** karo, pachhi `unionByName`.

```java
Dataset<Row> web = readMongo("orders_website")
        .select(col("order_id"), col("customer_id"), col("order_date"), col("amount"))
        .withColumn("source_channel", lit("WEBSITE"));

Dataset<Row> app = readMongo("orders_mobile_app")
        .select(col("orderNo").as("order_id"), col("custId").as("customer_id"),
                col("createdAt").as("order_date"), col("totalAmt").as("amount"))
        .withColumn("source_channel", lit("MOBILE_APP"));

Dataset<Row> pos = readMongo("orders_store_pos")
        .select(col("bill_no").as("order_id"), col("cust_code").as("customer_id"),
                col("bill_date").as("order_date"), col("bill_amount").as("amount"))
        .withColumn("source_channel", lit("STORE"));

Dataset<Row> allOrders = web.unionByName(app).unionByName(pos);
```

**Output: `all_orders` (single table)**

| order_id | customer_id | order_date | amount | source_channel |
|---|---|---|---|---|
| W-501 | C001 | 2026-10-01 | 29000 | WEBSITE |
| W-502 | C003 | 2026-10-02 | 55000 | WEBSITE |
| A-901 | C006 | 2026-10-05 | 2000 | MOBILE_APP |
| A-902 | C008 | 2026-10-06 | 2600 | MOBILE_APP |
| S-301 | C002 | 2026-10-01 | 2400 | STORE |

> Have "**channel-wise sales**" ek j query ma. Aa table pachhi Case 2 ni jem `fact_order` banavvama pan use thai sake.

### Case 3 – Kyare **NA** karvu?

- Jyare dims vaaramvaar badlata hoy (customer city, tier) → wide table ma badhi rows update karvi pade.
- Jyare ghana alag-alag reports banavana hoy → star schema vadhare flexible.
- Jyare data khub moto hoy ane same text repeat thato hoy → storage kharcho.

> 👉 **Best practice:** Curated ma **Dim + Fact (Case 1/2)** banavo, ane pachhi **Gold ma enathi single table (Case 3)** banavo. Etle source of truth ek j rahe.

---

## 9. 3 Cases ni Comparison

| Point | Case 1: Single → Multi | Case 2: Multi → Multi | Case 3: Multi → Single |
|---|---|---|---|
| Source | 1 collection (embedded) | Ghani collections (normalized) | Ghani collections |
| Output | Ghana dim + fact | Ghana dim + fact | 1 wide / union table |
| Main Spark operation | `explode`, `dropDuplicates` | `join`, `explode`, key lookup | `join` + `groupBy` / `unionByName` |
| Model | Star schema | Star schema (conformed dims) | Denormalized / OBT |
| Layer | Curated (Silver) | Curated (Silver) | Mostly Gold / Consumption |
| Dim completeness | Partial (snapshot) | Full | Full (repeat thay) |
| Query speed | Fast (joins sathe) | Fast (joins sathe) | **Fastest** (join vagar) |
| Storage | Ochhu | Ochhu | Vadhare |
| Update ease | Saral | Saral | Muskel |
| Best for | Embedded Mongo docs | Enterprise DWH, BI | Dashboard, ML, CRM, Export |
| Our example | `orders` → `dim_customer`, `dim_product`, `dim_date`, `fact_order_item`, `fact_order` | 4 collections → `dim_*`, `fact_sales`, `fact_payment` | `customer_360_summary`, `sales_order_details_flat`, `all_orders` |

---

## 10. Source → Target Mapping Sheet

Aa sheet ETL developer ane tester banne mate **sauthi upyogi** che – kayo source field kai target column ma jay che.

### `dim_customer`
| Source Collection | Source Field | Target Table | Target Column | Rule / Transformation |
|---|---|---|---|---|
| customers | — | dim_customer | customer_key | `row_number()` (surrogate key) |
| customers | customer_id | dim_customer | customer_id | as-is (business key) |
| customers | name | dim_customer | customer_name | `trim`, Title Case |
| customers | email | dim_customer | email | `lower(trim())` |
| customers | phones[0] | dim_customer | primary_phone | array no first element |
| customers | address.city | dim_customer | city | nested flatten |
| customers | address.state | dim_customer | state | nested flatten |
| customers | tier | dim_customer | customer_tier | null hoy to `'Unknown'` |
| customers | signup_date | dim_customer | signup_date | `to_date()` |

### `dim_product`
| Source Collection | Source Field | Target Table | Target Column | Rule |
|---|---|---|---|---|
| products | — | dim_product | product_key | `row_number()` |
| products | product_id | dim_product | product_id | as-is |
| products | name | dim_product | product_name | trim |
| products | category.main | dim_product | category | nested flatten |
| products | category.sub | dim_product | sub_category | nested flatten |
| products | brand | dim_product | brand | as-is |
| products | price | dim_product | current_price | cast decimal(10,2) |
| products | is_active | dim_product | is_active | boolean |

### `dim_date`
| Source Collection | Source Field | Target Table | Target Column | Rule |
|---|---|---|---|---|
| orders | order_date | dim_date | date_key | `yyyyMMdd` int |
| orders | order_date | dim_date | full_date | `to_date()` |
| orders | order_date | dim_date | day_name / month_name / year | `date_format()` |
| orders | order_date | dim_date | is_weekend | `dayofweek in (1,7)` |

### `dim_payment_mode`
| Source Collection | Source Field | Target Table | Target Column | Rule |
|---|---|---|---|---|
| orders | payment_mode | dim_payment_mode | payment_mode | distinct values |
| — (static) | — | dim_payment_mode | payment_mode_desc, is_online | manual lookup |

### `fact_sales`
| Source Collection | Source Field | Target Table | Target Column | Rule |
|---|---|---|---|---|
| orders | order_id | fact_sales | order_id | degenerate dim |
| orders | order_date | fact_sales | date_key | lookup `dim_date` |
| orders | customer.customer_id | fact_sales | customer_key | lookup `dim_customer` |
| orders | items[].product_id | fact_sales | product_key | `explode` + lookup `dim_product` |
| orders | payment_mode | fact_sales | payment_mode_key | lookup `dim_payment_mode` |
| payments | status | fact_sales | payment_status | join on `order_id` |
| orders | items[].qty | fact_sales | quantity | `explode` |
| orders | items[].unit_price | fact_sales | unit_price | `explode` |
| — | — | fact_sales | gross_amount | `quantity * unit_price` |
| — | — | fact_sales | net_amount | `gross_amount` if SUCCESS else 0 |

### `fact_payment`
| Source Collection | Source Field | Target Table | Target Column | Rule |
|---|---|---|---|---|
| payments | payment_id | fact_payment | payment_id | as-is |
| payments | order_id | fact_payment | order_id | as-is |
| payments | payment_date | fact_payment | date_key | lookup `dim_date` |
| orders | customer.customer_id | fact_payment | customer_key | join orders on `order_id` → lookup `dim_customer` |
| payments | mode | fact_payment | payment_mode_key | lookup `dim_payment_mode` |
| payments | status | fact_payment | payment_status | as-is |
| payments | amount | fact_payment | amount | cast decimal |

### Table Naming Convention (human readable)

| Layer | Prefix | Example |
|---|---|---|
| Raw | `raw_` | `raw_orders`, `raw_customers` |
| Curated – Dimension | `dim_` | `dim_customer`, `dim_product`, `dim_date` |
| Curated – Fact | `fact_` | `fact_sales`, `fact_payment`, `fact_order_item` |
| Gold – Summary / Wide | `agg_` / `rpt_` / `_summary` / `_flat` | `customer_360_summary`, `sales_order_details_flat` |

> Rule: dim nu naam **singular noun** (`dim_customer`, not `dim_customers`), fact nu naam **business event/process** (`fact_sales`, `fact_payment`).

---

## 11. Tips & Common Mistakes

| # | Mistake | Saachi rit |
|---|---|---|
| 1 | Fact nu **grain** nakki karya vagar table banavvu | Pehla lakho: "1 row = 1 ___" |
| 2 | Array explode karya vagar `order_total` sum karvu ane pachhi item join karvu → **double counting** | Header fact (`fact_order`) ane line fact (`fact_order_item`) alag rakho |
| 3 | Fact ma `customer_name`, `city` jeva text columns rakhva | Fact ma fakt **keys + numbers** |
| 4 | Surrogate key hard-code karvi | Hamesha dim sathe **join kari ne lookup** |
| 5 | `inner join` → match na thay to row gayab | `left join` + `-1 / Unknown` default dim row |
| 6 | Dim ma current price, fact ma pan current price | Fact ma **transaction vakhte ni value** (unit_price) |
| 7 | Dim pehla load na karvo | **Order: Dims pehla → Facts pachhi** (Airflow DAG ma dependency) |
| 8 | Raw layer ma j transform karvu | Raw **as-is** rakho, transform curated ma |
| 9 | Customer ni history overwrite | Jarur hoy to **SCD Type 2** (`valid_from`, `valid_to`, `is_current`) |
| 10 | Join key format mismatch (`C001` vs `c001 `) | Join pehla `trim` + `upper` |

### Airflow DAG order (typical)

```
 extract_mongo_to_raw
          │
          ▼
 ┌────────┴─────────┬────────────────┬─────────────────┐
 load_dim_customer  load_dim_product  load_dim_date   load_dim_payment_mode
 └────────┬─────────┴────────────────┴─────────────────┘
          ▼
 ┌────────┴─────────┐
 load_fact_sales    load_fact_payment
 └────────┬─────────┘
          ▼
 build_customer_360_summary   (Case 3 – Gold)
```

---

## Ek line ma yaad rakho 🧠

- **Collection** = App mate (ek document ma badhu).
- **Dim** = Kon / Shu / Kyare / Kya – description.
- **Fact** = Shu thayu ane ketlu – numbers.
- **Case 1** = Ek embedded collection ne **todo** (explode + distinct).
- **Case 2** = Ghani collections thi **complete** dim/fact (join + lookup).
- **Case 3** = Ghani collections ne **bhegi** karo ek table ma (join-merge ya union-merge) – reporting/ML mate.

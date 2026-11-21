// Generates the e-commerce sample data (8 ecom_*.json files) for ecommerce-order-pipeline and
// ecommerce-fulfillment-pipeline. Fixed seed, so the output is always the same.
// Every planted bad record is marked with a comment (CORRUPT / VALIDATION / ORPHAN / DUPLICATE).
//
//   node scripts/generate-ecommerce-sample-data.js            (run from the project root)
const fs = require('fs');
const path = require('path');
const outDir = process.argv[2] || path.join(__dirname, '..', 'src', 'main', 'resources', 'sample-data');

let seed = 20261109;
const rnd = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };
const int = (a, b) => a + Math.floor(rnd() * (b - a + 1));
const pick = arr => arr[int(0, arr.length - 1)];
const pad = (n, w) => String(n).padStart(w, '0');
const money = x => Math.round(x * 100) / 100;
const iso = d => d.toISOString().slice(0, 19);
const day = d => d.toISOString().slice(0, 10);
const addMin = (d, m) => new Date(d.getTime() + m * 60000);
const addDays = (d, n) => new Date(d.getTime() + n * 86400000);
const randomTs = (from, to) => new Date(from.getTime() + Math.floor(rnd() * (to.getTime() - from.getTime())));
const N = 'N';

function write(name, docs) {
  const body = '[\n' + docs.map(d => '  ' + JSON.stringify(d)).join(',\n') + '\n]\n';
  fs.writeFileSync(path.join(outDir, name + '.json'), body);
  console.log(name.padEnd(20), docs.length, 'documents');
}

// ------------------------------------------------------------------ ecom_categories
const categoryDefs = [
  ['CAT-01', 'mobile phones', 'ELECTRONICS'], ['CAT-02', 'laptops', 'ELECTRONICS'],
  ['CAT-03', 'audio', 'ELECTRONICS'], ['CAT-04', "men's clothing", 'FASHION'],
  ['CAT-05', "women's clothing", 'FASHION'], ['CAT-06', 'kitchen appliances', 'HOME'],
  ['CAT-07', 'furniture', 'HOME'], ['CAT-08', 'skin care', 'BEAUTY'],
  ['CAT-09', 'fitness', 'SPORTS'], ['CAT-10', 'fiction books', 'BOOKS'],
];
const categories = categoryDefs.map(([id, name, dept], i) => ({
  categoryId: id, categoryName: i % 3 === 0 ? '  ' + name + ' ' : name,   // padded -> sanitizer trims
  department: i % 4 === 0 ? dept.toLowerCase() : dept,                    // lower -> sanitizer upper-cases
  updatedAt: '2026-01-0' + (1 + (i % 9)) + 'T09:00:00', processedFlag: N,
}));
categories.push({ categoryId: 'CAT-11', categoryName: 'Toys', department: 'TOYS',            // VALIDATION
  updatedAt: '2026-01-05T09:00:00', processedFlag: N });
categories.push({ categoryId: 'CAT-03', categoryName: 'Audio & Headphones', department: 'ELECTRONICS', // DUPLICATE (newer wins)
  updatedAt: '2026-02-01T09:00:00', processedFlag: N });
write('ecom_categories', categories);

// ------------------------------------------------------------------ ecom_products
const productNames = {
  'CAT-01': [['Samsung', 'Galaxy S24', 74999], ['Apple', 'iPhone 15', 79900], ['OnePlus', 'OnePlus 12R', 39999], ['Xiaomi', 'Redmi Note 13', 17999]],
  'CAT-02': [['Dell', 'Inspiron 15', 58990], ['HP', 'Pavilion 14', 64999], ['Lenovo', 'IdeaPad Slim 5', 71990], ['Apple', 'MacBook Air M3', 114900]],
  'CAT-03': [['boAt', 'Rockerz 450', 1499], ['Sony', 'WH-1000XM5', 29990], ['JBL', 'Flip 6 Speaker', 9999], ['Noise', 'Buds VS104', 1299]],
  'CAT-04': [['Levis', '511 Slim Jeans', 3299], ['Allen Solly', 'Formal Shirt', 1899], ['Puma', 'Polo T-Shirt', 1499], ['Raymond', 'Blazer', 6999]],
  'CAT-05': [['Biba', 'Anarkali Kurta', 2499], ['W', 'Straight Kurta', 1799], ['Zara', 'Floral Dress', 3990], ['Fabindia', 'Cotton Saree', 4500]],
  'CAT-06': [['Prestige', 'Induction Cooktop', 2899], ['Philips', 'Air Fryer', 8999], ['Bajaj', 'Mixer Grinder', 3499], ['Kent', 'RO Water Purifier', 15500]],
  'CAT-07': [['Wakefit', 'Orthopedic Mattress', 12999], ['Nilkamal', 'Plastic Chair', 899], ['IKEA', 'Study Table', 7990], ['Urban Ladder', 'Bookshelf', 9999]],
  'CAT-08': [['Mamaearth', 'Vitamin C Face Wash', 299], ['Cetaphil', 'Moisturising Cream', 849], ['Minimalist', 'Niacinamide Serum', 599], ['Lakme', 'Sunscreen SPF 50', 399]],
  'CAT-09': [['Decathlon', 'Yoga Mat', 999], ['Boldfit', 'Dumbbell Set 10kg', 2499], ['Nivia', 'Football', 799], ['Fitbit', 'Charge 6', 14999]],
  'CAT-10': [['Penguin', 'The White Tiger', 399], ['HarperCollins', 'The Alchemist', 299], ['Rupa', 'Five Point Someone', 199], ['Bloomsbury', 'Harry Potter Box Set', 3999]],
};
const products = [];
const productPrice = {};
let p = 0;
for (const catId of Object.keys(productNames)) {
  for (const [brand, name, price] of productNames[catId]) {
    p++;
    const id = 'PRD-' + pad(p, 4);
    productPrice[id] = price;
    const cost = money(price * (0.55 + rnd() * 0.25));
    products.push({
      productId: p % 7 === 0 ? id.toLowerCase() : id,            // lower -> sanitizer upper-cases
      sku: (brand.slice(0, 3) + '-' + pad(p, 4)).toUpperCase(),
      productName: name, categoryId: catId, brand,
      listPrice: price >= 10000 && p % 2 === 0 ? price.toLocaleString('en-IN') + '.00' : price,  // "1,14,900.00" -> sanitizer
      costPrice: cost,
      status: p % 13 === 0 ? 'OUT_OF_STOCK' : 'ACTIVE',
      launchDate: day(addDays(new Date('2023-01-01T00:00:00Z'), p * 23)),
      updatedAt: iso(addDays(new Date('2026-01-10T10:00:00Z'), p)), processedFlag: N,
    });
  }
}
const validProductIds = Object.keys(productPrice);
products.push({ productId: 'PRD-0041', sku: 'TOY-0041', productName: 'Lego City Set', categoryId: 'CAT-11', brand: 'Lego',   // ORPHAN (category rejected)
  listPrice: 4999, costPrice: 3000, status: 'ACTIVE', launchDate: '2025-06-01', updatedAt: '2026-02-01T10:00:00', processedFlag: N });
products.push({ productId: 'PRD-0042', sku: 'SAM-0042', productName: 'Galaxy Tab S9', categoryId: 'CAT-02', brand: 'Samsung',  // CORRUPT
  listPrice: '72,999abc', costPrice: 50000, status: 'ACTIVE', launchDate: '2025-08-01', updatedAt: '2026-02-01T10:00:00', processedFlag: N });
products.push({ productId: 'PRD-0043', sku: 'BOA-0043', productName: 'Airdopes 141', categoryId: 'CAT-03', brand: 'boAt',      // VALIDATION
  listPrice: 999, costPrice: 1500, status: 'ACTIVE', launchDate: '2025-03-01', updatedAt: '2026-02-01T10:00:00', processedFlag: N });
products.push({ productId: 'PRD-0044', sku: 'XYZ-0044', productName: 'Mystery Gadget', categoryId: 'CAT-99', brand: 'NoName',  // ORPHAN
  listPrice: 1999, costPrice: 999, status: 'LIMITED', launchDate: '2025-03-01', updatedAt: '2026-02-01T10:00:00', processedFlag: N });
products.push({ ...products[4], productId: 'PRD-0005', listPrice: 54990, updatedAt: '2026-03-01T10:00:00' });  // DUPLICATE: price drop wins
// ---- quality-check demo (valid documents) ----
for (const i of [0, 1, 2]) products[i].color = ['Black', 'Blue', 'Silver'][i];   // SCHEMA_DRIFT: new field "color"
products[19].brand = null;                                                          // WARNING: brand missing
products[29].sku = null;                                                            // WARNING: sku missing
products.push({ productId: 'PRD-9001', sku: 'TST-9001', productName: 'QA Test Product', categoryId: 'CAT-01',
  brand: 'QA', listPrice: 'not-a-price', costPrice: 1, status: 'ACTIVE', launchDate: '2025-01-01',
  updatedAt: '2026-02-01T10:00:00', isTest: true, processedFlag: N });              // FILTERED: isTest -> never read
write('ecom_products', products);

// ------------------------------------------------------------------ ecom_customers
const firstNames = ['Aarav', 'Vivaan', 'Diya', 'Ananya', 'Ishaan', 'Kavya', 'Rohan', 'Meera', 'Arjun', 'Priya',
  'Nikhil', 'Sneha', 'Harsh', 'Pooja', 'Karan', 'Riya', 'Dev', 'Nisha', 'Yash', 'Tanvi'];
const lastNames = ['Patel', 'Shah', 'Mehta', 'Desai', 'Joshi', 'Iyer', 'Reddy', 'Nair', 'Gupta', 'Sharma', 'Verma', 'Kulkarni'];
const cities = [['Ahmedabad', 'Gujarat', '380015'], ['Surat', 'Gujarat', '395007'], ['Mumbai', 'Maharashtra', '400053'],
  ['Pune', 'Maharashtra', '411014'], ['Bengaluru', 'Karnataka', '560034'], ['Chennai', 'Tamil Nadu', '600040'],
  ['Hyderabad', 'Telangana', '500081'], ['Delhi', 'Delhi', '110017'], ['Jaipur', 'Rajasthan', '302017'], ['Kolkata', 'West Bengal', '700091']];
const domains = ['gmail.com', 'yahoo.in', 'outlook.com', 'rediffmail.com'];
const customers = [];
const customerCity = {};
for (let c = 1; c <= 37; c++) {
  const id = 'ECU-' + pad(c, 4);
  const fn = firstNames[(c * 7) % firstNames.length];
  const ln = lastNames[(c * 5) % lastNames.length];
  const [city, state, pin] = cities[c % cities.length];
  customerCity[id] = [city, state];
  const phone = String(int(6, 9)) + pad(int(0, 999999999), 9);
  customers.push({
    customerId: id,
    firstName: c % 5 === 0 ? fn.toUpperCase() : fn, lastName: c % 6 === 0 ? ln.toLowerCase() : ln,   // sanitizer proper-cases
    email: (c % 4 === 0 ? ' ' : '') + `${fn}.${ln}${c}@${domains[c % domains.length]}`.replace(/^./, ch => c % 3 === 0 ? ch.toUpperCase() : ch.toLowerCase()),
    phone: c % 8 === 0 ? '+91 ' + phone.slice(0, 5) + '-' + phone.slice(5) : phone,                    // sanitizer normalises
    city, state, pincode: pin,
    segment: c % 9 === 0 ? 'VIP' : (c % 3 === 0 ? 'prime' : 'REGULAR'),
    signupDate: day(addDays(new Date('2023-06-01T00:00:00Z'), c * 17)),
    updatedAt: iso(addDays(new Date('2026-01-01T08:00:00Z'), c)), processedFlag: N,
  });
}
customers[35].email = 'tanvi.kulkarni@@gmail';            // ECU-0036 VALIDATION (bad email)
customers.push({ customerId: 'ECU-0038', firstName: 'Manav', lastName: 'Trivedi', email: 'manav.trivedi@gmail.com',
  phone: '9825012345', city: 'Vadodara', state: 'Gujarat', pincode: '390007', segment: 'GOLD',          // VALIDATION
  signupDate: '2025-02-10', updatedAt: '2026-01-20T08:00:00', processedFlag: N });
customers.push({ customerId: 'ECU-0039', firstName: 'Hetal', lastName: 'Parmar', email: 'hetal.parmar@yahoo.in',
  phone: '9898989898', city: 'Rajkot', state: 'Gujarat', pincode: '360005', segment: 'REGULAR',
  signupDate: '2025-13-45', updatedAt: '2026-01-20T08:00:00', processedFlag: N });                      // CORRUPT
customers.push({ ...customers[9], city: 'Gandhinagar', state: 'Gujarat', pincode: '382010',              // DUPLICATE ECU-0010 moved city
  updatedAt: '2026-03-15T08:00:00' });
customerCity['ECU-0010'] = ['Gandhinagar', 'Gujarat'];
customers[36].phone = null;                                                         // WARNING: ECU-0037 phone missing
customers[6].pincode = null;                                                        // WARNING: ECU-0007 pincode missing
write('ecom_customers', customers);
const validCustomerIds = Object.keys(customerCity).filter(id => id !== 'ECU-0036');

// ------------------------------------------------------------------ ecom_orders + ecom_order_items
const statuses = ['DELIVERED', 'DELIVERED', 'DELIVERED', 'DELIVERED', 'DELIVERED', 'SHIPPED', 'SHIPPED',
  'CONFIRMED', 'PLACED', 'CANCELLED', 'RETURNED'];
const paymentModes = ['UPI', 'UPI', 'CARD', 'COD', 'NETBANKING', 'WALLET'];
const channels = ['WEB', 'ANDROID', 'ANDROID', 'IOS'];
const coupons = [null, null, null, 'DIWALI10', 'FIRST50', 'FREESHIP'];
const orders = [];
const orderInfo = {};
for (let o = 1; o <= 75; o++) {
  const id = 'ORD-' + pad(o, 5);
  const customerId = validCustomerIds[(o * 11) % validCustomerIds.length];
  let status = statuses[(o * 3) % statuses.length];
  const orderTs = status === 'SHIPPED' || status === 'PLACED' || status === 'CONFIRMED'
    ? randomTs(new Date('2026-10-20T08:00:00Z'), new Date('2026-10-30T20:00:00Z'))   // recent -> not delivered yet
    : randomTs(new Date('2026-01-05T08:00:00Z'), new Date('2026-10-10T20:00:00Z'));
  const [city, state] = customerCity[customerId];
  const coupon = pick(coupons);
  orderInfo[id] = { status, orderTs, customerId };
  orders.push({
    orderId: id, customerId, orderDate: iso(orderTs),
    orderStatus: o % 10 === 0 ? status.toLowerCase() : status,
    paymentMode: pick(paymentModes), channel: o % 9 === 0 ? 'android' : pick(channels),
    couponCode: coupon, shippingFee: coupon === 'FREESHIP' ? 0 : pick([0, 40, 49, 99]),
    shippingCity: city, shippingState: state,
    updatedAt: iso(addMin(orderTs, 5)), processedFlag: N,
  });
}
const badOrders = [
  { orderId: 'ORD-00076', customerId: 'ECU-0036', note: 'ORPHAN (customer rejected)' },
  { orderId: 'ORD-00077', customerId: 'ECU-9999', note: 'ORPHAN (unknown customer)' },
  { orderId: 'ORD-00078', customerId: 'ECU-0002', orderDate: '2027-01-15T10:00:00', note: 'VALIDATION (future date)' },
  { orderId: 'ORD-00079', customerId: 'ECU-0003', paymentMode: 'BITCOIN', note: 'VALIDATION (payment mode)' },
  { orderId: 'ORD-00080', customerId: 'ECU-0004', shippingFee: 'forty', note: 'CORRUPT (shipping fee)' },
];
for (const b of badOrders) {
  orders.push({ orderId: b.orderId, customerId: b.customerId, orderDate: b.orderDate || '2026-05-10T11:30:00',
    orderStatus: 'DELIVERED', paymentMode: b.paymentMode || 'UPI', channel: 'WEB', couponCode: null,
    shippingFee: b.shippingFee !== undefined ? b.shippingFee : 40, shippingCity: 'Mumbai', shippingState: 'Maharashtra',
    updatedAt: '2026-05-10T11:35:00', processedFlag: N });
}
// DUPLICATE: ORD-00012 first arrives as SHIPPED, later again as DELIVERED -> the newer document wins
const o12 = orders[11];
const ts12 = new Date('2026-09-01T10:00:00Z');
orderInfo['ORD-00012'].orderTs = ts12;
orderInfo['ORD-00012'].status = 'DELIVERED';
Object.assign(o12, { orderStatus: 'SHIPPED', orderDate: iso(ts12), updatedAt: iso(addMin(ts12, 5)) });
orders.push({ ...o12, orderStatus: 'DELIVERED', updatedAt: iso(addDays(ts12, 6)) });
orders[19].couponCode = 'SUMMER99';                                                // WARNING: unknown coupon (ORD-00020)
for (const t of [1, 2]) {                                                           // FILTERED: test orders from QA
  orders.push({ orderId: 'ORD-9000' + t, customerId: 'ECU-TEST', orderDate: '2026-06-01T10:00:00', orderStatus: 'PLACED',
    paymentMode: 'UPI', channel: 'WEB', couponCode: null, shippingFee: 0, shippingCity: 'Test', shippingState: 'Test',
    updatedAt: '2026-06-01T10:05:00', isTest: true, processedFlag: N });
}
write('ecom_orders', orders);

const items = [];
let it = 0;
const nextItemId = () => 'OIT-' + pad(++it, 6);
for (const orderId of Object.keys(orderInfo).concat(badOrders.map(b => b.orderId))) {
  const lines = int(1, 4);
  const used = new Set();
  for (let l = 0; l < lines; l++) {
    let productId = pick(validProductIds);
    while (used.has(productId)) productId = pick(validProductIds);
    used.add(productId);
    const price = productPrice[productId];
    const qty = price > 20000 ? 1 : int(1, 3);
    const discount = rnd() < 0.35 ? money(price * qty * pick([0.05, 0.1, 0.15])) : (rnd() < 0.5 ? null : 0);
    const ts = orderInfo[orderId] ? orderInfo[orderId].orderTs : new Date('2026-05-10T11:30:00Z');
    items.push({ orderItemId: nextItemId(), orderId, productId, quantity: qty,
      unitPrice: price >= 10000 && it % 3 === 0 ? price.toLocaleString('en-IN') : price,
      discountAmount: discount, updatedAt: iso(addMin(ts, 5)), processedFlag: N });
  }
}
const itemTemplate = { orderId: 'ORD-00001', updatedAt: iso(addMin(orderInfo['ORD-00001'].orderTs, 6)), processedFlag: N };
items.push({ ...itemTemplate, orderItemId: nextItemId(), productId: 'PRD-0042', quantity: 1, unitPrice: 72999, discountAmount: 0 });   // ORPHAN (product rejected)
items.push({ ...itemTemplate, orderItemId: nextItemId(), productId: 'PRD-0010', quantity: 0, unitPrice: 1299, discountAmount: 0 });    // VALIDATION qty 0
items.push({ ...itemTemplate, orderItemId: nextItemId(), productId: 'PRD-0011', quantity: 'two', unitPrice: 9999, discountAmount: 0 }); // CORRUPT
items.push({ ...itemTemplate, orderItemId: nextItemId(), productId: 'PRD-0012', quantity: 1, unitPrice: 1299, discountAmount: 2000 });  // VALIDATION discount
items.push({ ...items[2], quantity: (items[2].quantity || 1) + 1, updatedAt: iso(addDays(new Date(items[2].updatedAt + 'Z'), 1)) });  // DUPLICATE (qty changed)
{                                                                                   // WARNING: 40% discount (valid, but review)
  const big = items.find(x => x.orderId === 'ORD-00003');
  big.discountAmount = money(Number(String(big.unitPrice).replace(/,/g, '')) * big.quantity * 0.4);
}
items.push({ orderItemId: 'OIT-900001', orderId: 'ORD-90001', productId: 'PRD-0001', quantity: 1, unitPrice: 74999,
  discountAmount: 0, updatedAt: '2026-06-01T10:05:00', isTest: true, processedFlag: N });   // FILTERED: test order line
write('ecom_order_items', items);

// ------------------------------------------------------------------ ecom_warehouses
const warehouses = [
  ['WH-01', 'Ahmedabad Fulfilment Centre', 'ahmedabad', 'gujarat', '382210', 50000],
  ['WH-02', 'Bhiwandi Mega Warehouse', 'bhiwandi', 'maharashtra', '421302', 120000],
  ['WH-03', 'Bengaluru Hub', 'bengaluru', 'karnataka', '562149', '80,000'],
  ['WH-04', 'Gurugram Fulfilment Centre', 'gurugram', 'haryana', '122413', 95000],
  ['WH-05', 'Chennai Sortation Centre', 'chennai', 'tamil nadu', '600056', 40000],
  ['WH-06', 'Kolkata Hub', 'kolkata', 'west bengal', '711302', 35000],
].map(([id, name, city, state, pin, cap], i) => ({ warehouseId: id, warehouseName: name, city, state, pincode: pin,
  capacityUnits: cap, status: i === 5 ? 'inactive' : 'ACTIVE', updatedAt: '2026-01-02T10:00:00', processedFlag: N }));
warehouses.push({ warehouseId: 'WH-07', warehouseName: 'Jaipur Mini Hub', city: 'Jaipur', state: 'Rajasthan', pincode: '302013',
  capacityUnits: 'lots', status: 'ACTIVE', updatedAt: '2026-01-02T10:00:00', processedFlag: N });       // CORRUPT
write('ecom_warehouses', warehouses);

// ------------------------------------------------------------------ ecom_carriers
const carriers = [
  ['CRR-01', 'Delhivery', 'STANDARD', 'support@delhivery.example'],
  ['CRR-02', 'Blue Dart', 'EXPRESS', 'care@bluedart.example'],
  ['CRR-03', 'Ekart Logistics', 'standard', 'help@ekart.example'],
  ['CRR-04', 'Shadowfax', 'SAME_DAY', 'ops@shadowfax.example'],
  ['CRR-05', 'XpressBees', 'EXPRESS', 'CS@XpressBees.example'],
].map(([id, name, type, email]) => ({ carrierId: id, carrierName: name, serviceType: type, contactEmail: email,
  status: 'ACTIVE', updatedAt: '2026-01-02T10:00:00', processedFlag: N }));
carriers.push({ carrierId: 'CRR-06', carrierName: 'SkyDrop', serviceType: 'DRONE', contactEmail: 'hello@skydrop.example',
  status: 'ACTIVE', updatedAt: '2026-01-02T10:00:00', processedFlag: N });                              // VALIDATION
write('ecom_carriers', carriers);

// ------------------------------------------------------------------ ecom_shipments
const shipments = [];
let s = 0;
const nextShipmentId = () => 'SHP-' + pad(++s, 5);
const goodWarehouses = ['WH-01', 'WH-02', 'WH-03', 'WH-04', 'WH-05'];
const goodCarriers = ['CRR-01', 'CRR-02', 'CRR-03', 'CRR-04', 'CRR-05'];
for (const [orderId, info] of Object.entries(orderInfo)) {
  if (!['DELIVERED', 'SHIPPED', 'RETURNED'].includes(info.status)) continue;
  const shipped = addMin(info.orderTs, int(6 * 60, 36 * 60));
  const promised = addDays(shipped, int(3, 6));
  const inTransit = info.status === 'SHIPPED';
  const delivered = inTransit ? null : addDays(shipped, int(1, 8));   // sometimes later than promised -> late
  shipments.push({
    shipmentId: nextShipmentId(), orderId, warehouseId: pick(goodWarehouses),
    carrierId: s % 7 === 0 ? pick(goodCarriers).toLowerCase() : pick(goodCarriers),
    trackingNumber: 'TRK' + pad(int(0, 9999999999), 10),
    shippedDate: iso(shipped), promisedDate: day(promised), deliveredDate: delivered ? iso(delivered) : null,
    shipmentStatus: inTransit ? 'IN_TRANSIT' : 'DELIVERED',
    shippingCost: money(40 + rnd() * 160), weightKg: money(0.2 + rnd() * 12),
    updatedAt: iso(delivered || addMin(shipped, 30)), processedFlag: N,
  });
}
const someOrder = Object.keys(orderInfo).find(id => orderInfo[id].status === 'DELIVERED');
const base = { orderId: someOrder, warehouseId: 'WH-01', carrierId: 'CRR-01', trackingNumber: 'TRK0000000001',
  shippedDate: '2026-06-01T10:00:00', promisedDate: '2026-06-05', deliveredDate: '2026-06-04T15:00:00',
  shipmentStatus: 'DELIVERED', shippingCost: 80, weightKg: 1.5, updatedAt: '2026-06-04T15:00:00', processedFlag: N };
shipments.push({ ...base, shipmentId: nextShipmentId(), orderId: 'ORD-00077' });                     // ORPHAN (order rejected)
shipments.push({ ...base, shipmentId: nextShipmentId(), orderId: 'ORD-99999' });                     // ORPHAN (unknown order)
shipments.push({ ...base, shipmentId: nextShipmentId(), carrierId: 'CRR-06' });                      // ORPHAN (carrier rejected)
shipments.push({ ...base, shipmentId: nextShipmentId(), deliveredDate: null });                      // VALIDATION delivered w/o date
shipments.push({ ...base, shipmentId: nextShipmentId(), deliveredDate: '2026-05-30T10:00:00' });     // VALIDATION before shipped
shipments.push({ ...base, shipmentId: nextShipmentId(), weightKg: -1 });                             // VALIDATION weight
// DUPLICATE: the first DELIVERED shipment was also sent earlier as IN_TRANSIT -> the newer (DELIVERED) wins
const firstDelivered = shipments.find(x => x.shipmentStatus === 'DELIVERED');
shipments.push({ ...firstDelivered, shipmentStatus: 'IN_TRANSIT', deliveredDate: null,
  updatedAt: iso(addMin(new Date(firstDelivered.shippedDate + 'Z'), 10)) });
write('ecom_shipments', shipments);

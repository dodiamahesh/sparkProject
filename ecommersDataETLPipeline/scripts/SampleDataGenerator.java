import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * Generates the sample MongoDB data: 10 banking collections x 100 documents.
 * Every collection contains planted problems (corrupt values, rule violations, orphans, duplicates)
 * and "messy but fixable" values, so you can verify the pipeline counts yourself.
 *
 * Run from the project root (Java 17, no dependencies needed):
 *     java scripts/SampleDataGenerator.java
 *
 * Output:
 *     src/main/resources/sample-data/<collection>.json   (JSON array, works with mongoimport --jsonArray)
 *     docs/SAMPLE_DATA_REPORT.md                         (expected counts + list of every bad record)
 *
 * The random seed is fixed, so the output is the same every time.
 */
public class SampleDataGenerator {

    static final Path OUTPUT_DIR = Path.of("src/main/resources/sample-data");
    static final Path REPORT_FILE = Path.of("docs/SAMPLE_DATA_REPORT.md");
    static final int DOCUMENTS_PER_COLLECTION = 100;

    static final Random RANDOM = new Random(20261102L);
    static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    static final LocalDateTime MIN_TIME = LocalDateTime.of(2025, 1, 1, 0, 0);
    static final LocalDateTime MAX_TIME = LocalDateTime.of(2026, 10, 15, 23, 0);

    /** Use as a "bad value" to remove the field completely. */
    static final Object MISSING = new Object() {
        @Override
        public String toString() {
            return "(field missing)";
        }
    };

    // ------------------------------------------------------------------ reference data
    static final String[] MALE_NAMES = {"Rahul", "Amit", "Vikram", "Rohan", "Karan", "Arjun", "Nikhil", "Suresh",
            "Mahesh", "Harsh", "Jay", "Dhruv", "Parth", "Kunal", "Yash", "Ankit", "Sanjay", "Manish", "Deepak", "Ravi"};
    static final String[] FEMALE_NAMES = {"Priya", "Neha", "Pooja", "Anjali", "Kavya", "Riya", "Sneha", "Divya",
            "Isha", "Meera", "Nisha", "Shreya", "Komal", "Hetal", "Bhavna", "Asha", "Krupa", "Janvi", "Foram", "Mansi"};
    static final String[] LAST_NAMES = {"Shah", "Patel", "Mehta", "Desai", "Joshi", "Sharma", "Verma", "Iyer",
            "Reddy", "Nair", "Gupta", "Singh", "Kapoor", "Bhatt", "Trivedi", "Pandya", "Chauhan", "Rao", "Kulkarni", "Agarwal"};
    static final String[][] CITIES = {{"Ahmedabad", "Gujarat"}, {"Surat", "Gujarat"}, {"Vadodara", "Gujarat"},
            {"Rajkot", "Gujarat"}, {"Mumbai", "Maharashtra"}, {"Pune", "Maharashtra"}, {"Bengaluru", "Karnataka"},
            {"Chennai", "Tamil Nadu"}, {"Hyderabad", "Telangana"}, {"Delhi", "Delhi"}, {"Jaipur", "Rajasthan"},
            {"Kolkata", "West Bengal"}, {"Indore", "Madhya Pradesh"}, {"Lucknow", "Uttar Pradesh"}};
    static final String[] EMAIL_DOMAINS = {"gmail.com", "yahoo.in", "outlook.com", "rediffmail.com"};
    static final String[] BANK_CODES = {"HDFC", "ICIC", "SBIN", "UTIB", "KKBK"};
    static final String[][] MERCHANT_BRANDS = {{"DMart", "GROCERY"}, {"BigBasket", "GROCERY"},
            {"Reliance Fresh", "GROCERY"}, {"Croma", "ELECTRONICS"}, {"Reliance Digital", "ELECTRONICS"},
            {"Vijay Sales", "ELECTRONICS"}, {"Indian Oil", "FUEL"}, {"HP Petrol Pump", "FUEL"},
            {"Bharat Petroleum", "FUEL"}, {"MakeMyTrip", "TRAVEL"}, {"IRCTC", "TRAVEL"}, {"IndiGo", "TRAVEL"},
            {"Swiggy", "DINING"}, {"Zomato", "DINING"}, {"Barbeque Nation", "DINING"},
            {"Apollo Pharmacy", "HEALTHCARE"}, {"MedPlus", "HEALTHCARE"}, {"PVR Cinemas", "ENTERTAINMENT"},
            {"BookMyShow", "ENTERTAINMENT"}, {"Torrent Power", "UTILITIES"}, {"Adani Gas", "UTILITIES"},
            {"Jio Fiber", "UTILITIES"}};
    static final Map<String, String> MCC_BY_CATEGORY = Map.of("GROCERY", "5411", "ELECTRONICS", "5732",
            "FUEL", "5541", "TRAVEL", "4722", "DINING", "5812", "HEALTHCARE", "5912", "ENTERTAINMENT", "7832",
            "UTILITIES", "4900");
    static final String[][] BILLERS = {{"Torrent Power", "ELECTRICITY"}, {"MGVCL", "ELECTRICITY"},
            {"Adani Electricity", "ELECTRICITY"}, {"Jio Prepaid", "MOBILE"}, {"Airtel Postpaid", "MOBILE"},
            {"Vi Prepaid", "MOBILE"}, {"Tata Play", "DTH"}, {"Dish TV", "DTH"}, {"Adani Total Gas", "GAS"},
            {"Gujarat Gas", "GAS"}, {"AMC Water Tax", "WATER"}, {"LIC Premium", "INSURANCE"},
            {"HDFC Life", "INSURANCE"}, {"JioFiber", "BROADBAND"}, {"Airtel Xstream", "BROADBAND"}};
    static final Map<String, String[]> PROVIDERS_BY_METHOD = Map.of(
            "UPI", new String[]{"GPAY", "PHONEPE", "PAYTM", "BHIM"},
            "CARD", new String[]{"VISA", "MASTERCARD", "RUPAY"},
            "NETBANKING", new String[]{"HDFC", "ICICI", "SBI", "AXIS"},
            "WALLET", new String[]{"AMAZONPAY", "MOBIKWIK", "FREECHARGE"});

    // ------------------------------------------------------------------ model

    /** A planted problem: applied to {@code count} different documents. */
    record Issue(String errorType, String field, String description, int count,
                 BiConsumer<Map<String, Object>, Integer> mutation) {
    }

    /** A messy value that the pipeline's sanitizer fixes (the record stays VALID). */
    record Messy(String field, String description, Function<Object, Object> makeMessy) {
    }

    static final class Spec {
        String collection;
        String businessKey;
        String usedBy = "-";
        List<String> rules = List.of();
        int exactDuplicates;
        int newerDuplicates;
        BiConsumer<Map<String, Object>, Integer> newerVersionChange = (doc, k) -> {
        };
        IntFunction<Map<String, Object>> validDocument;
        List<Issue> issues = new ArrayList<>();
        List<Messy> messy = new ArrayList<>();
    }

    static final class Generated {
        Spec spec;
        List<Map<String, Object>> documents = new ArrayList<>();
        List<Map<String, Object>> validDocuments = new ArrayList<>();
        List<String[]> planted = new ArrayList<>();      // key, type, detail
        List<String[]> sanitizable = new ArrayList<>();  // key, detail
        Map<String, Integer> countByType = new LinkedHashMap<>();

        int rejected() {
            return planted.size();
        }

        int valid() {
            return documents.size() - rejected();
        }
    }

    // ================================================================== main

    public static void main(String[] args) throws IOException {
        List<Generated> all = new ArrayList<>();

        Generated customers = generate(customersSpec());
        all.add(customers);
        Generated accounts = generate(accountsSpec(keys(customers)));
        all.add(accounts);
        Generated merchants = generate(merchantsSpec());
        all.add(merchants);
        Generated transactions = generate(transactionsSpec(accounts.validDocuments, merchants.validDocuments));
        all.add(transactions);
        all.add(generate(paymentsSpec()));
        all.add(generate(cardsSpec(accounts.validDocuments)));
        all.add(generate(branchesSpec()));
        all.add(generate(fundTransfersSpec(keys(accounts))));
        all.add(generate(loansSpec(keys(customers))));
        all.add(generate(refundsSpec(transactions.validDocuments)));

        Files.createDirectories(OUTPUT_DIR);
        for (Generated generated : all) {
            Path file = OUTPUT_DIR.resolve(generated.spec.collection + ".json");
            Files.writeString(file, toJsonArray(generated.documents), StandardCharsets.UTF_8);
            System.out.printf("%-15s %3d docs  valid=%3d  rejected=%3d  %s%n", generated.spec.collection,
                    generated.documents.size(), generated.valid(), generated.rejected(), generated.countByType);
        }
        Files.createDirectories(REPORT_FILE.getParent());
        Files.writeString(REPORT_FILE, report(all), StandardCharsets.UTF_8);
        System.out.println("Report written to " + REPORT_FILE);
    }

    // ================================================================== generic generator

    static Generated generate(Spec spec) {
        Generated result = new Generated();
        result.spec = spec;
        int uniqueCount = DOCUMENTS_PER_COLLECTION - spec.exactDuplicates - spec.newerDuplicates;

        List<Map<String, Object>> base = new ArrayList<>();
        for (int i = 1; i <= uniqueCount; i++) {
            base.add(spec.validDocument.apply(i));
        }

        // Every document gets at most ONE planted problem, so counts are easy to check
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < uniqueCount; i++) {
            order.add(i);
        }
        Collections.shuffle(order, RANDOM);
        int next = 0;
        Set<Integer> badIndexes = new HashSet<>();

        for (Issue issue : spec.issues) {
            for (int k = 0; k < issue.count(); k++) {
                int index = order.get(next++);
                Map<String, Object> doc = base.get(index);
                issue.mutation().accept(doc, k);
                badIndexes.add(index);
                String shown = issue.field() == null ? "" : " -> " + issue.field() + " = "
                        + (doc.containsKey(issue.field()) ? json(doc.get(issue.field())) : "(field missing)");
                result.planted.add(new String[]{key(doc, spec), issue.errorType(), issue.description() + shown});
                result.countByType.merge(issue.errorType(), 1, Integer::sum);
            }
        }

        for (Messy messy : spec.messy) {
            int index = order.get(next++);
            Map<String, Object> doc = base.get(index);
            Object before = doc.get(messy.field());
            Object after = messy.makeMessy().apply(before);
            doc.put(messy.field(), after);
            result.sanitizable.add(new String[]{key(doc, spec),
                    messy.description() + ": " + messy.field() + " " + json(after) + " -> " + json(before)});
        }

        for (int i = 0; i < uniqueCount; i++) {
            if (!badIndexes.contains(i)) {
                result.validDocuments.add(base.get(i));
            }
        }

        // Duplicates are copies of clean documents, added at the END of the collection
        List<Map<String, Object>> duplicates = new ArrayList<>();
        for (int k = 0; k < spec.exactDuplicates; k++) {
            Map<String, Object> copy = new LinkedHashMap<>(base.get(order.get(next++)));
            duplicates.add(copy);
            result.planted.add(new String[]{key(copy, spec), "DUPLICATE",
                    "exact copy (same updatedAt) -> the copy inserted later is rejected"});
            result.countByType.merge("DUPLICATE", 1, Integer::sum);
        }
        for (int k = 0; k < spec.newerDuplicates; k++) {
            Map<String, Object> copy = new LinkedHashMap<>(base.get(order.get(next++)));
            LocalDateTime updated = LocalDateTime.parse((String) copy.get("updatedAt"), TIMESTAMP).plusDays(3);
            copy.put("updatedAt", updated.format(TIMESTAMP));
            spec.newerVersionChange.accept(copy, k);
            duplicates.add(copy);
            result.planted.add(new String[]{key(copy, spec), "DUPLICATE",
                    "newer version (updatedAt + 3 days) -> the OLDER document is rejected"});
            result.countByType.merge("DUPLICATE", 1, Integer::sum);
        }

        result.documents.addAll(base);
        result.documents.addAll(duplicates);
        for (Map<String, Object> doc : result.documents) {
            doc.put("processedFlag", "N");
        }
        return result;
    }

    // ================================================================== collection specs

    static Spec customersSpec() {
        Spec spec = new Spec();
        spec.collection = "customers";
        spec.businessKey = "customerId";
        spec.usedBy = "transaction-pipeline";
        spec.rules = List.of(
                "CORRUPT: dateOfBirth must be a valid date (yyyy-MM-dd)",
                "VALIDATION: customerId, firstName, lastName, email, phone are required",
                "VALIDATION: customerId like CUST-0001, valid email, phone = 10 digit Indian mobile",
                "VALIDATION: dateOfBirth not in future, gender in [M, F, O], kycStatus in [VERIFIED, PENDING, REJECTED]");
        spec.validDocument = i -> {
            boolean male = RANDOM.nextBoolean();
            String first = pick(male ? MALE_NAMES : FEMALE_NAMES);
            String last = pick(LAST_NAMES);
            String[] city = pick(CITIES);
            LocalDateTime created = randomTime();
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("customerId", "CUST-%04d".formatted(i));
            doc.put("firstName", first);
            doc.put("lastName", last);
            doc.put("email", (first + "." + last + i + "@" + pick(EMAIL_DOMAINS)).toLowerCase());
            doc.put("phone", randomPhone());
            doc.put("dateOfBirth", randomDate(1960, 2004).toString());
            doc.put("gender", male ? "M" : "F");
            doc.put("city", city[0]);
            doc.put("state", city[1]);
            doc.put("country", "India");
            doc.put("kycStatus", RANDOM.nextInt(4) == 0 ? "PENDING" : "VERIFIED");
            doc.put("createdAt", created.format(TIMESTAMP));
            doc.put("updatedAt", later(created).format(TIMESTAMP));
            return doc;
        };
        spec.issues = List.of(
                bad("CORRUPT", "dateOfBirth", "date cannot be parsed", "31-31-1990", "not-a-date", "1990-02-30"),
                bad("VALIDATION", "email", "invalid email", "rahul.shah@", "priya.gmail.com", "amit@@mail.com"),
                bad("VALIDATION", "firstName", "firstName missing / blank", MISSING, "   "),
                bad("VALIDATION", "phone", "phone is not 10 digits", "98765", "98AB765432"),
                bad("VALIDATION", "kycStatus", "unknown kycStatus", "UNKNOWN", "BLOCKED"));
        spec.messy = List.of(
                new Messy("firstName", "lower case + spaces (trim + proper case)", v -> "  " + str(v).toLowerCase() + " "),
                new Messy("lastName", "upper case (proper case)", v -> str(v).toUpperCase()),
                new Messy("email", "upper case + spaces (trim + lower case)", v -> " " + str(v).toUpperCase() + "  "),
                new Messy("phone", "+91 prefix with spaces (phone normalised)", v -> "+91 " + str(v).substring(0, 5) + " " + str(v).substring(5)),
                new Messy("phone", "leading 0 with dash (phone normalised)", v -> "0" + str(v).substring(0, 5) + "-" + str(v).substring(5)),
                new Messy("kycStatus", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("gender", "lower case (upper case)", v -> str(v).toLowerCase()));
        spec.exactDuplicates = 2;
        spec.newerDuplicates = 1;
        spec.newerVersionChange = (doc, k) -> {
            doc.put("city", "Gandhinagar");
            doc.put("state", "Gujarat");
        };
        return spec;
    }

    static Spec accountsSpec(List<String> validCustomerIds) {
        Spec spec = new Spec();
        spec.collection = "accounts";
        spec.businessKey = "accountId";
        spec.usedBy = "transaction-pipeline";
        spec.rules = List.of(
                "CORRUPT: balance must be a number, openedDate must be a valid date",
                "VALIDATION: accountId, customerId, accountType, currency, status are required",
                "VALIDATION: accountId like ACC-0001, balance >= 0, openedDate not in future",
                "VALIDATION: accountType in [SAVINGS, CURRENT, SALARY, NRI], currency in [INR, USD, EUR, GBP], status in [ACTIVE, DORMANT, CLOSED], valid IFSC",
                "ORPHAN: customerId must exist in dim_customer");
        spec.validDocument = i -> {
            String type = pick(new String[]{"SAVINGS", "SAVINGS", "SAVINGS", "CURRENT", "SALARY", "NRI"});
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("accountId", "ACC-%04d".formatted(i));
            doc.put("customerId", pick(validCustomerIds));
            doc.put("accountType", type);
            doc.put("branchCode", "BR-%03d".formatted(1 + RANDOM.nextInt(30)));
            doc.put("ifscCode", pick(BANK_CODES) + "0" + "%06d".formatted(RANDOM.nextInt(1_000_000)));
            doc.put("balance", money(500, 500_000));
            doc.put("currency", type.equals("NRI") ? "USD" : "INR");
            doc.put("status", pick(new String[]{"ACTIVE", "ACTIVE", "ACTIVE", "ACTIVE", "DORMANT", "CLOSED"}));
            doc.put("openedDate", randomDate(2015, 2024).toString());
            doc.put("updatedAt", randomTime().format(TIMESTAMP));
            return doc;
        };
        spec.issues = List.of(
                bad("CORRUPT", "balance", "balance is not a number", "12abc", "N/A", Map.of("amount", 5000)),
                bad("CORRUPT", "openedDate", "date cannot be parsed", "2024-15-01", "yesterday"),
                bad("VALIDATION", "customerId", "customerId missing", MISSING, null),
                bad("VALIDATION", "accountType", "unknown accountType", "FIXED", "LOAN"),
                bad("VALIDATION", "balance", "negative balance", new BigDecimal("-1500.00"), new BigDecimal("-20.50")),
                bad("VALIDATION", "currency", "unknown currency", "XYZ", "RUPEE"));
        spec.messy = List.of(
                new Messy("accountType", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("currency", "lower case + spaces (trim + upper case)", v -> " " + str(v).toLowerCase() + " "),
                new Messy("balance", "Indian comma format string (commas removed)", v -> indianFormat((BigDecimal) v)),
                new Messy("status", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("ifscCode", "lower case (upper case)", v -> str(v).toLowerCase()));
        spec.exactDuplicates = 1;
        spec.newerDuplicates = 1;
        spec.newerVersionChange = (doc, k) -> doc.put("balance", money(500, 500_000));
        return spec;
    }

    static Spec merchantsSpec() {
        Spec spec = new Spec();
        spec.collection = "merchants";
        spec.businessKey = "merchantId";
        spec.usedBy = "transaction-pipeline";
        spec.rules = List.of(
                "CORRUPT: onboardedDate must be a valid date",
                "VALIDATION: merchantId, merchantName, category are required, merchantId like MER-0001",
                "VALIDATION: category in [GROCERY, ELECTRONICS, FUEL, TRAVEL, DINING, HEALTHCARE, ENTERTAINMENT, UTILITIES]",
                "VALIDATION: mccCode = 4 digits, onboardedDate not in future, status in [ACTIVE, INACTIVE]");
        spec.validDocument = i -> {
            String[] brand = pick(MERCHANT_BRANDS);
            String[] city = pick(CITIES);
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("merchantId", "MER-%04d".formatted(i));
            doc.put("merchantName", brand[0] + " " + city[0]);
            doc.put("category", brand[1]);
            doc.put("mccCode", MCC_BY_CATEGORY.get(brand[1]));
            doc.put("city", city[0]);
            doc.put("country", "India");
            doc.put("status", RANDOM.nextInt(10) == 0 ? "INACTIVE" : "ACTIVE");
            doc.put("onboardedDate", randomDate(2018, 2024).toString());
            doc.put("updatedAt", randomTime().format(TIMESTAMP));
            return doc;
        };
        spec.issues = List.of(
                bad("VALIDATION", "merchantName", "merchantName missing / null / empty", MISSING, null, ""),
                bad("VALIDATION", "category", "unknown category", "UNKNOWN", "MISC"),
                bad("VALIDATION", "mccCode", "mccCode is not 4 digits", "54111", "ABCD", "12"),
                bad("CORRUPT", "onboardedDate", "date cannot be parsed", "2023/13/01", "01-01-20xx"));
        spec.messy = List.of(
                new Messy("category", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("status", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("merchantName", "extra spaces (trim)", v -> "   " + str(v) + "  "),
                new Messy("category", "mixed case + spaces (trim + upper case)", v -> " " + capitalize(str(v)) + " "));
        spec.exactDuplicates = 1;
        spec.newerDuplicates = 1;
        spec.newerVersionChange = (doc, k) -> doc.put("status", "INACTIVE");
        return spec;
    }

    static Spec transactionsSpec(List<Map<String, Object>> validAccounts, List<Map<String, Object>> validMerchants) {
        Spec spec = new Spec();
        spec.collection = "transactions";
        spec.businessKey = "transactionId";
        spec.usedBy = "transaction-pipeline";
        spec.rules = List.of(
                "CORRUPT: amount must be a number, transactionDate must be a valid timestamp",
                "VALIDATION: transactionId, accountId, customerId, merchantId, transactionType, channel, amount, currency, transactionDate, status are required",
                "VALIDATION: transactionId like TXN-000001, amount > 0, transactionDate not in future",
                "VALIDATION: transactionType in [DEBIT, CREDIT], channel in [POS, ONLINE, ATM, UPI, NETBANKING], currency in [INR, USD, EUR, GBP], status in [SUCCESS, FAILED, PENDING, REVERSED]",
                "ORPHAN: customerId must exist in dim_customer, accountId in dim_account, merchantId in dim_merchant");
        spec.validDocument = i -> {
            Map<String, Object> account = pick(validAccounts);
            Map<String, Object> merchant = pick(validMerchants);
            LocalDateTime time = randomTime();
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("transactionId", "TXN-%06d".formatted(i));
            doc.put("accountId", account.get("accountId"));
            doc.put("customerId", account.get("customerId"));
            doc.put("merchantId", merchant.get("merchantId"));
            doc.put("transactionType", RANDOM.nextInt(5) == 0 ? "CREDIT" : "DEBIT");
            doc.put("channel", pick(new String[]{"POS", "ONLINE", "ATM", "UPI", "NETBANKING"}));
            doc.put("amount", money(50, 75_000));
            doc.put("currency", account.get("currency").toString().trim().toUpperCase());
            doc.put("transactionDate", time.format(TIMESTAMP));
            doc.put("status", pick(new String[]{"SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING", "REVERSED"}));
            doc.put("description", "Payment at " + merchant.get("merchantName").toString().trim());
            doc.put("updatedAt", time.plusMinutes(5).format(TIMESTAMP));
            return doc;
        };
        spec.issues = List.of(
                bad("CORRUPT", "amount", "amount is not a number", "12abc", "N/A", "₹500", Map.of("value", 750)),
                bad("CORRUPT", "transactionDate", "timestamp cannot be parsed", "2026-02-30T10:00:00", "32/01/2026", "invalid-date"),
                bad("VALIDATION", "amount", "amount <= 0", new BigDecimal("-250.00"), BigDecimal.ZERO, new BigDecimal("-10")),
                bad("VALIDATION", "currency", "unknown currency", "XYZ", "RS"),
                bad("VALIDATION", "channel", "unknown channel", "FAX", "CHEQUE"),
                bad("VALIDATION", "accountId", "accountId missing", MISSING, null),
                bad("VALIDATION", "transactionDate", "transactionDate in the future", "2027-05-10T12:00:00", "2030-01-01T00:00:00"),
                bad("ORPHAN", "customerId", "customer does not exist", "CUST-9901", "CUST-9902"),
                bad("ORPHAN", "merchantId", "merchant does not exist", "MER-9901", "MER-9902"),
                bad("ORPHAN", "accountId", "account does not exist", "ACC-9901"));
        spec.messy = List.of(
                new Messy("channel", "lower case + spaces (trim + upper case)", v -> " " + str(v).toLowerCase() + " "),
                new Messy("channel", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("currency", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("amount", "comma format string (commas removed)", v -> String.format(Locale.US, "%,.2f", (BigDecimal) v)),
                new Messy("status", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("transactionType", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("description", "extra spaces (trim)", v -> "   " + str(v) + "   "),
                new Messy("amount", "number stored as string with spaces (trim)", v -> " " + ((BigDecimal) v).toPlainString() + " "));
        spec.exactDuplicates = 2;
        spec.newerDuplicates = 1;
        spec.newerVersionChange = (doc, k) -> doc.put("status", "REVERSED");
        return spec;
    }

    static Spec paymentsSpec() {
        // 35 payers, each makes several payments with the same details
        List<Map<String, Object>> payers = new ArrayList<>();
        for (int p = 1; p <= 35; p++) {
            boolean male = RANDOM.nextBoolean();
            String first = pick(male ? MALE_NAMES : FEMALE_NAMES);
            String last = pick(LAST_NAMES);
            Map<String, Object> payer = new LinkedHashMap<>();
            payer.put("payerId", "PYR-%03d".formatted(p));
            payer.put("payerName", first + " " + last);
            payer.put("payerEmail", (first + "." + last + p + "@" + pick(EMAIL_DOMAINS)).toLowerCase());
            payer.put("payerPhone", randomPhone());
            payer.put("payerCity", pick(CITIES)[0]);
            payers.add(payer);
        }

        Spec spec = new Spec();
        spec.collection = "payments";
        spec.businessKey = "paymentId";
        spec.usedBy = "payment-pipeline";
        spec.rules = List.of(
                "CORRUPT: amount must be a number, paymentDate must be a valid timestamp",
                "VALIDATION: paymentId, payerId, payerName, paymentMethod, paymentProvider, billerName, amount, currency, paymentDate, status are required",
                "VALIDATION: paymentId like PAY-0001, amount > 0, paymentDate not in future",
                "VALIDATION: paymentMethod in [UPI, CARD, NETBANKING, WALLET], currency in [INR, USD], status in [SUCCESS, FAILED, PENDING]",
                "VALIDATION: payerEmail valid email, payerPhone = 10 digit Indian mobile");
        spec.validDocument = i -> {
            Map<String, Object> payer = pick(payers);
            String[] biller = pick(BILLERS);
            String method = pick(new String[]{"UPI", "UPI", "UPI", "CARD", "NETBANKING", "WALLET"});
            LocalDateTime time = randomTime();
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("paymentId", "PAY-%04d".formatted(i));
            doc.putAll(payer);
            doc.put("paymentMethod", method);
            doc.put("paymentProvider", pick(PROVIDERS_BY_METHOD.get(method)));
            doc.put("billerName", biller[0]);
            doc.put("billerCategory", biller[1]);
            doc.put("amount", money(49, 15_000));
            doc.put("currency", "INR");
            doc.put("paymentDate", time.format(TIMESTAMP));
            doc.put("status", pick(new String[]{"SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"}));
            doc.put("referenceNumber", "REF" + (1_000_000_000L + RANDOM.nextInt(900_000_000)));
            doc.put("updatedAt", time.plusMinutes(2).format(TIMESTAMP));
            return doc;
        };
        spec.issues = List.of(
                bad("CORRUPT", "amount", "amount is not a number", "abc", "1O0.00", true),
                bad("CORRUPT", "paymentDate", "timestamp cannot be parsed", "2026-02-30T09:00:00", "15-08-2026"),
                bad("VALIDATION", "amount", "amount <= 0", BigDecimal.ZERO, new BigDecimal("-99.00")),
                bad("VALIDATION", "payerEmail", "invalid email", "neha@", "vikram.gmail.com"),
                bad("VALIDATION", "paymentMethod", "unknown paymentMethod", "CHEQUE", "CASH"),
                bad("VALIDATION", "payerId", "payerId missing", MISSING, null),
                bad("VALIDATION", "status", "unknown status", "DONE"),
                bad("VALIDATION", "paymentDate", "paymentDate in the future", "2027-01-01T10:00:00"));
        spec.messy = List.of(
                new Messy("paymentMethod", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("status", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("payerEmail", "upper case (lower case)", v -> str(v).toUpperCase()),
                new Messy("payerPhone", "+91- prefix (phone normalised)", v -> "+91-" + str(v)),
                new Messy("amount", "comma format string (commas removed)", v -> String.format(Locale.US, "%,.2f", (BigDecimal) v)),
                new Messy("currency", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("payerName", "lower case + spaces (trim + proper case)", v -> "  " + str(v).toLowerCase() + " "),
                new Messy("paymentProvider", "lower case (upper case)", v -> str(v).toLowerCase()));
        spec.exactDuplicates = 2;
        spec.newerDuplicates = 1;
        spec.newerVersionChange = (doc, k) -> doc.put("status", "SUCCESS");
        return spec;
    }

    static Spec cardsSpec(List<Map<String, Object>> validAccounts) {
        Spec spec = new Spec();
        spec.collection = "cards";
        spec.businessKey = "cardId";
        spec.rules = List.of(
                "CORRUPT: expiryDate must match MM/yy, creditLimit must be a number",
                "VALIDATION: cardId, customerId, accountId, cardType, network are required",
                "VALIDATION: maskedCardNumber must match XXXX-XXXX-XXXX-1234 (a full card number is a PII leak)",
                "VALIDATION: network in [VISA, MASTERCARD, RUPAY, AMEX], cardType in [DEBIT, CREDIT, PREPAID], status in [ACTIVE, BLOCKED, EXPIRED]");
        spec.validDocument = i -> {
            Map<String, Object> account = pick(validAccounts);
            String type = pick(new String[]{"DEBIT", "DEBIT", "CREDIT", "CREDIT", "PREPAID"});
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("cardId", "CARD-%04d".formatted(i));
            doc.put("customerId", account.get("customerId"));
            doc.put("accountId", account.get("accountId"));
            doc.put("cardType", type);
            doc.put("network", pick(new String[]{"VISA", "MASTERCARD", "RUPAY", "AMEX"}));
            doc.put("maskedCardNumber", "XXXX-XXXX-XXXX-%04d".formatted(RANDOM.nextInt(10_000)));
            doc.put("expiryDate", "%02d/%02d".formatted(1 + RANDOM.nextInt(12), 27 + RANDOM.nextInt(5)));
            doc.put("creditLimit", type.equals("CREDIT") ? money(50_000, 500_000) : null);
            doc.put("status", pick(new String[]{"ACTIVE", "ACTIVE", "ACTIVE", "BLOCKED", "EXPIRED"}));
            doc.put("issuedDate", randomDate(2020, 2025).toString());
            doc.put("updatedAt", randomTime().format(TIMESTAMP));
            return doc;
        };
        spec.issues = List.of(
                bad("VALIDATION", "maskedCardNumber", "full card number stored (PII)", "4111111111111111", "5500 0000 0000 0004"),
                bad("CORRUPT", "expiryDate", "expiryDate is not MM/yy", "13/28", "AB/CD"),
                bad("VALIDATION", "network", "unknown network", "DISCOVERY", "PAYPAL"),
                bad("CORRUPT", "creditLimit", "creditLimit is not a number", "fifty thousand", "50k"),
                bad("VALIDATION", "customerId", "customerId missing", MISSING, MISSING));
        spec.messy = List.of(
                new Messy("network", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("status", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("cardType", "lower case + spaces (trim + upper case)", v -> " " + str(v).toLowerCase() + " "));
        spec.exactDuplicates = 1;
        spec.newerDuplicates = 1;
        spec.newerVersionChange = (doc, k) -> doc.put("status", "BLOCKED");
        return spec;
    }

    static Spec branchesSpec() {
        Spec spec = new Spec();
        spec.collection = "branches";
        spec.businessKey = "branchCode";
        spec.rules = List.of(
                "CORRUPT: openedDate must be a valid date",
                "VALIDATION: branchCode, branchName, ifscCode, city are required",
                "VALIDATION: ifscCode must match ^[A-Z]{4}0[A-Z0-9]{6}$, pincode must be 6 digits (not starting with 0)",
                "VALIDATION: phone = 10 digit mobile, status in [ACTIVE, CLOSED]");
        String[] areas = {"Main", "Station Road", "MG Road", "Ring Road", "City Centre", "Market Yard", "Airport Road"};
        spec.validDocument = i -> {
            String[] city = pick(CITIES);
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("branchCode", "BR-%03d".formatted(i));
            doc.put("branchName", "Demo Bank " + city[0] + " " + pick(areas));
            doc.put("ifscCode", "DEMB0%06d".formatted(i));
            doc.put("city", city[0]);
            doc.put("state", city[1]);
            doc.put("pincode", String.valueOf(110_000 + RANDOM.nextInt(780_000)));
            doc.put("managerName", pick(RANDOM.nextBoolean() ? MALE_NAMES : FEMALE_NAMES) + " " + pick(LAST_NAMES));
            doc.put("phone", randomPhone());
            doc.put("openedDate", randomDate(1995, 2024).toString());
            doc.put("status", RANDOM.nextInt(12) == 0 ? "CLOSED" : "ACTIVE");
            doc.put("updatedAt", randomTime().format(TIMESTAMP));
            return doc;
        };
        spec.issues = List.of(
                bad("VALIDATION", "ifscCode", "invalid IFSC code", "DEMB1000123", "DEM0001", "12340001234"),
                bad("VALIDATION", "pincode", "pincode is not 6 digits", "38000", "ABCDEF"),
                bad("VALIDATION", "branchName", "branchName missing / blank", MISSING, "  "),
                bad("CORRUPT", "openedDate", "date cannot be parsed", "2019-02-29", "N/A"));
        spec.messy = List.of(
                new Messy("ifscCode", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("status", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("branchName", "extra spaces (trim)", v -> "  " + str(v) + "   "));
        spec.exactDuplicates = 1;
        spec.newerDuplicates = 1;
        spec.newerVersionChange = (doc, k) -> doc.put("managerName", "Hiral Vyas");
        return spec;
    }

    static Spec fundTransfersSpec(List<String> validAccountIds) {
        Spec spec = new Spec();
        spec.collection = "fund_transfers";
        spec.businessKey = "transferId";
        spec.rules = List.of(
                "CORRUPT: amount must be a number, transferDate must be a valid timestamp",
                "VALIDATION: transferId, fromAccountId, toAccountId, amount, transferType are required",
                "VALIDATION: fromAccountId must be different from toAccountId",
                "VALIDATION: transferType in [NEFT, RTGS, IMPS, UPI]; RTGS amount must be >= 200000",
                "VALIDATION: status in [SUCCESS, FAILED, PENDING]");
        spec.validDocument = i -> {
            String type = pick(new String[]{"NEFT", "NEFT", "IMPS", "IMPS", "UPI", "UPI", "RTGS"});
            BigDecimal amount = switch (type) {
                case "RTGS" -> money(200_000, 1_500_000);
                case "NEFT" -> money(1_000, 100_000);
                case "IMPS" -> money(100, 200_000);
                default -> money(10, 100_000);
            };
            String from = pick(validAccountIds);
            String to = pick(validAccountIds);
            while (to.equals(from)) {
                to = pick(validAccountIds);
            }
            LocalDateTime time = randomTime();
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("transferId", "FT-%04d".formatted(i));
            doc.put("fromAccountId", from);
            doc.put("toAccountId", to);
            doc.put("amount", amount);
            doc.put("currency", "INR");
            doc.put("transferType", type);
            doc.put("transferDate", time.format(TIMESTAMP));
            doc.put("status", pick(new String[]{"SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"}));
            doc.put("remarks", pick(new String[]{"Rent", "Family support", "Invoice payment", "Loan EMI", "Savings", "Fees"}));
            doc.put("updatedAt", time.plusMinutes(1).format(TIMESTAMP));
            return doc;
        };
        spec.issues = List.of(
                new Issue("VALIDATION", "toAccountId", "fromAccountId = toAccountId", 2,
                        (doc, k) -> doc.put("toAccountId", doc.get("fromAccountId"))),
                bad("CORRUPT", "amount", "amount is not a number", "1,00,000/-", "ten thousand", "abc"),
                new Issue("VALIDATION", "amount", "RTGS below minimum 200000", 2, (doc, k) -> {
                    doc.put("transferType", "RTGS");
                    doc.put("amount", k == 0 ? new BigDecimal("50000.00") : new BigDecimal("150000.00"));
                }),
                bad("VALIDATION", "transferType", "unknown transferType", "SWIFT", "CASH"),
                bad("CORRUPT", "transferDate", "timestamp cannot be parsed", "2026-06-31T10:00:00", "31/05/2026 10:00"));
        spec.messy = List.of(
                new Messy("transferType", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("status", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("amount", "comma format string (commas removed)", v -> String.format(Locale.US, "%,.2f", (BigDecimal) v)));
        spec.exactDuplicates = 2;
        spec.newerDuplicates = 1;
        spec.newerVersionChange = (doc, k) -> doc.put("status", "FAILED");
        return spec;
    }

    static Spec loansSpec(List<String> validCustomerIds) {
        Spec spec = new Spec();
        spec.collection = "loans";
        spec.businessKey = "loanId";
        spec.rules = List.of(
                "CORRUPT: principalAmount / emiAmount must be numbers, tenureMonths must be a whole number, disbursementDate valid date",
                "VALIDATION: loanId, customerId, loanType, principalAmount are required",
                "VALIDATION: loanType in [HOME, PERSONAL, AUTO, EDUCATION, GOLD], interestRate between 1 and 36",
                "VALIDATION: status in [APPROVED, ACTIVE, CLOSED, DEFAULTED]");
        spec.validDocument = i -> {
            String type = pick(new String[]{"HOME", "PERSONAL", "PERSONAL", "AUTO", "EDUCATION", "GOLD"});
            BigDecimal principal = switch (type) {
                case "HOME" -> money(1_500_000, 9_000_000);
                case "AUTO" -> money(300_000, 1_500_000);
                case "EDUCATION" -> money(200_000, 2_000_000);
                case "GOLD" -> money(25_000, 500_000);
                default -> money(50_000, 1_000_000);
            };
            int tenure = switch (type) {
                case "HOME" -> 120 + 12 * RANDOM.nextInt(21);
                case "GOLD" -> 12;
                default -> 12 * (1 + RANDOM.nextInt(7));
            };
            BigDecimal rate = BigDecimal.valueOf(7.5 + RANDOM.nextInt(1050) / 100.0).setScale(2, RoundingMode.HALF_UP);
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("loanId", "LN-%04d".formatted(i));
            doc.put("customerId", pick(validCustomerIds));
            doc.put("loanType", type);
            doc.put("principalAmount", principal);
            doc.put("interestRate", rate);
            doc.put("tenureMonths", tenure);
            doc.put("emiAmount", emi(principal, rate, tenure));
            doc.put("disbursementDate", randomDate(2019, 2025).toString());
            doc.put("status", pick(new String[]{"ACTIVE", "ACTIVE", "ACTIVE", "CLOSED", "APPROVED", "DEFAULTED"}));
            doc.put("updatedAt", randomTime().format(TIMESTAMP));
            return doc;
        };
        spec.issues = List.of(
                bad("VALIDATION", "interestRate", "interestRate out of range", new BigDecimal("45.50"), new BigDecimal("-2.00")),
                bad("CORRUPT", "principalAmount", "principalAmount is not a number", "ten lakh", "5,00,000/-"),
                bad("CORRUPT", "tenureMonths", "tenureMonths is not a whole number", "twelve", "12.5"),
                bad("VALIDATION", "customerId", "customerId missing", MISSING, MISSING),
                bad("VALIDATION", "loanType", "unknown loanType", "CAR", "CRYPTO"),
                bad("CORRUPT", "disbursementDate", "date cannot be parsed", "2025-04-31", "20-20-2020"));
        spec.messy = List.of(
                new Messy("loanType", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("status", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("principalAmount", "Indian comma format string (commas removed)", v -> indianFormat((BigDecimal) v)));
        spec.exactDuplicates = 1;
        spec.newerDuplicates = 1;
        spec.newerVersionChange = (doc, k) -> doc.put("status", "CLOSED");
        return spec;
    }

    static Spec refundsSpec(List<Map<String, Object>> validTransactions) {
        Spec spec = new Spec();
        spec.collection = "refunds";
        spec.businessKey = "refundId";
        spec.rules = List.of(
                "CORRUPT: refundAmount must be a number, refundDate must be a valid timestamp",
                "VALIDATION: refundId, transactionId, customerId, refundAmount, status are required",
                "VALIDATION: refundAmount > 0, status in [INITIATED, PROCESSED, REJECTED]");
        spec.validDocument = i -> {
            Map<String, Object> txn = pick(validTransactions);
            BigDecimal txnAmount = new BigDecimal(txn.get("amount").toString().replace(",", "").trim());
            LocalDateTime txnTime = LocalDateTime.parse((String) txn.get("transactionDate"), TIMESTAMP);
            LocalDateTime refundTime = txnTime.plusDays(1 + RANDOM.nextInt(10));
            if (refundTime.isAfter(MAX_TIME)) {
                refundTime = MAX_TIME;
            }
            BigDecimal refund = txnAmount.multiply(BigDecimal.valueOf(0.2 + RANDOM.nextInt(80) / 100.0))
                    .setScale(2, RoundingMode.HALF_UP);
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("refundId", "RF-%04d".formatted(i));
            doc.put("transactionId", txn.get("transactionId"));
            doc.put("customerId", txn.get("customerId"));
            doc.put("refundAmount", refund.max(new BigDecimal("1.00")));
            doc.put("reason", pick(new String[]{"Item returned", "Duplicate charge", "Service not delivered",
                    "Order cancelled", "Wrong amount charged"}));
            doc.put("refundDate", refundTime.format(TIMESTAMP));
            doc.put("status", pick(new String[]{"PROCESSED", "PROCESSED", "PROCESSED", "INITIATED", "REJECTED"}));
            doc.put("updatedAt", refundTime.plusHours(2).format(TIMESTAMP));
            return doc;
        };
        spec.issues = List.of(
                bad("CORRUPT", "refundAmount", "refundAmount is not a number", "abc", "₹200"),
                bad("VALIDATION", "refundAmount", "refundAmount <= 0", new BigDecimal("-150.00"), new BigDecimal("-1")),
                bad("VALIDATION", "transactionId", "transactionId missing", MISSING, null),
                bad("VALIDATION", "status", "unknown status", "DONE", "CANCELLED"),
                bad("CORRUPT", "refundDate", "timestamp cannot be parsed", "2026-13-01T10:00:00", "tomorrow"));
        spec.messy = List.of(
                new Messy("status", "lower case (upper case)", v -> str(v).toLowerCase()),
                new Messy("reason", "extra spaces (trim)", v -> "  " + str(v) + "  "),
                new Messy("refundAmount", "number stored as string with spaces (trim)", v -> " " + ((BigDecimal) v).toPlainString() + " "));
        spec.exactDuplicates = 1;
        spec.newerDuplicates = 1;
        spec.newerVersionChange = (doc, k) -> doc.put("status", "PROCESSED");
        return spec;
    }

    // ================================================================== report

    static String report(List<Generated> all) {
        StringBuilder md = new StringBuilder();
        md.append("# Sample data report\n\n");
        md.append("Generated by `scripts/SampleDataGenerator.java` (fixed random seed, so the output is always the same).\n");
        md.append("10 MongoDB collections x 100 documents = 1000 documents in database `bronze_db`.\n\n");
        md.append("Every bad document has exactly **one** planted problem, so the counts below are exact.\n");
        md.append("*Messy* values (extra spaces, wrong case, commas in numbers, `+91` phone prefix) are fixed by the\n");
        md.append("sanitizer, so those documents are still **valid**.\n\n");

        md.append("## 1. Summary (expected result of the FIRST run)\n\n");
        md.append("| Collection | Used by pipeline | Total | Valid | Rejected | CORRUPT | VALIDATION | ORPHAN | DUPLICATE | Messy but valid |\n");
        md.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        int total = 0;
        int valid = 0;
        int rejected = 0;
        for (Generated g : all) {
            md.append("| %s | %s | %d | **%d** | **%d** | %d | %d | %d | %d | %d |\n".formatted(
                    g.spec.collection, g.spec.usedBy, g.documents.size(), g.valid(), g.rejected(),
                    g.countByType.getOrDefault("CORRUPT", 0), g.countByType.getOrDefault("VALIDATION", 0),
                    g.countByType.getOrDefault("ORPHAN", 0), g.countByType.getOrDefault("DUPLICATE", 0),
                    g.sanitizable.size()));
            total += g.documents.size();
            valid += g.valid();
            rejected += g.rejected();
        }
        md.append("| **TOTAL** | | **%d** | **%d** | **%d** | | | | | |\n\n".formatted(total, valid, rejected));

        md.append("## 2. Expected job result per pipeline (first FULL or INCREMENTAL run)\n\n");
        md.append("| Pipeline | source_count | valid_count | rejected_count | duplicate_count | status |\n");
        md.append("|---|---:|---:|---:|---:|---|\n");
        for (String pipeline : List.of("payment-pipeline", "transaction-pipeline")) {
            int s = 0, v = 0, r = 0, d = 0;
            for (Generated g : all) {
                if (g.spec.usedBy.equals(pipeline)) {
                    s += g.documents.size();
                    v += g.valid();
                    r += g.rejected();
                    d += g.countByType.getOrDefault("DUPLICATE", 0);
                }
            }
            md.append("| %s | %d | %d | %d | %d | SUCCESS_WITH_REJECTS |\n".formatted(pipeline, s, v, r, d));
        }
        md.append("\nAfter the first run, MongoDB flags are: valid = `Y`, rejected = `E`, duplicate = `D`.\n");
        md.append("A second INCREMENTAL run reads 0 documents. A RETRY_REJECTED run reads only the `E` documents\n");
        md.append("(rejected minus duplicates).\n\n");

        md.append("## 3. Details per collection\n\n");
        for (Generated g : all) {
            md.append("### ").append(g.spec.collection).append("\n\n");
            md.append("Business key: `").append(g.spec.businessKey).append("` | used by: ").append(g.spec.usedBy)
                    .append(g.spec.usedBy.equals("-") ? " (no pipeline yet - build one as an exercise)" : "")
                    .append("\n\n");
            md.append("Rules:\n");
            for (String rule : g.spec.rules) {
                md.append("- ").append(rule).append("\n");
            }
            md.append("\n| Business key | Error type | Planted problem |\n|---|---|---|\n");
            for (String[] p : g.planted) {
                md.append("| %s | %s | %s |\n".formatted(p[0], p[1], escapeMd(p[2])));
            }
            md.append("\nMessy but valid (fixed by the sanitizer):\n\n| Business key | Fix |\n|---|---|\n");
            for (String[] s : g.sanitizable) {
                md.append("| %s | %s |\n".formatted(s[0], escapeMd(s[1])));
            }
            md.append("\n");
        }
        return md.toString();
    }

    // ================================================================== helpers

    static Issue bad(String type, String field, String description, Object... badValues) {
        return new Issue(type, field, description, badValues.length, (doc, k) -> {
            Object value = badValues[k];
            if (value == MISSING) {
                doc.remove(field);
            } else {
                doc.put(field, value);
            }
        });
    }

    static List<String> keys(Generated generated) {
        return generated.validDocuments.stream().map(doc -> (String) doc.get(generated.spec.businessKey)).toList();
    }

    static String key(Map<String, Object> doc, Spec spec) {
        return String.valueOf(doc.get(spec.businessKey));
    }

    static <T> T pick(T[] values) {
        return values[RANDOM.nextInt(values.length)];
    }

    static <T> T pick(List<T> values) {
        return values.get(RANDOM.nextInt(values.size()));
    }

    static String str(Object value) {
        return String.valueOf(value);
    }

    static String capitalize(String value) {
        return value.charAt(0) + value.substring(1).toLowerCase();
    }

    static String randomPhone() {
        StringBuilder phone = new StringBuilder().append(6 + RANDOM.nextInt(4));
        for (int i = 0; i < 9; i++) {
            phone.append(RANDOM.nextInt(10));
        }
        return phone.toString();
    }

    static LocalDateTime randomTime() {
        long seconds = java.time.Duration.between(MIN_TIME, MAX_TIME).getSeconds();
        return MIN_TIME.plusSeconds((long) (RANDOM.nextDouble() * seconds));
    }

    static LocalDateTime later(LocalDateTime time) {
        LocalDateTime result = time.plusDays(RANDOM.nextInt(60)).plusMinutes(RANDOM.nextInt(600));
        return result.isAfter(MAX_TIME) ? MAX_TIME : result;
    }

    static LocalDate randomDate(int fromYear, int toYear) {
        LocalDate from = LocalDate.of(fromYear, 1, 1);
        long days = LocalDate.of(toYear, 12, 31).toEpochDay() - from.toEpochDay();
        return from.plusDays(RANDOM.nextInt((int) days));
    }

    static BigDecimal money(double min, double max) {
        return BigDecimal.valueOf(min + RANDOM.nextDouble() * (max - min)).setScale(2, RoundingMode.HALF_UP);
    }

    static BigDecimal emi(BigDecimal principal, BigDecimal yearlyRate, int months) {
        double r = yearlyRate.doubleValue() / 12 / 100;
        double p = principal.doubleValue();
        double emi = p * r * Math.pow(1 + r, months) / (Math.pow(1 + r, months) - 1);
        return BigDecimal.valueOf(emi).setScale(2, RoundingMode.HALF_UP);
    }

    /** 1234567.50 -> "12,34,567.50" */
    static String indianFormat(BigDecimal amount) {
        String plain = amount.setScale(2, RoundingMode.HALF_UP).toPlainString();
        String integer = plain.substring(0, plain.indexOf('.'));
        String decimals = plain.substring(plain.indexOf('.'));
        if (integer.length() <= 3) {
            return integer + decimals;
        }
        String lastThree = integer.substring(integer.length() - 3);
        String rest = integer.substring(0, integer.length() - 3);
        StringBuilder grouped = new StringBuilder();
        for (int i = 0; i < rest.length(); i++) {
            if (i > 0 && (rest.length() - i) % 2 == 0) {
                grouped.append(',');
            }
            grouped.append(rest.charAt(i));
        }
        return grouped + "," + lastThree + decimals;
    }

    static String escapeMd(String text) {
        return text.replace("|", "\\|");
    }

    // ---- tiny JSON writer (no libraries needed)

    static String toJsonArray(List<Map<String, Object>> documents) {
        StringBuilder out = new StringBuilder("[\n");
        for (int i = 0; i < documents.size(); i++) {
            out.append("  ").append(json(documents.get(i)));
            out.append(i < documents.size() - 1 ? ",\n" : "\n");
        }
        return out.append("]\n").toString();
    }

    @SuppressWarnings("unchecked")
    static String json(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String text) {
            return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
        if (value instanceof BigDecimal number) {
            return number.toPlainString();
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Map<?, ?> map) {
            StringBuilder out = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) map).entrySet()) {
                if (!first) {
                    out.append(", ");
                }
                out.append(json(entry.getKey())).append(": ").append(json(entry.getValue()));
                first = false;
            }
            return out.append("}").toString();
        }
        throw new IllegalArgumentException("Cannot write " + value.getClass());
    }
}

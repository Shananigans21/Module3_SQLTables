import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

##### Part 1: Join and Filter #####

# 1. Employees in Boston with job titles
df_boston_employees = pd.read_sql("""
SELECT firstName, lastName, jobTitle
FROM employees
WHERE officeCode IN (
    SELECT officeCode FROM offices WHERE city = 'Boston'
);
""", conn)

# 2. Offices with zero employees
df_zero_employee_offices = pd.read_sql("""
SELECT officeCode, city
FROM offices o
LEFT JOIN employees e ON o.officeCode = e.officeCode
GROUP BY o.officeCode
HAVING COUNT(e.employeeNumber) = 0;
""", conn)

##### Part 2: Type of Join #####

# 3. All employees with their office city and state (if they have one)
df_all_employees_offices = pd.read_sql("""
SELECT e.firstName, e.lastName, 
       COALESCE(o.city, 'N/A') AS city, 
       COALESCE(o.state, 'N/A') AS state
FROM employees e
LEFT JOIN offices o ON e.officeCode = o.officeCode
ORDER BY e.firstName, e.lastName;
""", conn)

# 4. Customers who have NOT placed any orders
df_customers_no_orders = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o ON c.customerNumber = o.customerNumber
WHERE o.orderNumber IS NULL AND c.salesRepEmployeeNumber IS NOT NULL
ORDER BY c.contactLastName;
""", conn)

##### Part 3: Built-In Function #####

# 5. Customer contacts with payment details sorted by payment amount descending
df_payments = pd.read_sql("""
SELECT p.customerName, p.checkNumber, p.paymentDate, 
       CAST(p.amount AS REAL) AS amount
FROM payments p
ORDER BY amount DESC;
""", conn)

##### Part 4: Joining and Grouping #####

# 6. Employees whose customers have avg credit limit > 90k, with customer count
df_top_employees = pd.read_sql("""
SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS num_customers
FROM employees e
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber
HAVING AVG(c.creditLimit) > 90000
ORDER BY num_customers DESC
LIMIT 4;
""", conn)

# 7. Products ordered: productName, number of orders, total quantity ordered
df_top_products = pd.read_sql("""
SELECT p.productName, 
       COUNT(od.orderNumber) AS numorders, 
       SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
GROUP BY p.productCode
ORDER BY totalunits DESC;
""", conn)

##### Part 5: Multiple Joins #####

# 8. Product name, code, and total number of unique customers who ordered it
df_product_customers = pd.read_sql("""
SELECT p.productName, p.productCode, COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
JOIN orders o ON od.orderNumber = o.orderNumber
GROUP BY p.productCode
ORDER BY numpurchasers DESC;
""", conn)

# 9. Number of customers per office with officeCode and city
df_customers_per_office = pd.read_sql("""
SELECT o.officeCode, o.city, COUNT(c.customerNumber) AS n_customers
FROM offices o
LEFT JOIN employees e ON o.officeCode = e.officeCode
LEFT JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode
ORDER BY n_customers DESC;
""", conn)

##### Part 6: Subquery #####

# 10. Employees who sold products ordered by fewer than 20 customers
df_employees_underperforming_products = pd.read_sql("""
WITH ProductCustomerCounts AS (
    SELECT p.productCode, COUNT(DISTINCT o.customerNumber) AS cust_count
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY p.productCode
    HAVING cust_count < 20
)
SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
FROM employees e
JOIN offices o ON e.officeCode = o.officeCode
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders ord ON c.customerNumber = ord.customerNumber
JOIN orderdetails od ON ord.orderNumber = od.orderNumber
JOIN ProductCustomerCounts pcc ON od.productCode = pcc.productCode
ORDER BY e.employeeNumber;
""", conn)

# Close the connection
conn.close()

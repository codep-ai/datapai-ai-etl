# PII inventory — dbt-demo

Sensitive columns: **49** across 9 models

| Level | Columns |
|---|---:|
| PII_MEDIUM | 35 |
| PII_LOW | 14 |

## Sources of sensitivity (roots) and blast radius

| Root column | Level | Downstream columns |
|---|---|---:|
| `customer.first_name` | PII_MEDIUM | 3 |
| `customer.last_name` | PII_MEDIUM | 3 |
| `customer.phone` | PII_MEDIUM | 3 |
| `customer.fax` | PII_MEDIUM | 3 |
| `customer.email` | PII_MEDIUM | 3 |
| `employee.last_name` | PII_MEDIUM | 3 |
| `employee.first_name` | PII_MEDIUM | 3 |
| `raw_customers.first_name` | PII_MEDIUM | 3 |
| `raw_customers.last_name` | PII_MEDIUM | 3 |
| `raw_generated.phone` | PII_MEDIUM | 2 |
| `raw_generated.email` | PII_MEDIUM | 2 |
| `employee.birth_date` | PII_MEDIUM | 1 |
| `employee.phone` | PII_MEDIUM | 1 |
| `employee.fax` | PII_MEDIUM | 1 |
| `employee.email` | PII_MEDIUM | 1 |
| `customer.address` | PII_LOW | 3 |
| `customer.city` | PII_LOW | 3 |
| `customer.state` | PII_LOW | 3 |
| `raw_generated.address` | PII_LOW | 2 |
| `employee.address` | PII_LOW | 1 |
| `employee.city` | PII_LOW | 1 |
| `employee.state` | PII_LOW | 1 |

## Models carrying PII

| Model | PII_HIGH | FINANCIAL | PII_MEDIUM | PII_LOW |
|---|---:|---:|---:|---:|
| customers | 0 | 0 | 2 | 0 |
| dim_customer | 0 | 0 | 7 | 3 |
| generated_a | 0 | 0 | 2 | 1 |
| generated_b | 0 | 0 | 2 | 1 |
| obt_invoice | 0 | 0 | 7 | 3 |
| stg_customer | 0 | 0 | 5 | 3 |
| stg_customers | 0 | 0 | 2 | 0 |
| stg_employee | 0 | 0 | 6 | 3 |
| users | 0 | 0 | 2 | 0 |

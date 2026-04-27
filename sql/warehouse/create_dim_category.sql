-- Merchant category dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_category (
    category_id     SERIAL PRIMARY KEY,
    category_code   VARCHAR(50) UNIQUE,
    category_name   VARCHAR(100),
    category_group  VARCHAR(50)
);

-- Seed category data
INSERT INTO warehouse.dim_category (category_code, category_name, category_group)
VALUES
    ('GROCERIES',    'Groceries & Supermarkets',  'Daily Living'),
    ('DINING',       'Restaurants & Dining',       'Daily Living'),
    ('TRANSPORT',    'Transport & Travel',         'Transport'),
    ('FUEL',         'Fuel & Gas Stations',        'Transport'),
    ('UTILITIES',    'Utilities & Bills',          'Bills'),
    ('RENT',         'Rent & Housing',             'Bills'),
    ('HEALTHCARE',   'Healthcare & Pharmacy',      'Health'),
    ('ENTERTAINMENT','Entertainment & Leisure',    'Lifestyle'),
    ('SHOPPING',     'Online & Retail Shopping',   'Lifestyle'),
    ('TRANSFER',     'Bank Transfers',             'Finance'),
    ('ATM',          'ATM Withdrawal',             'Finance'),
    ('SALARY',       'Salary & Income',            'Income'),
    ('OTHER',        'Other / Uncategorised',      'Other')
ON CONFLICT (category_code) DO NOTHING;

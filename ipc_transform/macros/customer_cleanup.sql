/*
  Macro: is_test_account
  Returns TRUE if business_name looks like a test/dev/internal account.
  Use in WHERE clauses: WHERE NOT {{ is_test_account('"businessName"') }}
*/
{% macro is_test_account(col) %}
(
    LOWER(TRIM({{ col }})) IN (
        'test', 'test123', 'tests', 'tests1', 'testing', 'test dash',
        'test-dsh-001', 'test-dsh-002', 'test lb', 'lb test shop', 'lb testshop 2',
        'daash tech', 'daash tech test', 'dev testing', 'dev', 'dev team test',
        'exydevcoder', 'tech', 'tech team', 'kkb', 'hanifah',
        'gosource', 'gosource 1000', 'ipc-ldn',
        'test a', 'test-2', 'test-bm', 'testing business', 'testttt7',
        'xyz', 'yum',
        'daash team', 'daash team 2', 'daash', 'daashaap',
        'crea8', 'iandesign', 'ctlec'
    )
    OR LOWER(TRIM({{ col }})) LIKE 'techwhy%@%'
)
{% endmacro %}


/*
  Macro: clean_business_name
  Normalizes duplicate business names to their canonical form.
  Use: {{ clean_business_name('"businessName"') }} AS business_name
*/
{% macro clean_business_name(col) %}
CASE
    -- DAASH duplicates
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'papasgrill'
        THEN 'Papas Grill'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'chopchop'
        THEN 'Chop Chop'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'amalamaami'
        THEN 'Amala Maami'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'flavorgrill'
        THEN 'Flavor Grill'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'frutadelite'
        THEN 'Fruta De Lite'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'melonypine'
        THEN 'Melony Pine'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'misscravings'
        THEN 'Misscravings'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'nourikitchen'
        THEN 'Nouri Kitchen'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'pemisirelogisticsservices'
        THEN 'Pemisire Logistics Services'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'unclefries'
        THEN 'Uncle Fries'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'xtabelbuka'
        THEN 'Xtabel Buka'

    -- GoSource duplicates: CitySubs (all branches → single name)
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') IN (
        'citysubs', 'citysubmarinesandwichltd', 'citysubsmagodobranch',
        'citysubssandwichbakery', 'citysubyaba'
    ) THEN 'CitySubs Sandwich & Bakery'
    -- GoSource duplicates: Soo Pasta
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') IN ('soopasta', 'sopasta')
        THEN 'Soo Pasta'
    -- GoSource duplicates: Pasta N Grills
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') IN ('pastangrills', 'pastaandgrills')
        THEN 'Pasta N Grills'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'spicycorner'
        THEN 'Spicy Corner'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'wingsbistro'
        THEN 'Wings Bistro'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'twelvetakeout'
        THEN 'Twelve Takeout'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'bigbelly'
        THEN 'Big Belly'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'angydailyfood'
        THEN 'Angy Daily Food'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'sooyahbistro'
        THEN 'Sooyah Bistro'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'ziggyburgers'
        THEN 'Ziggy Burgers'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'epiccakesandmore'
        THEN 'Epic Cakes and More'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'faveyskitchen'
        THEN 'Favey''s Kitchen'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'ceephilfoods'
        THEN 'Cee Phil Foods'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'midekitchen'
        THEN 'Mide Kitchen'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'kallyrosecateringservices'
        THEN 'Kallyrose Catering Services'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'onewayeats'
        THEN 'One Way Eats'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'sweettoothbytee'
        THEN 'Sweettoothbytee'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'lovicravingsandmore'
        THEN 'Lovicravings and More'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'taiwofood'
        THEN 'Taiwo Food'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'mabelstore'
        THEN 'Mabel Store'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'freshpresse'
        THEN 'Fresh Presse'
    WHEN REGEXP_REPLACE(LOWER(TRIM({{ col }})), '[^a-z0-9]', '', 'g') = 'omotoyosi'
        THEN 'Omotoyosi'

    ELSE TRIM({{ col }})
END
{% endmacro %}

WITH items AS (
    SELECT
        "CInvoiceReference" as invoice_reference,
        "CInvoiceVoucher" as invoice_voucher
    FROM {{ ref('stg_qad__cinvoice') }}
),

final as (
    select
        *
    FROM items
)

SELECT * FROM final

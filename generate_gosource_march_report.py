#!/usr/bin/env python3
"""GoSource March 2026 Monthly Report — PDF generator.

Tight 5-slide deck for management meeting with team leads:
  1. March Wrap-up        — KPIs + Payment Method split
  2. YTD Progress         — Jan/Feb/Mar trend
  3. Top 10 Customers     — relative metrics only (no ₦ per customer)
  4. Customer Health      — retained/churned/new
  5. Summary & Actions    — wins, concerns, recs

Excluded (operational, not management-meeting material):
  Day of Week, Best/Worst Day, Order Quality, AR Aging, Top Products.

Per user prefs: per-customer revenue ₦ amounts are NEVER shown.
"""
import os
import psycopg2
from datetime import date
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
    PageBreak,
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

# ── Colors ────────────────────────────────────────────────────────────────
RED = colors.HexColor("#E63946")
DARK_RED = colors.HexColor("#C1121F")
LIGHT_PINK = colors.HexColor("#FCE4E6")
PINK_BORDER = colors.HexColor("#F4A6AD")
LIGHT_BLUE_BG = colors.HexColor("#E8F1FB")
BLUE_BORDER = colors.HexColor("#5B8DEF")
LIGHT_GREEN_BG = colors.HexColor("#D6F5E3")
GREEN_BORDER = colors.HexColor("#33A867")
LIGHT_YELLOW_BG = colors.HexColor("#FFF4D6")
YELLOW_BORDER = colors.HexColor("#E0A800")
GREY = colors.HexColor("#6c757d")
DARK_GREY = colors.HexColor("#333333")

# ── DB ────────────────────────────────────────────────────────────────────
PG = dict(
    database="PROD_ANALYTICS_DB",
    user=os.environ["PG_USER"].strip("\r"),
    password=os.environ["PG_PASSWORD"].strip("\r"),
    host=os.environ["PG_HOST"].strip("\r"),
    port=os.environ["PG_PORT"].strip("\r"),
)
conn = psycopg2.connect(**PG)
cur = conn.cursor()


def naira_m(n):
    if n is None:
        return "₦0"
    n = float(n)
    if abs(n) >= 1e6:
        return f"₦{n / 1e6:,.1f}M"
    return f"₦{n:,.0f}"


# ── Pull data ─────────────────────────────────────────────────────────────
def fetch_month(start, end):
    cur.execute(
        """
        SELECT count(*),
               coalesce(sum(receipts_total_price_amount),0)::bigint,
               coalesce(sum(receipts_service_charge_amount),0)::bigint,
               count(DISTINCT unified_customer_id_fk)
        FROM bv.bv_gosource_receipts
        WHERE receipts_created_at_date >= %s
          AND receipts_created_at_date < %s
          AND lower(receipts_status) = 'delivered'
        """,
        (start, end),
    )
    o, s, sv, c = cur.fetchone()
    cur.execute(
        """
        SELECT receipts_payment_method,
               count(*),
               coalesce(sum(receipts_total_price_amount),0)::bigint
        FROM bv.bv_gosource_receipts
        WHERE receipts_created_at_date >= %s
          AND receipts_created_at_date < %s
          AND lower(receipts_status) = 'delivered'
        GROUP BY 1
        """,
        (start, end),
    )
    pm = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    return dict(orders=o, sales=s, svc=sv, customers=c, payments=pm)


jan = fetch_month("2026-01-01", "2026-02-01")
feb = fetch_month("2026-02-01", "2026-03-01")
mar = fetch_month("2026-03-01", "2026-04-01")


# Top 10 customers (Mar) with Feb comparison — % only, no ₦
cur.execute(
    """
    WITH mar_c AS (
      SELECT c.customer_business_name AS name,
             count(*) AS orders,
             sum(r.receipts_total_price_amount)::bigint AS sales
      FROM bv.bv_gosource_receipts r
      JOIN bv.bv_gosource_customers c ON c.customer_id_pk = r.unified_customer_id_fk
      WHERE r.receipts_created_at_date >= '2026-03-01'
        AND r.receipts_created_at_date < '2026-04-01'
        AND lower(r.receipts_status) = 'delivered'
        AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    ),
    feb_c AS (
      SELECT c.customer_business_name AS name,
             sum(r.receipts_total_price_amount)::bigint AS sales
      FROM bv.bv_gosource_receipts r
      JOIN bv.bv_gosource_customers c ON c.customer_id_pk = r.unified_customer_id_fk
      WHERE r.receipts_created_at_date >= '2026-02-01'
        AND r.receipts_created_at_date < '2026-03-01'
        AND lower(r.receipts_status) = 'delivered'
        AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    )
    SELECT mar_c.name, mar_c.orders, mar_c.sales, coalesce(feb_c.sales, 0)
    FROM mar_c LEFT JOIN feb_c USING (name)
    ORDER BY mar_c.sales DESC LIMIT 10
    """
)
top10 = cur.fetchall()
top3_share = sum(r[2] for r in top10[:3]) / mar["sales"] * 100

# Biggest grower / decliner among customers with prior-month presence
cur.execute(
    """
    WITH mar_c AS (
      SELECT c.customer_business_name AS name, sum(r.receipts_total_price_amount)::bigint AS sales
      FROM bv.bv_gosource_receipts r
      JOIN bv.bv_gosource_customers c ON c.customer_id_pk = r.unified_customer_id_fk
      WHERE r.receipts_created_at_date >= '2026-03-01' AND r.receipts_created_at_date < '2026-04-01'
        AND lower(r.receipts_status) = 'delivered' AND c.customer_business_name IS NOT NULL
      GROUP BY 1 HAVING sum(r.receipts_total_price_amount) > 100000
    ),
    feb_c AS (
      SELECT c.customer_business_name AS name, sum(r.receipts_total_price_amount)::bigint AS sales
      FROM bv.bv_gosource_receipts r
      JOIN bv.bv_gosource_customers c ON c.customer_id_pk = r.unified_customer_id_fk
      WHERE r.receipts_created_at_date >= '2026-02-01' AND r.receipts_created_at_date < '2026-03-01'
        AND lower(r.receipts_status) = 'delivered' AND c.customer_business_name IS NOT NULL
      GROUP BY 1 HAVING sum(r.receipts_total_price_amount) > 100000
    )
    SELECT mar_c.name, ((mar_c.sales::numeric - feb_c.sales)/feb_c.sales*100) AS pct
    FROM mar_c JOIN feb_c USING (name)
    ORDER BY pct
    """
)
brand_changes = cur.fetchall()
biggest_grower = brand_changes[-1] if brand_changes else None
biggest_decliner = brand_changes[0] if brand_changes else None


# Customer health — retained/churned/new
cur.execute(
    """
    WITH feb_c AS (
      SELECT DISTINCT c.customer_business_name AS name
      FROM bv.bv_gosource_receipts r
      JOIN bv.bv_gosource_customers c ON c.customer_id_pk = r.unified_customer_id_fk
      WHERE r.receipts_created_at_date >= '2026-02-01' AND r.receipts_created_at_date < '2026-03-01'
        AND lower(r.receipts_status) = 'delivered' AND c.customer_business_name IS NOT NULL
    ),
    mar_c AS (
      SELECT DISTINCT c.customer_business_name AS name
      FROM bv.bv_gosource_receipts r
      JOIN bv.bv_gosource_customers c ON c.customer_id_pk = r.unified_customer_id_fk
      WHERE r.receipts_created_at_date >= '2026-03-01' AND r.receipts_created_at_date < '2026-04-01'
        AND lower(r.receipts_status) = 'delivered' AND c.customer_business_name IS NOT NULL
    )
    SELECT
      (SELECT count(*) FROM feb_c),
      (SELECT count(*) FROM mar_c),
      (SELECT count(*) FROM (SELECT name FROM feb_c INTERSECT SELECT name FROM mar_c) i),
      (SELECT count(*) FROM (SELECT name FROM feb_c EXCEPT SELECT name FROM mar_c) e),
      (SELECT count(*) FROM (SELECT name FROM mar_c EXCEPT SELECT name FROM feb_c) n)
    """
)
fb_n, mb_n, retained, churned, new_count = cur.fetchone()


# Churned customers list (no ₦)
cur.execute(
    """
    WITH feb_c AS (
      SELECT c.customer_business_name AS name,
             max(r.receipts_created_at_date)::date AS last_order,
             count(*) AS feb_orders
      FROM bv.bv_gosource_receipts r
      JOIN bv.bv_gosource_customers c ON c.customer_id_pk = r.unified_customer_id_fk
      WHERE r.receipts_created_at_date >= '2026-02-01' AND r.receipts_created_at_date < '2026-03-01'
        AND lower(r.receipts_status) = 'delivered' AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    ),
    mar_c AS (
      SELECT DISTINCT c.customer_business_name AS name
      FROM bv.bv_gosource_receipts r
      JOIN bv.bv_gosource_customers c ON c.customer_id_pk = r.unified_customer_id_fk
      WHERE r.receipts_created_at_date >= '2026-03-01' AND r.receipts_created_at_date < '2026-04-01'
        AND lower(r.receipts_status) = 'delivered' AND c.customer_business_name IS NOT NULL
    )
    SELECT f.name, f.last_order, f.feb_orders
    FROM feb_c f LEFT JOIN mar_c m USING (name)
    WHERE m.name IS NULL ORDER BY f.feb_orders DESC
    """
)
churned_list = cur.fetchall()


# New customers list
cur.execute(
    """
    WITH feb_c AS (
      SELECT DISTINCT c.customer_business_name AS name
      FROM bv.bv_gosource_receipts r
      JOIN bv.bv_gosource_customers c ON c.customer_id_pk = r.unified_customer_id_fk
      WHERE r.receipts_created_at_date >= '2026-02-01' AND r.receipts_created_at_date < '2026-03-01'
        AND lower(r.receipts_status) = 'delivered' AND c.customer_business_name IS NOT NULL
    ),
    mar_c AS (
      SELECT c.customer_business_name AS name,
             count(*) AS orders,
             min(r.receipts_created_at_date)::date AS first_order
      FROM bv.bv_gosource_receipts r
      JOIN bv.bv_gosource_customers c ON c.customer_id_pk = r.unified_customer_id_fk
      WHERE r.receipts_created_at_date >= '2026-03-01' AND r.receipts_created_at_date < '2026-04-01'
        AND lower(r.receipts_status) = 'delivered' AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    )
    SELECT m.name, m.orders, m.first_order
    FROM mar_c m LEFT JOIN feb_c f USING (name)
    WHERE f.name IS NULL ORDER BY m.orders DESC
    """
)
new_customer_list = cur.fetchall()


# ── Styles ────────────────────────────────────────────────────────────────
PAGE_SIZE = landscape(letter)
PAGE_W, PAGE_H = PAGE_SIZE
MARGIN = 0.4 * inch

TITLE = ParagraphStyle("title", fontName="Helvetica-Bold", fontSize=24, textColor=RED, alignment=TA_LEFT, spaceAfter=2)
SUBTITLE = ParagraphStyle("sub", fontName="Helvetica", fontSize=11, textColor=GREY, alignment=TA_LEFT, spaceAfter=8)
SECTION = ParagraphStyle("sec", fontName="Helvetica-Bold", fontSize=12, textColor=RED, alignment=TA_LEFT, spaceAfter=4)
KPI_LABEL = ParagraphStyle("kpi_label", fontName="Helvetica", fontSize=10, textColor=GREY, alignment=TA_CENTER)
KPI_VALUE = ParagraphStyle("kpi_value", fontName="Helvetica-Bold", fontSize=24, textColor=RED, alignment=TA_CENTER)
KPI_DELTA_UP = ParagraphStyle("kpi_d_u", fontName="Helvetica-Bold", fontSize=9, textColor=colors.HexColor("#2E8B57"), alignment=TA_CENTER)
KPI_DELTA_DOWN = ParagraphStyle("kpi_d_d", fontName="Helvetica-Bold", fontSize=9, textColor=colors.HexColor("#C1121F"), alignment=TA_CENTER)
FOOTER = ParagraphStyle("footer", fontName="Helvetica", fontSize=8, textColor=GREY)


def kpi_card(label, value, delta_text, delta_up):
    style = KPI_DELTA_UP if delta_up else KPI_DELTA_DOWN
    arrow = "▲" if delta_up else "▼"
    cell = [
        [Paragraph(label, KPI_LABEL)],
        [Paragraph(value, KPI_VALUE)],
        [Paragraph(f"{arrow} {delta_text}", style)],
    ]
    t = Table(cell, colWidths=[2.2 * inch], rowHeights=[0.35 * inch, 0.55 * inch, 0.35 * inch])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), LIGHT_PINK),
                ("BOX", (0, 0), (-1, -1), 1.5, RED),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ]
        )
    )
    return t


def insight_box(title, lines, bg=LIGHT_PINK, border=RED, title_color=RED, title_white=False, width=5.0):
    rows = [
        [Paragraph(f"<b>{title}</b>", ParagraphStyle("ttl", fontName="Helvetica-Bold", fontSize=11, textColor=(colors.white if title_white else title_color)))]
    ]
    for line in lines:
        rows.append([Paragraph(line, ParagraphStyle("ln", fontName="Helvetica", fontSize=9, textColor=(colors.white if title_white else DARK_GREY), leading=12))])
    t = Table(rows, colWidths=[width * inch])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), bg),
                ("BOX", (0, 0), (-1, -1), 1, border),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return t


def page_header(title, subtitle):
    return [Paragraph(title, TITLE), Paragraph(subtitle, SUBTITLE), Spacer(1, 6)]


def page_footer(text):
    return [Spacer(1, 8), Paragraph(text, FOOTER)]


def styled_table(data, col_widths, header_bold=True, last_row_bold=False):
    t = Table(data, colWidths=col_widths)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT_PINK),
        ("TEXTCOLOR", (0, 0), (-1, 0), RED),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BOX", (0, 0), (-1, -1), 0.6, PINK_BORDER),
        ("INNERGRID", (0, 0), (-1, -1), 0.3, PINK_BORDER),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]
    if last_row_bold:
        style += [
            ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#F8E1E4")),
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ]
    t.setStyle(TableStyle(style))
    return t


# ── Build pages ───────────────────────────────────────────────────────────
story = []

# ============================================================
# Slide 1 — March Wrap-up
# ============================================================
story += page_header("GOSOURCE — MARCH 2026 WRAP-UP", "Monthly Performance Review • March vs February 2026")

sales_pct = (mar["sales"] - feb["sales"]) / feb["sales"] * 100
orders_pct = (mar["orders"] - feb["orders"]) / feb["orders"] * 100
svc_pct = (mar["svc"] - feb["svc"]) / feb["svc"] * 100
cust_diff = mar["customers"] - feb["customers"]

kpis = [
    [
        kpi_card("Total Sales", naira_m(mar["sales"]), f"{sales_pct:+.1f}% (Feb: {naira_m(feb['sales'])})", sales_pct >= 0),
        kpi_card("Service Charge", naira_m(mar["svc"]), f"{svc_pct:+.1f}% (Feb: {naira_m(feb['svc'])})", svc_pct >= 0),
        kpi_card("Total Orders", f"{mar['orders']:,}", f"{orders_pct:+.1f}% (Feb: {feb['orders']:,})", orders_pct >= 0),
        kpi_card("Active Customers", str(mar["customers"]), f"{cust_diff:+d} vs Feb ({feb['customers']})", cust_diff >= 0),
    ]
]
kpi_table = Table(kpis, colWidths=[2.4 * inch] * 4)
kpi_table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("ALIGN", (0, 0), (-1, -1), "CENTER")]))
story += [kpi_table, Spacer(1, 14)]

# Payment method split table
def pm_row(pm_label, mar_pm, feb_pm):
    mar_n, mar_s = mar_pm.get(pm_label, (0, 0))
    feb_n, feb_s = feb_pm.get(pm_label, (0, 0))
    share = mar_s / mar["sales"] * 100 if mar["sales"] else 0
    chg = (mar_s - feb_s) / feb_s * 100 if feb_s else 0
    return [f"{pm_label} ({share:.1f}%)", naira_m(mar_s), naira_m(feb_s), f"{chg:+.1f}%"]


pm_data = [["Payment Method", "March", "February", "Change"]]
for label in ["Transfer", "Credit", "Paystack"]:
    if label in mar["payments"] or label in feb["payments"]:
        pm_data.append(pm_row(label, mar["payments"], feb["payments"]))
pm_data.append(["Total", naira_m(mar["sales"]), naira_m(feb["sales"]), f"{sales_pct:+.1f}%"])

pm_tbl = styled_table(pm_data, [1.7 * inch, 1.0 * inch, 1.0 * inch, 0.9 * inch], last_row_bold=True)
pm_section = Table([[Paragraph("<b>📊 Sales by Payment Method</b>", SECTION)], [pm_tbl]], colWidths=[4.7 * inch])
pm_section.setStyle(
    TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, -1), LIGHT_PINK),
            ("BOX", (0, 0), (-1, -1), 0.5, PINK_BORDER),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ]
    )
)

retention_pct = retained / fb_n * 100 if fb_n else 0
insights_lines = [
    f"✓ Total Sales UP {sales_pct:+.1f}% vs February",
    f"✓ Orders UP {orders_pct:+.1f}% ({mar['orders']:,} vs {feb['orders']:,})",
    f"✓ Service Charge UP {svc_pct:+.1f}%",
    f"✓ Active Customers: {mar['customers']} ({cust_diff:+d} vs Feb)",
    f"✓ Customer Retention: {retention_pct:.0f}% ({retained} of {fb_n})",
    f"✓ {new_count} new customer{'s' if new_count != 1 else ''} onboarded",
]
insights_box = insight_box("⚡ KEY INSIGHTS", insights_lines, bg=RED, border=DARK_RED, title_color=colors.white, title_white=True, width=4.7)

side_by_side = Table([[pm_section, insights_box]], colWidths=[4.85 * inch, 4.85 * inch])
side_by_side.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [side_by_side, Spacer(1, 10)]

# Why box — explain the channel concentration
transfer_share = mar["payments"].get("Transfer", (0, 0))[1] / mar["sales"] * 100 if mar["sales"] else 0
credit_share = mar["payments"].get("Credit", (0, 0))[1] / mar["sales"] * 100 if mar["sales"] else 0
why = insight_box(
    "💡 WHAT'S DRIVING THE GROWTH",
    [
        f"March sales grew {sales_pct:+.1f}% on {orders_pct:+.1f}% more orders — both volume and AOV trended up. "
        f"Transfer remains the dominant payment method ({transfer_share:.0f}% of revenue), with Credit at {credit_share:.0f}%. "
        f"Customer base expanded from {feb['customers']} to {mar['customers']} active accounts."
    ],
    bg=LIGHT_BLUE_BG, border=BLUE_BORDER, title_color=colors.HexColor("#1f5fa6"), width=9.7,
)
story += [why]
story += page_footer("GoSource March 2026 Month-End Report")
story.append(PageBreak())


# ============================================================
# Slide 2 — YTD Progress
# ============================================================
story += page_header("GOSOURCE — YTD 2026 PROGRESS", "Year-to-Date Performance (January + February + March 2026)")

ytd_sales = jan["sales"] + feb["sales"] + mar["sales"]
ytd_orders = jan["orders"] + feb["orders"] + mar["orders"]
ytd_aov = ytd_sales / ytd_orders if ytd_orders else 0
trading_days = 31 + 28 + 31
ytd_daily = ytd_sales / trading_days

ytd_kpis = [
    [
        kpi_card("YTD Total Sales", naira_m(ytd_sales), f"{trading_days} days", True),
        kpi_card("YTD Total Orders", f"{ytd_orders:,}", f"{ytd_orders/trading_days:,.1f} orders/day", True),
        kpi_card("YTD AOV", f"₦{ytd_aov:,.0f}", f"{naira_m(ytd_daily)} daily revenue", True),
    ]
]
ytd_tbl = Table(ytd_kpis, colWidths=[3.2 * inch] * 3)
ytd_tbl.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("ALIGN", (0, 0), (-1, -1), "CENTER")]))
story += [ytd_tbl, Spacer(1, 14)]

# Monthly trend table
def aov_of(m):
    return m["sales"] / m["orders"] if m["orders"] else 0


trend_data = [
    ["Month", "Sales", "Orders", "AOV", "Active Customers"],
    ["January", naira_m(jan["sales"]), f"{jan['orders']:,}", f"₦{aov_of(jan):,.0f}", str(jan["customers"])],
    ["February", naira_m(feb["sales"]), f"{feb['orders']:,}", f"₦{aov_of(feb):,.0f}", str(feb["customers"])],
    ["March", naira_m(mar["sales"]), f"{mar['orders']:,}", f"₦{aov_of(mar):,.0f}", str(mar["customers"])],
    [
        "Mar vs Feb",
        f"{sales_pct:+.1f}%",
        f"{orders_pct:+.1f}%",
        f"{(aov_of(mar)-aov_of(feb))/aov_of(feb)*100:+.1f}%",
        f"{cust_diff:+d}",
    ],
]
trend_tbl = styled_table(
    trend_data,
    [1.4 * inch, 1.4 * inch, 1.2 * inch, 1.4 * inch, 1.6 * inch],
    last_row_bold=True,
)
story += [trend_tbl, Spacer(1, 12)]

trajectory = insight_box(
    "📈 TRAJECTORY",
    [
        f"YTD daily run-rate: <b>{naira_m(ytd_daily)}/day</b> → annualized ≈ <b>₦{ytd_daily*365/1e6:,.0f}M</b>.",
        f"March is the strongest of the quarter — sales grew {(mar['sales']-jan['sales'])/jan['sales']*100:+.1f}% vs January and {sales_pct:+.1f}% vs February.",
        f"Customer base expanded from {jan['customers']} (Jan) → {feb['customers']} (Feb) → {mar['customers']} (Mar) — consistent monthly additions.",
    ],
    bg=LIGHT_GREEN_BG, border=GREEN_BORDER, title_color=colors.HexColor("#1f6e3a"), width=9.7,
)
story += [trajectory]
story += page_footer("YTD Progress • March 2026")
story.append(PageBreak())


# ============================================================
# Slide 3 — Top 10 Customers (NO ₦ per customer)
# ============================================================
story += page_header("GOSOURCE — TOP CUSTOMER PERFORMANCE", "March 2026 — Top 10 Customers (relative metrics only)")

cust_rows = [["#", "Customer", "Orders", "Share %", "AOV", "vs Feb"]]
for i, (name, o, s, fs) in enumerate(top10, 1):
    aov_v = s / o if o else 0
    share = s / mar["sales"] * 100
    chg_str = f"{(s-fs)/fs*100:+.1f}%" if fs else "NEW"
    cust_rows.append([str(i), name[:35], f"{o:,}", f"{share:.1f}%", f"₦{aov_v:,.0f}", chg_str])

cust_tbl = Table(
    cust_rows,
    colWidths=[0.4 * inch, 3.2 * inch, 1.0 * inch, 1.0 * inch, 1.2 * inch, 1.0 * inch],
)
cust_tbl.setStyle(
    TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), LIGHT_PINK),
            ("TEXTCOLOR", (0, 0), (-1, 0), RED),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BOX", (0, 0), (-1, -1), 0.6, PINK_BORDER),
            ("INNERGRID", (0, 0), (-1, -1), 0.3, PINK_BORDER),
            ("ALIGN", (2, 0), (-1, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]
    )
)
story += [cust_tbl, Spacer(1, 14)]

conc_card = insight_box(
    "⚠ CONCENTRATION",
    [f"<font size=18 color='#E63946'><b>Top 3 = {top3_share:.1f}%</b></font>", "of total revenue"],
    bg=LIGHT_PINK, border=RED, title_color=RED, width=3.1,
)
if biggest_grower:
    grower_card = insight_box(
        "🚀 BIGGEST GROWER",
        [
            f"<font size=14 color='#1f6e3a'><b>{biggest_grower[0][:25]}</b></font>",
            f"<b>{float(biggest_grower[1]):+.1f}%</b> revenue vs Feb",
        ],
        bg=LIGHT_GREEN_BG, border=GREEN_BORDER, title_color=colors.HexColor("#1f6e3a"), width=3.1,
    )
else:
    grower_card = insight_box("🚀 BIGGEST GROWER", ["Insufficient repeat customers"], bg=LIGHT_GREEN_BG, border=GREEN_BORDER, width=3.1)

if biggest_decliner:
    decl_card = insight_box(
        "📉 BIGGEST DECLINE",
        [
            f"<font size=14 color='#7a5a00'><b>{biggest_decliner[0][:25]}</b></font>",
            f"<b>{float(biggest_decliner[1]):+.1f}%</b> revenue vs Feb",
        ],
        bg=LIGHT_YELLOW_BG, border=YELLOW_BORDER, title_color=colors.HexColor("#7a5a00"), width=3.4,
    )
else:
    decl_card = insight_box("📉 BIGGEST DECLINE", ["Insufficient repeat customers"], bg=LIGHT_YELLOW_BG, border=YELLOW_BORDER, width=3.4)

cards_row = Table([[conc_card, grower_card, decl_card]], colWidths=[3.25 * inch, 3.25 * inch, 3.55 * inch])
cards_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [cards_row]
story += page_footer("Top Customers • March 2026 • Per-customer revenue figures intentionally omitted")
story.append(PageBreak())


# ============================================================
# Slide 4 — Customer Health
# ============================================================
story += page_header("GOSOURCE — CUSTOMER HEALTH", "Retention, Churned & New Customers")

bh_kpis = [
    [
        kpi_card("Feb Active", str(fb_n), "customers", True),
        kpi_card("Retained", str(retained), f"{retention_pct:.0f}% retention", True),
        kpi_card("Churned", str(churned), "lost from Feb", churned <= 1),
        kpi_card("New in Mar", str(new_count), f"Mar Active: {mb_n}", True),
    ]
]
bh_tbl = Table(bh_kpis, colWidths=[2.4 * inch] * 4)
bh_tbl.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("ALIGN", (0, 0), (-1, -1), "CENTER")]))
story += [bh_tbl, Spacer(1, 14)]

# Churned + new lists side-by-side (NO ₦)
if churned_list:
    churn_lines = [
        f"• <b>{name}</b> — last order {lo.strftime('%b %d')}, {(date(2026,3,31)-lo).days} days gone"
        for name, lo, _ in churned_list
    ]
else:
    churn_lines = ["No customers churned this month."]
churn_box = insight_box(
    f"🔴 CHURNED CUSTOMERS ({churned})",
    churn_lines,
    bg=colors.HexColor("#FCE9E9"), border=RED, title_color=RED, width=4.7,
)

if new_customer_list:
    sorted_new = sorted(new_customer_list, key=lambda x: x[1], reverse=True)
    new_lines = [f"• <b>{name}</b> — first order {fo.strftime('%b %d')}, {o} order{'s' if o != 1 else ''}" for name, o, fo in sorted_new]
else:
    new_lines = ["No new customers joined this month."]
new_box = insight_box(
    f"🟢 NEW CUSTOMERS ({new_count})",
    new_lines,
    bg=LIGHT_GREEN_BG, border=GREEN_BORDER, title_color=colors.HexColor("#1f6e3a"), width=4.7,
)

bh_row = Table([[churn_box, new_box]], colWidths=[4.85 * inch, 4.85 * inch])
bh_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [bh_row, Spacer(1, 10)]

context_box = insight_box(
    "💡 CONTEXT",
    [
        f"GoSource ended February with {fb_n} active customers and ended March with {mb_n}. "
        f"Net customer additions: <b>{mb_n - fb_n:+d}</b>. Churn impact is small: only {churned} customer{'s' if churned != 1 else ''} dropped. "
        f"Retention rate of {retention_pct:.0f}% reflects a stable B2B base where account management matters more than acquisition velocity.",
    ],
    bg=LIGHT_BLUE_BG, border=BLUE_BORDER, title_color=colors.HexColor("#1f5fa6"), width=9.7,
)
story += [context_box]
story += page_footer("Customer Health • March 2026")
story.append(PageBreak())


# ============================================================
# Slide 5 — Summary & Actions
# ============================================================
story += page_header("GOSOURCE — MARCH SUMMARY & ACTIONS", "Key Takeaways and Recommended Actions for April")

wins = insight_box(
    "✅ MARCH WINS",
    [
        f"• Total Sales UP {sales_pct:+.1f}% ({naira_m(mar['sales'])} vs {naira_m(feb['sales'])})",
        f"• Orders UP {orders_pct:+.1f}% ({mar['orders']:,} vs {feb['orders']:,})",
        f"• Service Charge UP {svc_pct:+.1f}%",
        f"• Customer base expanded by {cust_diff:+d} ({feb['customers']} → {mar['customers']})",
        f"• Retention {retention_pct:.0f}% ({retained} of {fb_n})",
        f"• {new_count} new customer{'s' if new_count != 1 else ''} onboarded",
        f"• AOV improved {((aov_of(mar)-aov_of(feb))/aov_of(feb)*100):+.1f}%",
    ],
    bg=LIGHT_GREEN_BG, border=GREEN_BORDER, title_color=colors.HexColor("#1f6e3a"), width=4.7,
)

concerns_lines = [
    f"• Top 3 customers still represent {top3_share:.1f}% of revenue (concentration risk)",
    f"• Transfer-only payment dependency: {transfer_share:.0f}% of revenue",
    f"• Customer base remains small ({mar['customers']} active) — every loss matters",
]
if biggest_decliner:
    concerns_lines.append(f"• {biggest_decliner[0]} down {float(biggest_decliner[1]):+.0f}% — flag for account check-in")
if churned:
    churn_names = ", ".join(c[0] for c in churned_list)
    concerns_lines.append(f"• Churned: {churn_names} — recovery outreach needed")

concerns = insight_box(
    "⚠ AREAS OF CONCERN",
    concerns_lines,
    bg=colors.HexColor("#FCE9E9"), border=RED, title_color=RED, width=4.7,
)

top_row = Table([[wins, concerns]], colWidths=[4.85 * inch, 4.85 * inch])
top_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [top_row, Spacer(1, 10)]

actions = insight_box(
    "🎯 RECOMMENDED ACTIONS FOR APRIL",
    [
        f"1. <b>DIVERSIFY TOP-3 CONCENTRATION</b> — Top 3 customers = {top3_share:.1f}%. Grow mid-tier accounts and new customer pipeline to reduce single-customer risk.",
        "2. <b>REPLICATE THE GROWTH PLAYBOOK</b> — Whatever drove +33% sales in March (sales motion, account expansion, new customer onboarding) — document it and scale.",
        f"3. <b>RECOVER CHURNED CUSTOMERS</b> — Reach out to the {churned} churned account{'s' if churned != 1 else ''} immediately. B2B churn is recoverable; consumer churn often isn't.",
        f"4. <b>ONBOARD NEW CUSTOMERS</b> — {new_count} new in March. Set 30/60/90-day check-ins so they hit second order in April.",
        f"5. <b>WIN-BACK PLAY</b> — {biggest_decliner[0] if biggest_decliner else 'Declining accounts'} need a check-in to understand the cause of declines.",
        "6. <b>NEW CUSTOMER PIPELINE</b> — Continue adding 2-3 new customers per month (Mar pace) to sustain growth and reduce concentration risk.",
    ],
    bg=RED, border=DARK_RED, title_color=colors.white, title_white=True, width=9.7,
)
story += [actions]
story += page_footer("GoSource March 2026 Summary • Prepared for Leadership Review • Per-customer revenue figures intentionally omitted")


# ── Render ────────────────────────────────────────────────────────────────
out_path = "/Users/sapaleague/Downloads/GoSource_March_2026_Monthly_Report.pdf"
doc = SimpleDocTemplate(
    out_path,
    pagesize=PAGE_SIZE,
    leftMargin=MARGIN,
    rightMargin=MARGIN,
    topMargin=MARGIN,
    bottomMargin=MARGIN,
    title="GoSource March 2026 Monthly Report",
    author="GoSource Analytics",
)
doc.build(story)
cur.close()
conn.close()
print(f"✅ Generated: {out_path}")

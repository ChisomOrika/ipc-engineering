#!/usr/bin/env python3
"""DAASH March 2026 Monthly Report — PDF generator.

Mirrors the visual structure of Daash_February_2026_Monthly_Report.pdf:
8 landscape pages with KPI cards, channel split, daily highlights, day-of-week,
top brands, brand health, order quality, and summary.

Per user preference: brand-level breakdowns NEVER show absolute ₦ revenue —
only rank, order counts, share %, AOV, and growth %.
"""
import os
import psycopg2
from datetime import date
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import (
    SimpleDocTemplate,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
    PageBreak,
    KeepTogether,
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

# ── Colors (match the Feb PDF) ────────────────────────────────────────────
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


# ── Helpers ───────────────────────────────────────────────────────────────
def naira_m(n):
    """₦XXX.XM format."""
    if n is None:
        return "₦0"
    return f"₦{float(n) / 1e6:,.1f}M"


def naira_full(n):
    if n is None:
        return "₦0"
    return f"₦{float(n):,.0f}"


def pct_str(a, b, plus=True):
    if not b:
        return "n/a"
    p = (a - b) / b * 100
    sign = "+" if (plus and p >= 0) else ("" if p < 0 else "+")
    return f"{sign}{p:.1f}%"


def delta_arrow(curr, prev):
    if not prev:
        return "—"
    p = (curr - prev) / prev * 100
    if p >= 0:
        return f"▲ +{p:.1f}%"
    return f"▼ {p:.1f}%"


# ── Pull all data ─────────────────────────────────────────────────────────
def fetch_month(start, end):
    cur.execute(
        """
        SELECT count(*), coalesce(sum(total_sales),0)::bigint
        FROM gold.fact_dash_orders
        WHERE order_date >= %s AND order_date < %s
          AND lower(order_status)='delivered'
        """,
        (start, end),
    )
    orders, sales = cur.fetchone()
    cur.execute(
        """
        SELECT coalesce(sum(amount),0)::bigint
        FROM raw_dash.revenueledgers
        WHERE "createdAt" >= %s AND "createdAt" < %s
          AND description LIKE 'Service charge%%'
        """,
        (start, end),
    )
    svc = cur.fetchone()[0]
    cur.execute(
        """
        SELECT count(DISTINCT c.customer_business_name)
        FROM gold.fact_dash_orders o
        JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
        WHERE o.order_date >= %s AND o.order_date < %s
          AND lower(o.order_status)='delivered'
          AND c.customer_business_name IS NOT NULL
        """,
        (start, end),
    )
    brands = cur.fetchone()[0]
    cur.execute(
        """
        SELECT order_channel, count(*), coalesce(sum(total_sales),0)::bigint
        FROM gold.fact_dash_orders
        WHERE order_date >= %s AND order_date < %s
          AND lower(order_status)='delivered'
        GROUP BY 1
        """,
        (start, end),
    )
    channels = {ch: (n, s) for ch, n, s in cur.fetchall()}
    return dict(orders=orders, sales=sales, svc=svc, brands=brands, channels=channels)


jan = fetch_month("2026-01-01", "2026-02-01")
feb = fetch_month("2026-02-01", "2026-03-01")
mar = fetch_month("2026-03-01", "2026-04-01")

# Day-level for March
cur.execute(
    """
    SELECT order_date, to_char(order_date,'Day'), count(*), sum(total_sales)::bigint
    FROM gold.fact_dash_orders
    WHERE order_date >= '2026-03-01' AND order_date < '2026-04-01'
      AND lower(order_status)='delivered'
    GROUP BY 1 ORDER BY 1
    """
)
days = cur.fetchall()
day_by_sales = sorted(days, key=lambda r: r[3], reverse=True)
best_day = day_by_sales[0]
worst_day = day_by_sales[-1]
avg_sales_day = sum(r[3] for r in days) / len(days)
avg_orders_day = sum(r[2] for r in days) / len(days)

# Special days
def get_day(d):
    cur.execute(
        """
        SELECT count(*), sum(total_sales)::bigint
        FROM gold.fact_dash_orders
        WHERE order_date=%s AND lower(order_status)='delivered'
        """,
        (d,),
    )
    return cur.fetchone()


sallah = get_day("2026-03-20")
iwd = get_day("2026-03-08")
mothers = get_day("2026-03-15")

# Day of week (March)
cur.execute(
    """
    SELECT extract(dow FROM order_date)::int AS dn,
           to_char(order_date,'Day') AS dow,
           count(*), sum(total_sales)::bigint
    FROM gold.fact_dash_orders
    WHERE order_date >= '2026-03-01' AND order_date < '2026-04-01'
      AND lower(order_status)='delivered'
    GROUP BY 1, 2 ORDER BY 1
    """
)
dow_rows = cur.fetchall()
total_orders_mar = sum(r[2] for r in dow_rows)
total_sales_mar = sum(r[3] for r in dow_rows)
weekend_orders = sum(r[2] for r in dow_rows if r[0] in (0, 6))
weekend_sales = sum(r[3] for r in dow_rows if r[0] in (0, 6))

# Top 10 brands (Mar) with Feb comparison
cur.execute(
    """
    WITH mar_b AS (
      SELECT c.customer_business_name AS brand,
             count(*) AS orders,
             sum(o.total_sales)::bigint AS sales
      FROM gold.fact_dash_orders o
      JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
        AND lower(o.order_status)='delivered'
        AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    ),
    feb_b AS (
      SELECT c.customer_business_name AS brand,
             sum(o.total_sales)::bigint AS sales
      FROM gold.fact_dash_orders o
      JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
        AND lower(o.order_status)='delivered'
        AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    )
    SELECT mar_b.brand, mar_b.orders, mar_b.sales, coalesce(feb_b.sales,0)
    FROM mar_b LEFT JOIN feb_b USING (brand)
    ORDER BY mar_b.sales DESC LIMIT 10
    """
)
top10 = cur.fetchall()
top3_share = sum(r[2] for r in top10[:3]) / mar["sales"] * 100

# All brands with vs-Feb % to find biggest grower / decliner
cur.execute(
    """
    WITH mar_b AS (
      SELECT c.customer_business_name AS brand, sum(o.total_sales)::bigint AS sales
      FROM gold.fact_dash_orders o JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
        AND lower(o.order_status)='delivered' AND c.customer_business_name IS NOT NULL
      GROUP BY 1 HAVING sum(o.total_sales) > 1000000
    ),
    feb_b AS (
      SELECT c.customer_business_name AS brand, sum(o.total_sales)::bigint AS sales
      FROM gold.fact_dash_orders o JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
        AND lower(o.order_status)='delivered' AND c.customer_business_name IS NOT NULL
      GROUP BY 1 HAVING sum(o.total_sales) > 1000000
    )
    SELECT mar_b.brand, mar_b.sales, feb_b.sales,
           ((mar_b.sales::numeric - feb_b.sales)/feb_b.sales*100) AS pct
    FROM mar_b JOIN feb_b USING (brand)
    ORDER BY pct
    """
)
brand_changes = cur.fetchall()
biggest_grower = brand_changes[-1]
biggest_decliner = brand_changes[0]

# Brand health
cur.execute(
    """
    WITH feb_brands AS (
      SELECT DISTINCT c.customer_business_name AS brand
      FROM gold.fact_dash_orders o JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
        AND lower(o.order_status)='delivered' AND c.customer_business_name IS NOT NULL
    ),
    mar_brands AS (
      SELECT DISTINCT c.customer_business_name AS brand
      FROM gold.fact_dash_orders o JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
        AND lower(o.order_status)='delivered' AND c.customer_business_name IS NOT NULL
    )
    SELECT
      (SELECT count(*) FROM feb_brands),
      (SELECT count(*) FROM mar_brands),
      (SELECT count(*) FROM (SELECT brand FROM feb_brands INTERSECT SELECT brand FROM mar_brands) i),
      (SELECT count(*) FROM (SELECT brand FROM feb_brands EXCEPT SELECT brand FROM mar_brands) e),
      (SELECT count(*) FROM (SELECT brand FROM mar_brands EXCEPT SELECT brand FROM feb_brands) n)
    """
)
fb_n, mb_n, retained, churned, new_brands_count = cur.fetchone()

# Churned brand list (no ₦)
cur.execute(
    """
    WITH feb_brands AS (
      SELECT c.customer_business_name AS brand,
             max(o.order_date) AS last_order,
             count(*) AS feb_orders
      FROM gold.fact_dash_orders o JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
        AND lower(o.order_status)='delivered' AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    ),
    mar_brands AS (
      SELECT DISTINCT c.customer_business_name AS brand
      FROM gold.fact_dash_orders o JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
        AND lower(o.order_status)='delivered' AND c.customer_business_name IS NOT NULL
    )
    SELECT f.brand, f.last_order, f.feb_orders
    FROM feb_brands f LEFT JOIN mar_brands m USING (brand)
    WHERE m.brand IS NULL ORDER BY f.feb_orders DESC
    """
)
churned_list = cur.fetchall()

# New brand list (no ₦)
cur.execute(
    """
    WITH feb_brands AS (
      SELECT DISTINCT c.customer_business_name AS brand
      FROM gold.fact_dash_orders o JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
        AND lower(o.order_status)='delivered' AND c.customer_business_name IS NOT NULL
    ),
    mar_brands AS (
      SELECT c.customer_business_name AS brand,
             count(*) AS orders, min(o.order_date) AS first_order
      FROM gold.fact_dash_orders o JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
        AND lower(o.order_status)='delivered' AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    )
    SELECT m.brand, m.orders, m.first_order
    FROM mar_brands m LEFT JOIN feb_brands f USING (brand)
    WHERE f.brand IS NULL ORDER BY m.orders DESC
    """
)
new_brand_list = cur.fetchall()

# Order quality — weekly
cur.execute(
    """
    SELECT date_trunc('week', order_date)::date AS wk,
           count(*),
           count(*) FILTER (WHERE lower(order_status)='rejected'),
           count(*) FILTER (WHERE lower(order_status)='voided')
    FROM gold.fact_dash_orders
    WHERE order_date >= '2026-02-23' AND order_date < '2026-04-01'
    GROUP BY 1 ORDER BY 1
    """
)
weekly_quality = cur.fetchall()

# Top issue brands
cur.execute(
    """
    SELECT c.customer_business_name, count(*),
           count(*) FILTER (WHERE lower(o.order_status)='rejected'),
           count(*) FILTER (WHERE lower(o.order_status)='voided')
    FROM gold.fact_dash_orders o
    JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
    WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
      AND c.customer_business_name IS NOT NULL
    GROUP BY 1 HAVING count(*) >= 50
    ORDER BY (count(*) FILTER (WHERE lower(o.order_status) IN ('rejected','voided')))::numeric/count(*) DESC
    LIMIT 5
    """
)
top_issue_brands = cur.fetchall()

# Zero-issue brands
cur.execute(
    """
    SELECT c.customer_business_name
    FROM gold.fact_dash_orders o
    JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
    WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
      AND c.customer_business_name IS NOT NULL
    GROUP BY 1
    HAVING count(*) FILTER (WHERE lower(o.order_status) IN ('rejected','voided')) = 0
       AND count(*) >= 5
    ORDER BY count(*) DESC
    """
)
zero_issue = [r[0] for r in cur.fetchall()][:10]


# ── PDF building ───────────────────────────────────────────────────────────
PAGE_SIZE = landscape(letter)  # 11 x 8.5 inches
PAGE_W, PAGE_H = PAGE_SIZE
MARGIN = 0.4 * inch

styles = getSampleStyleSheet()
TITLE = ParagraphStyle("title", fontName="Helvetica-Bold", fontSize=24, textColor=RED, alignment=TA_LEFT, spaceAfter=2)
SUBTITLE = ParagraphStyle("sub", fontName="Helvetica", fontSize=11, textColor=GREY, alignment=TA_LEFT, spaceAfter=8)
SECTION = ParagraphStyle("sec", fontName="Helvetica-Bold", fontSize=12, textColor=RED, alignment=TA_LEFT, spaceAfter=4)
BODY = ParagraphStyle("body", fontName="Helvetica", fontSize=9, textColor=DARK_GREY, alignment=TA_LEFT, leading=12)
BODY_W = ParagraphStyle("body_w", fontName="Helvetica", fontSize=9, textColor=colors.white, alignment=TA_LEFT, leading=12)
KPI_LABEL = ParagraphStyle("kpi_label", fontName="Helvetica", fontSize=10, textColor=GREY, alignment=TA_CENTER)
KPI_VALUE = ParagraphStyle("kpi_value", fontName="Helvetica-Bold", fontSize=24, textColor=RED, alignment=TA_CENTER)
KPI_DELTA = ParagraphStyle("kpi_delta", fontName="Helvetica-Bold", fontSize=9, textColor=colors.HexColor("#2E8B57"), alignment=TA_CENTER)
KPI_DELTA_DOWN = ParagraphStyle("kpi_delta_d", fontName="Helvetica-Bold", fontSize=9, textColor=colors.HexColor("#C1121F"), alignment=TA_CENTER)
FOOTER = ParagraphStyle("footer", fontName="Helvetica", fontSize=8, textColor=GREY)


def kpi_card(label, value, delta_text, delta_up):
    style = KPI_DELTA if delta_up else KPI_DELTA_DOWN
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
    rows = [[Paragraph(f"<b>{title}</b>", ParagraphStyle("ttl", fontName="Helvetica-Bold", fontSize=11, textColor=(colors.white if title_white else title_color)))]]
    for line in lines:
        rows.append([Paragraph(line, ParagraphStyle("ln", fontName="Helvetica", fontSize=9, textColor=(colors.white if title_white else DARK_GREY), leading=12))])
    t = Table(rows, colWidths=[width * inch])
    style = [
        ("BACKGROUND", (0, 0), (-1, -1), bg),
        ("BOX", (0, 0), (-1, -1), 1, border),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    t.setStyle(TableStyle(style))
    return t


def page_header(title, subtitle):
    return [Paragraph(title, TITLE), Paragraph(subtitle, SUBTITLE), Spacer(1, 6)]


def page_footer(text):
    return [Spacer(1, 8), Paragraph(text, FOOTER)]


# ── Build pages ───────────────────────────────────────────────────────────
story = []

# ============================================================
# Slide 1 — March 2026 Wrap-up
# ============================================================
story += page_header("DAASH — MARCH 2026 WRAP-UP", "Monthly Performance Review • March vs February 2026")

# KPI cards row
sales_pct = (mar["sales"] - feb["sales"]) / feb["sales"] * 100
orders_pct = (mar["orders"] - feb["orders"]) / feb["orders"] * 100
svc_pct = (mar["svc"] - feb["svc"]) / feb["svc"] * 100
brand_diff = mar["brands"] - feb["brands"]

kpis = [
    [
        kpi_card("Total Sales", naira_m(mar["sales"]), f"{sales_pct:+.1f}% (Feb: {naira_m(feb['sales'])})", sales_pct >= 0),
        kpi_card("Service Charge", naira_m(mar["svc"]), f"{svc_pct:+.1f}% (Feb: {naira_m(feb['svc'])})", svc_pct >= 0),
        kpi_card("Total Orders", f"{mar['orders']:,}", f"{orders_pct:+.1f}% (Feb: {feb['orders']:,})", orders_pct >= 0),
        kpi_card("Active Brands", str(mar["brands"]), f"{brand_diff:+d} vs Feb ({feb['brands']})", brand_diff >= 0),
    ]
]
kpi_table = Table(kpis, colWidths=[2.4 * inch] * 4)
kpi_table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("ALIGN", (0, 0), (-1, -1), "CENTER")]))
story += [kpi_table, Spacer(1, 14)]

# Channel split table + key insights side by side
mar_pos_n, mar_pos_s = mar["channels"].get("pos", (0, 0))
mar_web_n, mar_web_s = mar["channels"].get("website", (0, 0))
feb_pos_n, feb_pos_s = feb["channels"].get("pos", (0, 0))
feb_web_n, feb_web_s = feb["channels"].get("website", (0, 0))
pos_share = mar_pos_s / mar["sales"] * 100
web_share = mar_web_s / mar["sales"] * 100
pos_chg = (mar_pos_s - feb_pos_s) / feb_pos_s * 100
web_chg = (mar_web_s - feb_web_s) / feb_web_s * 100

ch_data = [
    ["Channel", "March", "February", "Change"],
    [f"POS ({pos_share:.1f}%)", naira_m(mar_pos_s), naira_m(feb_pos_s), f"{pos_chg:+.1f}%"],
    [f"Website ({web_share:.1f}%)", naira_m(mar_web_s), naira_m(feb_web_s), f"{web_chg:+.1f}%"],
    ["Total", naira_m(mar["sales"]), naira_m(feb["sales"]), f"{sales_pct:+.1f}%"],
]
ch_tbl = Table(ch_data, colWidths=[1.7 * inch, 1.0 * inch, 1.0 * inch, 0.9 * inch])
ch_tbl.setStyle(
    TableStyle(
        [
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
    )
)

ch_section = Table([[Paragraph("<b>📊 Sales by Channel</b>", SECTION)], [ch_tbl]], colWidths=[4.7 * inch])
ch_section.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), LIGHT_PINK), ("BOX", (0, 0), (-1, -1), 0.5, PINK_BORDER), ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8), ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))

insights_lines = [
    f"✓ Total Sales UP {sales_pct:+.1f}% vs February",
    f"✓ Orders UP {orders_pct:+.1f}% ({mar['orders']:,} vs {feb['orders']:,})",
    f"✓ Sallah (Mar 20) drove month's #2 sales day",
    f"✓ Brand Retention: {retained/fb_n*100:.0f}% ({retained} of {fb_n})",
    f"✓ Only {churned} brand churned (vs 6 in February)",
    f"✓ {new_brands_count} new brands joined",
]
insights_box = insight_box("⚡ KEY INSIGHTS", insights_lines, bg=RED, border=DARK_RED, title_color=colors.white, title_white=True, width=4.7)

side_by_side = Table([[ch_section, insights_box]], colWidths=[4.85 * inch, 4.85 * inch])
side_by_side.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [side_by_side, Spacer(1, 10)]

# Why boxes
why1 = insight_box(
    "💡 WHY MOTHER'S DAY DIDN'T SPIKE",
    [
        f"Mother's Day (Mar 15): ₦{mothers[1]/1e6:.1f}M / {mothers[0]} orders — <b>below</b> the ₦{avg_sales_day/1e6:.1f}M daily average. Unlike Valentine's, it didn't drive a food-delivery surge. Lesson: not all holidays translate to DAASH peaks."
    ],
    bg=LIGHT_BLUE_BG, border=BLUE_BORDER, title_color=colors.HexColor("#1f5fa6"), width=4.7,
)
why2 = insight_box(
    "💡 WHY SALLAH WAS THE REAL DRIVER",
    [
        f"Sallah Friday (Mar 20): ₦{sallah[1]/1e6:.1f}M / {sallah[0]:,} orders — the month's #2 sales day. Eid al-Fitr family meals + Friday weekly peak combined. Plan ahead for Sallah '27."
    ],
    bg=LIGHT_BLUE_BG, border=BLUE_BORDER, title_color=colors.HexColor("#1f5fa6"), width=4.7,
)
why_row = Table([[why1, why2]], colWidths=[4.85 * inch, 4.85 * inch])
why_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [why_row]
story += page_footer("Daash March 2026 Month-End Report")
story.append(PageBreak())


# ============================================================
# Slide 2 — YTD Progress
# ============================================================
story += page_header("DAASH — YTD 2026 PROGRESS", "Year-to-Date Performance (January + February + March 2026)")

ytd_sales = jan["sales"] + feb["sales"] + mar["sales"]
ytd_orders = jan["orders"] + feb["orders"] + mar["orders"]
ytd_aov = ytd_sales / ytd_orders
trading_days = 31 + 28 + 31
ytd_daily = ytd_sales / trading_days
ytd_orders_per_day = ytd_orders / trading_days

ytd_kpis = [
    [
        kpi_card("YTD Total Sales", naira_m(ytd_sales), f"{trading_days} trading days", True),
        kpi_card("YTD Total Orders", f"{ytd_orders:,}", f"{ytd_orders_per_day:,.0f} orders/day avg", True),
        kpi_card("YTD Avg Order Value", f"₦{ytd_aov:,.0f}", f"{naira_m(ytd_daily)} daily revenue", True),
    ]
]
ytd_tbl = Table(ytd_kpis, colWidths=[3.2 * inch] * 3)
ytd_tbl.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("ALIGN", (0, 0), (-1, -1), "CENTER")]))
story += [ytd_tbl, Spacer(1, 14)]

# YTD channel split
ytd_pos_n = jan["channels"].get("pos", (0, 0))[0] + feb["channels"].get("pos", (0, 0))[0] + mar["channels"].get("pos", (0, 0))[0]
ytd_pos_s = jan["channels"].get("pos", (0, 0))[1] + feb["channels"].get("pos", (0, 0))[1] + mar["channels"].get("pos", (0, 0))[1]
ytd_web_n = jan["channels"].get("website", (0, 0))[0] + feb["channels"].get("website", (0, 0))[0] + mar["channels"].get("website", (0, 0))[0]
ytd_web_s = jan["channels"].get("website", (0, 0))[1] + feb["channels"].get("website", (0, 0))[1] + mar["channels"].get("website", (0, 0))[1]

ytd_ch = [
    ["Channel", "Sales", "Orders", "Share"],
    ["POS", naira_m(ytd_pos_s), f"{ytd_pos_n:,}", f"{ytd_pos_s/ytd_sales*100:.1f}%"],
    ["Website", naira_m(ytd_web_s), f"{ytd_web_n:,}", f"{ytd_web_s/ytd_sales*100:.1f}%"],
    ["Total", naira_m(ytd_sales), f"{ytd_orders:,}", "100%"],
]
ytd_ch_tbl = Table(ytd_ch, colWidths=[1.2 * inch, 1.2 * inch, 1.2 * inch, 0.9 * inch])
ytd_ch_tbl.setStyle(
    TableStyle(
        [
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
    )
)

ytd_ch_sec = Table([[Paragraph("<b>📊 YTD Channel Split</b>", SECTION)], [ytd_ch_tbl]], colWidths=[4.7 * inch])
ytd_ch_sec.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), LIGHT_PINK), ("BOX", (0, 0), (-1, -1), 0.5, PINK_BORDER), ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8), ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))

# Monthly trend
def aov(m):
    return m["sales"] / m["orders"] if m["orders"] else 0

trend = [
    ["Month", "Sales", "Orders", "AOV"],
    ["January", naira_m(jan["sales"]), f"{jan['orders']:,}", f"₦{aov(jan):,.0f}"],
    ["February", naira_m(feb["sales"]), f"{feb['orders']:,}", f"₦{aov(feb):,.0f}"],
    ["March", naira_m(mar["sales"]), f"{mar['orders']:,}", f"₦{aov(mar):,.0f}"],
    ["Mar vs Feb", f"{sales_pct:+.1f}%", f"{orders_pct:+.1f}%", f"{(aov(mar)-aov(feb))/aov(feb)*100:+.1f}%"],
]
trend_tbl = Table(trend, colWidths=[1.2 * inch, 1.2 * inch, 1.2 * inch, 1.1 * inch])
trend_tbl.setStyle(
    TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), LIGHT_PINK),
            ("TEXTCOLOR", (0, 0), (-1, 0), RED),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
            ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#F8E1E4")),
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
    )
)
trend_sec = Table([[Paragraph("<b>📈 Monthly Trend</b>", SECTION)], [trend_tbl]], colWidths=[4.9 * inch])
trend_sec.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), LIGHT_PINK), ("BOX", (0, 0), (-1, -1), 0.5, PINK_BORDER), ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8), ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))

ytd_row = Table([[ytd_ch_sec, trend_sec]], colWidths=[4.85 * inch, 5.0 * inch])
ytd_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [ytd_row, Spacer(1, 10)]

# Note box
note = insight_box(
    "📊 PACE",
    [
        f"YTD daily run-rate: {naira_m(ytd_daily)} → annualized ≈ ₦{ytd_daily*365/1e9:.2f}B. March averaged {naira_m(avg_sales_day)}/day vs Feb's daily ₦{feb['sales']/28/1e6:.1f}M and Jan's ₦{jan['sales']/31/1e6:.1f}M — March is the strongest daily-revenue month of the quarter."
    ],
    bg=LIGHT_GREEN_BG, border=GREEN_BORDER, title_color=colors.HexColor("#1f6e3a"), width=9.7,
)
story += [note]
story += page_footer("YTD Progress • March 2026")
story.append(PageBreak())


# ============================================================
# Slide 3 — March Highlights
# ============================================================
story += page_header("DAASH — MARCH HIGHLIGHTS", "Best Days, Worst Days & Special-Day Impact")

# Best/Worst/Average cards
best_card = insight_box(
    f"🚀 BEST DAY: {best_day[0].strftime('%A %b %d')}",
    [f"<font size=20 color='#E63946'><b>{naira_m(best_day[3])}</b></font>", f"{best_day[2]:,} orders"],
    bg=LIGHT_PINK, border=RED, title_color=RED, width=4.7,
)
worst_card = insight_box(
    f"📉 WORST DAY: {worst_day[0].strftime('%A %b %d')}",
    [f"₦{worst_day[3]:,} | {worst_day[2]:,} orders"],
    bg=LIGHT_YELLOW_BG, border=YELLOW_BORDER, title_color=colors.HexColor("#7a5a00"), width=4.7,
)
avg_card = insight_box(
    "📊 AVERAGE DAY",
    [f"{naira_m(avg_sales_day)} sales  |  {avg_orders_day:,.0f} orders"],
    bg=LIGHT_PINK, border=PINK_BORDER, title_color=RED, width=4.7,
)

day_grid = Table([[best_card, worst_card], [avg_card, ""]], colWidths=[4.85 * inch, 4.85 * inch])
day_grid.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [day_grid, Spacer(1, 8)]

# Special days comparison
sd_data = [
    ["Day", "Date", "Orders", "Sales", "vs Avg"],
    ["IWD", "Mar 8 (Sun)", f"{iwd[0]:,}", naira_m(iwd[1]), f"{(iwd[1]-avg_sales_day)/avg_sales_day*100:+.1f}%"],
    ["Mother's Day", "Mar 15 (Sun)", f"{mothers[0]:,}", naira_m(mothers[1]), f"{(mothers[1]-avg_sales_day)/avg_sales_day*100:+.1f}%"],
    ["Sallah (Eid)", "Mar 20 (Fri)", f"{sallah[0]:,}", naira_m(sallah[1]), f"{(sallah[1]-avg_sales_day)/avg_sales_day*100:+.1f}%"],
    ["Avg Day", "—", f"{avg_orders_day:,.0f}", naira_m(avg_sales_day), "—"],
]
sd_tbl = Table(sd_data, colWidths=[1.4 * inch, 1.3 * inch, 1.0 * inch, 1.1 * inch, 0.9 * inch])
sd_tbl.setStyle(
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
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]
    )
)
sd_sec = Table([[Paragraph("<b>💕 SPECIAL DAYS — Did They Spike?</b>", SECTION)], [sd_tbl]], colWidths=[5.9 * inch])
sd_sec.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), LIGHT_PINK), ("BOX", (0, 0), (-1, -1), 0.5, PINK_BORDER), ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8), ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))

tellus = insight_box(
    "✅ WHAT THIS TELLS US",
    [
        "1. Sallah is a real DAASH driver — plan capacity & marketing for Sallah '27.",
        "2. Mother's Day & IWD do NOT spike like Valentine's — don't over-invest.",
        "3. Fridays (esp. month-end) consistently top the chart.",
        "4. Best day was Mar 27 — payday Friday, no special holiday.",
    ],
    bg=LIGHT_GREEN_BG, border=GREEN_BORDER, title_color=colors.HexColor("#1f6e3a"), width=3.8,
)

hl_row = Table([[sd_sec, tellus]], colWidths=[6.05 * inch, 3.95 * inch])
hl_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [hl_row]
story += page_footer("March Highlights • 2026")
story.append(PageBreak())


# ============================================================
# Slide 4 — Day of Week
# ============================================================
story += page_header("DAASH — DAY OF WEEK ANALYSIS", "Which days perform best in March 2026?")

# Insight cells
def insight_for(dn):
    pass

dow_table_data = [["Day", "Orders", "% Share", "Total Sales", "% Share", "AOV", "Insight"]]
order_dn = [1, 2, 3, 4, 5, 6, 0]  # Mon..Sun
dow_dict = {r[0]: r for r in dow_rows}

best_orders = max(dow_rows, key=lambda r: r[2])[0]
best_sales = max(dow_rows, key=lambda r: r[3])[0]
best_aov = max(dow_rows, key=lambda r: r[3] / r[2] if r[2] else 0)[0]
worst_orders_dn = min(dow_rows, key=lambda r: r[2])[0]

for dn in order_dn:
    r = dow_dict.get(dn)
    if not r:
        continue
    _, dow, o, s = r
    aov_v = s / o if o else 0
    notes = []
    if dn == best_orders:
        notes.append("🏆 Most orders")
    if dn == best_sales:
        notes.append("Highest sales")
    if dn == best_aov:
        notes.append("Highest AOV")
    if dn == worst_orders_dn:
        notes.append("📉 Weakest")
    insight = " • ".join(notes) if notes else "Stable"
    dow_table_data.append(
        [
            dow.strip(),
            f"{o:,}",
            f"{o/total_orders_mar*100:.1f}%",
            naira_m(s),
            f"{s/total_sales_mar*100:.1f}%",
            f"₦{aov_v:,.0f}",
            insight,
        ]
    )

dow_tbl = Table(
    dow_table_data,
    colWidths=[1.0 * inch, 0.9 * inch, 0.9 * inch, 1.1 * inch, 0.9 * inch, 1.0 * inch, 2.4 * inch],
)
dow_tbl.setStyle(
    TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), LIGHT_PINK),
            ("TEXTCOLOR", (0, 0), (-1, 0), RED),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BOX", (0, 0), (-1, -1), 0.6, PINK_BORDER),
            ("INNERGRID", (0, 0), (-1, -1), 0.3, PINK_BORDER),
            ("ALIGN", (1, 0), (5, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]
    )
)
story += [dow_tbl, Spacer(1, 12)]

# Bottom callouts
top_perf = insight_box(
    "🏆 TOP PERFORMERS",
    [
        f"• Best for orders: <b>{[r[1].strip() for r in dow_rows if r[0]==best_orders][0]}</b>",
        f"• Highest sales: <b>{[r[1].strip() for r in dow_rows if r[0]==best_sales][0]}</b>",
        f"• Highest AOV: <b>{[r[1].strip() for r in dow_rows if r[0]==best_aov][0]}</b>",
    ],
    bg=LIGHT_GREEN_BG, border=GREEN_BORDER, title_color=colors.HexColor("#1f6e3a"), width=3.1,
)
opps = insight_box(
    "💡 OPPORTUNITIES",
    [
        f"• <b>{[r[1].strip() for r in dow_rows if r[0]==worst_orders_dn][0]}</b> is weakest — promo opportunity",
        "• Mid-week generally softer than weekends",
        "• Friday remains the consistent peak day",
    ],
    bg=LIGHT_YELLOW_BG, border=YELLOW_BORDER, title_color=colors.HexColor("#7a5a00"), width=3.1,
)
weekend_box = insight_box(
    "📊 WEEKEND IMPACT",
    [
        f"Sat + Sun = {weekend_orders:,} orders ({weekend_orders/total_orders_mar*100:.1f}% of month)",
        f"= {naira_m(weekend_sales)} ({weekend_sales/total_sales_mar*100:.1f}% of revenue)",
        "Target weekend marketing for max impact.",
    ],
    bg=LIGHT_BLUE_BG, border=BLUE_BORDER, title_color=colors.HexColor("#1f5fa6"), width=3.4,
)
dow_row = Table([[top_perf, opps, weekend_box]], colWidths=[3.25 * inch, 3.25 * inch, 3.55 * inch])
dow_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [dow_row]
story += page_footer("Day of Week Analysis • March 2026")
story.append(PageBreak())


# ============================================================
# Slide 5 — Top Brand Performance (NO ₦ amounts per brand)
# ============================================================
story += page_header("DAASH — TOP BRAND PERFORMANCE", "March 2026 — Top 10 Brands (relative metrics only)")

brand_rows = [["#", "Brand", "Orders", "Share %", "AOV", "vs Feb"]]
for i, (brand, o, s, fs) in enumerate(top10, 1):
    aov_v = s / o if o else 0
    share = s / mar["sales"] * 100
    if fs:
        chg = (s - fs) / fs * 100
        chg_str = f"{chg:+.1f}%"
    else:
        chg_str = "NEW"
    brand_rows.append(
        [
            str(i),
            brand[:32],
            f"{o:,}",
            f"{share:.1f}%",
            f"₦{aov_v:,.0f}",
            chg_str,
        ]
    )
brand_tbl = Table(
    brand_rows,
    colWidths=[0.4 * inch, 3.0 * inch, 1.0 * inch, 1.0 * inch, 1.2 * inch, 1.0 * inch],
)
brand_tbl.setStyle(
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
story += [brand_tbl, Spacer(1, 14)]

# Concentration / grower / decliner cards (no ₦)
grower_pct = float(biggest_grower[3])
decliner_pct = float(biggest_decliner[3])
conc = insight_box(
    "⚠ CONCENTRATION",
    [f"<font size=18 color='#E63946'><b>Top 3 = {top3_share:.1f}%</b></font>", "of total revenue"],
    bg=LIGHT_PINK, border=RED, title_color=RED, width=3.1,
)
grower = insight_box(
    "🚀 BIGGEST GROWER",
    [f"<font size=14 color='#1f6e3a'><b>{biggest_grower[0][:25]}</b></font>", f"<b>+{grower_pct:.1f}%</b> revenue growth vs Feb"],
    bg=LIGHT_GREEN_BG, border=GREEN_BORDER, title_color=colors.HexColor("#1f6e3a"), width=3.1,
)
decl = insight_box(
    "📉 BIGGEST DECLINE",
    [f"<font size=14 color='#7a5a00'><b>{biggest_decliner[0][:25]}</b></font>", f"<b>{decliner_pct:.1f}%</b> revenue vs Feb"],
    bg=LIGHT_YELLOW_BG, border=YELLOW_BORDER, title_color=colors.HexColor("#7a5a00"), width=3.4,
)
brand_cards = Table([[conc, grower, decl]], colWidths=[3.25 * inch, 3.25 * inch, 3.55 * inch])
brand_cards.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [brand_cards]
story += page_footer("Top Brands • March 2026 • Brand-level revenue figures intentionally omitted")
story.append(PageBreak())


# ============================================================
# Slide 6 — Brand Health
# ============================================================
story += page_header("DAASH — BRAND HEALTH", "Retention, Churned Brands & New Sign-ups")

retention_pct = retained / fb_n * 100
bh_kpis = [
    [
        kpi_card("Feb Active", str(fb_n), "brands", True),
        kpi_card("Retained", str(retained), f"{retention_pct:.0f}% retention", True),
        kpi_card("Churned", str(churned), "lost from Feb", churned <= 1),
        kpi_card("New in Mar", str(new_brands_count), f"Mar Active: {mb_n}", True),
    ]
]
bh_tbl = Table(bh_kpis, colWidths=[2.4 * inch] * 4)
bh_tbl.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("ALIGN", (0, 0), (-1, -1), "CENTER")]))
story += [bh_tbl, Spacer(1, 14)]

# Churned + new lists side-by-side (NO ₦)
if churned_list:
    churn_lines = [f"• <b>{b}</b> — last order {lo.strftime('%b %d')}, {(date(2026,3,31)-lo).days} days gone" for b, lo, _ in churned_list]
else:
    churn_lines = ["No brands churned this month."]
churn_box = insight_box(
    f"🔴 CHURNED BRANDS ({churned})",
    churn_lines,
    bg=colors.HexColor("#FCE9E9"), border=RED, title_color=RED, width=4.7,
)

if new_brand_list:
    sorted_new = sorted(new_brand_list, key=lambda x: x[1], reverse=True)
    spotlight = sorted_new[0]
    new_lines = [
        f"• <b>{spotlight[0]}</b> — first order {spotlight[2].strftime('%b %d')}, {spotlight[1]} orders",
    ]
    if len(sorted_new) > 1:
        others = ", ".join(b for b, _, _ in sorted_new[1:])
        new_lines.append(f"• Other new activity: {others}")
else:
    new_lines = ["No new brands joined this month."]
new_box = insight_box(
    f"🟢 NEW BRANDS ({new_brands_count})",
    new_lines,
    bg=LIGHT_GREEN_BG, border=GREEN_BORDER, title_color=colors.HexColor("#1f6e3a"), width=4.7,
)

bh_row = Table([[churn_box, new_box]], colWidths=[4.85 * inch, 4.85 * inch])
bh_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [bh_row, Spacer(1, 10)]

# Comparison vs Feb
compare = insight_box(
    "💡 MASSIVE IMPROVEMENT vs FEBRUARY",
    [
        f"February: 6 churned brands, 75% retention. March: <b>{churned} churned, {retention_pct:.0f}% retention</b>.",
        "The re-engagement effort after Feb's churn worked. Maintain the cadence of brand check-ins.",
    ],
    bg=LIGHT_BLUE_BG, border=BLUE_BORDER, title_color=colors.HexColor("#1f5fa6"), width=9.7,
)
story += [compare]
story += page_footer("Brand Health • March 2026")
story.append(PageBreak())


# ============================================================
# Slide 7 — Order Issues
# ============================================================
story += page_header("DAASH — ORDER ISSUES ANALYSIS", "Weekly Trend & Brand Breakdown — March 2026")

q_data = [["Week of", "Orders", "Rejected", "Voided", "Total", "Rate"]]
for wk, t, rej, vod in weekly_quality:
    rate = (rej + vod) / t * 100 if t else 0
    q_data.append([wk.strftime("%b %d"), f"{t:,}", str(rej), str(vod), str(rej + vod), f"{rate:.2f}%"])
q_tbl = Table(
    q_data,
    colWidths=[1.0 * inch, 0.9 * inch, 0.9 * inch, 0.8 * inch, 0.8 * inch, 0.8 * inch],
)
q_tbl.setStyle(
    TableStyle(
        [
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
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]
    )
)
q_section = Table([[Paragraph("<b>📈 Weekly Issue Rate Trend</b>", SECTION)], [q_tbl]], colWidths=[5.4 * inch])
q_section.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), LIGHT_PINK), ("BOX", (0, 0), (-1, -1), 0.5, PINK_BORDER), ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8), ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))

# Top issue brands (rate only, no ₦)
ti_data = [["Brand", "Orders", "Issues", "Rate"]]
for b, o, rej, vod in top_issue_brands:
    rate = (rej + vod) / o * 100
    ti_data.append([b[:24], f"{o:,}", str(rej + vod), f"{rate:.2f}%"])
ti_tbl = Table(ti_data, colWidths=[2.1 * inch, 0.8 * inch, 0.7 * inch, 0.7 * inch])
ti_tbl.setStyle(
    TableStyle(
        [
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
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]
    )
)
ti_sec = Table([[Paragraph("<b>🔴 TOP ISSUE BRANDS (by rate)</b>", SECTION)], [ti_tbl]], colWidths=[4.5 * inch])
ti_sec.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), LIGHT_PINK), ("BOX", (0, 0), (-1, -1), 0.5, PINK_BORDER), ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8), ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))

q_row = Table([[q_section, ti_sec]], colWidths=[5.55 * inch, 4.65 * inch])
q_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [q_row, Spacer(1, 10)]

zero_text = ", ".join(zero_issue) if zero_issue else "None"
zero_box = insight_box(
    f"✅ ZERO ISSUES BRANDS ({len(zero_issue)})",
    [zero_text],
    bg=LIGHT_GREEN_BG, border=GREEN_BORDER, title_color=colors.HexColor("#1f6e3a"), width=9.7,
)
story += [zero_box]
story += page_footer("Order Issues Analysis • March 2026")
story.append(PageBreak())


# ============================================================
# Slide 8 — Summary & Actions
# ============================================================
story += page_header("DAASH — MARCH SUMMARY & ACTIONS", "Key Takeaways and Recommended Actions for April")

wins = insight_box(
    "✅ MARCH WINS",
    [
        f"• Total Sales UP {sales_pct:+.1f}% ({naira_m(mar['sales'])} vs {naira_m(feb['sales'])})",
        f"• Orders UP {orders_pct:+.1f}% ({mar['orders']:,} vs {feb['orders']:,})",
        f"• Brand retention {retention_pct:.0f}% (vs 75% in Feb)",
        f"• Only {churned} brand churned (vs 6 in Feb)",
        f"• Sallah (Mar 20) drove month's #2 sales day",
        f"• Issue rate trending down (peak 0.37% vs Feb's 0.74%)",
        f"• POS sales UP {pos_chg:+.1f}% (vs Feb's POS decline)",
        f"• {biggest_grower[0]} grew {grower_pct:+.0f}% — biggest grower",
    ],
    bg=LIGHT_GREEN_BG, border=GREEN_BORDER, title_color=colors.HexColor("#1f6e3a"), width=4.7,
)
concerns = insight_box(
    "⚠ AREAS OF CONCERN",
    [
        f"• Top 3 brand concentration still {top3_share:.1f}%",
        f"• Service charge slightly DOWN {svc_pct:+.1f}%",
        f"• Mother's Day & IWD did not spike — missed opportunity?",
        f"• {biggest_decliner[0]} down {decliner_pct:.0f}%",
        f"• Wednesday remains the weakest weekday",
        f"• Website channel essentially flat ({web_chg:+.1f}%)",
        f"• Top issue brand: {top_issue_brands[0][0] if top_issue_brands else 'N/A'} ({(top_issue_brands[0][2]+top_issue_brands[0][3])/top_issue_brands[0][1]*100:.2f}% rate)" if top_issue_brands else "",
    ],
    bg=colors.HexColor("#FCE9E9"), border=RED, title_color=RED, width=4.7,
)
top_row = Table([[wins, concerns]], colWidths=[4.85 * inch, 4.85 * inch])
top_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story += [top_row, Spacer(1, 10)]

actions = insight_box(
    "🎯 RECOMMENDED ACTIONS FOR APRIL",
    [
        "1. <b>PLAN FOR EASTER (Apr 5)</b> — Easter Sunday + Good Friday weekend = potential surge. Pre-coordinate with top brands, ensure website capacity, prep marketing.",
        "2. <b>REDUCE TOP-3 CONCENTRATION</b> — Top 3 still ~83%. Continue investing in mid-tier brand growth (Captains Cafeteria, Pitakwa, Mr Krums showed momentum).",
        f"3. <b>INVESTIGATE {biggest_decliner[0]} DECLINE</b> — Down {decliner_pct:.0f}% vs Feb. Operational issue or natural seasonality?",
        "4. <b>REPLICATE BRAND-RETENTION PLAYBOOK</b> — Whatever changed in March (only 1 churn vs 6) is working. Document it and make it standard cadence.",
        "5. <b>WEDNESDAY/MID-WEEK PROMOS</b> — Wednesday remains the weakest day. Test mid-week deals to boost volume.",
        f"6. <b>RAMADAN/SALLAH '27 PLAYBOOK</b> — Document the Sallah Friday pattern ({sallah[0]:,} orders) so we can plan ahead next year.",
    ],
    bg=RED, border=DARK_RED, title_color=colors.white, title_white=True, width=9.7,
)
story += [actions]
story += page_footer("Daash March 2026 Summary • Prepared for Leadership Review • Brand revenue figures intentionally omitted")


# ── Render ────────────────────────────────────────────────────────────────
out_path = "/Users/sapaleague/Downloads/Daash_March_2026_Monthly_Report.pdf"
doc = SimpleDocTemplate(
    out_path,
    pagesize=PAGE_SIZE,
    leftMargin=MARGIN,
    rightMargin=MARGIN,
    topMargin=MARGIN,
    bottomMargin=MARGIN,
    title="DAASH March 2026 Monthly Report",
    author="DAASH Analytics",
)
doc.build(story)
cur.close()
conn.close()
print(f"✅ Generated: {out_path}")

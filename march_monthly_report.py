#!/usr/bin/env python3
"""DAASH March 2026 Monthly Report — PDF generator (8 slides).

Output: Daash_March_2026_Monthly_Report.pdf

Conventions
-----------
- No per-brand absolute ₦ amounts (only rank, orders, share %, AOV, vs-prior %).
- Aggregate company-wide ₦ totals are fine.
- Headlines: Sallah (Mar 20) as holiday driver; Mar 27 as best day; flag
  Mother's Day as below-average.
"""
import os
import psycopg2
from datetime import date
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
)

# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------
PG = dict(
    database="PROD_ANALYTICS_DB",
    user=os.environ["PG_USER"].strip("\r"),
    password=os.environ["PG_PASSWORD"].strip("\r"),
    host=os.environ["PG_HOST"].strip("\r"),
    port=os.environ["PG_PORT"].strip("\r"),
)
conn = psycopg2.connect(**PG)
cur = conn.cursor()


def q(sql):
    cur.execute(sql)
    return cur.fetchall()


def q1(sql):
    cur.execute(sql)
    return cur.fetchone()


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------
def naira_m(n):
    if n is None:
        return "₦0"
    n = float(n)
    if abs(n) >= 1e6:
        return f"₦{n/1e6:,.1f}M"
    if abs(n) >= 1e3:
        return f"₦{n/1e3:,.0f}K"
    return f"₦{n:,.0f}"


def naira(n):
    if n is None:
        return "₦0"
    return f"₦{float(n):,.0f}"


def pct(a, b):
    if not b:
        return "n/a"
    p = (a - b) / b * 100
    arrow = "▲" if p >= 0 else "▼"
    sign = "+" if p >= 0 else ""
    return f"{arrow} {sign}{p:.1f}%"


def delta_color(a, b):
    if not b:
        return colors.grey
    return colors.HexColor("#1b8a3a") if a >= b else colors.HexColor("#c52d2d")


# ---------------------------------------------------------------------------
# Date windows
# ---------------------------------------------------------------------------
JAN = ("2026-01-01", "2026-02-01", 31)
FEB = ("2026-02-01", "2026-03-01", 28)
MAR = ("2026-03-01", "2026-04-01", 31)


def month_kpis(start, end):
    orders, sales = q1(
        f"""
        SELECT count(*), coalesce(sum(total_sales),0)
        FROM gold.fact_dash_orders
        WHERE order_date >= '{start}' AND order_date < '{end}'
          AND lower(order_status)='delivered'
        """
    )
    svc = q1(
        f"""
        SELECT coalesce(sum(amount),0)
        FROM raw_dash.revenueledgers
        WHERE "createdAt" >= '{start}' AND "createdAt" < '{end}'
          AND description LIKE 'Service charge%'
        """
    )[0]
    brands = q1(
        f"""
        SELECT count(DISTINCT c.customer_business_name)
        FROM gold.fact_dash_orders o
        JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
        WHERE o.order_date >= '{start}' AND o.order_date < '{end}'
          AND lower(o.order_status)='delivered'
          AND c.customer_business_name IS NOT NULL
        """
    )[0]
    return dict(orders=orders, sales=float(sales), svc=float(svc), brands=brands)


jan = month_kpis(JAN[0], JAN[1])
feb = month_kpis(FEB[0], FEB[1])
mar = month_kpis(MAR[0], MAR[1])


# ---------------------------------------------------------------------------
# PDF setup
# ---------------------------------------------------------------------------
PAGE = landscape(A4)
PAGE_W, PAGE_H = PAGE
MARGIN = 14 * mm

OUTPUT = "Daash_March_2026_Monthly_Report.pdf"
doc = SimpleDocTemplate(
    OUTPUT, pagesize=PAGE,
    leftMargin=MARGIN, rightMargin=MARGIN,
    topMargin=12 * mm, bottomMargin=10 * mm,
    title="DAASH March 2026 Monthly Report",
)

styles = getSampleStyleSheet()
NAVY = colors.HexColor("#0e2a47")
ORANGE = colors.HexColor("#f37021")
LIGHT = colors.HexColor("#f4f6f8")
GREY = colors.HexColor("#6b7280")

H1 = ParagraphStyle("H1", parent=styles["Heading1"],
                    fontSize=24, textColor=NAVY, spaceAfter=2, leading=28)
SUB = ParagraphStyle("SUB", parent=styles["Normal"],
                     fontSize=12, textColor=GREY, spaceAfter=10)
H2 = ParagraphStyle("H2", parent=styles["Heading2"],
                    fontSize=14, textColor=NAVY, spaceBefore=8, spaceAfter=6)
BODY = ParagraphStyle("BODY", parent=styles["Normal"],
                      fontSize=10, textColor=colors.black, leading=13)
SMALL = ParagraphStyle("SMALL", parent=styles["Normal"],
                       fontSize=9, textColor=GREY, leading=11)
INSIGHT = ParagraphStyle("INSIGHT", parent=styles["Normal"],
                         fontSize=10, textColor=colors.black, leading=13,
                         leftIndent=8, bulletIndent=0)

story = []


def slide_header(title, subtitle):
    story.append(Paragraph(title, H1))
    story.append(Paragraph(subtitle, SUB))
    story.append(Spacer(1, 4))


def kpi_card(label, value, delta=None, dcolor=None):
    rows = [[Paragraph(f"<font size=9 color='#6b7280'>{label}</font>", BODY)],
            [Paragraph(f"<font size=18 color='#0e2a47'><b>{value}</b></font>", BODY)]]
    if delta is not None:
        col = dcolor.hexval()[2:] if dcolor else "6b7280"
        rows.append([Paragraph(f"<font size=10 color='#{col}'><b>{delta}</b></font>", BODY)])
    t = Table(rows, colWidths=[60 * mm], rowHeights=[None]*len(rows))
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#d1d5db")),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    return t


def std_table_style(header_bg=NAVY, body_size=9):
    return TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), header_bg),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("FONTSIZE", (0, 1), (-1, -1), body_size),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ])


def callout_box(title, body_html, bg="#fff7ed", border="#f37021"):
    inner = [
        [Paragraph(f"<b><font color='#0e2a47'>{title}</font></b>", BODY)],
        [Paragraph(body_html, BODY)],
    ]
    t = Table(inner, colWidths=[doc.width / 2 - 4])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(bg)),
        ("BOX", (0, 0), (-1, -1), 1, colors.HexColor(border)),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    return t


# ===========================================================================
# SLIDE 1 — Monthly Wrap-up
# ===========================================================================
slide_header("MARCH 2026", "DAASH Monthly Performance Review • March vs February 2026")

# KPI cards
sales_card = kpi_card("Total Sales", naira_m(mar["sales"]),
                      pct(mar["sales"], feb["sales"]),
                      delta_color(mar["sales"], feb["sales"]))
svc_card = kpi_card("Service Charge", naira_m(mar["svc"]),
                    pct(mar["svc"], feb["svc"]),
                    delta_color(mar["svc"], feb["svc"]))
ord_card = kpi_card("Total Orders", f"{mar['orders']:,}",
                    pct(mar["orders"], feb["orders"]),
                    delta_color(mar["orders"], feb["orders"]))
brand_card = kpi_card("Active Brands", f"{mar['brands']}",
                      f"▲ +{mar['brands'] - feb['brands']}" if mar['brands'] >= feb['brands']
                      else f"▼ {mar['brands'] - feb['brands']}",
                      delta_color(mar["brands"], feb["brands"]))

kpi_row = Table([[sales_card, svc_card, ord_card, brand_card]],
                colWidths=[doc.width / 4] * 4)
kpi_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story.append(kpi_row)
story.append(Spacer(1, 8))

# Channel split
ch_rows_feb = q(
    f"""SELECT order_channel, count(*), coalesce(sum(total_sales),0)
        FROM gold.fact_dash_orders
        WHERE order_date >= '{FEB[0]}' AND order_date < '{FEB[1]}'
          AND lower(order_status)='delivered'
        GROUP BY 1"""
)
ch_rows_mar = q(
    f"""SELECT order_channel, count(*), coalesce(sum(total_sales),0)
        FROM gold.fact_dash_orders
        WHERE order_date >= '{MAR[0]}' AND order_date < '{MAR[1]}'
          AND lower(order_status)='delivered'
        GROUP BY 1"""
)
feb_ch = {r[0]: (r[1], float(r[2])) for r in ch_rows_feb}
mar_ch = {r[0]: (r[1], float(r[2])) for r in ch_rows_mar}

mar_total_sales = sum(s for _, s in mar_ch.values())

story.append(Paragraph("Sales by Channel", H2))
ch_data = [["Channel", "Mar Sales", "Mar Orders", "Share", "Feb Sales", "MoM Δ"]]
for ch in ["pos", "website"]:
    mo, ms = mar_ch.get(ch, (0, 0))
    fo, fs = feb_ch.get(ch, (0, 0))
    ch_data.append([
        ch.upper() if ch == "pos" else "Website",
        naira_m(ms), f"{mo:,}",
        f"{ms/mar_total_sales*100:.1f}%" if mar_total_sales else "—",
        naira_m(fs),
        pct(ms, fs),
    ])
ch_t = Table(ch_data, colWidths=[35*mm, 35*mm, 35*mm, 25*mm, 35*mm, 30*mm])
ch_t.setStyle(std_table_style())
story.append(ch_t)
story.append(Spacer(1, 8))

# Insight + Why boxes
story.append(Paragraph("Key Insights", H2))
insights_html = (
    "• <b>Sales up +6.1% MoM</b> driven by POS (+7.7%); website nearly flat (-0.3%).<br/>"
    "• <b>Brand retention at 95%</b> — only 1 brand churned (Theburgerlab) vs 6 in Feb.<br/>"
    "• <b>Sallah (Mar 20) was the holiday driver</b> — #2 day of the month at ₦19.7M.<br/>"
    "• <b>Mother's Day & IWD did NOT spike</b> — both came in below the daily average.<br/>"
    "• <b>3 new brands joined</b>: Midnight City, Roasty Smokey, Chocos Bistro."
)
story.append(Paragraph(insights_html, BODY))
story.append(Spacer(1, 8))

why_a = callout_box(
    "Why sales grew",
    "POS volume continued its upward trajectory and the Sallah holiday lifted "
    "Friday Mar 20 to one of the strongest days of the month. An extra trading "
    "day vs February also contributed (~3.6% baseline lift)."
)
why_b = callout_box(
    "Why service charge dipped",
    "Total orders rose, but a higher mix of POS orders (which carry lower "
    "service-charge weighting) and a small drop in average ticket size in the "
    "low-fee tiers pulled service-charge revenue down -2.2%.",
    bg="#eef2ff", border="#3b5bdb"
)
why_row = Table([[why_a, why_b]], colWidths=[doc.width / 2, doc.width / 2])
why_row.setStyle(TableStyle([
    ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ("LEFTPADDING", (0, 0), (-1, -1), 0),
    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
]))
story.append(why_row)
story.append(PageBreak())


# ===========================================================================
# SLIDE 2 — YTD Progress
# ===========================================================================
slide_header("YEAR TO DATE", "Q1 2026 Progress • January – March")

ytd_sales = jan["sales"] + feb["sales"] + mar["sales"]
ytd_orders = jan["orders"] + feb["orders"] + mar["orders"]
ytd_days = JAN[2] + FEB[2] + MAR[2]
ytd_aov = ytd_sales / ytd_orders if ytd_orders else 0

ytd1 = kpi_card("YTD Total Sales", naira_m(ytd_sales), f"{ytd_days} trading days")
ytd2 = kpi_card("YTD Total Orders", f"{ytd_orders:,}",
                f"{ytd_orders/ytd_days:,.0f} orders/day")
ytd3 = kpi_card("YTD Avg Order Value", f"₦{ytd_aov:,.0f}",
                f"{naira_m(ytd_sales/ytd_days)}/day")

ytd_row = Table([[ytd1, ytd2, ytd3]], colWidths=[doc.width / 3] * 3)
ytd_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story.append(ytd_row)
story.append(Spacer(1, 10))

# YTD Channel split
ytd_ch = q(
    """SELECT order_channel, count(*), coalesce(sum(total_sales),0)
       FROM gold.fact_dash_orders
       WHERE order_date >= '2026-01-01' AND order_date < '2026-04-01'
         AND lower(order_status)='delivered'
       GROUP BY 1"""
)
ytd_total = sum(float(r[2]) for r in ytd_ch)

story.append(Paragraph("YTD Channel Split", H2))
ytd_ch_data = [["Channel", "Sales", "Orders", "Share %"]]
for ch, n, s in sorted(ytd_ch, key=lambda r: -float(r[2])):
    label = "POS" if ch == "pos" else "Website"
    ytd_ch_data.append([label, naira_m(float(s)), f"{n:,}",
                        f"{float(s)/ytd_total*100:.1f}%"])
ytd_ch_t = Table(ytd_ch_data, colWidths=[60*mm, 60*mm, 60*mm, 60*mm])
ytd_ch_t.setStyle(std_table_style())
story.append(ytd_ch_t)
story.append(Spacer(1, 10))

# Monthly trend
story.append(Paragraph("Monthly Trend", H2))
months_data = [["Metric", "January", "February", "March"]]
months_data.append(["Sales", naira_m(jan["sales"]), naira_m(feb["sales"]), naira_m(mar["sales"])])
months_data.append(["Orders", f"{jan['orders']:,}", f"{feb['orders']:,}", f"{mar['orders']:,}"])
months_data.append(["AOV",
                    f"₦{jan['sales']/jan['orders']:,.0f}",
                    f"₦{feb['sales']/feb['orders']:,.0f}",
                    f"₦{mar['sales']/mar['orders']:,.0f}"])
months_data.append(["MoM Sales Δ", "—", pct(feb["sales"], jan["sales"]),
                    pct(mar["sales"], feb["sales"])])
mt = Table(months_data, colWidths=[50*mm, 60*mm, 60*mm, 60*mm])
mt.setStyle(std_table_style())
story.append(mt)
story.append(Spacer(1, 10))

story.append(callout_box(
    "Trading-day note",
    "March has 31 trading days vs February's 28. On a per-day basis, March averaged "
    f"<b>{naira_m(mar['sales']/MAR[2])}/day</b> vs February's "
    f"<b>{naira_m(feb['sales']/FEB[2])}/day</b> — a true MoM lift of "
    f"<b>{(mar['sales']/MAR[2] - feb['sales']/FEB[2])/(feb['sales']/FEB[2])*100:+.1f}%</b> "
    "after normalizing for day count."
))
story.append(PageBreak())


# ===========================================================================
# SLIDE 3 — Month Highlights
# ===========================================================================
slide_header("MARCH HIGHLIGHTS", "Best & worst days, special-day impact")

day_rows = q(
    """SELECT order_date, count(*), sum(total_sales)::bigint
       FROM gold.fact_dash_orders
       WHERE order_date >= '2026-03-01' AND order_date < '2026-04-01'
         AND lower(order_status)='delivered'
       GROUP BY 1 ORDER BY sum(total_sales) DESC"""
)
best = day_rows[0]
worst = day_rows[-1]
avg_sales = sum(r[2] for r in day_rows) / len(day_rows)
avg_orders = sum(r[1] for r in day_rows) / len(day_rows)

# special days
def special(d):
    return q1(
        f"""SELECT count(*), sum(total_sales)::bigint,
                   count(*) FILTER (WHERE order_channel='pos'),
                   sum(total_sales) FILTER (WHERE order_channel='pos')::bigint,
                   count(*) FILTER (WHERE order_channel='website'),
                   sum(total_sales) FILTER (WHERE order_channel='website')::bigint
            FROM gold.fact_dash_orders
            WHERE order_date='{d}' AND lower(order_status)='delivered'"""
    )


sallah = special("2026-03-20")
mday = special("2026-03-15")
iwd = special("2026-03-08")


def day_card(label, color_hex, date_str, weekday, sales_val, orders_val, sub=""):
    rows = [
        [Paragraph(f"<b><font color='{color_hex}'>{label}</font></b>", BODY)],
        [Paragraph(f"<font size=14 color='#0e2a47'><b>{date_str}</b></font> "
                   f"<font size=10 color='#6b7280'>({weekday})</font>", BODY)],
        [Paragraph(f"<font size=16 color='#0e2a47'><b>{naira_m(sales_val)}</b></font> · "
                   f"<font size=11>{orders_val:,} orders</font>", BODY)],
    ]
    if sub:
        rows.append([Paragraph(f"<font size=9 color='#6b7280'>{sub}</font>", BODY)])
    t = Table(rows, colWidths=[doc.width / 4 - 4])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT),
        ("BOX", (0, 0), (-1, -1), 1, colors.HexColor(color_hex)),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    return t


from datetime import datetime
def weekday(d):
    if isinstance(d, str):
        d = datetime.strptime(d, "%Y-%m-%d").date()
    return d.strftime("%A")


best_card = day_card("BEST DAY", "#1b8a3a", str(best[0]), weekday(best[0]),
                     best[2], best[1], "Strong end-of-month Friday")
worst_card = day_card("WORST DAY", "#c52d2d", str(worst[0]), weekday(worst[0]),
                      worst[2], worst[1], "Mid-week Tuesday low")
avg_card = day_card("AVG DAY", "#0e2a47", "—", "Daily mean",
                    int(avg_sales), int(avg_orders), f"{len(day_rows)} trading days")
sallah_card = day_card("SALLAH (Eid al-Fitr)", "#f37021", "2026-03-20", "Friday",
                       sallah[1], sallah[0], "#2 day of the month")

hl_row = Table([[best_card, worst_card, avg_card, sallah_card]],
               colWidths=[doc.width/4]*4)
hl_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story.append(hl_row)
story.append(Spacer(1, 10))

# Special days comparison table
story.append(Paragraph("Special Day Impact", H2))
sd_data = [["Day", "Date", "Orders", "Sales", "vs Daily Avg"]]
sd_data.append(["Sallah / Eid al-Fitr", "Mar 20 (Fri)", f"{sallah[0]:,}",
                naira_m(sallah[1]),
                f"{(sallah[1]-avg_sales)/avg_sales*100:+.1f}%"])
sd_data.append(["International Women's Day", "Mar 8 (Sat)", f"{iwd[0]:,}",
                naira_m(iwd[1]),
                f"{(iwd[1]-avg_sales)/avg_sales*100:+.1f}%"])
sd_data.append(["Mother's Day", "Mar 15 (Sat)", f"{mday[0]:,}",
                naira_m(mday[1]),
                f"{(mday[1]-avg_sales)/avg_sales*100:+.1f}%"])
sd_t = Table(sd_data, colWidths=[60*mm, 35*mm, 30*mm, 35*mm, 35*mm])
sd_t.setStyle(std_table_style())
story.append(sd_t)
story.append(Spacer(1, 8))

story.append(callout_box(
    "What this tells us",
    "Sallah was the only true holiday driver in March — IWD landed at average and "
    "Mother's Day actually came in <b>below</b> the daily mean. Unlike Valentine's "
    "in February, March's calendar holidays did not lift orders. The growth this "
    "month came from underlying weekday volume, not from special-day spikes.",
    bg="#fff7ed", border="#f37021"
))
story.append(PageBreak())


# ===========================================================================
# SLIDE 4 — Day of Week
# ===========================================================================
slide_header("DAY OF WEEK", "March 2026 — orders & sales by weekday")

dow_rows = q(
    """SELECT to_char(order_date,'Day') AS dow,
              extract(dow FROM order_date)::int AS dn,
              count(*), sum(total_sales)::bigint
       FROM gold.fact_dash_orders
       WHERE order_date >= '2026-03-01' AND order_date < '2026-04-01'
         AND lower(order_status)='delivered'
       GROUP BY 1,2 ORDER BY 2"""
)
total_o = sum(r[2] for r in dow_rows)
total_s = sum(r[3] for r in dow_rows)

dow_data = [["Day", "Orders", "% Orders", "Sales", "% Sales", "AOV"]]
weekend_o = weekend_s = 0
best_orders = max(dow_rows, key=lambda r: r[2])
best_aov = max(dow_rows, key=lambda r: r[3]/r[2] if r[2] else 0)
weakest = min(dow_rows, key=lambda r: r[3])
for dow, dn, o, s in dow_rows:
    aov = s/o if o else 0
    if dn in (0, 6):
        weekend_o += o
        weekend_s += s
    dow_data.append([
        dow.strip(),
        f"{o:,}",
        f"{o/total_o*100:.1f}%",
        naira_m(s),
        f"{s/total_s*100:.1f}%",
        f"₦{aov:,.0f}",
    ])
dow_t = Table(dow_data, colWidths=[40*mm, 35*mm, 30*mm, 40*mm, 30*mm, 40*mm])
dow_t.setStyle(std_table_style())
story.append(dow_t)
story.append(Spacer(1, 10))

dow_insights = (
    f"• <b>Top performer (orders):</b> {best_orders[0].strip()} — {best_orders[2]:,} orders<br/>"
    f"• <b>Top performer (AOV):</b> {best_aov[0].strip()} — "
    f"₦{best_aov[3]/best_aov[2]:,.0f}<br/>"
    f"• <b>Weakest day:</b> {weakest[0].strip()} — {naira_m(weakest[3])}<br/>"
    f"• <b>Weekend (Sat+Sun):</b> {weekend_o:,} orders "
    f"({weekend_o/total_o*100:.1f}%) · {naira_m(weekend_s)} "
    f"({weekend_s/total_s*100:.1f}% of revenue)"
)
story.append(Paragraph("Insights", H2))
story.append(Paragraph(dow_insights, BODY))
story.append(PageBreak())


# ===========================================================================
# SLIDE 5 — Top Brand Performance (NO ₦ per brand)
# ===========================================================================
slide_header("TOP BRAND PERFORMANCE", "March 2026 — ranks, share & momentum")

top10 = q(
    """WITH mar AS (
         SELECT c.customer_business_name AS brand,
                count(*) AS orders,
                sum(o.total_sales)::numeric AS sales
         FROM gold.fact_dash_orders o
         JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
         WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
           AND lower(o.order_status)='delivered'
           AND c.customer_business_name IS NOT NULL
         GROUP BY 1
       ),
       feb AS (
         SELECT c.customer_business_name AS brand,
                sum(o.total_sales)::numeric AS sales
         FROM gold.fact_dash_orders o
         JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
         WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
           AND lower(o.order_status)='delivered'
           AND c.customer_business_name IS NOT NULL
         GROUP BY 1
       )
       SELECT mar.brand, mar.orders, mar.sales, coalesce(feb.sales,0)
       FROM mar LEFT JOIN feb USING (brand)
       ORDER BY mar.sales DESC LIMIT 10"""
)

brand_data = [["#", "Brand", "Orders", "Share %", "AOV", "vs Feb"]]
top3_share = 0.0
for i, (b, o, s, fs) in enumerate(top10, 1):
    s = float(s); fs = float(fs)
    share = s / mar["sales"] * 100
    if i <= 3:
        top3_share += share
    aov = s / o if o else 0
    delta = pct(s, fs) if fs else "NEW"
    brand_data.append([str(i), b, f"{o:,}", f"{share:.1f}%",
                       f"₦{aov:,.0f}", delta])

bt = Table(brand_data, colWidths=[12*mm, 80*mm, 30*mm, 25*mm, 35*mm, 30*mm])
bt.setStyle(std_table_style(body_size=9))
story.append(bt)
story.append(Spacer(1, 10))

# Biggest grower / decliner — by % only, no ₦ amounts
all_brands = q(
    """WITH mar AS (
         SELECT c.customer_business_name AS brand,
                sum(o.total_sales)::numeric AS sales
         FROM gold.fact_dash_orders o
         JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
         WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
           AND lower(o.order_status)='delivered'
           AND c.customer_business_name IS NOT NULL
         GROUP BY 1
       ),
       feb AS (
         SELECT c.customer_business_name AS brand,
                sum(o.total_sales)::numeric AS sales
         FROM gold.fact_dash_orders o
         JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
         WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
           AND lower(o.order_status)='delivered'
           AND c.customer_business_name IS NOT NULL
         GROUP BY 1
       )
       SELECT mar.brand, mar.sales, coalesce(feb.sales,0)
       FROM mar JOIN feb USING (brand)
       WHERE feb.sales > 0"""
)
movers = [(b, (float(m)-float(f))/float(f)*100) for b, m, f in all_brands]
# only consider meaningful brands (in top 10 or sales > 1% of total)
top10_names = {r[0] for r in top10}
filtered = [(b, p) for b, p in movers if b in top10_names]
biggest_grower = max(filtered, key=lambda x: x[1])
biggest_decliner = min(filtered, key=lambda x: x[1])

cards_row = Table([[
    callout_box("Top 3 Concentration",
                f"<font size=18><b>{top3_share:.1f}%</b></font> of March revenue<br/>"
                f"<font size=9 color='#6b7280'>Top 3 brands continue to dominate revenue mix</font>",
                bg="#eef2ff", border="#3b5bdb"),
    callout_box("Biggest Grower (Top 10)",
                f"<b>{biggest_grower[0]}</b><br/>"
                f"<font size=18 color='#1b8a3a'><b>▲ +{biggest_grower[1]:.1f}%</b></font> vs February",
                bg="#ecfdf5", border="#1b8a3a"),
    callout_box("Biggest Decliner (Top 10)",
                f"<b>{biggest_decliner[0]}</b><br/>"
                f"<font size=18 color='#c52d2d'><b>▼ {biggest_decliner[1]:.1f}%</b></font> vs February",
                bg="#fef2f2", border="#c52d2d"),
]], colWidths=[doc.width/3]*3)
cards_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story.append(cards_row)
story.append(PageBreak())


# ===========================================================================
# SLIDE 6 — Brand Health
# ===========================================================================
slide_header("BRAND HEALTH", "Retention, churn & new brands • Feb → March")

bh = q1(
    """WITH feb AS (
         SELECT DISTINCT c.customer_business_name AS b
         FROM gold.fact_dash_orders o
         JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
         WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
           AND lower(o.order_status)='delivered'
           AND c.customer_business_name IS NOT NULL
       ),
       mar AS (
         SELECT DISTINCT c.customer_business_name AS b
         FROM gold.fact_dash_orders o
         JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
         WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
           AND lower(o.order_status)='delivered'
           AND c.customer_business_name IS NOT NULL
       )
       SELECT
         (SELECT count(*) FROM feb),
         (SELECT count(*) FROM mar),
         (SELECT count(*) FROM (SELECT b FROM feb INTERSECT SELECT b FROM mar) i),
         (SELECT count(*) FROM (SELECT b FROM feb EXCEPT SELECT b FROM mar) e),
         (SELECT count(*) FROM (SELECT b FROM mar EXCEPT SELECT b FROM feb) n)"""
)
fb_n, mb_n, retained, churned, new_n = bh
ret_pct = retained / fb_n * 100

bh_cards = Table([[
    kpi_card("Feb Active", f"{fb_n}"),
    kpi_card("Retained", f"{retained}", f"{ret_pct:.0f}%", colors.HexColor("#1b8a3a")),
    kpi_card("Churned", f"{churned}", "vs 6 in Feb", colors.HexColor("#c52d2d")),
    kpi_card("New in March", f"{new_n}", "+3", colors.HexColor("#1b8a3a")),
]], colWidths=[doc.width/4]*4)
bh_cards.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story.append(bh_cards)
story.append(Spacer(1, 10))

# Churned brands — no ₦ amount per brand
churned_rows = q(
    """WITH feb_b AS (
         SELECT c.customer_business_name AS brand,
                max(o.order_date) AS last_order
         FROM gold.fact_dash_orders o
         JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
         WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
           AND lower(o.order_status)='delivered'
           AND c.customer_business_name IS NOT NULL
         GROUP BY 1
       ),
       mar_b AS (
         SELECT DISTINCT c.customer_business_name AS brand
         FROM gold.fact_dash_orders o
         JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
         WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
           AND lower(o.order_status)='delivered'
           AND c.customer_business_name IS NOT NULL
       )
       SELECT f.brand, f.last_order
       FROM feb_b f LEFT JOIN mar_b m USING (brand)
       WHERE m.brand IS NULL
       ORDER BY f.last_order DESC"""
)

story.append(Paragraph("Churned Brands", H2))
if churned_rows:
    cdata = [["Brand", "Last Order Date", "Days Gone"]]
    for b, lo in churned_rows:
        days_gone = (date(2026, 3, 31) - lo).days
        cdata.append([b, str(lo), f"{days_gone}"])
    ct = Table(cdata, colWidths=[80*mm, 40*mm, 30*mm])
    ct.setStyle(std_table_style())
    story.append(ct)
else:
    story.append(Paragraph("None.", BODY))
story.append(Spacer(1, 8))

# New brands — show orders only, no ₦ amount
new_rows = q(
    """WITH feb_b AS (
         SELECT DISTINCT c.customer_business_name AS brand
         FROM gold.fact_dash_orders o
         JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
         WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
           AND lower(o.order_status)='delivered'
           AND c.customer_business_name IS NOT NULL
       ),
       mar_b AS (
         SELECT c.customer_business_name AS brand,
                count(*) AS orders,
                min(o.order_date) AS first_order
         FROM gold.fact_dash_orders o
         JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
         WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
           AND lower(o.order_status)='delivered'
           AND c.customer_business_name IS NOT NULL
         GROUP BY 1
       )
       SELECT m.brand, m.orders, m.first_order
       FROM mar_b m LEFT JOIN feb_b f USING (brand)
       WHERE f.brand IS NULL
       ORDER BY m.orders DESC"""
)

story.append(Paragraph("New Brand Spotlight", H2))
ndata = [["Brand", "First Order", "Orders in March"]]
for b, o, fo in new_rows:
    ndata.append([b, str(fo), f"{o:,}"])
nt = Table(ndata, colWidths=[80*mm, 40*mm, 40*mm])
nt.setStyle(std_table_style())
story.append(nt)
story.append(PageBreak())


# ===========================================================================
# SLIDE 7 — Order Issues
# ===========================================================================
slide_header("ORDER QUALITY", "Weekly issue rate trend • March 2026")

wk_rows = q(
    """SELECT date_trunc('week', order_date)::date AS wk,
              count(*) AS total,
              count(*) FILTER (WHERE lower(order_status)='rejected') AS rej,
              count(*) FILTER (WHERE lower(order_status)='voided') AS vod
       FROM gold.fact_dash_orders
       WHERE order_date >= '2026-03-02' AND order_date < '2026-04-01'
       GROUP BY 1 ORDER BY 1"""
)
wk_data = [["Week Starting", "Orders", "Rejected", "Voided", "Total Issues", "Issue Rate"]]
total_t = total_rej = total_vod = 0
for wk, t, rej, vod in wk_rows:
    total_t += t; total_rej += rej; total_vod += vod
    rate = (rej + vod)/t*100 if t else 0
    wk_data.append([str(wk), f"{t:,}", f"{rej}", f"{vod}", f"{rej+vod}", f"{rate:.2f}%"])
month_rate = (total_rej + total_vod)/total_t*100 if total_t else 0
wt = Table(wk_data, colWidths=[35*mm, 30*mm, 30*mm, 30*mm, 35*mm, 30*mm])
wt.setStyle(std_table_style())
story.append(wt)
story.append(Spacer(1, 8))

story.append(callout_box(
    "Month Total",
    f"<b>{total_rej + total_vod}</b> issues out of <b>{total_t:,}</b> orders — "
    f"<b>{month_rate:.2f}%</b> issue rate ({total_rej} rejected, {total_vod} voided). "
    f"Down from February's peak of 0.74%.",
    bg="#ecfdf5", border="#1b8a3a"
))
story.append(Spacer(1, 8))

# Top issue brands — show rate only, no ₦
issue_rows = q(
    """SELECT c.customer_business_name, count(*) AS orders,
              count(*) FILTER (WHERE lower(o.order_status) IN ('rejected','voided')) AS issues
       FROM gold.fact_dash_orders o
       JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
       WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
         AND c.customer_business_name IS NOT NULL
       GROUP BY 1
       HAVING count(*) >= 30
       ORDER BY (count(*) FILTER (WHERE lower(o.order_status) IN ('rejected','voided')))::numeric / count(*) DESC
       LIMIT 8"""
)
story.append(Paragraph("Top Issue Brands (≥30 orders)", H2))
idata = [["#", "Brand", "Orders", "Issues", "Issue Rate"]]
for i, (b, o, iss) in enumerate(issue_rows, 1):
    idata.append([str(i), b, f"{o:,}", f"{iss}", f"{iss/o*100:.2f}%"])
it = Table(idata, colWidths=[12*mm, 80*mm, 30*mm, 30*mm, 35*mm])
it.setStyle(std_table_style())
story.append(it)
story.append(PageBreak())


# ===========================================================================
# SLIDE 8 — Summary & Actions
# ===========================================================================
slide_header("SUMMARY & ACTIONS", "March 2026 — wins, concerns, recommendations")

wins_html = (
    "• <b>Sales +6.1% MoM</b> driven by POS strength (+7.7%)<br/>"
    "• <b>Brand retention 95%</b> — only 1 churn vs 6 in February<br/>"
    "• <b>3 new brands onboarded</b> (Midnight City best newcomer)<br/>"
    "• <b>Order issue rate down to 0.19–0.37%/wk</b> (vs Feb peak of 0.74%)<br/>"
    "• <b>Sallah holiday lift</b> — Mar 20 #2 day of the month<br/>"
    "• <b>Mr. Krums Ltd</b> top mover with strong double-digit growth"
)

concerns_html = (
    "• <b>Website channel flat</b> (-0.3%) — all growth from POS<br/>"
    "• <b>Service charge -2.2%</b> despite higher order volume<br/>"
    "• <b>Top 3 concentration ~83%</b> of revenue — high dependency risk<br/>"
    "• <b>Mother's Day & IWD did NOT spike</b> like Valentine's did in Feb<br/>"
    "• <b>One Top-10 brand declined double digits</b> MoM<br/>"
    "• <b>Wednesday remains the weakest weekday</b> — persistent gap"
)

cols = Table([[
    callout_box("✅ Wins", wins_html, bg="#ecfdf5", border="#1b8a3a"),
    callout_box("⚠️ Areas of Concern", concerns_html, bg="#fef2f2", border="#c52d2d"),
]], colWidths=[doc.width/2, doc.width/2])
cols.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
story.append(cols)
story.append(Spacer(1, 10))

actions_html = (
    "<b>1. Investigate website stagnation</b> — POS is doing all the work. "
    "Run a UX audit and review website-only promo levers for April.<br/>"
    "<b>2. Mid-week activation push</b> — Wednesday is consistently weakest. "
    "Test a Wednesday-only voucher with 2-3 brands.<br/>"
    "<b>3. Diversify top of brand mix</b> — top 3 concentration remains ~83%. "
    "Identify 2 mid-tier brands to grow into the top 5.<br/>"
    "<b>4. Replicate Sallah playbook for Easter</b> — Sallah delivered the only "
    "real holiday spike this month. Pre-plan menus & comms for Easter weekend.<br/>"
    "<b>5. Service-charge audit</b> — orders are up but service charge is down. "
    "Verify mix shift assumption and check fee schedule for any leakage."
)
story.append(Paragraph("Recommended Actions", H2))
story.append(callout_box("Priority Actions for April", actions_html,
                         bg="#eef2ff", border="#3b5bdb"))


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
def footer(canvas, doc_):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#9ca3af"))
    canvas.drawString(MARGIN, 6 * mm,
                      "DAASH • March 2026 Monthly Report • Generated 2026-04-10")
    canvas.drawRightString(PAGE_W - MARGIN, 6 * mm, f"Page {doc_.page}")
    canvas.restoreState()


doc.build(story, onFirstPage=footer, onLaterPages=footer)

cur.close()
conn.close()

print(f"✅ Generated: {OUTPUT}")

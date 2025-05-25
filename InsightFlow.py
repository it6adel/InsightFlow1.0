# chatbot_app.py (COMPLETE - Tier 1 FINAL + ALL Fixes + ALL Data Fetching Logic RESTORED)
import os
import logging
import json
import re
import datetime
import google.generativeai as genai
import psycopg2
import pandas as pd
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from dotenv import load_dotenv
from decimal import Decimal

# --- Configuration & Setup ---
load_dotenv()
# Changed logging level to DEBUG to see more details if needed
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')

KNOWN_COUNTRIES = ['Algeria', 'Morocco', 'Tunisia', 'Libya', 'Egypt']
KNOWN_CITIES_SAMPLE = ['Algiers', 'Oran', 'Constantine', 'Annaba', 'Blida', 'Setif', 'Tlemcen',
                       'Rabat', 'Casablanca', 'Marrakech', 'Fes', 'Tangier', 'Agadir', 'Meknes',
                       'Tunis', 'Sfax', 'Sousse', 'Kairouan', 'Bizerte', 'Gabes',
                       'Tripoli', 'Benghazi', 'Misrata', 'Tobruk', 'Sabha',
                       'Cairo', 'Alexandria', 'Giza', 'Shubra El Kheima', 'Port Said', 'Suez', 'Luxor']

DEFINITIONS = {
    "aov": "Average Order Value (AOV) is the average amount of money each customer spends per transaction on successfully delivered orders.",
    "delivered revenue": "Revenue generated only from orders successfully delivered and paid for.",
    "gross profit": "Total delivered revenue minus the direct cost of the goods sold (COGS).",
    "failure rate": "The percentage of placed orders not successfully delivered (cancelled, refused, failed delivery).",
    "sales funnel": "A visualization showing how orders progress through key stages (Placed, Confirmed, Shipped, Delivered), highlighting drop-offs.",
    "cod": "Cash on Delivery - payment method where customers pay in cash upon delivery.",
    "kpi": "Key Performance Indicator - a measurable value demonstrating effectiveness.",
    "revenue anomaly": "A significant spike or drop in revenue compared to recent trends.",
    "high failure products": "Products with a notably high rate of refusal or delivery failure after shipping.",
    "cancellation reasons": "The breakdown of stated reasons why orders were cancelled or failed delivery."
}

app = Flask(__name__)
CORS(app, resources={r"/chat*": {"origins": "*"}})

model = None
try:
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    if not gemini_api_key: raise ValueError("GEMINI_API_KEY not found.")
    genai.configure(api_key=gemini_api_key)
    model = genai.GenerativeModel('gemini-1.5-flash')
    logging.info("Gemini Model Loaded Successfully.")
except Exception as e:
    logging.error(f"Fatal Error Configuring Gemini: {e}.")

def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"), host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"))
        logging.info("Database Connection Successful.")
        return conn
    except psycopg2.OperationalError as db_err: logging.error(f"Database Connection Error: {db_err}")
    except Exception as e: logging.error(f"Unexpected error connecting to DB: {e}")
    if conn: conn.close()
    return None

# --- Intent Recognition (Refined City Suggestion and order) ---
def interpret_query_intent(query: str) -> tuple[str | None, dict]:
    logging.info(f"Interpreting query: '{query}'")
    query_lower = query.lower().strip(); original_query = query; context = {}

    # Keywords (Keep as before)
    revenue_kws = ["revenue", "sales", "income", "takings", "money made", "value", "earnings", "amount made"]
    delivered_kws = ["delivered", "completed", "successful", "fulfilled"]
    profit_kws = ["profit", "margin", "gross profit", "profitability"]
    funnel_kws = ["funnel", "conversion", "pipeline", "stages", "process flow"]
    explain_kws_define = ["what is", "what's", "define", "meaning of"]
    explain_kws_general = ["explain", "describe", "show me", "tell me about", "details on"] + explain_kws_define
    compare_kws = ["compare", "difference", "vs", "versus", "between", "comparison"]
    failure_kws = ["failure rate", "failed orders", "refused", "cancelled", "cancellation rate", "return rate", "undelivered"]
    reason_kws = ["reason", "cause", "why", "breakdown"]
    product_kws = ["product", "item", "sku", "merchandise", "goods"]
    shipping_kws = ["shipping", "shipped", "delivery", "fulfillment", "post-ship"]
    geo_kws = ["country", "countries", "city", "cities", "region", "area", "location"]
    time_periods = {
        'last_quarter': ["last quarter", "past quarter", "previous quarter"], 'last_month': ["last month", "past month", "previous month"],
        'this_month_mtd': ["this month", "current month"], 'last_90_days': ["last 90 days", "past 90 days"],
        'last_30_days': ["last 30 days", "past 30 days"], 'last_7_days': ["last 7 days", "past 7 days", "last week", "previous week"],
        'year_to_date': ["year to date", "ytd", "this year"], }
    help_kws = ["help", "what can you do", "capabilities", "commands", "info", "guide"]
    anomaly_kws = ["anomaly", "anomalies", "unusual", "spike", "drop", "significant change", "biggest change", "outlier"]
    solution_kws = ["solution", "solve", "fix", "improve", "address", "what can we do", "suggestion", "what to do about"]

    # --- Intent Rules (Order matters - specific before general) ---
    if any(kw in query_lower for kw in help_kws): return "get_help", {}

    matched_define_keyword = None
    for kw_def in explain_kws_define:
        if query_lower.startswith(kw_def + " "): matched_define_keyword = kw_def; break
    if matched_define_keyword:
         term_part = original_query[len(matched_define_keyword):].strip(); term_part = re.sub(r"^(an|a|the)\s+", "", term_part, flags=re.IGNORECASE).strip(); term_to_check = term_part.lower().replace('?','').strip()
         if term_to_check in DEFINITIONS: context['term'] = term_to_check; return "explain_term", context
         for known_term in DEFINITIONS:
             if known_term in term_to_check: context['term'] = known_term; return "explain_term", context

    # --- REFINED Intent: Suggest Improvement for High Failure City (Checked BEFORE geo compare) ---
    # Must contain solution keywords, failure keywords, AND a known city name
    if (any(kw in query_lower for kw in solution_kws) and
        any(kw in query_lower for kw in failure_kws + ["cancellations", "issues"])):
        extracted_city = None
        for known_city_sample in KNOWN_CITIES_SAMPLE:
            if re.search(r'\b' + re.escape(known_city_sample) + r'\b', original_query, re.IGNORECASE):
                extracted_city = known_city_sample; break
        if extracted_city:
            context['city'] = extracted_city; context['period'] = 'last_90_days'
            logging.info(f"Intent: suggest_improvement_for_high_failure_city, City: {context['city']}")
            return "suggest_improvement_for_high_failure_city", context
        elif any(kw in query_lower for kw in ["city", "cities"]): # If "city" keyword used but no specific one from known list
            logging.warning("City improvement keywords matched, but no specific city identified from known list.")
            return None, {"error": "Which city are you asking about for improvement suggestions? Please mention a specific city from our list (e.g., Algiers, Cairo)."}

    # --- REFINED Intent: Compare Geographic Failure Rate ---
    if (any(kw in query_lower for kw in compare_kws) and
        any(kw in query_lower for kw in failure_kws) and
        ("between" in query_lower and "and" in query_lower or "vs" in query_lower or "versus" in query_lower) ):
        country1_raw, country2_raw = None, None
        compare_pattern_between = re.compile(r"between\s+([\w\s]+?)\s+and\s+([\w\s]+?)(?:$|\s+last|\s+in|\s+for)", re.IGNORECASE)
        match_b = compare_pattern_between.search(original_query)
        if match_b: country1_raw = match_b.group(1).strip(); country2_raw = match_b.group(2).strip()
        else:
            compare_pattern_vs = re.compile(r"([\w\s]+?)\s+(?:vs|versus)\s+([\w\s]+?)(?:$|\s+last|\s+in|\s+for)", re.IGNORECASE)
            match_v = compare_pattern_vs.search(original_query)
            if match_v: country1_raw = match_v.group(1).strip(); country2_raw = match_v.group(2).strip()
        if country1_raw and country2_raw:
            c1_title=country1_raw.title(); c2_title=country2_raw.title(); known_titles_set={c.title() for c in KNOWN_COUNTRIES}
            logging.debug(f"GeoCompare Validation: Extracted='{c1_title}', '{c2_title}'. KnownSet={known_titles_set}")
            if c1_title in known_titles_set and c2_title in known_titles_set:
                context['countries']=[c1_title,c2_title]; context['period']='last_month'; [(context.update({'period':p_key}) if any(p in query_lower for p in phrases) else None) for p_key,phrases in time_periods.items() if any(p in query_lower for p in phrases)];
                logging.info(f"Intent: compare_failure_rate_geo, Countries: {c1_title},{c2_title}"); return "compare_failure_rate_geo",context
            else: logging.warning(f"Invalid countries for compare: '{country1_raw}','{country2_raw}'"); return None,{"error":f"Sorry, I can only compare countries within {', '.join(KNOWN_COUNTRIES)}."}
        elif any(kw in query_lower for kw in geo_kws): logging.warning("Geo compare kws matched, but countries not extracted."); return None,{"error":"Please specify two countries clearly (e.g., '...between Algeria and Egypt')."}

    # (Other intents: High Failure Products, Cancellation Reasons, Anomaly, Profit, Funnel, Revenue as before)
    if (any(kw in query_lower for kw in product_kws + failure_kws) and any(kw in query_lower for kw in shipping_kws + ["high", "worst", "often"])):
        context['period']='last_90_days'; context['threshold']=5; context['top_n']=5; [(context.update({'period':p_key}) if any(p in query_lower for p in phrases) else None) for p_key, phrases in time_periods.items() if any(p in query_lower for p in phrases)]; return "get_high_failure_products", context
    if (any(kw in query_lower for kw in failure_kws + reason_kws) and any(kw in query_lower for kw in reason_kws + ["breakdown", "summary", "distribution", "cancelled", "failing"])):
         context['period'] = 'last_90_days'; context['top_n'] = 7; [(context.update({'period':p_key}) if any(p in query_lower for p in phrases) else None) for p_key, phrases in time_periods.items() if any(p in query_lower for p in phrases)]; return "get_cancellation_reasons", context
    if (any(kw in query_lower for kw in anomaly_kws) and any(kw in query_lower for kw in revenue_kws)): context['period']='last_90_days'; context['time_grain']='day'; return "find_revenue_anomaly", context
    if any(kw in query_lower for kw in profit_kws): context['period']='last_month'; [(context.update({'period':p_key}) if any(p in query_lower for p in phrases) else None) for p_key, phrases in time_periods.items() if any(p in query_lower for p in phrases)]; return "get_gross_profit", context
    if any(kw in query_lower for kw in explain_kws_general + ["show"]) and any(kw in query_lower for kw in funnel_kws): context['period']='last_90_days'; [(context.update({'period':p_key}) if any(p in query_lower for p in phrases) else None) for p_key, phrases in time_periods.items() if any(p in query_lower for p in phrases)]; return "explain_sales_funnel", context
    if any(kw in query_lower for kw in revenue_kws):
        is_profit_query = any(kw in query_lower for kw in profit_kws); is_anomaly_query = any(kw in query_lower for kw in anomaly_kws)
        is_compare_failure_query = (any(kw in query_lower for kw in compare_kws) and any(kw in query_lower for kw in failure_kws))
        if not context and not is_profit_query and not is_anomaly_query and not is_compare_failure_query: # Check if context is still empty
            context['period'] = 'last_month'; [(context.update({'period':p_key}) if any(p in query_lower for p in phrases) else None) for p_key, phrases in time_periods.items() if any(p in query_lower for p in phrases)]; logging.info(f"Inferred 'get_delivered_revenue' for period: {context['period']}"); return "get_delivered_revenue", context

    logging.warning(f"Could not determine intent for query: '{query}'"); return None, {"error": "Intent not understood. Try asking 'help'."}

# --- Data Fetching (ALL INTENT LOGIC RESTORED) ---
def fetch_data_for_intent(intent: str, context: dict) -> pd.DataFrame | dict | float | list | str | None:
    logging.info(f"Fetching data for intent: '{intent}', Context: {context}")
    if intent in ["get_help", "explain_term"]: return {}

    conn = get_db_connection()
    if not conn: return {"error": "Database connection failed."}

    data_result = None; query = ""; params = ()
    def get_date_filter(period_key, date_column='o.order_date'):
        if period_key == 'last_quarter': return f"AND {date_column} >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' AND {date_column} < DATE_TRUNC('quarter', CURRENT_DATE)"
        elif period_key == 'this_month_mtd': return f"AND {date_column} >= DATE_TRUNC('month', CURRENT_DATE) AND {date_column} < CURRENT_DATE + INTERVAL '1 day'"
        elif period_key == 'last_90_days': return f"AND {date_column} >= (CURRENT_DATE - INTERVAL '90 days')::date"
        elif period_key == 'last_30_days': return f"AND {date_column} >= (CURRENT_DATE - INTERVAL '30 days')::date"
        elif period_key == 'last_7_days': return f"AND {date_column} >= (CURRENT_DATE - INTERVAL '7 days')::date AND {date_column} < CURRENT_DATE + INTERVAL '1 day'"
        elif period_key == 'year_to_date': return f"AND {date_column} >= DATE_TRUNC('year', CURRENT_DATE) AND {date_column} < CURRENT_DATE + INTERVAL '1 day'"
        else: return f"AND {date_column} >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND {date_column} < DATE_TRUNC('month', CURRENT_DATE)"

    cursor = None
    try:
        cursor = conn.cursor()

        if intent == "suggest_improvement_for_high_failure_city": # RESTORED
            city = context.get("city"); period = context.get("period", "last_90_days")
            if not city: return {"error": "City name not identified."}
            date_filter = get_date_filter(period, 'o.order_date')
            query_stats = f"SELECT COUNT(DISTINCT o.order_id) AS total_orders, COUNT(DISTINCT CASE WHEN o.order_status IN ('Refused Delivery', 'Delivery Failed', 'Cancelled by Customer', 'Cancelled by Admin') THEN o.order_id ELSE NULL END) AS failed_orders FROM public.orders o JOIN public.addresses a ON o.shipping_address_id = a.address_id WHERE a.city = %s {date_filter} AND a.country IS NOT NULL;"
            cursor.execute(query_stats, (city,)); stats_result = cursor.fetchone()
            city_total_orders = int(stats_result[0]) if stats_result and stats_result[0] is not None else 0
            city_failed_orders = int(stats_result[1]) if stats_result and stats_result[1] is not None else 0
            city_failure_rate = (float(city_failed_orders) * 100.0 / float(city_total_orders)) if city_total_orders > 0 else 0.0
            query_reasons = f"SELECT o.cancellation_reason, COUNT(DISTINCT o.order_id) AS reason_count FROM public.orders o JOIN public.addresses a ON o.shipping_address_id = a.address_id WHERE a.city = %s AND o.order_status IN ('Refused Delivery', 'Delivery Failed', 'Cancelled by Customer', 'Cancelled by Admin') AND o.cancellation_reason IS NOT NULL {date_filter} GROUP BY o.cancellation_reason ORDER BY reason_count DESC LIMIT 3;"
            cursor.execute(query_reasons, (city,)); reasons_results = cursor.fetchall(); top_reasons = []
            if reasons_results: colnames_reasons = [desc[0] for desc in cursor.description]; top_reasons = [dict(zip(colnames_reasons, row)) for row in reasons_results]; [r.update({'reason_count': int(r['reason_count'])}) for r in top_reasons]
            data_result = {"city": city, "total_orders": city_total_orders, "failed_orders": city_failed_orders, "failure_rate_percent": round(city_failure_rate, 1), "top_cancellation_reasons": top_reasons}
        
        elif intent == "get_delivered_revenue": # RESTORED (Example)
            period = context.get('period', 'last_month'); date_filter = get_date_filter(period, 'delivered_at'); query = f"SELECT SUM(order_total) AS total_revenue FROM public.orders WHERE order_status = 'Delivered' {date_filter};"; cursor.execute(query); result = cursor.fetchone(); data_result = float(result[0]) if result and result[0] is not None else 0.0
        
        elif intent == "get_gross_profit": # RESTORED
            period = context.get('period', 'last_month'); date_filter = get_date_filter(period, 'o.delivered_at'); query = f""" SELECT SUM((oi.price_per_unit - COALESCE(oi.cost_per_unit, 0)) * oi.quantity) AS gross_profit FROM public.order_items oi JOIN public.orders o ON oi.order_id = o.order_id WHERE o.order_status = 'Delivered' {date_filter}; """; cursor.execute(query); result = cursor.fetchone(); data_result = float(result[0]) if result and result[0] is not None else 0.0
        
        elif intent == "get_cancellation_reasons": # RESTORED
            period = context.get('period', 'last_90_days'); top_n = context.get('top_n', 7); date_filter = get_date_filter(period, 'order_date'); query = f""" SELECT cancellation_reason, COUNT(DISTINCT order_id) as reason_count FROM public.orders WHERE order_status IN ('Cancelled by Customer', 'Cancelled by Admin', 'Refused Delivery', 'Delivery Failed') AND cancellation_reason IS NOT NULL {date_filter} GROUP BY cancellation_reason ORDER BY reason_count DESC LIMIT %s; """; params = (top_n,); cursor.execute(query, params); results = cursor.fetchall()
            if results: colnames = [desc[0] for desc in cursor.description]; data_result = [dict(zip(colnames, row)) for row in results]; [r.update({'reason_count': int(r['reason_count'])}) for r in data_result]
            else: data_result = []
        
        elif intent == "explain_sales_funnel": # RESTORED
             period = context.get('period', 'last_90_days'); date_interval = '30 days' if period == 'last_30_days' else ('7 days' if period == 'last_7_days' else '90 days'); query = f""" SELECT '1. Placed' AS stage, COUNT(DISTINCT order_id) AS order_count FROM public.orders WHERE order_date >= CURRENT_DATE - INTERVAL '{date_interval}' UNION ALL SELECT '2. Confirmed/Processing' AS stage, COUNT(DISTINCT order_id) AS order_count FROM public.orders WHERE order_date >= CURRENT_DATE - INTERVAL '{date_interval}' AND order_status NOT IN ('Pending Confirmation', 'Cancelled by Customer', 'Cancelled by Admin') UNION ALL SELECT '3. Shipped' AS stage, COUNT(DISTINCT order_id) AS order_count FROM public.orders WHERE order_date >= CURRENT_DATE - INTERVAL '{date_interval}' AND shipped_at IS NOT NULL AND order_status NOT IN ('Cancelled by Customer', 'Cancelled by Admin') UNION ALL SELECT '4. Delivered' AS stage, COUNT(DISTINCT order_id) AS order_count FROM public.orders WHERE order_date >= CURRENT_DATE - INTERVAL '{date_interval}' AND order_status = 'Delivered' ORDER BY stage ASC;"""; cursor.execute(query); results = cursor.fetchall();
             if results: colnames = [desc[0] for desc in cursor.description]; data_result = [dict(zip(colnames, row)) for row in results]; [r.update({'order_count': int(r['order_count'])}) for r in data_result]
             else: data_result = []
        
        elif intent == "compare_failure_rate_geo": # RESTORED (with existing logging)
             countries = context.get("countries"); period = context.get("period", "last_month")
             logging.info(f"Geo Compare FETCH - Countries: {countries}, Period: {period}")
             if not countries or not isinstance(countries, list) or len(countries) != 2: logging.error(f"Geo Compare FETCH - Invalid 'countries': {countries}"); return {"error":"Internal error: Country data invalid."}
             if not all(isinstance(c, str) for c in countries): logging.error(f"Geo Compare FETCH - Non-string country name: {countries}"); return {"error": "Internal error: Country names invalid."}
             date_filter = get_date_filter(period, 'o.order_date'); query = f""" SELECT a.country, COUNT(DISTINCT o.order_id) AS total_orders, COUNT(DISTINCT CASE WHEN o.order_status IN ('Cancelled by Customer', 'Cancelled by Admin', 'Refused Delivery', 'Delivery Failed') THEN o.order_id ELSE NULL END) AS failed_orders FROM public.orders o LEFT JOIN public.addresses a ON o.shipping_address_id = a.address_id WHERE a.country IN %s {date_filter} AND a.country IS NOT NULL GROUP BY a.country; """; params = (tuple(countries),); logging.info(f"Geo Compare FETCH - SQL: {query}, Params: {params}");
             cursor.execute(query, params); results = cursor.fetchall(); logging.info(f"Geo Compare FETCH - DB Results: {results}"); country_stats = {}
             if results: colnames = [desc[0] for desc in cursor.description]; [country_stats.update({r['country']: {"total": int(r['total_orders']), "failed": int(r['failed_orders']), "failure_rate": round((float(r['failed_orders'])*100.0/float(r['total_orders'])) if r['total_orders']>0 else 0.0, 1)}}) for r in map(lambda row: dict(zip(colnames, row)), results)]
             for c_name in countries: country_stats.setdefault(c_name, {"total": 0, "failed": 0, "failure_rate": 0.0})
             data_result = country_stats; logging.info(f"Geo Compare FETCH - Processed Stats: {data_result}")
        
        elif intent == "get_high_failure_products": # RESTORED
             period = context.get('period', 'last_90_days'); threshold = context.get('threshold', 5); top_n = context.get('top_n', 5); date_interval = '30 days' if period == 'last_30_days' else ('7 days' if period == 'last_7_days' else '90 days'); query = f""" WITH PS AS ( SELECT oi.product_id, COUNT(DISTINCT o.order_id) AS ts, COUNT(DISTINCT CASE WHEN o.order_status IN ('Refused Delivery', 'Delivery Failed', 'Returned') THEN o.order_id ELSE NULL END) AS tfps FROM public.order_items oi JOIN public.orders o ON oi.order_id = o.order_id WHERE o.shipped_at IS NOT NULL AND o.order_date >= CURRENT_DATE - INTERVAL '{date_interval}' AND o.order_status NOT IN ('Cancelled by Customer', 'Cancelled by Admin') GROUP BY oi.product_id ) SELECT p.product_name, ps.ts, ps.tfps, CASE WHEN ps.ts=0 THEN 0.0 ELSE (ps.tfps::NUMERIC * 100.0 / ps.ts::NUMERIC) END AS frp FROM PS ps JOIN public.products p ON ps.product_id = p.product_id WHERE ps.ts >= %s ORDER BY frp DESC, tfps DESC LIMIT %s; """; params = (threshold, top_n); cursor.execute(query, params); results = cursor.fetchall()
             if results: colnames = ['product_name', 'times_shipped', 'times_failed_post_ship', 'failure_rate_percent']; data_result = [dict(zip(colnames, [r[0], int(r[1]), int(r[2]), round(float(r[3]),1)])) for r in results]
             else: data_result = []
        
        elif intent == "find_revenue_anomaly": # RESTORED
             period = context.get('period', 'last_90_days'); time_grain = context.get('time_grain', 'day'); date_interval = '30 days' if period == 'last_30_days' else ('7 days' if period == 'last_7_days' else '90 days'); query = f""" WITH TR AS (SELECT DATE_TRUNC(%s, delivered_at) AS tp, SUM(order_total) AS pr FROM public.orders WHERE order_status = 'Delivered' AND delivered_at >= CURRENT_DATE - INTERVAL '{date_interval}' GROUP BY tp), RL AS (SELECT tp, pr, LAG(pr, 1, 0.0) OVER (ORDER BY tp ASC) AS ppr FROM TR) SELECT TO_CHAR(tp, 'YYYY-MM-DD') AS ps, pr, ppr, (pr - ppr) AS rc FROM RL WHERE tp >= CURRENT_DATE - INTERVAL '{date_interval}' AND (pr IS NOT NULL AND ppr IS NOT NULL) ORDER BY ABS(pr - ppr) DESC LIMIT 5; """; params = (time_grain,); cursor.execute(query, params); results = cursor.fetchall()
             if results: colnames = ['period_str','period_revenue','prev_period_revenue','revenue_change']; data_result = [dict(zip(colnames, [r[0], float(r[1]), float(r[2]), float(r[3])])) for r in results]
             else: data_result = []
        else:
            logging.warning(f"No data fetching logic defined for intent: {intent}")
            data_result = {"error": f"Analysis not implemented for '{intent}' yet."} # More specific error

        if not (isinstance(data_result, dict) and 'error' in data_result):
             log_snippet = str(data_result)[:250] + ('...' if len(str(data_result)) > 250 else ''); logging.info(f"Data fetched for '{intent}': {log_snippet}")

    except psycopg2.Error as db_err:
        logging.error(f"Database query error for intent '{intent}': {db_err} --- SQL: {cursor.query if cursor and hasattr(cursor, 'query') else 'N/A'}")
        if conn: conn.rollback();
        data_result = {"error": "Database query failed."}
    except Exception as e:
        logging.exception(f"Unexpected error fetching data for intent '{intent}':")
        data_result = {"error": "Unexpected error fetching data."}
    finally:
        if cursor: cursor.close();
        if conn: conn.close(); logging.info("DB connection closed (fetch_data).")
    return data_result

# --- Narrative Generation (Includes new intent prompt and refined others) ---
def generate_narrative(intent: str, data: any, original_query: str, context: dict) -> str:
    # --- PASTE THE FULL generate_narrative FUNCTION HERE ---
    # --- It should include the logic for ALL intents, including suggest_improvement_for_high_failure_city ---
    # --- and the refined prompts for existing intents asking Gemini for more insights/suggestions ---
    logging.info(f"Generating narrative for intent: '{intent}'")
    if intent == "get_help": return ("I can provide insights on:\n*   **Delivered Revenue or Gross Profit:** Ask like 'What was delivered revenue last quarter?', 'gross profit last 7 days'\n*   **Sales Funnel:** 'Explain the sales funnel'\n*   **Failure Rate Comparison:** 'Compare failure rate between Algeria and Egypt'\n*   **Problem Products:** 'Which products have high failure rates after shipping?'\n*   **Cancellation Reasons:** 'Show cancellation reason breakdown'\n*   **Revenue Anomalies:** 'Any unusual revenue changes lately?'\n*   **Solutions for Problem Cities:** 'Improve delivery issues for Cairo'\n*   **Definitions:** 'What is AOV?', 'define COD'\n\n**Tips:** Specify time periods (last month, last 90 days, etc.) for better results.")
    if intent == "explain_term":
        term = context.get('term', '').lower(); definition = DEFINITIONS.get(term)
        if definition: return f"Okay, here's the definition for '{term}': {definition}"
        else: return f"Sorry, I don't have a specific definition for '{context.get('term', original_query)}'. Try asking 'help'."
    if model is None: return "Error: AI engine unavailable."
    if data is None: return "Sorry, no data was available for analysis."
    if isinstance(data, dict) and "error" in data: return f"Sorry, couldn't get data due to: {data.get('error', 'an issue')}."

    data_string_for_prompt = ""; prompt_instructions = ""
    try:
        base_prompt = (f"You are 'InsightBot', a BI assistant for a COD e-commerce business (PC Gaming Accessories, North Africa). Explain data insights clearly and concisely to a non-technical manager called Adel. Focus on the key takeaway and suggest areas for investigation or general types of solutions if applicable based on the data.\n\nUser Query: '{original_query}'\n\nRelevant Data Summary:\n")
        if intent == "suggest_improvement_for_high_failure_city":
            if isinstance(data, dict) and "city" in data:
                city=data['city']; rate=data['failure_rate_percent']; reasons=data['top_cancellation_reasons']; reason_summary = "\n".join([f"- {r['cancellation_reason']} ({r['reason_count']} orders)" for r in reasons]) if reasons else "No specific top reasons logged for this city in the period."
                data_string_for_prompt = (f"City: {city}\nFailure Rate ({context.get('period','last 90 days').replace('_',' ')}): {rate:.1f}%\nTotal Placed: {data['total_orders']}\nTotal Failed: {data['failed_orders']}\nTop Reasons in {city}:\n{reason_summary}")
                prompt_instructions = (f"The city of {city} has a failure rate of {rate:.1f}%. Based on its top cancellation reasons (if available), suggest 2-3 general areas the business should investigate OR potential types of solutions to consider to reduce failures in {city}. Be practical and actionable. If specific reasons are common (like 'Address validation failed'), suggest related solutions. If reasons are more generic, suggest broader investigation points.")
            else: raise TypeError("City improvement data invalid.")
        elif intent == "get_delivered_revenue":
             if isinstance(data, (int, float)): revenue_fmt = f"${data:,.2f}"; period = context.get('period', 'the period').replace('_', ' '); data_string_for_prompt = f"Total delivered revenue for {period}: {revenue_fmt}"; prompt_instructions = f"State the total delivered revenue ({revenue_fmt}) for {period} and briefly comment if this figure seems high or low in a general e-commerce context, if possible."
             else: raise TypeError("Revenue data invalid.")
        elif intent == "get_gross_profit":
             if isinstance(data, (int, float)): profit_fmt = f"${data:,.2f}"; period = context.get('period', 'the period').replace('_', ' '); data_string_for_prompt = f"Gross profit ({period}): {profit_fmt}"; prompt_instructions = f"State the gross profit ({profit_fmt}) from delivered orders for {period}. Briefly explain what this figure represents for the business."
             else: raise TypeError("Profit data invalid.")
        elif intent == "get_cancellation_reasons":
             if isinstance(data, list) and data: period = context.get('period', '').replace('_', ' '); reason_list = [f"- {i['cancellation_reason']}: {i['reason_count']} orders" for i in data]; data_string_for_prompt = f"Top {len(data)} failure reasons ({period}):\n" + "\n".join(reason_list); prompt_instructions = (f"Summarize the top 2-3 most common failure reasons ({period}). For each, briefly suggest what kind of business area this points to (e.g., logistics, product info, customer communication) and what general type of action might address it.")
             elif isinstance(data, list) and not data: return f"No failed orders with reasons found ({context.get('period', '').replace('_', ' ')})."
             else: raise TypeError("Cancellation reason data invalid.")
        elif intent == "explain_sales_funnel":
             if isinstance(data, list) and data: funnel_summary = "\n".join([f"- {i['stage']}: {i['order_count']} orders" for i in data]); period = context.get('period','').replace('_',' '); data_string_for_prompt = f"Sales Funnel ({period}):\n{funnel_summary}"; placed = data[0]['order_count']; delivered = data[-1]['order_count']; conversion = f"{(float(delivered) * 100.0 / float(placed)):.1f}%" if placed > 0 else "N/A"; prompt_instructions = (f"Explain this funnel (Placed: {placed}, Delivered: {delivered}, Period: {period}). Highlight overall conversion ({conversion}) & biggest drop-off stage. What does this imply for the business?")
             else: return "No funnel data found."
        elif intent == "compare_failure_rate_geo":
             if isinstance(data, dict):
                 countries = context.get("countries", list(data.keys())); period = context.get('period', 'the period').replace('_', ' ');
                 if len(countries) == 2:
                     c1, c2 = countries[0], countries[1]; s1 = data.get(c1); s2 = data.get(c2); parts = []
                     if s1: parts.append(f"{c1}: {s1['failure_rate']:.1f}% ({s1['failed']}/{s1['total']}).")
                     else: parts.append(f"No data for {c1} in {period}.")
                     if s2: parts.append(f"{c2}: {s2['failure_rate']:.1f}% ({s2['failed']}/{s2['total']}).")
                     else: parts.append(f"No data for {c2} in {period}.")
                     data_string_for_prompt = f"Failure Rate Comparison ({period}):\n" + "\n".join(parts); prompt_instructions = f"Compare failure rates between {c1} and {c2} ({period}). State which was notably higher, mentioning rates. What might this suggest about operations in those countries?"
                 else: raise ValueError("Incorrect country data.")
             else: raise TypeError("Comparison data invalid.")
        elif intent == "get_high_failure_products":
            if isinstance(data, list) and data: period = context.get('period', '').replace('_', ' '); threshold = context.get('threshold', '?'); plist = [f"- {i['product_name']}: {i['failure_rate_percent']:.1f}% ({i['times_failed_post_ship']}/{i['times_shipped']})" for i in data]; data_string_for_prompt = (f"Top Products by Post-Ship Failure ({period}, shipped >= {threshold}):\n" + "\n".join(plist)); prompt_instructions = (f"Identify products with notable post-shipping failure rates ({period}). Summarize, highlighting top 1-2 products/rates. Mention min shipments ({threshold}). What general issues might cause high failure rates for these types of products (e.g. packaging, description, defects)?")
            elif isinstance(data, list) and not data: return (f"Good news! No products shipped {context.get('threshold', '?')}+ times had high post-shipping failure rates in {context.get('period', '').replace('_', ' ')}.")
            else: raise TypeError("High failure product data invalid.")
        elif intent == "find_revenue_anomaly":
             if isinstance(data, list) and data: grain = context.get('time_grain', 'period'); period = context.get('period','').replace('_',' '); summary = [f"- {i['period_str']}: Change ${i['revenue_change']:,.2f} (Prev: ${i['prev_period_revenue']:,.2f}, Curr: ${i['period_revenue']:,.2f})" for i in data]; data_string_for_prompt = f"Largest {grain}ly revenue changes ({period}):\n" + "\n".join(summary); top = data[0]; change_dir = "increase" if top['revenue_change'] > 0 else "decrease"; prompt_instructions = (f"Describe the single biggest {grain}ly revenue anomaly ({period}). Mention date ({top['period_str']}), direction ({change_dir}), approx change (${abs(top['revenue_change']):,.0f}), and resulting revenue (${top['period_revenue']:,.0f}). Suggest 1-2 common business reasons for such a change (e.g., promotions, stock issues, external event).")
             elif isinstance(data, list) and not data: return f"Analyzed recent revenue but found no major {context.get('time_grain', 'period')}-over-{context.get('time_grain', 'period')} changes."
             else: raise TypeError("Revenue anomaly data invalid.")
        else: data_string_for_prompt = json.dumps(data, indent=2, default=str); prompt_instructions = "Briefly summarize this data."

        final_prompt = f"{base_prompt}```json\n{data_string_for_prompt}\n```\n\nTask: {prompt_instructions}\n\nResponse:"
        logging.info(f"\n--- Sending Prompt to Gemini ---\n{final_prompt}\n-----------------------------\n")
        response = model.generate_content(final_prompt)
        if response.parts:
            narrative = response.text.strip(); logging.info(f"--- Received Narrative --- \n{narrative}\n--------------")
            if len(narrative)<len(prompt_instructions)+30 and data_string_for_prompt.strip().split('\n')[0] in narrative.replace('\n',' ').replace('$','').replace(',',''):
                 logging.warning("Gemini may have just repeated input data. Returning summary."); return f"Data Summary:\n{data_string_for_prompt}"
            return narrative
        else:
            logging.warning(f"Gemini returned no content. Feedback: {response.prompt_feedback}"); safety_str = str(response.prompt_feedback.safety_ratings) if response.prompt_feedback else "N/A"; return f"Analysis engine provided no narrative. (Safety Feedback: {safety_str})"
    except Exception as e:
        logging.exception(f"Error during narrative generation (intent: {intent}):"); return "Sorry, an internal error occurred generating the insight."

# --- Flask Routes ---
@app.route('/chat', methods=['POST'])
def chat_handler():
    if not model: return jsonify({"response": "Error: AI engine unavailable."}), 503
    try:
        user_data = request.get_json();
        if not user_data or 'query' not in user_data: return jsonify({"error": "Missing 'query'."}), 400
        user_query = user_data['query'].strip();
        if not user_query: return jsonify({"error": "Query empty."}), 400
        logging.info(f"Received query via /chat: '{user_query}'")
        intent, context = interpret_query_intent(user_query)
        if not intent:
            error_msg = context.get("error", "I didn't understand that. Try asking 'help'.")
            response_text = f"Sorry, {error_msg}"; status_code = 200
        elif intent == "get_help" or intent == "explain_term":
             response_text = generate_narrative(intent, None, user_query, context); status_code = 200
        else:
            fetched_data = fetch_data_for_intent(intent, context)
            response_text = generate_narrative(intent, fetched_data, user_query, context)
            is_error_response = ("Error:" in response_text or ("Sorry," in response_text and ("unavailable" in response_text or "internal error" in response_text or "Database query failed" in response_text or "fetching data" in response_text or "definition" in response_text or "couldn't get data" in response_text)))
            if is_error_response: status_code = 503 if "engine" in response_text else 500; logging.error(f"Responding with status {status_code} for query '{user_query}'. Response: {response_text}")
            else: status_code = 200
        return jsonify({"response": response_text}), status_code
    except Exception as e:
        logging.exception("Critical error in /chat handler:"); return jsonify({"error": "Internal server error."}), 500

@app.route('/chat-interface')
def chat_interface():
    try: return render_template('interface.html')
    except Exception as e: logging.exception("Error rendering chat interface:"); return "Error loading chat interface.", 500

# --- Main Execution ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)
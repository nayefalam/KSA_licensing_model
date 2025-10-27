import pandas as pd
import os
import tkinter as tk
from tkinter import ttk, messagebox
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import numpy as np
from math import pi
from scipy.stats import percentileofscore 
import sqlite3

# --- Configuration ---
DATA_CSV_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'brand_metrics_final_v2.csv')
PRODUCTS_DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'licensing_data.db')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'reports')

# --- Scoring Weights (Keep as before) ---
WEIGHTS = {
    'hype': 0.30, 'sentiment': 0.20, 'quality': 0.25,
    'popularity': 0.15, 'saturation': 0.10
}

# --- Global variables ---
plot_canvas_widget = None
df_all_metrics = None 
df_all_products = None 
brand_list = [] # Master list of all brand names

# --- Normalization and Scoring Functions (Keep as before) ---
def normalize(series, higher_is_better=True, min_possible=None, max_possible=None):
    min_val = series.min() if min_possible is None else min_possible
    max_val = series.max() if max_possible is None else max_possible
    if min_possible is not None or max_possible is not None:
        series = series.clip(lower=min_possible, upper=max_possible)
        min_val = series.min(); max_val = series.max()
    if max_val == min_val: return pd.Series([50] * len(series), index=series.index)
    if higher_is_better: norm = ((series - min_val) / (max_val - min_val)) * 100
    else: norm = ((max_val - series) / (max_val - min_val)) * 100
    return norm.fillna(50)

def calculate_suitability_score(row):
    score = (row['norm_tweet_volume'] * WEIGHTS['hype'] +
             row['norm_avg_tweet_sentiment'] * WEIGHTS['sentiment'] +
             row['norm_avg_perceived_quality'] * WEIGHTS['quality'] +
             row['norm_popularity'] * WEIGHTS['popularity'] +
             row['norm_market_saturation'] * WEIGHTS['saturation'])
    return round(score, 1)

def generate_recommendation(score):
    if score >= 75: return "HIGH POTENTIAL"
    elif score >= 50: return "MODERATE POTENTIAL"
    elif score >= 25: return "LOW POTENTIAL"
    else: return "VERY LOW POTENTIAL"

# --- Data Loading Function (Keep as before) ---
def load_all_data():
    """Loads all metrics and product data into global variables at startup."""
    global df_all_metrics, df_all_products, brand_list
    try:
        df_all_metrics = pd.read_csv(DATA_CSV_PATH)
        if df_all_metrics.empty:
            messagebox.showerror("Error", f"Metrics file is empty: {DATA_CSV_PATH}")
            return False

        conn = sqlite3.connect(PRODUCTS_DB_PATH)
        df_all_products = pd.read_sql_query("""
            SELECT p.*, b.brand_name
            FROM products p JOIN brands b ON p.brand_id = b.id
            WHERE p.platform = 'Amazon.sa'
        """, conn)
        conn.close()
        df_all_products['avg_rating'] = pd.to_numeric(df_all_products['avg_rating'], errors='coerce').fillna(0)
        df_all_products['num_reviews'] = pd.to_numeric(df_all_products['num_reviews'], errors='coerce').fillna(0).astype(int)
        df_all_products['price'] = pd.to_numeric(df_all_products['price'], errors='coerce')

        # --- Pre-calculate all scores and ranks ---
        df_all_metrics['norm_tweet_volume'] = normalize(df_all_metrics['tweet_volume'], higher_is_better=True)
        df_all_metrics['norm_avg_tweet_sentiment'] = normalize(df_all_metrics['avg_tweet_sentiment'], higher_is_better=True, min_possible=-1.0, max_possible=1.0)
        df_all_metrics['norm_avg_perceived_quality'] = normalize(df_all_metrics['avg_perceived_quality'], higher_is_better=True, min_possible=0.0, max_possible=5.0)
        log_reviews = np.log1p(df_all_metrics['avg_num_reviews'])
        df_all_metrics['norm_popularity'] = normalize(log_reviews, higher_is_better=True)
        max_saturation_limit = 25
        df_all_metrics['saturation_capped'] = df_all_metrics['market_saturation'].clip(upper=max_saturation_limit)
        df_all_metrics['norm_market_saturation'] = normalize(df_all_metrics['saturation_capped'], higher_is_better=False)
        
        df_all_metrics['rank_hype'] = df_all_metrics['tweet_volume'].apply(lambda x: percentileofscore(df_all_metrics['tweet_volume'], x, kind='rank'))
        df_all_metrics['rank_sentiment'] = df_all_metrics['avg_tweet_sentiment'].apply(lambda x: percentileofscore(df_all_metrics['avg_tweet_sentiment'], x, kind='rank'))
        df_all_metrics['rank_quality'] = df_all_metrics['avg_perceived_quality'].apply(lambda x: percentileofscore(df_all_metrics['avg_perceived_quality'], x, kind='rank'))
        df_all_metrics['rank_popularity'] = df_all_metrics['avg_num_reviews'].apply(lambda x: percentileofscore(df_all_metrics['avg_num_reviews'], x, kind='rank'))
        df_all_metrics['rank_saturation'] = 100 - df_all_metrics['market_saturation'].apply(lambda x: percentileofscore(df_all_metrics['market_saturation'], x, kind='rank')) # Inverse
        
        df_all_metrics['suitability_score'] = df_all_metrics.apply(calculate_suitability_score, axis=1)
        
        brand_list = sorted(df_all_metrics['brand_name'].unique())
        
        print("Data loaded and pre-processed successfully.")
        return True

    except FileNotFoundError as e:
        messagebox.showerror("Data Error", f"Required data file not found.\n{e}\nPlease run the notebook/scrapers first.")
        return False
    except Exception as e:
        messagebox.showerror("Fatal Error", f"Could not load or process data file.\nError: {e}")
        return False

# --- GUI Functions ---

def generate_report():
    """Fetches brand from GLOBAL df, updates GUI text and chart."""
    global plot_canvas_widget, df_all_metrics, df_all_products

    brand_name_query = brand_entry.get()
    if not brand_name_query:
        messagebox.showwarning("Input Needed", "Please select or type a brand name.")
        return

    # --- FIX: Find match (case-insensitive) ---
    found = False
    for b_name in brand_list:
        if b_name.lower() == brand_name_query.lower():
            brand_name_query = b_name 
            found = True
            break
            
    if not found:
        messagebox.showerror("Not Found", f"Brand '{brand_name_query}' not found. Please select from the list or check spelling.")
        report_text.set("Report will appear here.\nTop products will appear here.")
        if plot_canvas_widget:
            plot_canvas_widget.get_tk_widget().destroy(); plot_canvas_widget = None
        return
    
    brand_data = df_all_metrics[df_all_metrics['brand_name'] == brand_name_query].iloc[0]
    actual_brand_name = brand_data['brand_name']

    # --- Generate Text Report (v3 - Enhanced) ---
    recommendation = generate_recommendation(brand_data['suitability_score'])
    report = f"""
-------------------------------------------
BRAND REPORT: {actual_brand_name}
-------------------------------------------
Overall Score & Recommendation:
  - SUITABILITY SCORE: {brand_data['suitability_score']:.1f} / 100
  - RECOMMENDATION:      {recommendation}
-------------------------------------------
Metrics Breakdown (Value | Norm Score | Rank):
  - Hype (Tweets):    {brand_data['tweet_volume']:>7,.0f} | {brand_data['norm_tweet_volume']:>3.0f}/100 | {brand_data['rank_hype']:>3.0f}th pctile
  - Sentiment (Tweet): {brand_data['avg_tweet_sentiment']:>7.2f} | {brand_data['norm_avg_tweet_sentiment']:>3.0f}/100 | {brand_data['rank_sentiment']:>3.0f}th pctile
  - Quality (Amz Rat): {brand_data['avg_perceived_quality']:>7.1f} | {brand_data['norm_avg_perceived_quality']:>3.0f}/100 | {brand_data['rank_quality']:>3.0f}th pctile
  - Popularity (Amz Rev):{brand_data['avg_num_reviews']:>7.1f} | {brand_data['norm_popularity']:>3.0f}/100 | {brand_data['rank_popularity']:>3.0f}th pctile
  - Saturation (Amz Prod):{brand_data['market_saturation']:>7.0f} | {brand_data['norm_market_saturation']:>3.0f}/100 | {brand_data['rank_saturation']:>3.0f}th pctile
-------------------------------------------
    """
    report_text.set(report)

    # --- Generate Top Products List ---
    if df_all_products is not None:
        brand_products = df_all_products[df_all_products['brand_name'] == actual_brand_name].copy()
        brand_products.sort_values(by=['avg_rating', 'num_reviews'], ascending=[False, False], inplace=True)
        top_products = brand_products.head(5) 

        products_report = "Top 5 Amazon.sa Products (by Rating):\n" + "-"*35 + "\n"
        if not top_products.empty:
            for _, product in top_products.iterrows():
                price_str = f"{product['price']:.2f} SAR" if pd.notna(product['price']) else "N/A"
                product_name_short = product['product_name'][:45] + '...' if len(product['product_name']) > 45 else product['product_name']
                products_report += f"- {product_name_short}\n"
                products_report += f"    Rating: {product['avg_rating']:.1f} ({product['num_reviews']} rev) | Price: {price_str}\n"
        else:
            products_report += "(No products found for this brand)\n"
        top_products_text.set(products_report)
    else:
        top_products_text.set("Could not load product data.")

    # --- Generate and Embed Radar Chart ---
    if plot_canvas_widget:
        plot_canvas_widget.get_tk_widget().destroy()

    avg_metrics = { 
        'norm_tweet_volume': df_all_metrics['norm_tweet_volume'].mean(),
        'norm_avg_tweet_sentiment': df_all_metrics['norm_avg_tweet_sentiment'].mean(),
        'norm_avg_perceived_quality': df_all_metrics['norm_avg_perceived_quality'].mean(),
        'norm_popularity': df_all_metrics['norm_popularity'].mean(),
        'norm_market_saturation': df_all_metrics['norm_market_saturation'].mean()
    }
    fig, ax = create_radar_figure(brand_data, avg_metrics, actual_brand_name)
    plot_canvas = FigureCanvasTkAgg(fig, master=chart_frame)
    plot_canvas_widget = plot_canvas
    plot_canvas_widget.draw()
    plot_canvas_widget.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)

def create_radar_figure(brand_data_row, avg_data, brand_name):
    # (Keep this function exactly the same as before - v3)
    metrics = ['Hype', 'Sentiment', 'Quality', 'Popularity', 'Low Saturation']
    values_brand = [
        brand_data_row['norm_tweet_volume'], brand_data_row['norm_avg_tweet_sentiment'],
        brand_data_row['norm_avg_perceived_quality'], brand_data_row['norm_popularity'],
        brand_data_row['norm_market_saturation']
    ]
    values_brand += values_brand[:1]
    values_avg = [
        avg_data['norm_tweet_volume'], avg_data['norm_avg_tweet_sentiment'],
        avg_data['norm_avg_perceived_quality'], avg_data['norm_popularity'],
        avg_data['norm_market_saturation']
    ]
    values_avg += values_avg[:1]
    angles = [n / float(len(metrics)) * 2 * pi for n in range(len(metrics))]
    angles += angles[:1]
    fig, ax = plt.subplots(figsize=(5, 5), subplot_kw=dict(polar=True))
    ax.set_xticks(angles[:-1]); ax.set_xticklabels(metrics, color='grey', size=8)
    ax.set_yticks(np.arange(0, 101, 25)); ax.set_yticklabels(["0", "25", "50", "75", "100"], color="grey", size=7)
    ax.set_ylim(0, 100)
    ax.plot(angles, values_avg, linewidth=1, linestyle='solid', label='Average Brand', color='grey', alpha=0.6)
    ax.fill(angles, values_avg, 'grey', alpha=0.2)
    ax.plot(angles, values_brand, linewidth=2, linestyle='solid', label=brand_name, color='blue')
    ax.fill(angles, values_brand, 'blue', alpha=0.4)
    ax.set_title(f'{brand_name} vs. Average', size=10, y=1.1)
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.2), ncol=2, fontsize=8)
    return fig, ax

# --- NEW FUNCTION for Autocomplete ---
def on_keyrelease(event):
    """Called when a key is released in the Combobox."""
    global brand_list
    
    value = event.widget.get().lower() # Get current typed value
    
    if not value:
        # If box is empty, show all brands
        event.widget['values'] = brand_list
    else:
        # Filter the master brand_list
        filtered_list = [b for b in brand_list if b.lower().startswith(value)]
        event.widget['values'] = filtered_list

# --- Setup GUI ---
root = tk.Tk()
root.title("Brand Licensing Suitability Tool") 
root.geometry("950x700") 

style = ttk.Style(); style.theme_use('clam')

# --- Load data *before* creating input widgets ---
print("Loading and processing data, please wait...")
if not load_all_data():
    print("Failed to load data. Exiting application.")
    root.destroy() 
else:
    print("Data load complete. Launching GUI.")
    
    # --- Input Frame ---
    input_frame = ttk.Frame(root, padding="10"); input_frame.pack(side=tk.TOP, fill=tk.X)
    ttk.Label(input_frame, text="Select or Type Brand:").pack(side=tk.LEFT, padx=5) 
    
    # --- FIX: Combobox with event binding ---
    brand_entry = ttk.Combobox(input_frame, width=40, values=brand_list) # State is 'normal' by default
    brand_entry.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
    
    # Bind the <KeyRelease> event to our new function
    brand_entry.bind('<KeyRelease>', on_keyrelease) 
    
    if brand_list: brand_entry.current(0) 
    
    generate_button = ttk.Button(input_frame, text="Generate Report", command=generate_report)
    generate_button.pack(side=tk.LEFT, padx=5)

    # --- Output Frame (Split Vertically) ---
    output_frame = ttk.Frame(root, padding="10"); output_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    # --- Left Column (Text Reports) ---
    left_column = ttk.Frame(output_frame); left_column.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))

    report_frame = ttk.LabelFrame(left_column, text="Report Summary", padding="10")
    report_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 10))
    report_text = tk.StringVar()
    report_text.set("Select a brand from the list above and click 'Generate Report'.")
    report_label = ttk.Label(report_frame, textvariable=report_text, wraplength=450, justify=tk.LEFT, font=("Courier", 9)) 
    report_label.pack(anchor="nw")

    top_products_frame = ttk.LabelFrame(left_column, text="Top Amazon Products", padding="10")
    top_products_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
    top_products_text = tk.StringVar()
    top_products_text.set("Top products will appear here.")
    top_products_label = ttk.Label(top_products_frame, textvariable=top_products_text, wraplength=450, justify=tk.LEFT, font=("Courier", 8)) 
    top_products_label.pack(anchor="nw")

    # --- Right Column (Chart) ---
    chart_frame = ttk.LabelFrame(output_frame, text="Radar Profile", padding="10")
    chart_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    # --- Run GUI ---
    root.mainloop()
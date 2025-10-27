import pandas as pd
import os
import tkinter as tk
from tkinter import ttk, messagebox
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import numpy as np
from math import pi

# --- Configuration ---
DATA_CSV_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'brand_metrics_final.csv')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'reports') # Still useful if saving needed

# --- Scoring Weights ---
WEIGHTS = {
    'hype': 0.40,
    'quality': 0.30,
    'popularity': 0.20,
    'saturation': 0.10
}

# --- Global variable to hold the plot canvas ---
plot_canvas_widget = None

# --- Normalization and Scoring Functions (Keep as before) ---
def normalize(series, higher_is_better=True):
    min_val = series.min()
    max_val = series.max()
    if max_val == min_val:
        return pd.Series([50] * len(series), index=series.index)
    if higher_is_better:
        norm = ((series - min_val) / (max_val - min_val)) * 100
    else:
        norm = ((max_val - series) / (max_val - min_val)) * 100
    return norm.fillna(50)

def calculate_suitability_score(row, max_reviews_log):
    norm_hype = row['norm_tweet_volume']
    norm_quality = (row['avg_perceived_quality'] / 5.0) * 100
    log_reviews = np.log1p(row['avg_num_reviews'])
    norm_popularity = (log_reviews / max_reviews_log) * 100 if max_reviews_log > 0 else 50
    norm_saturation = row['norm_market_saturation']
    score = (norm_hype * WEIGHTS['hype'] +
             norm_quality * WEIGHTS['quality'] +
             norm_popularity * WEIGHTS['popularity'] +
             norm_saturation * WEIGHTS['saturation'])
    return round(score, 1)

def generate_recommendation(score):
    if score >= 75: return "HIGH POTENTIAL: Strong combination of hype and market fit."
    elif score >= 50: return "MODERATE POTENTIAL: Decent metrics, investigate further."
    elif score >= 25: return "LOW POTENTIAL: Significant weaknesses in hype or market data."
    else: return "VERY LOW POTENTIAL: Major concerns across multiple metrics."

# --- Data Loading and Processing Function (Keep as before) ---
def load_and_process_data():
    try:
        df = pd.read_csv(DATA_CSV_PATH)
        if df.empty:
            messagebox.showerror("Error", f"Data file is empty: {DATA_CSV_PATH}")
            return None, None # Return None for max_reviews_log too

        # --- Calculate Normalized Metrics ---
        df['norm_tweet_volume'] = normalize(df['tweet_volume'], higher_is_better=True)
        max_saturation_limit = 25 # Assuming this was our scrape limit
        df['saturation_capped'] = df['market_saturation'].clip(upper=max_saturation_limit)
        df['norm_market_saturation'] = normalize(df['saturation_capped'], higher_is_better=False)
        # Calculate max_reviews_log safely
        max_reviews_log = np.log1p(df['avg_num_reviews']).max()
        # Handle case where max_reviews_log might be 0 or NaN if all reviews are 0
        if pd.isna(max_reviews_log) or max_reviews_log <= 0:
             max_reviews_log = 1 # Avoid division by zero, set a small positive value
             
        # Calculate norm_popularity safely
        df['norm_popularity'] = normalize(np.log1p(df['avg_num_reviews']), higher_is_better=True)


        # Calculate score for all brands
        df['suitability_score'] = df.apply(lambda row: calculate_suitability_score(row, max_reviews_log), axis=1)

        return df, max_reviews_log

    except FileNotFoundError:
        messagebox.showerror("Error", f"Data file not found: {DATA_CSV_PATH}\nPlease run the Jupyter notebook first.")
        return None, None
    except Exception as e:
        messagebox.showerror("Error", f"Could not load or process data file.\nError: {e}")
        return None, None

# --- GUI Functions ---

def generate_report():
    global plot_canvas_widget

    brand_name_query = brand_entry.get()
    if not brand_name_query:
        messagebox.showwarning("Input Needed", "Please enter a brand name.")
        return

    df, max_reviews_log = load_and_process_data()
    if df is None: return

    brand_data_matches = df[df['brand_name'].str.contains(brand_name_query, case=False, na=False)]

    if brand_data_matches.empty:
        messagebox.showerror("Not Found", f"Brand '{brand_name_query}' not found.")
        report_text.set("Report will appear here.")
        if plot_canvas_widget:
            plot_canvas_widget.get_tk_widget().destroy()
            plot_canvas_widget = None
        return
    elif len(brand_data_matches) > 1:
        brand_data = brand_data_matches.iloc[0]
        actual_brand_name = brand_data['brand_name']
        messagebox.showinfo("Multiple Matches", f"Found multiple matches for '{brand_name_query}'.\nDisplaying report for: {actual_brand_name}")
    else:
        brand_data = brand_data_matches.iloc[0]
        actual_brand_name = brand_data['brand_name']

    # --- Generate Text Report ---
    recommendation = generate_recommendation(brand_data['suitability_score'])
    report = f"""
-------------------------------------------
BRAND REPORT: {actual_brand_name}
-------------------------------------------
Metrics:
  - Tweet Volume (Hype Proxy): {brand_data['tweet_volume']:,.0f} tweets
  - Amazon Market Saturation:  {brand_data['market_saturation']:,.0f} products found
  - Avg. Amazon Quality:       {brand_data['avg_perceived_quality']:.1f} / 5.0 stars
  - Avg. Amazon Reviews:       {brand_data['avg_num_reviews']:.1f} reviews/product
-------------------------------------------
SUITABILITY SCORE: {brand_data['suitability_score']:.1f} / 100
RECOMMENDATION:      {recommendation}
-------------------------------------------
    """
    report_text.set(report)

    # --- Generate and Embed Radar Chart ---
    if plot_canvas_widget:
        plot_canvas_widget.get_tk_widget().destroy()

    avg_metrics = {
        'norm_tweet_volume': df['norm_tweet_volume'].mean(),
        'avg_perceived_quality': df['avg_perceived_quality'].mean(),
        'norm_popularity': df['norm_popularity'].mean(),
        'norm_market_saturation': df['norm_market_saturation'].mean()
    }

    # --- Call corrected function ---
    fig, ax = create_radar_figure(brand_data, avg_metrics, actual_brand_name)

    plot_canvas = FigureCanvasTkAgg(fig, master=chart_frame)
    plot_canvas_widget = plot_canvas
    plot_canvas_widget.draw()
    plot_canvas_widget.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)


def create_radar_figure(brand_data, avg_data, brand_name):
    """Creates a Matplotlib Figure object AND AXES for the radar chart.""" # Docstring updated
    metrics = ['Hype', 'Quality', 'Popularity', 'Low Saturation']

    values_brand = [
        brand_data['norm_tweet_volume'],
        (brand_data['avg_perceived_quality'] / 5.0) * 100,
        brand_data['norm_popularity'],
        brand_data['norm_market_saturation']
    ]
    values_brand += values_brand[:1]

    values_avg = [
        avg_data['norm_tweet_volume'],
        (avg_data['avg_perceived_quality'] / 5.0) * 100,
        avg_data['norm_popularity'],
        avg_data['norm_market_saturation']
    ]
    values_avg += values_avg[:1]

    angles = [n / float(len(metrics)) * 2 * pi for n in range(len(metrics))]
    angles += angles[:1]

    # --- Create fig and ax ---
    fig, ax = plt.subplots(figsize=(5, 5), subplot_kw=dict(polar=True))
    
    # --- Use ax object for plotting ---
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(metrics, color='grey', size=8)
    ax.set_yticks(np.arange(0, 101, 25))
    ax.set_yticklabels(["0", "25", "50", "75", "100"], color="grey", size=7)
    ax.set_ylim(0, 100)

    ax.plot(angles, values_avg, linewidth=1, linestyle='solid', label='Average Brand', color='grey', alpha=0.6)
    ax.fill(angles, values_avg, 'grey', alpha=0.2)

    ax.plot(angles, values_brand, linewidth=2, linestyle='solid', label=brand_name, color='blue')
    ax.fill(angles, values_brand, 'blue', alpha=0.4)

    ax.set_title(f'{brand_name} vs. Average', size=10, y=1.1)
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.2), ncol=2, fontsize=8)

    # --- FIX: Return both fig and ax ---
    return fig, ax

# --- Setup GUI (Keep as before) ---
root = tk.Tk()
root.title("Brand Licensing Suitability Tool")
root.geometry("800x650") 

style = ttk.Style()
style.theme_use('clam') 

input_frame = ttk.Frame(root, padding="10")
input_frame.pack(side=tk.TOP, fill=tk.X)

ttk.Label(input_frame, text="Enter Brand Name:").pack(side=tk.LEFT, padx=5)
brand_entry = ttk.Entry(input_frame, width=40)
brand_entry.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
generate_button = ttk.Button(input_frame, text="Generate Report", command=generate_report)
generate_button.pack(side=tk.LEFT, padx=5)

output_frame = ttk.Frame(root, padding="10")
output_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

report_frame = ttk.LabelFrame(output_frame, text="Report", padding="10", width=350)
report_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
report_frame.pack_propagate(False) 

report_text = tk.StringVar()
report_text.set("Enter a brand name above and click 'Generate Report'.")
report_label = ttk.Label(report_frame, textvariable=report_text, wraplength=330, justify=tk.LEFT, font=("Courier", 9))
report_label.pack(anchor="nw")

chart_frame = ttk.LabelFrame(output_frame, text="Radar Profile", padding="10")
chart_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

root.mainloop()

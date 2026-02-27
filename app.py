import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

# Set page configuration
st.set_page_config(
    page_title="Advertising Performance Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
        margin: 0.5rem 0;
    }
    .recommendation-box {
        background-color: #e8f4f8;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #17becf;
        margin: 0.5rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #ffc107;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# --- DATA LOADING FUNCTIONS ---

@st.cache_data
def load_pla_data():
 """Load PLA (Performance by Listing Ads) data from Google Drive"""
 try:
 # Use Google Drive URL from secrets instead of local file
 pla_url = st.secrets.get("PLA_CSV_URL", "pla_onetim_2026-02-26.csv")

 df = pd.read_csv(pla_url)
 # Convert date column
 df['day_date'] = pd.to_datetime(df['day_date'], format='%d-%m-%Y')
 # Convert numeric columns
 numeric_cols = ['unique_views', 'clicks', 'spend', 'atc', 'total_views',
 'listings', 'direct_units', 'indirect_units', 'direct_rev', 'indirect_rev']
 for col in numeric_cols:
 df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
 return df
 except Exception as e:
 st.error(f"Error loading PLA data from Google Drive: {e}")
 st.info("Make sure PLA_CSV_URL is configured in secrets.toml")
 return None

@st.cache_data
def load_pca_data():
 """Load PCA (Product Creative Ads) data from Google Drive"""
 try:
 # Use Google Drive URL from secrets instead of local file
 pca_url = st.secrets.get("PCA_CSV_URL", "pca_onetim_2026-02-26.csv")

 df = pd.read_csv(pca_url)
 # Convert date column
 df['day_date'] = pd.to_datetime(df['day_date'], format='%d-%m-%Y')
 # Convert numeric columns
 numeric_cols = ['viewcount', 'clicks', 'adspend', 'direct_units', 'indirect_units',
 'ppv', 'direct_rev', 'indirect_rev']
 for col in numeric_cols:
 df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
 return df
 except Exception as e:
 st.error(f"Error loading PCA data from Google Drive: {e}")
 st.info("Make sure PCA_CSV_URL is configured in secrets.toml")
 return None

# --- KPI CALCULATION FUNCTIONS ---

def calculate_pla_kpis(df):
    """Calculate KPIs for PLA data"""
    df = df.copy()
    # Avoid division by zero
    df['CTR'] = np.where(df['total_views'] > 0, df['clicks'] / df['total_views'], 0)
    df['Direct_CVR'] = np.where(df['spend'] > 0, df['direct_units'] / df['spend'], 0)
    df['Indirect_CVR'] = np.where(df['spend'] > 0, df['indirect_units'] / df['spend'], 0)
    df['Direct_ROI'] = np.where(df['spend'] > 0, df['direct_rev'] / df['spend'], 0)
    df['Indirect_ROI'] = np.where(df['spend'] > 0, df['indirect_rev'] / df['spend'], 0)
    df['Total_ROI'] = df['Direct_ROI'] + df['Indirect_ROI']
    df['Total_Revenue'] = df['direct_rev'] + df['indirect_rev']
    df['Total_Units'] = df['direct_units'] + df['indirect_units']
    df['ROAS'] = np.where(df['spend'] > 0, df['Total_Revenue'] / df['spend'], 0)
    return df

def calculate_pca_kpis(df):
    """Calculate KPIs for PCA data"""
    df = df.copy()
    # Avoid division by zero
    df['CTR'] = np.where(df['viewcount'] > 0, df['clicks'] / df['viewcount'], 0)
    df['Direct_CVR'] = np.where(df['adspend'] > 0, df['direct_units'] / df['adspend'], 0)
    df['Indirect_CVR'] = np.where(df['adspend'] > 0, df['indirect_units'] / df['adspend'], 0)
    df['Direct_ROI'] = np.where(df['adspend'] > 0, df['direct_rev'] / df['adspend'], 0)
    df['Indirect_ROI'] = np.where(df['adspend'] > 0, df['indirect_rev'] / df['adspend'], 0)
    df['Total_ROI'] = df['Direct_ROI'] + df['Indirect_ROI']
    df['Total_Revenue'] = df['direct_rev'] + df['indirect_rev']
    df['Total_Units'] = df['direct_units'] + df['indirect_units']
    df['ROAS'] = np.where(df['adspend'] > 0, df['Total_Revenue'] / df['adspend'], 0)
    return df

# --- BUDGET OPTIMIZATION FUNCTIONS ---

def optimize_budget_historical(df, total_budget, data_type='pla'):
    """
    Optimize budget allocation based on historical performance
    Returns recommended budget allocation by brand/supercategory
    """
    if data_type == 'pla':
        spend_col = 'spend'
        roi_col = 'Total_ROI'
        group_cols = ['brand', 'analytic_super_category']
    else:
        spend_col = 'adspend'
        roi_col = 'Total_ROI'
        group_cols = ['brand', 'super_category']

    # Aggregate performance by groups
    perf_df = df.groupby(group_cols).agg({
        spend_col: 'sum',
        roi_col: 'mean',
        'Total_Revenue': 'sum',
        'Total_Units': 'sum'
    }).reset_index()

    # Calculate efficiency score (ROI * historical spend weight)
    perf_df['Efficiency_Score'] = perf_df[roi_col] * np.log1p(perf_df[spend_col])
    perf_df['Efficiency_Score'] = perf_df['Efficiency_Score'].fillna(0)

    # Normalize efficiency scores
    if perf_df['Efficiency_Score'].max() > 0:
        perf_df['Efficiency_Score'] = perf_df['Efficiency_Score'] / perf_df['Efficiency_Score'].max()

    # Allocate budget proportionally to efficiency scores
    total_efficiency = perf_df['Efficiency_Score'].sum()
    if total_efficiency > 0:
        perf_df['Recommended_Budget'] = (perf_df['Efficiency_Score'] / total_efficiency) * total_budget
    else:
        # Equal allocation if no efficiency data
        perf_df['Recommended_Budget'] = total_budget / len(perf_df)

    # Calculate expected outcomes
    perf_df['Expected_ROI'] = perf_df[roi_col] * perf_df['Recommended_Budget']
    perf_df['Expected_Revenue'] = perf_df['Expected_ROI']

    return perf_df.sort_values('Efficiency_Score', ascending=False)

def generate_recommendations(df, data_type='pla'):
    """Generate actionable recommendations based on performance data"""
    recommendations = []

    if data_type == 'pla':
        spend_col = 'spend'
        roi_col = 'Total_ROI'
        ctr_col = 'CTR'
        group_cols = ['brand', 'analytic_super_category']
    else:
        spend_col = 'adspend'
        roi_col = 'Total_ROI'
        ctr_col = 'CTR'
        group_cols = ['brand', 'super_category']

    # Top performers
    top_performers = df.groupby(group_cols)[roi_col].mean().nlargest(3)
    if not top_performers.empty:
        recommendations.append({
            'type': 'success',
            'title': '🚀 Top Performing Categories',
            'message': f"Increase budget for: {', '.join([f'{k[0]} ({k[1]})' for k in top_performers.index])}"
        })

    # Underperformers
    low_performers = df.groupby(group_cols)[roi_col].mean().nsmallest(3)
    if not low_performers.empty:
        recommendations.append({
            'type': 'warning',
            'title': '⚠️ Underperforming Categories',
            'message': f"Review/reduce budget for: {', '.join([f'{k[0]} ({k[1]})' for k in low_performers.index])}"
        })

    # High spend, low ROI
    high_spend_low_roi = df[df[spend_col] > df[spend_col].quantile(0.75)]
    high_spend_low_roi = high_spend_low_roi[high_spend_low_roi[roi_col] < df[roi_col].quantile(0.25)]
    if not high_spend_low_roi.empty:
        brands = high_spend_low_roi[group_cols[0]].unique()[:3]
        recommendations.append({
            'type': 'warning',
            'title': '💰 High Spend, Low ROI Alert',
            'message': f"Optimize campaigns for: {', '.join(brands)}"
        })

    # High CTR opportunities
    high_ctr = df[df[ctr_col] > df[ctr_col].quantile(0.75)]
    if not high_ctr.empty:
        brands = high_ctr[group_cols[0]].unique()[:3]
        recommendations.append({
            'type': 'info',
            'title': '🎯 High CTR Opportunities',
            'message': f"Scale campaigns for: {', '.join(brands)}"
        })

    return recommendations

# --- VISUALIZATION FUNCTIONS ---

def create_kpi_summary(df, title, data_type='pla'):
    """Create KPI summary cards"""
    if data_type == 'pla':
        spend_col = 'spend'
        revenue_col = 'Total_Revenue'
        roi_col = 'Total_ROI'
    else:
        spend_col = 'adspend'
        revenue_col = 'Total_Revenue'
        roi_col = 'Total_ROI'

    total_spend = df[spend_col].sum()
    total_revenue = df[revenue_col].sum()
    avg_roi = df[roi_col].mean()
    total_units = df['Total_Units'].sum()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Spend", "2.1f")

    with col2:
        st.metric("Total Revenue", "2.1f")

    with col3:
        st.metric("Average ROI", "2.2f")

    with col4:
        st.metric("Total Units", f"{total_units:,.0f}")

def create_performance_chart(df, group_by, metric, data_type='pla'):
    """Create performance visualization"""
    if data_type == 'pla':
        spend_col = 'spend'
    else:
        spend_col = 'adspend'

    if group_by == 'brand':
        group_col = 'brand'
    elif group_by == 'supercategory':
        group_col = 'analytic_super_category' if data_type == 'pla' else 'super_category'
    else:
        group_col = group_by

    # Aggregate data
    agg_df = df.groupby(group_col).agg({
        spend_col: 'sum',
        metric: 'mean'
    }).reset_index().sort_values(spend_col, ascending=False).head(10)

    fig = px.bar(agg_df, x=group_col, y=metric,
                 title=f'{metric} by {group_by.title()} (Top 10 by Spend)',
                 color=spend_col, color_continuous_scale='Blues')

    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

def create_roi_vs_spend_scatter(df, data_type='pla'):
    """Create ROI vs Spend scatter plot"""
    if data_type == 'pla':
        spend_col = 'spend'
        roi_col = 'Total_ROI'
        group_cols = ['brand', 'analytic_super_category']
    else:
        spend_col = 'adspend'
        roi_col = 'Total_ROI'
        group_cols = ['brand', 'super_category']

    # Aggregate by groups
    agg_df = df.groupby(group_cols).agg({
        spend_col: 'sum',
        roi_col: 'mean',
        'Total_Revenue': 'sum'
    }).reset_index()

    # Create hover text
    agg_df['hover_text'] = agg_df.apply(lambda row: f"{row[group_cols[0]]} ({row[group_cols[1]]})<br>Spend: ₹{row[spend_col]:,.0f}<br>ROI: {row[roi_col]:.2f}<br>Revenue: ₹{row['Total_Revenue']:,.0f}", axis=1)

    fig = px.scatter(agg_df, x=spend_col, y=roi_col,
                    size='Total_Revenue', color=roi_col,
                    title='ROI vs Spend Analysis',
                    labels={'x': 'Spend (₹)', 'y': 'ROI'},
                    hover_data={'hover_text': True},
                    color_continuous_scale='RdYlGn')

    fig.update_traces(hovertemplate='%{customdata[0]}')
    st.plotly_chart(fig, use_container_width=True)

# --- MAIN APPLICATION ---

def main():
    st.markdown('<div class="main-header">📊 Advertising Performance Analytics</div>', unsafe_allow_html=True)

    # Load data
    pla_df = load_pla_data()
    pca_df = load_pca_data()

    if pla_df is None and pca_df is None:
        st.error("Unable to load data files. Please ensure pla_onetim_2026-02-26.csv and pca_onetim_2026-02-26.csv are in the same directory.")
        return

    # Sidebar
    st.sidebar.title("🎛️ Controls")

    # Data source selection
    data_source = st.sidebar.selectbox(
        "Select Data Source",
        ["PLA (Brand Level)", "PCA (Campaign Level)"],
        index=0
    )

    data_type = 'pla' if data_source == "PLA (Brand Level)" else 'pca'
    df = pla_df if data_type == 'pla' else pca_df

    if df is None:
        st.error(f"Unable to load {data_source} data.")
        return

    # Calculate KPIs
    df_with_kpis = calculate_pla_kpis(df) if data_type == 'pla' else calculate_pca_kpis(df)

    # Date filter
    if len(df_with_kpis['day_date'].unique()) > 1:
        date_range = st.sidebar.date_input(
            "Select Date Range",
            [df_with_kpis['day_date'].min(), df_with_kpis['day_date'].max()]
        )
        if len(date_range) == 2:
            df_filtered = df_with_kpis[(df_with_kpis['day_date'] >= pd.to_datetime(date_range[0])) &
                                      (df_with_kpis['day_date'] <= pd.to_datetime(date_range[1]))]
        else:
            df_filtered = df_with_kpis
    else:
        df_filtered = df_with_kpis

    # Main content
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Overview", "🎯 Performance Analysis", "💰 Budget Optimizer", "🔍 Insights"])

    with tab1:
        st.header("Overview Dashboard")
        create_kpi_summary(df_filtered, f"{data_source} Performance", data_type)

        # Performance by key dimensions
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Performance by Brand")
            create_performance_chart(df_filtered, 'brand', 'Total_ROI', data_type)

        with col2:
            st.subheader("Performance by Supercategory")
            create_performance_chart(df_filtered, 'supercategory', 'Total_ROI', data_type)

        # ROI vs Spend Analysis
        st.subheader("ROI vs Spend Scatter Plot")
        create_roi_vs_spend_scatter(df_filtered, data_type)

    with tab2:
        st.header("Detailed Performance Analysis")

        # Metric selection
        metric_options = ['Total_ROI', 'CTR', 'Direct_CVR', 'Indirect_CVR', 'Direct_ROI', 'Indirect_ROI', 'ROAS']
        selected_metric = st.selectbox("Select Metric to Analyze", metric_options)

        # Grouping options
        if data_type == 'pla':
            group_options = ['brand', 'analytic_super_category', 'analytic_vertical', 'page_context', 'slot_type']
        else:
            group_options = ['brand', 'super_category', 'page_type', 'creative_type']

        selected_group = st.selectbox("Group By", group_options)

        # Create analysis chart
        if selected_group in df_filtered.columns:
            agg_df = df_filtered.groupby(selected_group)[selected_metric].agg(['mean', 'sum', 'count']).reset_index()
            agg_df = agg_df.sort_values('mean', ascending=False)

            fig = px.bar(agg_df.head(15), x=selected_group, y='mean',
                        title=f'Average {selected_metric} by {selected_group}',
                        labels={'mean': f'Average {selected_metric}'})
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

            # Show top/bottom performers
            col1, col2 = st.columns(2)

            with col1:
                st.subheader(f"Top 5 {selected_group}")
                st.dataframe(agg_df.head(5)[[selected_group, 'mean', 'sum', 'count']],
                           use_container_width=True)

            with col2:
                st.subheader(f"Bottom 5 {selected_group}")
                st.dataframe(agg_df.tail(5)[[selected_group, 'mean', 'sum', 'count']],
                           use_container_width=True)

    with tab3:
        st.header("💰 Groundbreaking Budget Optimizer")

        # Budget input
        total_budget = st.number_input(
            "Enter Total Budget for Upcoming BSD Sales (₹)",
            min_value=10000,
            max_value=10000000,
            value=100000,
            step=10000
        )

        if st.button("🚀 Optimize Budget Allocation"):
            with st.spinner("Analyzing historical performance and optimizing budget allocation..."):
                optimization_results = optimize_budget_historical(df_filtered, total_budget, data_type)

                st.success("Budget optimization completed!")

                # Display results
                st.subheader("Recommended Budget Allocation")

                # Summary metrics
                total_recommended = optimization_results['Recommended_Budget'].sum()
                total_expected_revenue = optimization_results['Expected_Revenue'].sum()
                avg_expected_roi = optimization_results['Expected_ROI'].mean()

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Recommended Budget", "2.1f")
                with col2:
                    st.metric("Expected Revenue", "2.1f")
                with col3:
                    st.metric("Expected ROI", "2.2f")

                # Detailed allocation table
                st.subheader("Detailed Budget Allocation")
                display_df = optimization_results[[
                    'brand', 'analytic_super_category' if data_type == 'pla' else 'super_category',
                    'Recommended_Budget', 'Expected_Revenue', 'Expected_ROI', 'Efficiency_Score'
                ]].copy()

                display_df.columns = ['Brand', 'Category', 'Budget (₹)', 'Expected Revenue (₹)', 'Expected ROI', 'Efficiency Score']
                display_df = display_df.round(2)
                st.dataframe(display_df, use_container_width=True)

                # Visualization
                fig = px.treemap(optimization_results,
                               path=['brand', 'analytic_super_category' if data_type == 'pla' else 'super_category'],
                               values='Recommended_Budget',
                               title='Budget Allocation Treemap')
                st.plotly_chart(fig, use_container_width=True)

                # Performance comparison
                fig2 = px.scatter(optimization_results, x='Recommended_Budget', y='Expected_ROI',
                                size='Expected_Revenue', color='brand',
                                title='Budget vs Expected Performance')
                st.plotly_chart(fig2, use_container_width=True)

    with tab4:
        st.header("🔍 AI-Powered Insights & Recommendations")

        # Generate recommendations
        recommendations = generate_recommendations(df_filtered, data_type)

        if recommendations:
            for rec in recommendations:
                if rec['type'] == 'success':
                    st.markdown(f'<div class="recommendation-box"><strong>{rec["title"]}</strong><br>{rec["message"]}</div>', unsafe_allow_html=True)
                elif rec['type'] == 'warning':
                    st.markdown(f'<div class="warning-box"><strong>{rec["title"]}</strong><br>{rec["message"]}</div>', unsafe_allow_html=True)
                else:
                    st.info(f"**{rec['title']}**\n\n{rec['message']}")
        else:
            st.info("No specific recommendations available based on current data.")

        # Key insights
        st.subheader("Key Performance Insights")

        # Calculate key metrics
        total_spend = df_filtered['spend' if data_type == 'pla' else 'adspend'].sum()
        total_revenue = df_filtered['Total_Revenue'].sum()
        avg_roi = df_filtered['Total_ROI'].mean()
        avg_ctr = df_filtered['CTR'].mean()

        insights = []

        if avg_roi > 2.0:
            insights.append("✅ Excellent ROI performance - you're generating strong returns on ad spend")
        elif avg_roi > 1.0:
            insights.append("⚠️ Moderate ROI - there's room for optimization")
        else:
            insights.append("❌ Poor ROI - immediate attention needed to improve campaign efficiency")

        if avg_ctr > 0.02:
            insights.append("🎯 Strong CTR indicates good ad relevance and targeting")
        elif avg_ctr > 0.01:
            insights.append("📊 Average CTR - consider A/B testing ad creative")
        else:
            insights.append("📉 Low CTR - review ad creative and targeting strategy")

        direct_revenue_pct = (df_filtered['direct_rev'].sum() / total_revenue * 100) if total_revenue > 0 else 0
        if direct_revenue_pct > 70:
            insights.append("💪 Strong direct revenue contribution - excellent conversion optimization")
        elif direct_revenue_pct > 50:
            insights.append("🔄 Balanced direct/indirect revenue mix")
        else:
            insights.append("🔗 Heavy reliance on indirect revenue - focus on direct conversion optimization")

        for insight in insights:
            st.markdown(f'<div class="metric-card">{insight}</div>', unsafe_allow_html=True)

        # Data export
        st.subheader("Export Analysis Results")
        if st.button("📊 Export to CSV"):
            csv_data = df_filtered.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv_data,
                file_name=f"{data_type}_analysis_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

if __name__ == "__main__":
    main()
        image_cv = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)

        if image_cv is None: return {"error": "Decode Failed"}

        # Resize for stability
        h, w = image_cv.shape[:2]
        if max(h, w) > 1500:
            scale = 1500 / max(h, w)
            image_cv = cv2.resize(image_cv, (int(w*scale), int(h*scale)))

        height, width, _ = image_cv.shape
        total_area = width * height

        # --- CV Features ---
        gray = cv2.cvtColor(image_cv, cv2.COLOR_BGR2GRAY)
        faces = face_cascade.detectMultiScale(gray, 1.1, 5, minSize=(50, 50))
        has_face = len(faces) > 0
        
        brightness = np.mean(gray)
        if brightness < 90: brightness_level = "Low (Dark)"
        elif brightness < 180: brightness_level = "Medium (Balanced)"
        else: brightness_level = "High (Bright)"

        # --- Visual Style ---
        texture_score = np.std(gray)
        if texture_score < 40: visual_style = "2D / Flat"
        elif texture_score < 60: visual_style = "Mixed"
        else: visual_style = "3D / Photo"

        # --- PRODUCT DETECTION ---
        blurred = cv2.GaussianBlur(gray, (5, 5), 0)
        edges = cv2.Canny(blurred, 50, 150)
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 15)) 
        closed = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, kernel)
        
        roi_start_x = int(width * 0.40) 
        closed[:, :roi_start_x] = 0 
        
        contours, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        product_mask = np.zeros_like(gray)
        significant_area = 0.0
        
        if contours:
            for c in contours:
                area = cv2.contourArea(c)
                x, y, fw, fh = cv2.boundingRect(c)
                is_border = (fw > 0.95 * width) or (fh > 0.95 * height)
                if area > (0.02 * total_area) and not is_border:
                    significant_area += area
                    cv2.drawContours(product_mask, [c], -1, 255, -1)

        product_area_pct = min((significant_area / total_area) * 100, 100.0)
        if product_area_pct < 15: product_size_bucket = "Small (<15%)"
        elif product_area_pct < 40: product_size_bucket = "Medium (15-40%)"
        else: product_size_bucket = "Large (>40%)"

        # --- CONTRAST ---
        if cv2.countNonZero(product_mask) == 0:
            bg_brightness = np.mean(gray)
            contrast_val = 0
            bg_label = "Unknown"
        else:
            prod_brightness = cv2.mean(gray, mask=product_mask)[0]
            bg_mask = cv2.bitwise_not(product_mask)
            bg_brightness = cv2.mean(gray, mask=bg_mask)[0]
            contrast_val = abs(prod_brightness - bg_brightness)
            
            if bg_brightness < 90: bg_label = "Dark Background"
            elif bg_brightness < 170: bg_label = "Medium Background"
            else: bg_label = "Light/White Background"

        if contrast_val < 40: contrast_label = "Low Contrast"
        elif contrast_val < 90: contrast_label = "Medium Contrast"
        else: contrast_label = "High Contrast"

        # --- TEXT ANALYSIS ---
        image_rgb = cv2.cvtColor(image_cv, cv2.COLOR_BGR2RGB)
        ocr_results = ocr_reader.readtext(image_rgb, detail=1, paragraph=False)
        
        text_blocks = []
        
        # Regex (With Monthly Support)
        price_suffix = r"(?:/-|/M|/MO|/MONTH|\s+PER\s+MONTH)?"
        hook_price_regex = re.compile(r"((?:FROM|STARTS?|STARTING|JUST|ONLY|NOW|AT|@)\s*(?:[^0-9\s]{0,3})\s*[\d,.]+" + price_suffix + r")")
        loose_price_regex = re.compile(r"((?:₹|\$|€|£|RS\.?|INR|\?)\s*[\d,.]+" + price_suffix + r")")
        offer_regex = re.compile(r"(\d{1,2}\s?% (?:OFF)?|SALE|FREE SHIPPING|FREE|BOGO|DEAL|OFFER|FLAT \d+%)")

        callout_y_top = float('inf')
        has_callout_block = False
        
        headline_text = "None"

        for result in ocr_results:
            bbox, text, conf = result
            text_clean = text.upper()
            
            tl, tr, br, bl = bbox
            box_width = tr[0] - tl[0]
            box_height = bl[1] - tl[1]
            box_area = box_width * box_height
            box_center_x = (tl[0] + tr[0]) / 2
            box_top_y = tl[1]
            box_bottom_y = bl[1]
            
            # Check Callout
            is_callout = False
            if hook_price_regex.search(text_clean) or loose_price_regex.search(text_clean) or offer_regex.search(text_clean):
                is_callout = True
                has_callout_block = True
                if box_top_y < callout_y_top:
                    callout_y_top = box_top_y

            text_blocks.append({
                'text': text,
                'h': box_height,
                'cx': box_center_x,
                'y_top': box_top_y,
                'y_bottom': box_bottom_y,
                'area': box_area,
                'is_callout': is_callout
            })

        # --- 1. CONSTRUCT HEADLINE (ALL TEXT) ---
        # We define "Headline" here as the aggregation of ALL significant text on the image
        # Filter: Height > 15px to ignore tiny noise
        significant_blocks = [b for b in text_blocks if b['h'] > 15]
        
        # Sort Top-to-Bottom so it reads naturally
        significant_blocks.sort(key=lambda x: x['y_top'])
        
        if significant_blocks:
            headline_text = " | ".join([b['text'] for b in significant_blocks])
        else:
            headline_text = "None"

        # --- 2. TITLE DETECTION ---
        title_text = "None"
        
        if has_callout_block:
            # Strategy A: Text strictly above the callout
            candidates = [b for b in text_blocks if b['y_bottom'] < callout_y_top and not b['is_callout']]
            candidates.sort(key=lambda x: x['y_bottom'], reverse=True)
            
            for cand in candidates:
                if cand['cx'] < (width * 0.75) and cand['h'] > 10: 
                    title_text = cand['text']
                    break
        
        if title_text == "None":
            # Strategy B: Largest Font on Left
            max_title_h = 0
            for b in text_blocks:
                is_left = b['cx'] < (width * 0.60)
                is_middle = (height * 0.10) < b['y_bottom'] < (height * 0.90)
                if is_left and is_middle and not b['is_callout']:
                    if b['h'] > max_title_h:
                        max_title_h = b['h']
                        title_text = b['text']

        # --- PRICE EXTRACTION ---
        # Use ALL detected text blocks for extraction, not just the headline, to ensure small offers are caught
        raw_text_full = " ".join([b['text'] for b in text_blocks])
        cleaned_text = raw_text_full.upper()
        
        callout_type = "None"
        extracted_price = None
        extracted_offer = None

        hook_match = hook_price_regex.search(cleaned_text)
        loose_match = loose_price_regex.search(cleaned_text)
        suffix_match = re.compile(r"([\d,.]+/-)").search(cleaned_text)
        offer_match = offer_regex.search(cleaned_text)
        
        if hook_match:
            callout_type = "Price Hook"
            extracted_price = hook_match.group(1)
        elif loose_match:
            callout_type = "Price Only"
            extracted_price = loose_match.group(1)
        elif suffix_match:
            callout_type = "Price Only"
            extracted_price = suffix_match.group(1)
        
        if offer_match:
            if "Price" in callout_type: callout_type = "Price + Offer"
            else: callout_type = "Offer"
            extracted_offer = offer_match.group(1)

        if extracted_price:
            extracted_price = extracted_price.replace("?", "₹").replace("~", "₹")
            extracted_price = re.sub(r"([A-Z])(₹)", r"\1 \2", extracted_price)

        return {
            "title_text": title_text,       
            "headline_text": headline_text, # Contains ALL significant text
            "bg_label": bg_label,
            "contrast_label": contrast_label,
            "product_area_pct": product_area_pct,
            "product_size_bucket": product_size_bucket,
            "visual_style": visual_style,
            "has_face": has_face,
            "brightness_level": brightness_level,
            "callout_type": callout_type,
            "extracted_price": extracted_price, 
            "extracted_offer": extracted_offer,
            "raw_text": raw_text_full
        }
    except Exception as e:
        return {"error": str(e)}

# --- 2. REPORTING UI ---

def display_full_data(df_sorted, metric, image_name_col):
    st.markdown("--- \n ## 3. Detailed Data")
    cols = [
        image_name_col, metric, 
        'title_text', 'headline_text', 
        'extracted_price', 'extracted_offer',
        'bg_label', 'contrast_label',
        'product_size_bucket', 'product_area_pct', 'has_face'
    ]
    cols = [c for c in cols if c in df_sorted.columns]
    
    display_df = df_sorted[cols].copy()
    if 'product_area_pct' in display_df.columns:
        display_df['product_area_pct'] = display_df['product_area_pct'].apply(lambda x: f"{x:.1f}%" if isinstance(x, (int, float)) else x)

    st.dataframe(display_df, use_container_width=True)

def display_aggregate_report(above_bench_df, below_bench_df, metric, benchmark):
    st.markdown("--- \n ## 2. Aggregate Analysis")
    st.markdown(f"Comparing **{len(above_bench_df)}** Top Performers (> {benchmark}) vs. **{len(below_bench_df)}** Low Performers (<= {benchmark}).")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### Background")
        if not above_bench_df.empty:
            st.bar_chart(above_bench_df['bg_label'].value_counts(normalize=True))
    with col2:
        st.markdown("### Face Detection")
        if not above_bench_df.empty:
            face_counts = above_bench_df['has_face'].astype(str).value_counts(normalize=True)
            st.bar_chart(face_counts.reindex(['True', 'False']).fillna(0))
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### Contrast Level")
        if not above_bench_df.empty:
            st.caption(f"Above {benchmark}")
            st.bar_chart(above_bench_df['contrast_label'].value_counts(normalize=True))
        if not below_bench_df.empty:
            st.caption(f"Below {benchmark}")
            st.bar_chart(below_bench_df['contrast_label'].value_counts(normalize=True))

    with col2:
        st.markdown("### Product Size")
        if not above_bench_df.empty:
            st.caption(f"Above {benchmark}")
            st.bar_chart(above_bench_df['product_size_bucket'].value_counts(normalize=True))
        if not below_bench_df.empty:
            st.caption(f"Below {benchmark}")
            st.bar_chart(below_bench_df['product_size_bucket'].value_counts(normalize=True))
    
    st.markdown("### Top Text Elements")
    def get_top_phrases(series):
        counts = Counter(series.dropna()).most_common(5)
        return pd.DataFrame(counts, columns=["Phrase", "Count"]) if counts else None

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Top Titles (Above Avg)**")
        df_t = get_top_phrases(above_bench_df['title_text'])
        if df_t is not None: st.dataframe(df_t, use_container_width=True, hide_index=True)
        st.markdown(f"**Top Prices (Above Avg)**")
        df_p = get_top_phrases(above_bench_df['extracted_price'])
        if df_p is not None: st.dataframe(df_p, use_container_width=True, hide_index=True)
        
    with col2:
        st.markdown(f"**Top Titles (Below Avg)**")
        df_tb = get_top_phrases(below_bench_df['title_text'])
        if df_tb is not None: st.dataframe(df_tb, use_container_width=True, hide_index=True)
        st.markdown(f"**Top Prices (Below Avg)**")
        df_pb = get_top_phrases(below_bench_df['extracted_price'])
        if df_pb is not None: st.dataframe(df_pb, use_container_width=True, hide_index=True)

def display_best_vs_worst(df_sorted, metric, images_dict):
    st.markdown("--- \n ## 1. Best vs. Worst (Overall)") 
    if len(df_sorted) == 0: return

    best = df_sorted.iloc[0]
    worst = df_sorted.iloc[-1]
    
    col1, col2 = st.columns(2)
    
    def get_img_bytes(name, img_dict, img_lookup):
        name_str = str(name).strip()
        if name_str in img_dict: return img_dict[name_str]
        if name_str.lower() in img_lookup: return img_dict[img_lookup[name_str.lower()]]
        return None

    img_lookup_display = {}
    for name in images_dict.keys():
        img_lookup_display[name.lower()] = name
        name_no_ext = os.path.splitext(name)[0]
        img_lookup_display[name_no_ext.lower()] = name

    for col, item, title in [(col1, best, "🥇 Best"), (col2, worst, "🥉 Worst")]:
        with col:
            st.markdown(f"### {title}")
            st.markdown(f"**{item[metric]:.4f}** ({metric})")
            
            img_name = item['image_name']
            img_bytes = get_img_bytes(img_name, images_dict, img_lookup_display)
            
            if img_bytes:
                st.image(img_bytes, use_column_width=True)
            else:
                st.error(f"Image '{img_name}' not found.")
            
            if "error" in item:
                st.error(f"Analysis Failed: {item['error']}")
            else:
                st.success(f"**Title:** {item.get('title_text', 'N/A')}")
                with st.expander("See Full Headline/Copy"):
                    st.write(item.get('headline_text', 'N/A'))
                st.info(f"**Background:** {item.get('bg_label', '-')}")
                st.info(f"**Contrast:** {item.get('contrast_label', '-')}")
                st.write(f"**Face:** {'Yes' if item.has_face else 'No'}")
                st.write(f"**Product Size:** {item.get('product_size_bucket', 'N/A')} ({item.get('product_area_pct', 0):.1f}%)")
                st.write(f"**Price:** {item.extracted_price or '-'}")
                st.write(f"**Offer:** {item.extracted_offer or '-'}")

# --- 3. MAIN APP ---

st.set_page_config(layout="wide")
st.title("Creative Analysis Dashboard")

st.sidebar.header("1. Upload Files")
csv_file = st.sidebar.file_uploader("Upload Metrics CSV", type="csv")
uploaded_images = st.sidebar.file_uploader("Upload Creative Images", type=["png", "jpg", "jpeg", "webp"], accept_multiple_files=True)

st.sidebar.header("2. Configure Analysis")
metric_col = st.sidebar.text_input("Metric Column (e.g. CTR)", "CTR")
image_name_col = st.sidebar.text_input("Image Name Column", "image_name")
benchmark_val = st.sidebar.number_input("Benchmark Value (Split High/Low)", value=1.1, step=0.1)

if st.sidebar.button("Run Analysis", use_container_width=True):
    if not csv_file or not uploaded_images:
        st.error("Please upload both a CSV file and at least one Image before running.")
        st.stop()

    with st.spinner("Loading AI Models..."):
        face_cascade, ocr_reader = load_models()
        if not face_cascade: st.stop()

    try:
        df = pd.read_csv(csv_file)
        if image_name_col in df.columns:
            df[image_name_col] = df[image_name_col].astype(str).str.strip()
        
        if metric_col not in df.columns or image_name_col not in df.columns:
            st.error(f"Columns not found! CSV has: {list(df.columns)}")
            st.stop()
    except Exception as e:
        st.error(f"CSV Error: {e}")
        st.stop()

    images_dict = {f.name.strip(): f.getvalue() for f in uploaded_images}
    
    images_lookup = {}
    for name in images_dict.keys():
        images_lookup[name] = name 
        images_lookup[name.lower()] = name 
        name_no_ext = os.path.splitext(name)[0]
        images_lookup[name_no_ext] = name 
        images_lookup[name_no_ext.lower()] = name 

    all_features = []
    bar = st.progress(0, text="Analyzing...")
    total_rows = len(df)
    
    for i, row in df.iterrows():
        csv_name = row[image_name_col]
        
        target_key = None
        if csv_name in images_dict: target_key = csv_name
        elif csv_name.lower() in images_lookup: target_key = images_lookup[csv_name.lower()]
        elif csv_name in images_lookup: target_key = images_lookup[csv_name]
        elif csv_name.lower() in images_lookup: target_key = images_lookup[csv_name.lower()]
            
        if target_key:
            feats = analyze_image_features(images_dict[target_key], face_cascade, ocr_reader)
            all_features.append({**row.to_dict(), **feats, 'image_name': csv_name})
        
        bar.progress((i + 1) / total_rows)
    bar.empty()
    
    if not all_features:
        st.error("No matching images analyzed.")
        with st.expander("Debug: Mismatch Checker", expanded=True):
            col_a, col_b = st.columns(2)
            with col_a:
                st.write("**CSV Filenames (First 5):**")
                st.write(df[image_name_col].head(5).tolist())
            with col_b:
                st.write("**Uploaded Filenames (First 5):**")
                st.write(list(images_dict.keys())[:5])
        st.stop()

    res_df = pd.DataFrame(all_features)
    
    if metric_col in res_df.columns:
        res_df[metric_col] = pd.to_numeric(res_df[metric_col], errors='coerce')
        res_df = res_df.sort_values(by=metric_col, ascending=False)
    
    valid_df = res_df[res_df.get('error').isna()] if 'error' in res_df.columns else res_df
    
    if not valid_df.empty and metric_col in valid_df.columns:
        mean_val = valid_df[metric_col].mean()
        col1, col2 = st.columns(2)
        col1.metric(label=f"Dataset Average {metric_col}", value=f"{mean_val:.4f}")
        col2.metric(label="Benchmark Used", value=f"{benchmark_val}")
        
        display_best_vs_worst(valid_df, metric_col, images_dict)

        display_aggregate_report(
            valid_df[valid_df[metric_col] > benchmark_val], 
            valid_df[valid_df[metric_col] <= benchmark_val], 
            metric_col,
            benchmark_val
        )
    else:
        st.warning("Analysis complete, but no valid metrics found to aggregate.")

    display_full_data(res_df, metric_col, image_name_col)

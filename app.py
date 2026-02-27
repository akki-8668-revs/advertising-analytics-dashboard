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
        st.metric("Total Spend", f"₹{total_spend:,.2f}")

    with col2:
        st.metric("Total Revenue", f"₹{total_revenue:,.2f}")

    with col3:
        st.metric("Average ROI", f"{avg_roi:.2f}")

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
                    st.metric("Total Recommended Budget", f"₹{total_recommended:,.2f}")
                with col2:
                    st.metric("Expected Revenue", f"₹{total_expected_revenue:,.2f}")
                with col3:
                    st.metric("Expected ROI", f"{avg_expected_roi:.2f}")

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

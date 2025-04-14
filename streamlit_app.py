import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery
import plotly.express as px
import plotly.graph_objects as go
from functools import lru_cache
import time

client = bigquery.Client(project="ontario-sunshine")

# Dashboard Title and Description
st.title("Ontario Sunshine List")
st.write("""
Welcome to the Ontario Sunshine List Insights Dashboard — an interactive tool for exploring Ontario Public Sector Salary Disclosure data, commonly referred to as the 'Sunshine List.'

The Sunshine List requires organizations receiving significant public funding to annually disclose the names, positions, salaries, and total taxable benefits of employees earning $100,000 or more.
This dashboard is designed to provide a comprehensive analysis of high earners, compensation trends, and employment patterns across various sectors government ministries, Crown agencies, municipalities, hospitals, school boards, universities, colleges, and other publicly funded organizations.
""")

### SALARY TRENDS ###

st.header("Overall Salary Trends")
st.write("""
View the overall trend in average and median salaries from 1996 till present.
""")

query_salary_trends = """
    SELECT *
    FROM `ontario-sunshine.sunshine_dbt_dev_dataset.fct_salary_trends`
"""

query_job = client.query(query_salary_trends)
df_salary_trends = query_job.result().to_dataframe()

df_salary_trends['calendar_year'] = pd.to_datetime(df_salary_trends['calendar_year'], format='%Y')
df_salary_trends = df_salary_trends.sort_values('calendar_year')

fig_avg = px.line(
    df_salary_trends,
    x='calendar_year',
    y='avg_total_compensation',
    title='Average Total Compensation (CAD) Over Time',
    labels={'avg_total_compensation': 'Average Total Compensation ($)', 'calendar_year': 'Year'}
    )

fig_avg.update_yaxes(tickprefix="$", tickformat=",.0f")
fig_avg.update_xaxes(range=[pd.to_datetime('1996-01-01'), df_salary_trends['calendar_year'].max()])

fig_med = px.line(
    df_salary_trends,
    x='calendar_year',
    y='median_total_compensation',
    title='Median Total Compensation (CAD) Over Time',
    labels={'median_total_compensation': 'Median Total Compensation ($)', 'calendar_year': 'Year'}
    )

fig_med.update_yaxes(tickprefix="$", tickformat=",.0f")
fig_med.update_xaxes(range=[pd.to_datetime('1996-01-01'), df_salary_trends['calendar_year'].max()])

events = {
    "Dot Com Bubble Burst": "dot_com_bubble_burst",
    "NAFTA": "nafta",
    "2008 Global Financial Crisis": "2008_global_financial_crisis",
    "European Debt Crisis": "european_debt_crisis",
    "COVID-19 Pandemic": "covid_19_pandemic",
    "Post-COVID Inflation Surge": "post_covid_inflation_surge",
    "Ontario Bill 124 - limit public sector wage increases to 1 percent per year": "ontario_bill_124"
}

selected_events = st.selectbox(
    "Select Economic Event(s) to Overlay",
    options=list(events.keys())
)

for event_name, column in events.items():
    if event_name in selected_events and column in df_salary_trends.columns:
        # Get the dates (years) where the event indicator equals 1
        event_dates = df_salary_trends[df_salary_trends[column] == 1]['calendar_year']
        for event_date in event_dates:
            # Convert the Pandas Timestamp to native Python datetime for the start of the event
            dt_start = event_date.to_pydatetime()
            # Assume the event spans the entire calendar year:
            dt_end = (event_date + pd.DateOffset(years=1)).to_pydatetime()
            # Add a rectangle shape for the event period (using yref="paper" spans full vertical area)
            fig_avg.add_shape(
                type="rect",
                x0=dt_start,
                x1=dt_end,
                y0=0,
                y1=1,
                xref="x",
                yref="paper",
                fillcolor="rgba(255, 0, 0, 0.2)",  # red with 20% opacity
                line_width=0,
            )
            fig_med.add_shape(
                type="rect",
                x0=dt_start,
                x1=dt_end,
                y0=0,
                y1=1,
                xref="x",
                yref="paper",
                fillcolor="rgba(255, 0, 0, 0.2)",
                line_width=0,
            )


# Display salary_trends
st.plotly_chart(fig_avg)
st.plotly_chart(fig_med)


### TOP JOBS ###

query_top_jobs = """
    SELECT *
    FROM `ontario-sunshine.sunshine_dbt_dev_dataset.fct_top_jobs`
"""

query2 = client.query(query_top_jobs)
df_top_jobs = query2.result().to_dataframe()

st.header("Top Jobs")
st.write("""
The chart below displays the top 50 job titles for each year, with average and median total compensation.
Note: Positions with fewer than 30 employees are excluded to focus on roles that more accurately reflect broader public sector trends, rather than outliers like CEOs or presidents with limited representation.
""")

df_top_jobs['calendar_year'] = pd.to_datetime(df_top_jobs['calendar_year'], format='%Y')
df_top_jobs['year'] = df_top_jobs['calendar_year'].dt.year  # easier to work with in the selectbox
df_top_jobs = df_top_jobs.sort_values('calendar_year')

year_options = sorted(df_top_jobs['year'].unique())
selected_year = st.selectbox("Select Year", options=year_options)

df_year = df_top_jobs[df_top_jobs['year'] == selected_year]

top_jobs = df_year.sort_values("avg_total_compensation", ascending=False).head(50)

melt_df = top_jobs.melt(
    id_vars="job_title",
    value_vars=["avg_total_compensation", "median_total_compensation"],
    var_name="Compensation Type",
    value_name="Salary"
)

fig_top_jobs = px.bar(
    melt_df,
    x="Salary",
    y="job_title",
    color="Compensation Type",
    barmode="group",
    orientation="h",
    title=f"Average vs. Median Total Compensation by Job in {selected_year}",
    labels={"job_title": "Job Title", "Salary": "Compensation ($)"}
)

fig_top_jobs.update_layout(yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig_top_jobs)

### Top Employers ###


query_employer = """
    SELECT *
    FROM `ontario-sunshine.sunshine_dbt_dev_dataset.fct_employer_analysis`
"""

query_employer_job = client.query(query_employer)
df_employer = query_employer_job.result().to_dataframe()

st.header("Top Employers & Growth Analysis")
st.write(
    "This dashboard shows which employers are experiencing growth in both employee count "
    "and compensation over time. The bubble chart below plots each employer (a bubble) where the x‑axis shows the % growth in average total compensation "
    "and the y‑axis shows the % growth in employee count over the available period."
    "\nThe bubble size corresponds to the most recent employee count. "
    "Select an individual employer below for a detailed analysis."
)

df_employer['calendar_year'] = pd.to_datetime(df_employer['calendar_year'], format='%Y')
df_employer['year'] = df_employer['calendar_year'].dt.year
df_employer = df_employer.sort_values('calendar_year')

# ----------------------------
# Aggregate Data for Growth Analysis (per employer)
# ----------------------------
# Group the data by employer and get the earliest (start) and latest (end) values for key metrics.
df_growth = df_employer.groupby("employer").agg(
    start_year=("year", "min"),
    end_year=("year", "max"),
    start_employee_count=("employee_count", "first"),
    end_employee_count=("employee_count", "last"),
    start_avg_comp=("avg_total_compensation", "first"),
    end_avg_comp=("avg_total_compensation", "last")
).reset_index()

# Optionally filter for only employers with at least 2 years of data:
df_growth = df_growth[df_growth["start_year"] != df_growth["end_year"]]

# Avoid division by zero by filtering out rows where the start value is 0.
df_growth = df_growth[(df_growth["start_employee_count"] != 0) & (df_growth["start_avg_comp"] != 0)]

df_growth["employee_growth_pct"] = ((df_growth["end_employee_count"] - df_growth["start_employee_count"]) /
                                    df_growth["start_employee_count"]) * 100
df_growth["comp_growth_pct"] = ((df_growth["end_avg_comp"] - df_growth["start_avg_comp"]) /
                                df_growth["start_avg_comp"]) * 100

# Compute growth percentages
df_growth["employee_growth_pct"] = df_growth["employee_growth_pct"].clip(-1000, 1000)
df_growth["comp_growth_pct"] = df_growth["comp_growth_pct"].clip(-1000, 1000)


# Clean employer names
df_growth["employer"] = df_growth["employer"].str.lower().str.replace(r"[^a-z0-9]", "", regex=True)

# Cap growth values
df_growth["employee_growth_pct"] = df_growth["employee_growth_pct"].clip(-1000, 1000)
df_growth["comp_growth_pct"] = df_growth["comp_growth_pct"].clip(-1000, 1000)

# Create normalized size
df_growth["size_normalized"] = np.log(df_growth["end_employee_count"] + 1) * 20

# Create plot
fig_growth = px.scatter(
    df_growth,
    x="comp_growth_pct",
    y="employee_growth_pct",
    labels={
        "comp_growth_pct": "Compensation Growth %",
        "employee_growth_pct": "Employee Growth %"
    },
    size="size_normalized",
    hover_name="employer",
    log_x=True,
    log_y=True,
    size_max=50,
    title="Employer Growth (Log Scale)"
)

st.plotly_chart(fig_growth)


# Detailed Analysis per Employer
st.subheader("Detailed Employer Analysis")

# Select one employer for a deep dive
selected_employer = st.selectbox("Select Employer for Detailed Analysis", options=df_employer['employer'].unique())
df_emp = df_employer[df_employer['employer'] == selected_employer]

# Create a time series chart of compensation metrics for the selected employer
fig_emp = go.Figure()

# Trace for Average Total Compensation
fig_emp.add_trace(go.Scatter(
    x=df_emp['calendar_year'],
    y=df_emp['avg_total_compensation'],
    mode='lines+markers',
    name='Avg Total Compensation'
))

# Trace for Median Total Compensation
fig_emp.add_trace(go.Scatter(
    x=df_emp['calendar_year'],
    y=df_emp['median_total_compensation'],
    mode='lines+markers',
    name='Median Total Compensation'
))

# Shaded area for the compensation range (min – max)
fig_emp.add_trace(go.Scatter(
    x=pd.concat([df_emp['calendar_year'], df_emp['calendar_year'][::-1]]),
    y=pd.concat([df_emp['max_total_comp'], df_emp['min_total_comp'][::-1]]),
    fill='toself',
    fillcolor='rgba(0,100,80,0.2)',  # translucent greenish area
    line=dict(color='rgba(255,255,255,0)'),
    hoverinfo='skip',
    showlegend=True,
    name='Compensation Range'
))

# Secondary y-axis trace for YOY Change (if applicable)
fig_emp.add_trace(go.Scatter(
    x=df_emp['calendar_year'],
    y=df_emp['yoy_change'],
    mode='lines+markers',
    name='YOY Change (%)',
    yaxis='y2'
))

# Update layout for dual y-axes
fig_emp.update_layout(
    title=f"Compensation Metrics for {selected_employer}",
    xaxis_title="Year",
    yaxis=dict(title="Compensation ($)"),
    yaxis2=dict(
        title="YOY Change (%)",
        overlaying="y",
        side="right"
    ),
    legend=dict(x=0, y=1.15, orientation="h")
)
st.plotly_chart(fig_emp)

# Create an Employee Count Bar Chart
fig_emp_count = px.bar(
    df_emp,
    x='calendar_year',
    y='employee_count',
    title=f"Employee Count Over Time for {selected_employer}",
    labels={'employee_count': 'Employee Count', 'calendar_year': 'Year'}
)
st.plotly_chart(fig_emp_count)
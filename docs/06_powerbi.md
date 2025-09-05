# Exercise 6: Power BI Dashboard

## Objective
Create an interactive business intelligence dashboard in Power BI that demonstrates your ability to visualize data and deliver business insights.

## Your Task
Build a Power BI report (`/analytics/report.pbix`) connecting to your Gold layer tables through DuckDB. You will need to create appropriate DAX measures based on the pseudo-code provided.

## Requirements

### 1. Data Connection & Modeling

**Connection Setup:**
- Use DuckDB ODBC driver to connect to `duckdb/warehouse.duckdb`
- Import Gold layer tables (dimensions and facts)
- Configure refresh settings appropriately

**Data Model:**
- Recreate star schema relationships in Power BI
- Set proper cardinality and cross-filter direction
- Mark date table for time intelligence
- Hide technical columns from report view

### 2. DAX Measures

Create a measures table with business KPIs based on these specifications:

**Sales Metrics (implement these measures):**
```
Total Sales:
    - Sum the net_amount from fact sales table
    
Total Quantity:
    - Sum the quantity from fact sales table
    
Average Order Value:
    - Calculate total sales divided by distinct count of orders
    - Handle division by zero
```

**Growth Metrics (implement these measures):**
```
Sales Year-to-Date:
    - Calculate cumulative sales from start of year to current date
    - Use time intelligence functions with your date dimension
    
Sales Month-to-Date:
    - Calculate cumulative sales from start of month to current date
    
Year-over-Year Growth %:
    - Compare current period sales to same period last year
    - Calculate percentage change
    - Handle missing prior year data
    
Rolling 30-Day Average:
    - Calculate average daily sales over last 30 days
    - Use date functions to create rolling window
```

**Customer Metrics (implement these measures):**
```
Active Customers:
    - Count distinct customers with orders in period
    
Customer Lifetime Value:
    - Calculate average total spend per customer
    - Consider only customers with complete history
    
New vs Returning Ratio:
    - Identify first-time buyers vs repeat customers
    - Calculate ratio for selected period
    
VIP Revenue Contribution:
    - Calculate percentage of revenue from VIP customers
```

**Operational Metrics (implement these measures):**
```
Return Rate:
    - Calculate returned quantity divided by sold quantity
    - Express as percentage
    
Average Delivery Days:
    - Calculate days between ship and delivery dates
    - Exclude in-transit shipments
    
On-Time Delivery Rate:
    - Count shipments delivered within SLA
    - Divide by total delivered shipments
    - Express as percentage
    
Inventory Turnover:
    - Calculate sales divided by average inventory value
    - Annualize if needed
```

**Data Quality Metrics (implement these measures):**
```
Data Freshness:
    - Calculate hours since last data refresh
    - Use current time and max ingestion timestamp
    
Rejection Rate:
    - Calculate rejected rows divided by total processed
    - Track by table and time period
    
Completeness Score:
    - Calculate percentage of non-null critical fields
    - Weight by importance
```

### 3. Report Pages

**Page 1: Executive Dashboard**
- KPI cards: Total Sales, Orders, Customers, YoY Growth
- Sales trend line chart (with forecast)
- Top 10 products by revenue (bar chart)
- Geographic sales heatmap
- Sales by category (donut chart)
- Include slicers: Date range, Region, Category

**Page 2: Customer Analytics**
- Customer segmentation matrix
- Customer lifetime value distribution
- New vs returning customer trends
- VIP customer contribution analysis
- Customer acquisition funnel
- Churn analysis indicators

**Page 3: Product Performance**
- Product performance scatter plot (Revenue vs Margin)
- Category hierarchy drill-through
- Price change impact analysis
- Inventory turnover metrics
- Product lifecycle stage indicators
- ABC analysis visualization

**Page 4: Operations Monitor**
- Delivery performance gauges
- Shipping cost trends
- Carrier performance comparison
- Sensor anomaly detection alerts
- Store performance heatmap
- Supply chain lead time analysis

**Page 5: Data Quality Dashboard**
- Pipeline health status
- Rejection rates by table (over time)
- Data freshness indicators
- Schema drift detection
- Missing data heatmap
- Processing time trends

### 4. Advanced Features

**Implement at least 3 of the following:**
- Drill-through pages for detailed analysis
- Bookmarks for different user personas
- What-if parameters for scenario analysis
- Custom tooltips with additional context
- Row-level security (RLS) for store managers
- Mobile-optimized layout
- R/Python visuals for advanced analytics

## Design Guidelines

### Visual Best Practices
1. **Consistent Color Scheme**: Use company colors or a professional palette
2. **Clear Hierarchy**: Most important metrics at top/left
3. **Appropriate Chart Types**: Choose visualizations that match the data
4. **Minimal Clutter**: Remove unnecessary gridlines, borders
5. **Responsive Layout**: Test on different screen sizes

### Performance Optimization
- Use aggregation tables where possible
- Limit visual-level filters
- Optimize DAX calculations
- Consider DirectQuery vs Import mode trade-offs
- Implement incremental refresh if needed

### User Experience
- Add clear titles and subtitles
- Include data refresh timestamp
- Provide filter reset buttons
- Add navigation buttons between pages
- Include help tooltips for complex metrics

## Testing Your Report

1. **Data Accuracy**:
   - Cross-check totals with SQL queries
   - Verify calculations match business rules
   - Test filter combinations

2. **Performance**:
   - Page load times < 3 seconds
   - Smooth interactivity
   - Efficient refresh times

3. **Usability**:
   - Test with sample user scenarios
   - Verify all interactions work
   - Check mobile view rendering

## Evaluation Criteria

1. **Completeness**: All required pages and metrics implemented
2. **Accuracy**: Calculations are correct and match source data
3. **Design**: Professional, clean, and intuitive layout
4. **Performance**: Fast loading and responsive interactions
5. **Insights**: Actionable business intelligence delivered
6. **Innovation**: Creative use of Power BI features

## Common Pitfalls to Avoid
- Don't overcrowd visuals with too much information
- Avoid using 3D charts or excessive animations
- Don't forget to format numbers appropriately
- Remember to test cross-filtering behavior
- Don't ignore performance implications of complex DAX

## What NOT to Do
- Do not hardcode values in measures
- Do not create circular dependencies in calculations
- Do not use pie charts for >5 categories
- Do not forget to document your measures

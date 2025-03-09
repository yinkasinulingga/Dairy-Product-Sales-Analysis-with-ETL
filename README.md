---

# **Dairy Product Sales Analysis with ETL**  

## **Introduction**  
This project aims to **automate the ETL (Extract, Transform, Load) process and visualize dairy product sales data** to improve production efficiency, distribution, and stock management. With accurate data analysis, businesses can **optimize marketing strategies, prevent losses, and increase profits**.  

## **Project Objectives**  
- **Manage stock and production** optimally to prevent product shortages or overstocking.  
- **Identify sales trends and customer distribution** to enhance marketing strategies.  
- **Simplify data monitoring** through **interactive visualizations in Kibana and Tableau**.  

## **ETL Process Flow**  
1. **Extract** → Retrieve data from PostgreSQL and store it in CSV format.  
2. **Transform** →  
   - **Remove unnecessary columns** to make the dataset more concise and efficient.  
   - **Handle missing values** → Delete rows with missing values categorized as MCAR (Missing Completely at Random) and with a small proportion relative to the total dataset.  
   - **Remove duplicate data** to ensure each transaction is unique.  
   - **Change data types** → Convert the *quantity* column from numeric to integer for consistency in analysis.  
   - **Standardize column formats** → Change column names to lowercase and replace spaces with underscores (`_`) to facilitate data access.  
   - **Add a unique index column** → Create an *id* column as a unique identifier for each data row.  
   - **Save the transformed data** into a CSV file to be used in the *Load* stage.  
3. **Load** → Store the processed data into **Elasticsearch** for further analysis.  
4. **Visualization** → Use **Kibana and Tableau** to present sales trends and product distribution.  

## **Technologies Used**  
- **ETL Pipeline**: Apache Airflow  
- **Database**: PostgreSQL, Elasticsearch  
- **Programming**: Python, Pandas, NumPy  
- **Data Visualization**: Kibana, Tableau, Power BI  

## **Conclusions**  
- **More efficient stock management** → Analyzing demand trends helps businesses reduce the risk of overstocking or understocking, avoiding waste and lost sales opportunities.  
- **Enhanced marketing strategy** → Understanding customer purchasing patterns enables more targeted promotions and improves marketing effectiveness.  
- **Optimized product distribution** → Data analytics helps identify regions with the highest demand, allowing faster and more efficient deliveries.  

## **Suggestions for Future Development**  
- **Integrate with demand prediction models** → Use machine learning to forecast future demand based on historical patterns.  
- **Add external data** → Include external factors such as market trends and weather conditions to improve prediction accuracy.  
- **Automate visualization reporting** → Implement dashboards with automatic updates to assist management teams in making quicker decisions.  

## **Business Impact**  
🔹 **Increase profitability** → Data-driven stock optimization and marketing strategies can boost revenue and reduce losses.  
🔹 **Operational efficiency** → ETL automation reduces manual data processing time, enabling the team to focus on strategic decision-making.  
🔹 **Improved customer satisfaction** → Better product availability and more efficient delivery enhance customer experience and brand loyalty.  

## **How to Run the Project**  
1. Clone this repository  
2. Install dependencies with `pip install -r requirements.txt`  
3. Run Apache Airflow to execute the ETL pipeline  
4. Use Kibana or Tableau for data visualization  

## **Contact**  
For any questions or suggestions, please contact:  
📧 **Email**: yinkasinulingga@gmail.com

---

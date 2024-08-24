<h2 align="center">
  Welcome to My PySpark Text to CSV ETL Project!
  <img src="https://media.giphy.com/media/hvRJCLFzcasrR4ia7z/giphy.gif" width="28">
</h2>

<!-- Intro -->
<h3 align="center">
        <samp>&gt; Hey There!, I am
                <b><a target="_blank" href="https://yourwebsite.com">Shubham Dalvi</a></b>
        </samp>
</h3>

<p align="center"> 
  <samp>
    <br>
    「 I am a data engineer with a passion for big data, distributed computing, and data visualization 」
    <br>
    <br>
  </samp>
</p>

<div align="center">
<a href="https://git.io/typing-svg"><img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&pause=1000&random=false&width=435&lines=Spark+%7C+DataBricks++%7C+Power+BI+;Snowflake+%7C+Azure++%7C+Airflow;3+yrs+of+IT+experience+as+Analyst+%40+;Accenture+;Passionate+Data+Engineer+" alt="Typing SVG" /></a>
</div>

<p align="center">
 <a href="https://linkedin.com/in/yourprofile" target="_blank">
  <img src="https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" alt="yourprofile"/>
 </a>
</p>
<br />

<!-- About Section -->
# About Me
 
<p>
 <img align="right" width="350" src="/assets/programmer.gif" alt="Coding gif" />
  
 ✌️ &emsp; Enjoy solving data problems <br/><br/>
 ❤️ &emsp; Passionate about big data technologies, distributed systems, and data visualizations<br/><br/>
 📧 &emsp; Reach me : dshubhamp1999@gmail.com<br/><br/>

</p>

<br/>
<br/>
<br/>

## Skills and Technologies

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-013243?style=for-the-badge&logo=matplotlib&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)
![VSCode](https://img.shields.io/badge/Visual_Studio-0078d7?style=for-the-badge&logo=visual%20studio&logoColor=white)

<br/>

## Project Overview

This project focuses on converting text data into a CSV format using PySpark, with an ETL (Extract, Transform, Load) process that includes exploratory data analysis (EDA). The objective is to transform raw text data into a structured CSV format and perform various analyses to extract valuable insights.

## Table of Contents
- [Technologies Used](#technologies-used)
- [Skills Demonstrated](#skills-demonstrated)
- [Data Extraction](#data-extraction)
- [Data Transformation](#data-transformation)
- [Exploratory Data Analysis (EDA)](#exploratory-data-analysis-eda)
- [Saving Results](#saving-results)
- [Usage Instructions](#usage-instructions)

## Technologies Used
- **PySpark**: For distributed data processing.
- **Pandas**: For data manipulation and transformation.
- **Matplotlib**: For data visualization.
- **Jupyter Notebook**: For interactive data analysis.

## Skills Demonstrated
- **Data Engineering**: Efficient handling and processing of text data.
- **PySpark**: Advanced usage of PySpark DataFrame operations and SQL functions.
- **Data Transformation**: Converting and cleaning text data for analysis.
- **Exploratory Data Analysis (EDA)**: Understanding data distributions and patterns.
- **Visualization**: Plotting data distributions for insights.
- **Performance Optimization**: Managing large datasets efficiently.

## Data Extraction
### Initial Setup
The project begins with the initialization of PySpark and reading the text file.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Doc_reader").getOrCreate()

df = spark.read.text("DSA-For-Data-Engineer.txt")
'''

## Data Transformation

### Extracting Topics and Questions
Transforming text data to extract topics and questions into separate columns.

```python
from pyspark.sql.functions import when, col, rlike

transformed_df = df.withColumn("Topic", when(col("value").rlike(r"^[ A-Z]"), col("value")).otherwise(None)) \
                    .withColumn("Questions", when(col("value").rlike(r"^\d+\."), col("value")).otherwise(None))

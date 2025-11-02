# 🚦 Big Data Analysis of Road Accidents

### Real-Time Processing and Prediction of Road Accident Severity using Apache Spark, PySpark, and Kafka

---

## 📌 Project Overview

This project focuses on **real-time big data analysis** of road accidents in the United States.  
It leverages **Apache Spark**, **PySpark**, and **Apache Kafka** to process, analyze, and predict accident severity from millions of records efficiently.

The goal is to build a **data pipeline** capable of:
- Processing massive accident datasets in real time
- Performing advanced feature engineering and machine learning
- Predicting the **severity of road accidents** to improve traffic safety and prevention measures

---

## 🧱 Project Structure

bigdata-accidents/
│
├── dataset/ # Contains the original dataset (2.9 GB, excluded from repo)
│ └── US_Accidents.csv
│
├── notebooks/ # All Jupyter notebooks (analysis, preprocessing, training, streaming)
│ ├── US_Accidents_Analysis.ipynb # Data preprocessing, cleaning, feature engineering with Spark
│ ├── train.ipynb # file for testing Model training (Decision Tree, Logistic Regression, Random Forest) 
│ ├── train_producer.ipynb # file contains the best model training  Kafka producer for real-time simulation and 
│ ├── kafka_consumer_2.ipynb # Kafka consumer for streaming predictions
│ └── ...
├── .gitignore # Excludes large files and temporary artifacts
└── README.md # Project documentation

---

## 🧠 Dataset

- **Name:** [US_Accidents Dataset](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)  
- **Size:** ~2.9 GB  
- **Source:** Kaggle  
- **Location:** `/dataset/US_Accidents.csv` (excluded from repo due to size limits)

The dataset contains more than **7 million records** of road accidents across the United States, with attributes such as:
- Weather conditions
- Road type
- Time of day
- State, city, and location coordinates
- Severity levels (1–4)

---

## ⚙️ Data Preprocessing and Feature Engineering

All preprocessing steps were implemented in **`US_Accidents_Analysis.ipynb`** using **PySpark**:

1. **Data Cleaning**  
   - Handling missing values and irrelevant columns  
   - Removing duplicates and inconsistent entries  

2. **Feature Engineering**  
   - Transforming raw data into machine learning-ready format  
   - Encoding categorical columns (e.g., weather, street, state)  
   - Combining all numeric and encoded features into a single `features` column  

3. **Data Export**  
   - Saving the final processed dataset in **Parquet format** for efficient storage and model training

---

## 🤖 Model Training

All training and evaluation steps were performed in **`train.ipynb`** using **Spark MLlib**:

- Data loaded from **Parquet** file  
- Column `Severity` renamed to `label` (required by Spark MLlib)  
- Dataset split:
  - 80% for **training**
  - 20% for **testing**
  
### Algorithms Tested:
- 🌳 **DecisionTreeClassifier**
- ⚙️ **LogisticRegression**
- 🌲 **RandomForestClassifier**

### Results:
- **Best Model:** Decision Tree  
- **Accuracy:** **91.89 %**
- **Interpretation:** The model correctly predicts the severity of accidents for ~92% of the test data.

---

## 🔄 Real-Time Processing (Kafka Integration)

The **streaming pipeline** is built with **Apache Kafka**:

- **Producer (`kafka_producer.py`)**  
  Sends cleaned accident data records in real time to a Kafka topic.

- **Consumer (`kafka_consumer.py`)**  
  Listens to the topic, processes each record through the trained model, and predicts the accident severity in real time.

This setup demonstrates **real-time predictive analytics**, which could be integrated into intelligent transportation systems.

---

## 🧩 Technologies Used

| Category | Tools / Frameworks |
|-----------|-------------------|
| Big Data Processing | Apache Spark, PySpark |
| Streaming | Apache Kafka |
| Machine Learning | Spark MLlib |
| Language | Python |
| Environment | WSL (Ubuntu), Jupyter Notebook |
| Version Control | Git & GitHub |

---

## 📊 Results & Insights

- Built a **complete data pipeline** from ingestion → preprocessing → training → streaming.  
- Achieved **91.89% accuracy** in predicting accident severity.  
- Demonstrated the feasibility of **real-time prediction** using Spark + Kafka.  
- Provided a scalable base for future smart transportation analytics.

---

## 🚀 Future Improvements

- Integrate **real-time dashboards** using Spark Streaming + Dash or Power BI  
- Deploy the trained model as an API (Flask / FastAPI)  
- Experiment with **Gradient Boosting** or **XGBoost** models  
- Add **geo-visualization** of accident hotspots

---

## 👩‍💻 Author

**Sara Souhail**  
Master’s student in Artificial Intelligence and Emerging Technologies  
[GitHub](https://github.com/SaraSouhail) • [LinkedIn](https://www.linkedin.com/in/sara-souhail/)

---

⭐ *If you found this project interesting, don’t forget to star the repo!*

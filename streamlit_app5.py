import streamlit as st
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
import matplotlib.pyplot as plt

@st.cache_resource
def get_spark():
    return SparkSession.builder\
        .appName("Flight_Fare_Prediction")\
        .master("local[*]")\
        .getOrCreate()

spark=get_spark()
model=PipelineModel.load("best_model_GradientBoosted")

@st.cache_resource
def load_data():
    return spark.read.csv("hdfs://localhost:9000/user/sunbeam/flight_data/clean_data.csv",
        header=True,
        inferSchema=True)
df=load_data().cache()

st.title("Flight Fare Prediction")
def get_unique_values(column):
    return [row[column] for row in df.select(column).distinct().collect()]
airline=st.selectbox("Airline", get_unique_values("airline"))
source=st.selectbox("Source City", get_unique_values("source_city"))
dest_options=[city for city in get_unique_values("destination_city") if city != source]
dest=st.selectbox("Destination City", dest_options)
dept_time=st.selectbox("Departure Time", get_unique_values("departure_time"))
arr_time=st.selectbox("Arrival Time", get_unique_values("arrival_time"))
stops=st.selectbox("Stops", get_unique_values("stops"))
travel_class=st.selectbox("Class", get_unique_values("class"))
duration=st.slider("Duration (hrs)", 0.5, 10.0, step=0.25)
days_left=st.slider("Days Left", 1, 60)

if st.button("Predict Fare"):
    try:
        input_row=[{
            "airline": airline,
            "source_city": source,
            "departure_time": dept_time,
            "stops": stops,
            "arrival_time": arr_time,
            "destination_city": dest,
            "class": travel_class,
            "duration": float(duration),
            "days_left": int(days_left)
        }]
        input_df=spark.createDataFrame(input_row)
        result=model.transform(input_df).select("prediction").collect()[0][0]
        st.success(f"Predicted Fare: {int(result)}")

        st.subheader("Similar Flights")
        similar=df.filter(
            (df["class"]==travel_class)&
            (df["days_left"]==days_left)&
            (df["source_city"]==source)&
            (df["destination_city"]==dest)
        ).select("airline", "class", "days_left", "stops", "price").limit(5)
        st.dataframe(similar.toPandas()) 


        st.header("Flight Fare Data Insights")

        pdf = df.toPandas()
        st.subheader("Average Price by Airline")
        fig, ax=plt.subplots(figsize=(8, 5))
        for cls in pdf['class'].unique():
            avg_price=(pdf[pdf['class']==cls]
                .groupby('airline')['price']
                .mean()
                .sort_values())
            ax.plot(avg_price.index.to_numpy(),        
                avg_price.values.astype(float),   
                marker='o',
            label=cls)
        ax.set_ylabel("Average Price")
        ax.set_xlabel("Airline")
        ax.set_title("Average Price by Airline")
        ax.legend(title="Class")
        plt.xticks(rotation=45)
        st.pyplot(fig)

        st.subheader("Price Trend as Days Left Decreases (Per Class)")
        fig, ax=plt.subplots(figsize=(8, 5))
        for cls in pdf['class'].unique():
            trend=(pdf[pdf['class']==cls]
                .groupby('days_left')['price']
                .mean()
                .sort_index())
            ax.plot(trend.index.to_numpy(),
            trend.values.astype(float),
            marker='o',
            label=cls)

        ax.set_ylabel("Average Price")
        ax.set_xlabel("Days Left")
        ax.set_title("Price Trend by Class")
        ax.legend(title="Class")
        st.pyplot(fig)

    except Exception as e:
        st.error(f"Something went wrong: {e}")
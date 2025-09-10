
# CropYieldPipeline

## 🌾 Project Overview

CropYieldPipeline is a Scala Spark-based data analytics project designed to process and visualize agricultural crop yield data. It leverages Apache Spark for distributed data processing and XChart for generating insightful visualizations. The goal is to understand how factors like climate stress, rainfall deviation, and agricultural support impact crop yields.

## ⚙️ Requirements

To run this project, ensure the following tools are installed:

- Java JDK 8 or higher
- Apache Spark 3.x
- Scala 2.12 or higher
- sbt (Scala Build Tool)
- IntelliJ IDEA (recommended IDE)

## 🚀 How to Run the Project

1. **Clone the Repository**
   ```bash
   git clone https://github.com/yourusername/CropYieldPipeline.git
   cd CropYieldPipeline
   ```

2. **Build the Project**
   ```bash
   sbt clean compile
   ```

3. **Run the Application**
   Open IntelliJ IDEA, import the project, navigate to `BronzeExtractor.scala`, and run the `main` method.

4. **View the Plots**
   After execution, the following plots will be saved in the `src/plots/` directory:

   - `yield_by_support.png`: Bar chart showing average yield by support level.
   - `yield_vs_rainfall.png`: Scatter plot showing yield vs rainfall deviation.
   - `yield_vs_climate_stress.png`: Scatter plot showing yield vs climate stress index.

## 📁 Project Structure

```
CropYieldPipeline/
├── .idea/                  # IntelliJ project settings
├── .bsp/                   # Build Server Protocol files
├── project/                # sbt project configuration
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── data/       # CSV and Parquet data files
│   │   └── scala/          # Scala source code
│   └── plots/              # Output PNG plots
├── target/                 # sbt build artifacts
├── build.sbt               # sbt build definition
├── .gitignore              # Git ignore rules
└── README.md               # Project documentation
```

## 📊 Visual Insights

The visualizations generated help in understanding:
- The effectiveness of agricultural support programs.
- The impact of rainfall deviation on crop yield.
- The influence of climate stress on agricultural productivity.

---

Feel free to contribute or raise issues to improve the pipeline!

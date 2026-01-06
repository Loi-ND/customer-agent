from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, floor, lit, sum, sequence, explode, row_number
from typing import Tuple, List
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql import SparkSession
import os

def assign_customer_to_agent(customers_data_path: str, agents_data_path: str, output_dir: str) -> None:
    spark = SparkSession.builder.appName("Assign customer to agents")\
                            .master("local[4]")\
                            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # Đọc dữ liệu đầu vào cho bài toán
    agents_df = spark.read.csv(agents_data_path, header=True, inferSchema=True)
    customers_df = spark.read.csv(customers_data_path, header=True, inferSchema=True)

    # Phrase 1: Tính toán và phân chia lại dữ liệu
    categories = [c for c in agents_df.columns if c not in ["id", "capacity"]]
    express = reduce(lambda x, y: x + y, [agents_df[category].cast("int") for category in categories])
        
    agents_df = agents_df.withColumn("avail_count", express)
    agents_df = agents_df.withColumn("base", floor(col("capacity")/col("avail_count")))\
                         .withColumn("rem", col("capacity") - col("base")*col("avail_count"))
    for category in categories:
        agents_df = agents_df.withColumn(f"{category}_remain", when(col(category) == True, col("base")).otherwise(0))
        agents_df = agents_df.withColumn(f"{category}_remain", when((col(category) == True) &(col("rem") > 0), col(f"{category}_remain") + 1).otherwise(col(f"{category}_remain")))\
                             .withColumn("rem", when((col(category) == True) & (col("rem") > 0), col("rem") - 1).otherwise(col("rem")))
    agents_df = agents_df.withColumn("remain", lit(0))
    agents_df = agents_df.drop("avail_count", "base", "rem")
    ##############################################
    # Xử lý và lấy ra các cặp (demand, count) biểu thị yêu cầu và số lượng ứng với yêu cầu đó
    demands_order_by_total_slots = customers_df.select(col("demand")).groupby("demand").count().alias("count").orderBy(col("count").asc())
    demands_order_by_total_slots.show()
    categories = [(row["demand"], row["count"]) for row in demands_order_by_total_slots.collect()]

    # Phrase 2: dựa vào dữ liệu sau khi xử lý bắt đầu thực hiện gán customers cho agents
    for category, demand in categories:
        # Cập nhật giá trị cho cột remain với remain sẽ là giá trị remain còn lại 
        # sau khi gán lượt trước cộng với remain ứng với loại bảo hiểm đó
        agents_df = agents_df.withColumn("remain", col("remain") + col(f"{category}_remain"))
        # Tạo thêm cột mới là giới hạn mới của loại bảo hiểm đó
        agents_df = agents_df.withColumn(f"{category}_cap", col("remain"))
        # Lấy ra id và remain tương ứng với mỗi agent
        agents = agents_df.select(col("id"), col("remain")).filter((col(category) == True) & (col("remain") > 0))
    
        # Lấy ra customers cần gán agents tư vấn bao gồm id
        customers = customers_df.select(col("id")).filter(col("demand") == category)
            
        # Tính toán trước giá trị tổng số khả năng tiếp nhận khách hàng ứng với loại bảo hiểm
        total_cap = agents.select(sum(col("remain")).alias("total")).collect()
        agent_cap = total_cap[0]["total"]
        
        if agent_cap <= demand:
            ##################
            # Phần này xử lý với trường hợp mà tổng số khả năng đáp ứng nhỏ hơn hoặc bằng với tổng số yêu cầu
            ##################
            print("le")
            # Gán số lượng khách hàng cho mỗi agents với khả năng tối đa
            agents = agents.withColumn("assigned", col("remain"))
            
            # Cập nhật bảng agents_df sau khi xử lý gán khách hàng cho agents
            agents_df = agents_df.join(agents, agents_df.id == agents.id, how="left").select(agents_df["*"], agents["assigned"])
            agents_df = agents_df.fillna({"assigned": 0})
            agents_df = agents_df.withColumn("remain", col("remain") - col("assigned"))
            agents_df = agents_df.withColumnRenamed("assigned", f"{category}_slots")
        else:
            print("gt")
            
            ##################
            # Phần này xử lý với trường hợp mà tổng số khả năng đáp ứng lớn hơn so với tổng số yêu cầu
            ##################
            
            # Thêm các cột về số lượng khách hàng được gán "assigned" và cột phần dư thập phân sau khi tính toán 
            agents = agents.withColumn("assigned", floor((col("remain")*demand)/agent_cap))\
                           .withColumn("decimal_part", (col("remain")*demand)/agent_cap - floor((col("remain")*demand)/agent_cap))
    
            # Tính toán số lượng yêu cầu của khách hàng khi chưa được đáp ứng sau khi chia ở bên trên
            remain_demand = demand - agents.select(sum(col("assigned")).alias("total")).collect()[0]["total"]
            tmp_agents = (
                agents
                    .orderBy(col("decimal_part").desc(), col("id"))
                    .limit(remain_demand)
                    .select(col("id"))
            )
    
            # Với mỗi agent sẽ gán cho 1 khách hàng
            tmp_agents = tmp_agents.withColumn("added", lit(1))
            agents = agents.join(tmp_agents, agents.id == tmp_agents.id, how="left").select(agents["*"], tmp_agents["added"])
            
            # Thay thế giá trị null của cột added sau khi join
            agents = agents.fillna({"added": 0})
    
            # Cập nhật cột assigned
            agents = agents.withColumn("assigned", col("assigned") + col("added"))
            agents = agents.select(col("id"), col("assigned"))
    
            # Cập nhật bảng agents_df sau khi xử lý gán khách hàng cho agents
            agents_df = agents_df.join(agents, agents_df.id == agents.id, how="left").select(agents_df["*"], agents["assigned"])
            agents_df = agents_df.fillna({"assigned": 0})
            agents_df = agents_df.withColumn("remain", col("remain") - col("assigned"))
            agents_df = agents_df.withColumnRenamed("assigned", f"{category}_slots")
        
        agents = agents.withColumn("rep", explode(sequence(lit(1), col("assigned"))))
        agents = agents.select(col("id"))
        w = Window.orderBy(col("id")) 
        agents = agents.withColumn("index", row_number().over(w) - 1)
        customers = customers.withColumn("index", row_number().over(w) - 1)
        customer_agent = customers.join(agents, "index", how="inner").select(customers["id"].alias("customer_id"), agents["id"].alias("agent_id"))
        customer_agent.toPandas().to_csv(os.path.join(output_dir, f"{category}.csv"), index=False)
    agents_df = agents_df.select(col("id"), col("travel"), col("life_health"), col("motor"), col("travel_slots"), col("life_health_slots"), col("motor_slots"), col("capacity"))
    agents_df = agents_df.withColumn("sum_assigned", col("travel_slots") + col("life_health_slots") + col("motor_slots"))
    agents_df = agents_df.orderBy(["capacity", "travel", "life_health", "motor"], ascending=[False, False, False, False])
    agents_df.toPandas().to_csv(os.path.join(output_dir, "output.csv"), index=False)
    spark.stop()
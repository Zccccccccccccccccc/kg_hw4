import pandas as pd
from py2neo import Graph

# 1. 配置数据库连接
# 请将 password 修改为你自己的 Neo4j 密码
graph = Graph("bolt://127.0.0.1:7687", auth=("neo4j", "88888888"), name="finalhw")

def build_kg(csv_path):
    print(f"开始构建知识图谱，读取文件: {csv_path}")
    df = pd.read_csv(csv_path).head(100000)

    # 2. 创建唯一性约束 (保证导入速度和数据唯一)
    print("正在创建数据库约束...")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:App) REQUIRE a.id IS UNIQUE")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Developer) REQUIRE d.name IS UNIQUE")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE")

    # 3. 定义 Cypher 导入语句
    # 使用了 UNWIND 批量处理，并利用了所有清洗出的字段
    query = """
    UNWIND $rows AS row
    // 基础防御：确保 ID 不为空
    WITH row WHERE row.App_Id IS NOT NULL
    
    // 1. 创建 App 节点并设置丰富属性
    MERGE (app:App {id: row.App_Id})
    SET app.name = row.App_Name,
        app.rating = row.Average_User_Rating,
        app.reviews = row.Reviews,
        app.version = row.Version,
        app.url = row.AppStore_Url,
        app.free = row.Free

    // 2. 创建关联维度节点
    MERGE (dev:Developer {name: row.Developer})
    SET dev.id = row.DeveloperId, dev.url = row.Developer_Url
    
    MERGE (genre:Genre {name: row.Primary_Genre})
    MERGE (cr:ContentRating {level: row.Content_Rating})
    MERGE (pt:PriceTier {label: row.Price_Tier})
    MERGE (sb:SizeBucket {label: row.Size_Bucket})
    MERGE (ios:IOSVersion {v: row.Required_IOS_Version})
    MERGE (year:Year {value: row.Release_Year})

    // 3. 建立关系
    MERGE (app)-[:DEVELOPED_BY]->(dev)
    MERGE (app)-[:BELONGS_TO]->(genre)
    MERGE (app)-[:CLASSIFIED_AS]->(cr)
    MERGE (app)-[:HAS_PRICE_TIER]->(pt)
    MERGE (app)-[:SPACE_OCCUPIED]->(sb)
    MERGE (app)-[:REQUIRES_OS]->(ios)
    MERGE (app)-[:RELEASED_IN]->(year)
    """

    # 4. 分批次导入数据
    batch_size = 1000
    total = len(df)
    
    print("正在导入数据...")
    for i in range(0, total, batch_size):
        batch = df.iloc[i:i+batch_size].to_dict('records')
        try:
            graph.run(query, rows=batch)
            print(f"已处理: {i + len(batch)} / {total}")
        except Exception as e:
            print(f"在处理第 {i} 条附近的数据时出错: {e}")
            break

    print("知识图谱构建完成！")

if __name__ == "__main__":
    build_kg('AppleStore_Cleaned.csv')
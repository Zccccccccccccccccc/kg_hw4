import pandas as pd
from neo4j import GraphDatabase
import time

class TuGraphImporter:
    def __init__(self, uri, user, password, graph_name="default"):
        self.uri = uri
        self.user = user
        self.password = password
        self.graph_name = graph_name
        # 初始化驱动
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        # 立即测试连接
        self._test_connection()

    def _test_connection(self):
        try:
            with self.driver.session(database=self.graph_name) as session:
                session.run("RETURN 1")
            print(f"✅ 成功连接到 TuGraph (子图: {self.graph_name})")
        except Exception as e:
            print(f"❌ 身份验证失败或连接错误: {e}")
            raise e

    def close(self):
        self.driver.close()

    def query(self, cypher, parameters=None):
        with self.driver.session(database=self.graph_name) as session:
            result = session.run(cypher, parameters)
            return result.data()

    def setup_schema(self):
        """
        第一步：在 TuGraph 中创建 Schema (Label 和属性)
        """
        print("正在检查并定义 TuGraph Schema...")
        
        # 1. 定义顶点 App
        try:
            self.query("CALL db.createVertexLabel('App', 'id', 'id', 'STRING', false, 'name', 'STRING', true, 'price', 'DOUBLE', true, 'rating', 'DOUBLE', true)")
            print("- 已创建 App 顶点标签")
        except Exception as e:
            if "already exists" in str(e):
                print("- App 标签已存在")
            else:
                print(f"- App 标签处理异常: {e}")

        # 2. 定义顶点 Genre
        try:
            self.query("CALL db.createVertexLabel('Genre', 'name', 'name', 'STRING', false)")
            print("- 已创建 Genre 顶点标签")
        except Exception as e:
            if "already exists" in str(e):
                print("- Genre 标签已存在")

        # 3. 定义顶点 ContentRating
        try:
            self.query("CALL db.createVertexLabel('ContentRating', 'level', 'level', 'STRING', false)")
            print("- 已创建 ContentRating 顶点标签")
        except Exception:
            pass

        # 4. 定义边 BELONGS_TO
        try:
            self.query("CALL db.createEdgeLabel('BELONGS_TO', '[[\"App\",\"Genre\"]]')")
            print("- 已创建 BELONGS_TO 边标签")
        except Exception:
            pass

        # 5. 定义边 RATED_AS
        try:
            self.query("CALL db.createEdgeLabel('RATED_AS', '[[\"App\",\"ContentRating\"]]')")
            print("- 已创建 RATED_AS 边标签")
        except Exception:
            pass

    def import_csv(self, file_path, limit=100000):
        """
        第二步：读取 CSV 并写入数据
        """
        print(f"正在读取文件: {file_path}...")
        try:
            df = pd.read_csv(file_path)
            print(f"📊 CSV 文件读取成功，发现列: {list(df.columns)}")
            df = df.head(limit)
        except Exception as e:
            print(f"❌ 读取 CSV 失败: {e}")
            return

        # 自动映射关键列名（忽略大小写和空格）
        col_map = {col.lower().strip(): col for col in df.columns}
        
        # 寻找 ID 列，通常是 'id' 或第一个列
        id_col = col_map.get('id', df.columns[0])
        name_col = col_map.get('track_name', col_map.get('name', None))
        price_col = col_map.get('price', None)
        rating_col = col_map.get('user_rating', None)
        genre_col = col_map.get('prime_genre', None)
        cont_rating_col = col_map.get('cont_rating', None)

        print(f"🔍 自动匹配列名 -> ID: [{id_col}], Name: [{name_col}], Genre: [{genre_col}]")

        print(f"开始写入节点与关系 (共 {len(df)} 条待处理)...")
        start_time = time.time()
        
        count = 0
        for index, row in df.iterrows():
            raw_id = row.get(id_col)
            
            # 严格检查 ID，如果 ID 为空则跳过
            if pd.isna(raw_id):
                continue

            try:
                # 统一转换 ID 为字符串格式
                app_id = str(int(float(raw_id)))
            except:
                app_id = str(raw_id)

            params = {
                "app_id": app_id,
                "app_name": str(row.get(name_col, 'Unknown')) if name_col else 'Unknown',
                "price": float(row.get(price_col, 0.0)) if price_col and not pd.isna(row.get(price_col)) else 0.0,
                "rating": float(row.get(rating_col, 0.0)) if rating_col and not pd.isna(row.get(rating_col)) else 0.0,
                "genre_name": str(row.get(genre_col, 'Other')) if genre_col else 'Other',
                "cont_rating": str(row.get(cont_rating_col, '4+')) if cont_rating_col else '4+'
            }

            cypher = """
            MERGE (a:App {id: $app_id})
            SET a.name = $app_name, a.price = $price, a.rating = $rating
            MERGE (g:Genre {name: $genre_name})
            MERGE (a)-[:BELONGS_TO]->(g)
            MERGE (cr:ContentRating {level: $cont_rating})
            MERGE (a)-[:RATED_AS]->(cr)
            """
            
            try:
                self.query(cypher, params)
                count += 1
            except Exception as e:
                print(f"⚠️ 写入行 {index} (ID: {app_id}) 失败: {e}")
                if count == 0 and index > 5: # 如果前几条都失败，直接退出
                    print("❌ 连续写入失败，请检查数据库配置或数据格式。")
                    break

            if count > 0 and count % 500 == 0:
                print(f"已处理 {count} 条记录...")

        end_time = time.time()
        print(f"✅ 导入流程结束。成功写入: {count} 条。总耗时: {end_time - start_time:.2f} 秒")

if __name__ == "__main__":
    CONFIG = {
        "uri": "bolt://192.168.31.179:7687",
        "user": "admin",
        "pass": "73@TuGraph", 
        "graph": "finalhw"
    }

    importer = TuGraphImporter(CONFIG["uri"], CONFIG["user"], CONFIG["pass"], CONFIG["graph"])
    try:
        importer.setup_schema()
        importer.import_csv('AppleStore_Cleaned.csv')
    finally:
        importer.close()
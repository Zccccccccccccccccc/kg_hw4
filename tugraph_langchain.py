import os
import json
import time
from neo4j import GraphDatabase
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

# --- 基础配置 ---
# TuGraph 连接信息 (使用 Bolt 协议)
URI = "bolt://192.168.31.179:7687"
AUTH = ("admin", "73@TuGraph")
DB_NAME = "finalhw"

# LLM 配置 (硅基流动 SiliconFlow)
API_KEY = "sk-jaeowvubagerozsqmytiuqdpvlvjgohuvwrrhchhuoeujeit" 
BASE_URL = "https://api.siliconflow.cn/v1"
MODEL_NAME = "Qwen/Qwen3-VL-32B-Instruct"

class TuGraphQuestionParser:
    """
    基于 TuGraph 和 LLM 的智能问答系统类
    仿照医疗问答系统实现：手动控制 Text-to-Cypher 流程
    """
    def __init__(self):
        # 1. 初始化数据库驱动
        try:
            self.driver = GraphDatabase.driver(URI, auth=AUTH)
            # 测试连接
            with self.driver.session(database=DB_NAME) as session:
                session.run("RETURN 1")
            print(f"✅ TuGraph (Bolt) 连接成功，当前子图: {DB_NAME}")
        except Exception as e:
            print(f"❌ TuGraph 连接失败: {e}")
            raise e

        # 2. 初始化 LLM
        self.llm = ChatOpenAI(
            model=MODEL_NAME, 
            temperature=0, 
            openai_api_key=API_KEY, 
            base_url=BASE_URL
        )

        # 3. 定义 Prompt 模板
        self.prompt = ChatPromptTemplate.from_messages([
            (
                "system",
                """Task: Generate Cypher statement to query a graph database.
Instructions:
Use only the provided relationship types and properties in the schema.
Do not use any other relationship types or properties that are not provided.
Schema:
{schema}

Examples: Here is an examples of generated Cypher statements for a particular question:
{example}

Note: Do not include any explanations or apologies in your responses.
Do not include any text except the generated Cypher statement."""
            ),
            ("human", "{input}"),
        ])

        # 4. 构建图谱 Schema 上下文 (针对 AppStore 数据库)
        self.schema_info = """Node properties:
App {id: STRING, name: STRING, price: DOUBLE, rating: DOUBLE}
Genre {name: STRING}
ContentRating {level: STRING}

The relationships:
(:App)-[:BELONGS_TO]->(:Genre)
(:App)-[:RATED_AS]->(:ContentRating)"""

        # 5. 定义 Few-shot 示例，引导 LLM 生成正确的 Cypher
        self.example_info = """# 有多少个 App 属于 'Games' 类别？
MATCH (:App)-[:BELONGS_TO]->(g:Genre {name: "Games"}) RETURN count(*) AS result

# 找出评分高于 4.5 且免费的游戏。
MATCH (a:App)-[:BELONGS_TO]->(:Genre {name: "Games"}) WHERE a.rating > 4.5 AND a.price = 0 RETURN a.name AS result

# Minecraft 这个应用属于什么分级？
MATCH (a:App {name: "Minecraft"})-[:RATED_AS]->(cr:ContentRating) RETURN cr.level AS result

# 哪种分类的应用平均价格最高？
MATCH (a:App)-[:BELONGS_TO]->(g:Genre) RETURN g.name AS genre, avg(a.price) AS avg_price ORDER BY avg_price DESC LIMIT 1"""

        # 6. 创建执行链
        self.chain = self.prompt | self.llm
        print("✅ AppleStore 问答链 (Text-to-Cypher) 已就绪。")

    def _parse_query_to_cypher(self, question):
        """将自然语言转换为 Cypher 语句"""
        try:
            ai_msg = self.chain.invoke({
                "schema": self.schema_info,
                "example": self.example_info,
                "input": question
            })
            # 清理生成的文本，防止带 Markdown 标签
            cypher = ai_msg.content.strip().replace("```cypher", "").replace("```", "").strip()
            return cypher
        except Exception as e:
            print(f"⚠️ Cypher 生成失败: {e}")
            return None

    def execute_cypher(self, cypher):
        """在 TuGraph 中执行 Cypher 并获取结果"""
        try:
            with self.driver.session(database=DB_NAME) as session:
                res = session.run(cypher)
                results = []
                for record in res:
                    # 提取所有返回值
                    for value in record.values():
                        results.append(str(value))
                return list(set(results)) # 去重
        except Exception as e:
            print(f"⚠️ Cypher 执行失败: {e}\n语句: {cypher}")
            return None

    def parser_main(self, res_classify):
        """
        适配原有接口：解析问题并返回结果列表
        """
        question = res_classify.get('text', "")
        if not question:
            return []

        # 1. 生成 Cypher
        cypher = self._parse_query_to_cypher(question)
        if not cypher:
            return [{"question_type": "error", "sql": [], "answer": "无法解析问题。"}]

        # 2. 执行查询
        print(f"[生成的 Cypher]: {cypher}")
        raw_results = self.execute_cypher(cypher)

        # 3. 组织回答
        if raw_results is None:
            answer = "数据库查询出错。"
        elif not raw_results:
            answer = "未找到相关信息。"
        else:
            res_str = "、".join(raw_results[:15]) # 限制展示数量
            answer = f"查询到如下信息：\n{res_str}"

        return [{
            "question_type": "tugraph_generated",
            "sql": [cypher],
            "answer": answer
        }]

    def close(self):
        self.driver.close()

def start_interactive_session():
    """交互式对话入口"""
    try:
        handler = TuGraphQuestionParser()
    except Exception:
        return

    print("\n" + "="*50)
    print("🚀 TuGraph App 知识图谱问答系统 (自定义 Prompt 版)")
    print("输入 'exit' 或 'quit' 退出连接")
    print("="*50)

    while True:
        user_input = input("\n👤 用户: ").strip()
        if user_input.lower() in ['exit', 'quit']:
            break
        if not user_input:
            continue

        start_time = time.time()
        res = handler.parser_main({"text": user_input})
        
        if res:
            print(f"🤖 AI: {res[0]['answer']}")
            print(f"⏱️ 耗时: {time.time() - start_time:.2f}s")
        
    handler.close()

if __name__ == '__main__':
    start_interactive_session()
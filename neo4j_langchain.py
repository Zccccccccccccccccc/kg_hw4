import os
import json
from langchain_openai import ChatOpenAI
from langchain_neo4j import Neo4jGraph, GraphCypherQAChain

class LLMQuestionParser:
    """
    基于 LLM 和知识图谱的智能问答系统类
    核心目标：实现 GraphRAG（图谱增强的检索生成）
    """
    def __init__(self):
        # --- 1. 基础配置 (硅基流动 API 与 数据库凭证) ---
        self.api_key = 'sk-jaeowvubagerozsqmytiuqdpvlvjgohuvwrrhchhuoeujeit'
        self.base_url = "https://api.siliconflow.cn/v1"
        self.model_name = "Qwen/Qwen3-VL-32B-Instruct"

        # 数据库连接信息
        self.neo4j_url = "bolt://127.0.0.1:7687"
        self.username = "neo4j"
        self.password = "88888888"
        self.database_name = "finalhw"

        # --- 2. 初始化 Neo4j 图连接 ---
        try:
            self.graph = Neo4jGraph(
                url=self.neo4j_url,
                username=self.username,
                password=self.password,
                database=self.database_name
            )
            # Schema 感知：使 LLM 感知当前的节点标签、属性和关系
            self.graph.refresh_schema()
            print(f"✅ Neo4j 图数据库连接成功，当前库: {self.database_name}")
        except Exception as e:
            print(f"❌ Neo4j 连接失败: {e}")
            raise RuntimeError("无法连接到 Neo4j。")

        # --- 3. 初始化大语言模型 (硅基流动) ---
        try:
            self.llm = ChatOpenAI(
                model=self.model_name,
                openai_api_key=self.api_key,
                openai_api_base=self.base_url,
                temperature=0.1,  # 较低的温度有助于生成更稳定的 Cypher 代码
                max_retries=3
            )
        except Exception as e:
            print(f"❌ LLM 初始化失败: {e}")
            raise e

        # --- 4. 构建问答链 (GraphCypherQAChain) ---
        # 核心逻辑：生成 Cypher -> 执行查询 -> 总结回答
        self.chain = GraphCypherQAChain.from_llm(
            llm=self.llm,
            graph=self.graph,
            verbose=True,           # 透明度：控制台查看生成的 Cypher 过程
            allow_dangerous_requests=True,
            validate_cypher=True,   # 鲁棒性：自动纠正 Cypher 语法错误
            return_intermediate_steps=True
        )

    def parser_main(self, res_classify):
        """
        解析主函数：将自然语言问句解析为 Cypher 并获取回答
        res_classify: 字典格式，例如 {"text": "有多少个 App 属于 'Games' 类别？"}
        """
        raw_question = res_classify.get('text', "")
        if not raw_question:
            return []
        
        try:
            # 执行图查询链
            result = self.chain.invoke({"query": raw_question})
            
            # 提取生成的中间 Cypher 语句
            cypher = ""
            if "intermediate_steps" in result:
                for step in result["intermediate_steps"]:
                    if isinstance(step, dict) and "query" in step:
                        cypher = step["query"]
                        break
            
            # 返回格式：匹配系统 AnswerSearcher 的预期格式
            if cypher:
                return [{
                    "question_type": "llm_generated", 
                    "sql": [cypher], 
                    "answer": result.get("result")
                }]
        except Exception as e:
            print(f"⚠️ 硅基流动 LLM 生成 Cypher 失败: {e}")
            
        return []

def start_interactive_session():
    """
    交互式对话入口，支持用户主动断开连接
    """
    try:
        parser = LLMQuestionParser()
    except Exception:
        return

    print("\n" + "="*50)
    print("🚀 知识图谱问答系统已启动（类封装版）")
    print("输入 'exit' 或 '退出' 断开连接")
    print("="*50)

    while True:
        user_input = input("\n👤 用户: ").strip()

        if user_input.lower() in ['exit', 'quit', '退出']:
            print("\n👋 正在断开连接，再见！")
            break

        if not user_input:
            continue

        # 模拟 res_classify 格式传入
        test_data = {"text": user_input}
        results = parser.parser_main(test_data)

        if results:
            # 打印 AI 组织的总结回答
            print(f"\n📢 回答: {results[0]['answer']}")
        else:
            print("\n📢 回答: 抱歉，未能从数据库中检索到相关结果。")

if __name__ == '__main__':
    start_interactive_session()
from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
import logging
import re
import mysql.connector
from groclake.modellake import ModelLake

load_dotenv()

app = Flask("7b661615-574f-41d7-a4f8-fb80ef456066")

logging.basicConfig(level=logging.DEBUG)

def generate_default_table_schema():
    return {
        "id": "INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY",
        "name": "VARCHAR(255) NOT NULL",
        "description": "TEXT",
        "created_at": "DATETIME DEFAULT CURRENT_TIMESTAMP"
    }

@app.route('/cartesin-api.plotch.io/agentlake/agent/text2sql/query', methods=['POST'])
def query():
    data = request.json

    header = data.get('header', {})
    body = data.get('body', {})
    apc_id = header.get('apc_id')
    server_agent_uuid = header.get('server_agent_uuid')

    query = body.get('query', "Generate a SQL query based on the intent")
    entities = body.get('entities', [])
    intent = body.get('intent')

    if intent == "mysql_create_table":
        # Extract table details with defaults
        table_name = None
        table_description = None
        table_schema = None

        for entity in entities:
            if entity.get('type') == 'table_name':
                table_name = entity.get('value', 'default_table')
            elif entity.get('type') == 'table_description':
                table_description = entity.get('value', 'No description provided.')
            elif entity.get('type') == 'table_schema':
                table_schema = entity.get('value', generate_default_table_schema())

        logging.debug(f"table_schema: {table_schema}")

        try:
            connection = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD'),
                database=os.getenv('MYSQL_DATABASE')
            )
            if connection.is_connected():
                logging.debug("Connection to MySQL was successful!")

            cursor = connection.cursor()

            columns_definition = []
            for col_name, col_dtype in table_schema.items():
                if not isinstance(col_name, str) or not isinstance(col_dtype, str):
                    return jsonify({'error': f"Invalid column format for {col_name}: {col_dtype}"}), 400
                columns_definition.append(f"{col_name} {col_dtype}")
            logging.debug(columns_definition)

            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns_definition)}
                );
            """

            cursor.execute(create_table_query)
            connection.commit()
            logging.debug(f"Table '{table_name}' created successfully!")

        except mysql.connector.Error as e:
            return jsonify({"error": str(e)}), 500

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                logging.debug("MySQL connection closed.")

        return jsonify({
            "header": {
                "Content-Type": "application/json",
                "apc_id": apc_id,
                "auth_token": "dummy",
                "client_agent_uuid": 'client_agent_uuid',
                "message": "response",
                "server_agent_uuid": server_agent_uuid,
                "version": "1.0"
            },
            "body": {
                "entities": {
                    "query_text": query
                },
                "intent": intent,
                "metadata": {
                    "context": "query generated from a seller who is selling"
                },
                "query": query,
                "response": f"The {table_name} table has been created",
                "status": 200
            }
        }), 200

    if intent == "mysql_query_create":
        table_name = "Unknown"
        table_description = "No description provided"

        for entity in entities:
            if entity.get('type') == 'table_name':
                table_name = entity.get('value', "Unknown")
            elif entity.get('type') == 'table_description':
                table_description = entity.get('value', "No description provided")

        prompt = f"""
        You are an AI specializing in converting English questions into SQL queries.
        - If a table schema is provided, generate the SQL query based strictly on that schema.
        - If no schema is provided, assume reasonable default column names and data types based on the context of the question.

        Here is the information:
        - Table Name: {table_name}
        - Question: \"{query}\"
        - Description: {table_description}

        Generate the SQL query based on this information, ensuring it is syntactically correct and formatted for readability.
        """

        try:
            payload = {
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": query}
                ]
            }

            chat_response = ModelLake().chat_complete(payload)
            logging.debug(f"ModelLake Response: {chat_response}")

            def extract_sql_query(text):
                match = re.search(r"SELECT .*?;", text, re.DOTALL)
                return match.group(0).strip() if match else "Could not generate SQL query."

            sql_query = extract_sql_query(chat_response['answer'])
            logging.debug(f"Generated SQL Query: {sql_query}")

            return jsonify({
                "header": {
                    "Content-Type": "application/json",
                    "apc_id": apc_id,
                    "auth_token": "dummy",
                    "client_agent_uuid": 'client_agent_uuid',
                    "message": "response",
                    "server_agent_uuid": server_agent_uuid,
                    "version": "1.0"
                },
                "body": {
                    "entities": {
                        "query_text": query
                    },
                    "intent": intent,
                    "metadata": {
                        "context": "query generated from a seller who is selling"
                    },
                    "query": query,
                    "response": sql_query,
                    "status": 200
                }
            }), 200

        except Exception as e:
            logging.error(f"Error generating query: {e}")
            return jsonify({"error": "Error generating query.", "details": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

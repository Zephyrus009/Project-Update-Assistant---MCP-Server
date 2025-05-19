# server.py
from mcp.server.fastmcp import FastMCP
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

connection_string = os.getenv('CONNECTION_STRING')

engine = create_engine(connection_string)

# Create an MCP server
mcp = FastMCP("Project Update Assistant")

@mcp.tool()
def vapt_reporter(asset_name: str):
    """" Get VAPT report for the given asset name"""
    if len(asset_name) == 0:
        return "Asset Name not found in you current directory"
    """ Read the data from VAPT reports """
    data = pd.read_sql_query(f"select * from vapt_reports s1 where asset_name = '{asset_name}' and updated_at = (select MAX(updated_at) from vapt_reports s2 where s2.asset_name = s1.asset_name)",engine)
    data = data.to_dict(orient="records")
    return data

@mcp.tool()
def development_reporter(project_name: str,feature_name):
    if len(project_name) == 0:
        return "Project Name not found in you current directory"
    """ If feature name is not provided, get all the features for the given project name"""
    if len(feature_name) == 0:
        feature_name = pd.read_sql_query(f"select distinct feature_name from development_synthetic_data where project_name = '{project_name}'", engine)
        feature_name = str(tuple(feature_name['feature_name'].tolist())).replace(',)', ')')
        """" Get Development report for the given asset name"""
        data = pd.read_sql_query(f"select * from development_synthetic_data s1 where project_name = '{project_name}' and feature_name in {feature_name} and created_at = (select MAX(created_at) from development_synthetic_data s2 where s2.project_name = s1.project_name and s2.feature_name = s1.feature_name )", engine)
    else:
        query = f"""select * from development_synthetic_data s1 where project_name = '{project_name}' and feature_name in ('{feature_name}') and created_at = (select MAX(created_at) from development_synthetic_data s2 where s2.project_name = s1.project_name and s2.feature_name = s1.feature_name )"""
        data = pd.read_sql_query(query, engine)
    data = data.to_dict(orient="records")
    return data

@mcp.tool()
def deployment_reporter(application_name: str, environment):
    if len(application_name) == 0:
        return "Application Name not found in you current directory"
    """ If environment is not provided, get all the environments for the given application name"""
    if len(environment) == 0:
        environment = pd.read_sql_query(f"select distinct environment from deployment_details where application_name = '{application_name}'", engine)
        environment = str(tuple(environment['environment'].tolist())).replace(',)', ')')
        """ Get Deployment report for the given asset name"""
        data = pd.read_sql_query(f"select * from deployment_details s1 where application_name = '{application_name}' and environment in {environment} and updated_at = (select MAX(updated_at) from deployment_details s2 where s2.application_name = s1.application_name and s2.environment = s1.environment )", engine)
    else:
        """ Get Deployment report for the given asset name"""
        query = f"select * from deployment_details s1 where application_name = '{application_name}' and environment in ('{environment}') and updated_at = (select MAX(updated_at) from deployment_details s2 where s2.application_name = s1.application_name and s2.environment = s1.environment )"
        data = pd.read_sql_query(query, engine)
    data = data.to_dict(orient="records")
    return data


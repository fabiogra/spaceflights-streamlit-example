import asyncio
import json
import sys
import subprocess
import time
from asyncio.subprocess import PIPE, STDOUT
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pygraphviz as pgv
import streamlit as st

st.set_page_config(page_title='Streamlit Kedro Launcher',
                    layout='wide', 
                    initial_sidebar_state='expanded')


@st.cache(show_spinner=False)
def load_json_pipeline():
    path_pipeline = 'data/08_reporting/kedro-pipeline.json'
    subprocess.run(["python", "-m", "kedro", "viz", "--save-file", path_pipeline])
    return json.load(open(path_pipeline))


def get_node_color(name, compleated_node, running_node, error_node):
    if name in compleated_node:
        return "Green"
    elif name in running_node:
        return "Orange"
    elif name in error_node:
        return "Red"
    else:
        return "Gray"

def get_edge_color(source, target, completed_node, running_node, error_node):
    if source in completed_node and target in completed_node:
        return "Green"
    elif source in completed_node and target in running_node:
        return "Orange"
    elif source in completed_node and target in error_node:
        return "Red"
    else:
        return "Gray"
    
def create_node_and_edges(pipeline,
                          pipeline_name="__default__",
                          exclude=[], 
                          layout='dot', 
                          export_to='pygraph_image.png',
                          compleated_node=[],
                          running_node=[],
                          error_node=[],
                          rankdir='TB'):
    G = pgv.AGraph(directed=True, 
                   strict=True, 
                   rankdir=rankdir,
                   ranksep='0.3'
                   )

    id_enabled = []
    map_shapes = {'data': 'cylinder', 'task': 'box', 'parameters':'plain'}
    
    
    lk_node_name_id = {}
    
    for node in pipeline['nodes']:
        if pipeline_name in node["pipelines"] and not node['type'] in exclude:
            lk_node_name_id[node['id']] = node['full_name']
            G.add_node(node['id'], 
                    label=node['full_name'].replace('_',' ').replace('params:',''), 
                    shape=map_shapes.get(node['type'], 'box'),
                    style="rounded,setlinewidth(3)",
                    height=1,
                    fontsize = 20,
                    fontname = "Sans",
                    color=get_node_color(node['full_name'], 
                                        compleated_node,
                                        running_node,
                                        error_node))
            id_enabled.append(node['id'])

    for edge in pipeline['edges']:
            if edge['source'] in id_enabled and edge['target'] in id_enabled:
                G.add_edge(edge['source'], 
                        edge['target'], 
                        color=get_edge_color(lk_node_name_id[edge['source']], #todo get name from id 
                                             lk_node_name_id[edge['target']],#todo get name from id
                                             compleated_node,
                                             running_node,
                                             error_node))
    G.layout(layout)
    G.draw(export_to, 
           prog="dot",
           format='png', 
           args="-v",
           )


def show_pipeline():
    image_pipeline.image('pygraph_image.png')


async def _read_stream(stream, cb):  
    while True:
        line = await stream.readline()
        if line:
            cb(line)
        else:
            break

async def _stream_subprocess(cmd):  
    process = await asyncio.create_subprocess_exec(*cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

    await asyncio.wait([
        _read_stream(process.stdout, lambda x: write_logs(x)),
        _read_stream(process.stderr, lambda x: write_logs(x))
    ])
    return await process.wait()


def execute(cmd):  
    if sys.platform.startswith('win'):
        loop = asyncio.ProactorEventLoop() # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()
    rc = loop.run_until_complete(_stream_subprocess(cmd))
    _ = loop.close()
    return rc

def write_logs(line):
    log_parts = line.decode("utf").split(' - ')
    if len(log_parts) == 4:
        global LOGS_DF
        global compleated_node
        global running_node
        global error_node
        
        LOGS_DF = pd.concat([pd.DataFrame([log_parts], 
                                        columns=['datetime', 'module', 'log', 'message']), 
                            LOGS_DF])
        
        fig = go.Figure(data=[go.Table(
                        header=dict(values=list(LOGS_DF.columns),
                                    fill_color='#f5b807',
                                    align='left'),
                        columnwidth=[0.5, 0.3, 1, 2],
                        cells=dict(values=[LOGS_DF.datetime, 
                                        LOGS_DF.log, 
                                        LOGS_DF.module, 
                                        LOGS_DF.message],
                                    font={'color':'white'},
                                    fill_color='#4c4c4b',
                                    align='left'))
                    ])
        fig.update_layout(width=1000, 
                            height=600, 
                            margin=dict(l=0, r=0, t=0, b=0))
        LOGS_CONTAINER.write(fig)
        
        row = LOGS_DF.iloc[0]
        if row['module'] == 'kedro.io.data_catalog': 
            node_name = row['message'].split("`")[1]
            running_node.append(node_name)
        elif row['module'] == 'kedro.pipeline.node':
            node_name = row['message'].split("(")[0].split(":")[-1][1:]
            running_node.append(node_name)
        elif row['module'] == 'kedro.runner.sequential_runner' and row['log'] == 'INFO' and row['message'][:9] == 'Completed':
            compleated_node = compleated_node + running_node
            running_node = []
        elif row['log'] == 'ERROR':
            error_node = running_node

        create_node_and_edges(pipeline, 
                            exclude=exclude,
                            compleated_node=compleated_node,
                            running_node=running_node,
                            error_node=error_node,
                            rankdir=rankdir)
        show_pipeline()


with st.spinner("Loading kedro pipelines..."):
    pipeline = load_json_pipeline()

######################## Sidebar ########################
st.sidebar.markdown("## Pipeline")
pipeline_selector = st.sidebar.empty()
start_date_selector = st.sidebar.empty()
text_pipeline_info = st.sidebar.empty()

st.sidebar.markdown("***")

st.sidebar.markdown("## Layout Configuration")

rankdir = st.sidebar.radio(label="Type Layout",
                    options=["LR", "TB"],
                    format_func=lambda x: {'TB':'Top to bottom', 
                                           'LR': 'Left to right'}[x])

if st.sidebar.checkbox("Exclude paramater nodes"):
    exclude = ['parameters']
else:
    exclude = []

st.sidebar.markdown("***")

start_button = st.sidebar.empty()

use_pipeline = pipeline_selector.selectbox("Select pipeline:", 
                                           [p['id'] for p in pipeline['pipelines']])
########################################################

text_info = st.empty()
image_pipeline = st.empty()

compleated_node=[]
running_node=[]
error_node=[]

create_node_and_edges(pipeline, 
                    exclude=exclude,
                    rankdir=rankdir,
                    pipeline_name=use_pipeline
                    )
show_pipeline()
LOGS_DF = pd.DataFrame([])
LOGS_CONTAINER = st.empty()

if start_button.button('Run Kedro Pipeline', key='start'):
    text_info.info(f"{use_pipeline} pipeline is running...")
    execute(["python", "-m", "kedro", "run", "--pipeline", use_pipeline])
    text_info.success("Finished!")
    time.sleep(2)
    text_info.empty()
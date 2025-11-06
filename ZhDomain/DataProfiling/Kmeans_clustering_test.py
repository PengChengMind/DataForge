#coding=gbk
from sentence_transformers import SentenceTransformer
import numpy as np
import json
import plotly.offline as offline
from sklearn import cluster
from sklearn.datasets import make_blobs
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import glob
import random


import plotly.graph_objects as go
import plotly.express as px
import numpy as np

from typing import Dict
from pathlib import Path
from IPython.core.display import display, HTML
import plotly.graph_objs as go
import numpy as np
from plotly.graph_objs import *

def get_data_feature(file_name ):
    model = SentenceTransformer('./sentence-transformers-LaBSE/')
    input_file = open(file_name, 'r', encoding='utf-8')
    sentences = []
    while True:
        line = input_file.readline()
        if not line:
            break
        line_json = json.loads(line)
        text_samples = line_json["text"]
        sentences.append(text_samples)

    embeds = model.encode(sentences)
    return embeds,sentences


def get_KMeans_num(embeds):
    num = 20
    wcss = []
    for i in range(1, num):
        kmeans = KMeans(n_clusters=i, init="k-means++", max_iter=500, n_init=10, random_state=123)
        kmeans.fit(embeds)
        wcss.append(kmeans.inertia_)

    fig = go.Figure(data=go.Scatter(x=list(range(1,num+1)), y=wcss))

    fig.update_layout(title='WCSS vs. Cluster number',
                      xaxis_title='Clusters',
                      yaxis_title='WCSS')
    fig.show()


def KMeans_TSNE(embeds,sentences,save_fig_name,n_clusters):
    from sklearn.decomposition import PCA
    from sklearn.manifold import TSNE
    import plotly.express as px
    # Fit into the model
    tsne = TSNE(n_components=3, random_state=0)
    projections = tsne.fit_transform(embeds, )
    model = KMeans(n_clusters=n_clusters, init='random', n_init=10, max_iter=300, tol=1e-04, random_state=0)
    # 训练加预测
    y_pred = model.fit_predict(embeds)
    sentences = [sent[:100] for sent in sentences]

    trace = go.Scatter3d(
        x=projections[:, 0],
        y=projections[:, 1],
        z=projections[:, 2],
        mode='markers',
        marker=dict(
            size=5,
            color=y_pred,
            colorscale='Viridis',
            opacity=0.8
        ),
        text=sentences
    )
    layout = Layout(
        margin=dict(
            l=0,
            r=0,
            b=0,
            t=0
        )
    )
    fig = go.Figure(data=[trace], layout=layout)
    fig.update_layout(hovermode='closest', hoverlabel=dict(bgcolor="white", font_size=12))
    fig.show( width=800, height=800)
    offline.plot(fig, filename=save_fig_name, auto_open=False)


if __name__ == '__main__':
    file_name = "/dataset/train_data_part1.json"
    embeds,sentences = get_data_feature(file_name)
    save_fig_name = file_name.split("\\")[-1] + ".html"
    n_clusters = 5
    KMeans_TSNE(embeds,sentences,save_fig_name,n_clusters)


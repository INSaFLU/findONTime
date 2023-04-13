
import os
import sys
from typing import List

import dash_bootstrap_components as dbc
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
from plotly.subplots import make_subplots


def get_sample_name_from_merged(merged):
    file_name = os.path.basename(merged)
    file_name, ext = os.path.splitext(file_name)
    if ext == ".gz":
        file_name, ext = os.path.splitext(file_name)

    return file_name


def get_sample_runs(sample_name, results_df):
    sample_df = results_df[results_df["sample_name"] == sample_name]
    return sample_df["run_id"].unique()


def accid_per_sample(accid_list, sample_names, results_df):
    """
    return results df with accid entries for each sample for each accid in results"""

    accid_stats = pd.DataFrame()

    for accid in accid_list:
        for sample_name in sample_names:

            sample_df = results_df[results_df["sample_name"] == sample_name]
            time_elapsed = sample_df["time_elapsed"].unique()[0]
            description = sample_df["description"].unique()[0]
            taxid = sample_df["taxid"].unique()[0]
            sample_df = sample_df[sample_df["accid"] == accid]

            if sample_df.shape[0] == 0:
                sample_stat = pd.DataFrame({"coverage": [np.nan], "depth": [np.nan], "ref_proportion": [
                    np.nan], "taxid": [taxid], "mapped_reads": [np.nan],
                    "description": [description], "time_elapsed": [time_elapsed]})
            else:
                sample_stat = sample_df[
                    [
                        "coverage",
                        "depth",
                        "ref_proportion",
                        "description",
                        "taxid",
                        "mapped_reads",
                        "time_elapsed",
                        "leaf_id"]
                ]

            sample_stat.insert(0, "accid", accid)
            sample_stat.insert(0, "sample_name", sample_name)

            accid_stats = pd.concat([accid_stats, sample_stat])

    return accid_stats


# metadir = "/home/bioinf/Desktop/CODE/INSaFLU-TELEVIR_cml_upload/test_new"
# processed_file = os.path.join(metadir, "processed.tsv")
#
# results_dir = "/home/bioinf/Desktop/CODE/INSaFLU-TELEVIR_cml_upload/test_new"
# results_file = os.path.join(results_dir, "barcode_01_another.tsv")
#
# plots_dir = "/home/bioinf/Desktop/CODE/INSaFLU-TELEVIR_cml_upload/test_new/plots"


class TelevirPlotResults:
    stats: List[str] = ["coverage", "depth", "ref_proportion", "mapped_reads"]

    def __init__(self, results_df: pd.DataFrame, processed_df: pd.DataFrame):
        self.results_df = results_df
        self.processed_df = processed_df

        self.prep_accid_stats()

    def preprocess_df(self):
        self.processed_df["sample_name"] = self.processed_df["merged"].apply(
            get_sample_name_from_merged)

        self.results_df["time_elapsed"] = self.results_df["sample_name"].map(
            self.processed_df.set_index("sample_name")["time"])

    def prep_accid_stats(self):
        self.preprocess_df()

        sample_names = self.results_df["sample_name"].unique()
        accids = self.results_df["accid"].unique()

        self.accid_stats = accid_per_sample(
            accids, sample_names, self.results_df)

    def plot_accid_stats_max(self, stat: str = "coverage", ax=None):
        """
        Plots the max value of a stat for every accid by time elapsed.
        """
        data_max = self.accid_stats.groupby(["accid", "time_elapsed"])[
            stat].max().reset_index()

        fig = px.line(data_max, x="time_elapsed", y=stat,
                      color="accid", template="seaborn")

        return fig


def plot_project_results(projects_results: List[str], processed: pd.DataFrame, plots_dir: str, write_html: bool = True):
    """
    plot project results
    """

    stat = "coverage"

    fig = go.Figure()

    figures = []

    for project_result_df in projects_results:

        try:
            project_result_df = pd.read_csv(project_result_df, sep="\t")
        except pd.errors.EmptyDataError:
            continue

        plotter = TelevirPlotResults(
            project_result_df,
            processed,
        )

        fig_proj = plotter.plot_accid_stats_max(stat=stat)

        figures.append(fig_proj)

    if len(figures) == 0:
        return None

    fig = make_subplots(rows=len(figures), cols=1)

    for i, figure in enumerate(figures):
        for trace in range(len(figure["data"])):
            fig.append_trace(figure["data"][trace], row=i+1, col=1)

    if write_html:

        fig.write_html(os.path.join(plots_dir, "project_results.html"))
        return None

    else:
        return fig

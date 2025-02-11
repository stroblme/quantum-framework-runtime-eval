from kedro.extras.datasets.pandas import CSVDataSet
from kedro.extras.datasets.text import TextDataSet
from kedro.extras.datasets.plotly import JSONDataSet
from kedro.io import Version
from kedro.framework.hooks import hook_impl

from typing import Any, Dict

import glob
from pathlib import Path

import os
import logging
from kedro.io import DataCatalog

import time

log = logging.getLogger(__name__)


class ProjectHooks:
    @hook_impl
    def after_context_created(self, context):
        pass


class PipelineHooks:
    @property
    def _logger(self):
        return logging.getLogger(self.__class__.__name__)

    @hook_impl
    def before_pipeline_run(self, run_params: Dict[str, Any], pipeline, catalog):
        """A hook implementation to add a catalog entry
        based on the filename passed to the command line, e.g.:
            kedro run --params=input:iris_1.csv
            kedro run --params=input:iris_2.csv
            kedro run --params=input:iris_3.csv
        """

        # ----------------------------------------------------------------
        # If we are running a new experiment setup (e.g. executing the "pre"
        # pipeline), we want to cleanup all the files that are **not**
        # versioned but were used as results in previous runs
        # ----------------------------------------------------------------

        if run_params["pipeline_name"] == "prepare":
            tempFiles = glob.glob("data/02_intermediate/*.csv")
            for f in tempFiles:
                os.remove(f)
            # tempFiles = glob.glob("data/08_reporting/*.tmp")
            # for f in tempFiles:
            #     os.remove(f)

        # cleanup "results" if we are at the beginning our our experiment
        if (
            run_params["pipeline_name"] is None  # Running the Default pipeline
            or run_params["pipeline_name"] == "prepare"
        ):
            tempFiles = glob.glob("data/03_qasm_circuits/*.txt")
            for f in tempFiles:
                os.remove(f)
            tempFiles = glob.glob("data/04_measures/*.csv")
            for f in tempFiles:
                os.remove(f)
            tempFiles = glob.glob("data/05_execution_results/*.csv")
            for f in tempFiles:
                os.remove(f)

            tempFiles = glob.glob("data/06_execution_durations/*.csv")
            for f in tempFiles:
                os.remove(f)

        self.start_run = time.time()

    @hook_impl
    def after_pipeline_run(self, run_params: Dict[str, Any], pipeline, catalog):
        self.finish_run = time.time()

        log.info(f"Run took {self.finish_run - self.start_run}s")

        # after visualization, we don't need the tmp files anymore
        # if (
        #     run_params["pipeline_name"] is None  # Running the Default pipeline
        #     or run_params["pipeline_name"] == "visualize"
        # ):

        # only cleanup if the last (visualize) pipeline ran
        if (
            run_params["pipeline_name"] is None
            or run_params["pipeline_name"] == "visualize"
        ):
            tempFiles = glob.glob("data/02_intermediate/*.csv")
            for f in tempFiles:
                os.remove(f)
            tempFiles = glob.glob("data/03_qasm_circuits/*.txt")
            for f in tempFiles:
                os.remove(f)
            tempFiles = glob.glob("data/04_measures/*.csv")
            for f in tempFiles:
                os.remove(f)
            tempFiles = glob.glob("data/05_execution_results/*.csv")
            for f in tempFiles:
                os.remove(f)
            tempFiles = glob.glob("data/06_execution_durations/*.csv")
            for f in tempFiles:
                os.remove(f)
            tempFiles = glob.glob("data/08_reporting/*.tmp")
            for f in tempFiles:
                os.remove(f)

        if run_params["pipeline_name"] == "prepare":
            # Cleanup all previously generated temp files,
            # we will generate them again below
            tempFiles = glob.glob("data/08_reporting/*.tmp")
            for f in tempFiles:
                os.remove(f)
            # ----------------------------------------------------------------
            # This section ensures that the reporting dictionary always
            # contains the proper output data catalogs so that kedro-viz is happy
            # ----------------------------------------------------------------

            try:
                # get the evaluation matrix where information about
                # all the possible combinations is stored
                evaluation_matrix = catalog.datasets.evaluation_matrix.load()
            except Exception as e:
                log.exception(e)
                return

            # generate a list of names that will be used as plots
            # later in the visualization pipeline
            # If you add new visualization outputs, you must also
            # create the file names here
            names = []
            for fw in evaluation_matrix["frameworks"]:
                for q in evaluation_matrix["qubits"]:
                    names.append(f"{fw}_qubits_{q}_time")
                for d in evaluation_matrix["depths"]:
                    names.append(f"{fw}_depth_{d}_time")
                for s in evaluation_matrix["shots"]:
                    names.append(f"{fw}_shots_{s}_time")

            for s in evaluation_matrix["shots"]:
                for d in evaluation_matrix["depths"]:
                    names.append(f"shots_{s}_depth_{d}_time")
                    names.append(f"shots_{s}_depth_{d}_measures")

            for d in evaluation_matrix["depths"]:
                for q in evaluation_matrix["qubits"]:
                    names.append(f"qubits_{q}_depth_{d}_time")
                    names.append(f"qubits_{q}_depth_{d}_measures")

            for q in evaluation_matrix["qubits"]:
                for s in evaluation_matrix["shots"]:
                    names.append(f"shots_{s}_qubits_{q}_time")
                    names.append(f"shots_{s}_qubits_{q}_measures")

            # use the dummy dataset to get the version of the current
            # kedro run, so that it matches the ones from the versioned datasets
            version = Version(
                None, catalog.datasets.dummy_versioned_dataset._version.save
            )

            # iterate all the names and create a JSONDataSet (plotly) for each one
            for name in names:
                filepath = os.path.join("data/08_reporting/", f"{name}.json")

                dataset_template = JSONDataSet(filepath=filepath, version=version)
                catalog.add(name, dataset_template)

                # create a dictionary if necessary
                # (versioned datasets need dictionaries)
                try:
                    os.mkdir(filepath)
                except FileExistsError:
                    # directory already exists
                    pass

                # create a .tmp file which we will use later in the
                # pipeline_registry to create node outputs dynamically
                with open(os.path.join("data/08_reporting/", f"{name}.tmp"), "w") as fw:
                    fw.write("")

    @hook_impl
    def on_pipeline_error(self, run_params: Dict[str, Any], pipeline, catalog):
        # tempFiles = glob.glob("data/02_intermediate/*.csv")
        # for f in tempFiles:
        #     os.remove(f)
        # tempFiles = glob.glob("data/05_execution_results/*.csv")
        # for f in tempFiles:
        #     os.remove(f)
        # tempFiles = glob.glob("data/06_execution_durations/*.csv")
        # for f in tempFiles:
        #     os.remove(f)
        tempFiles = glob.glob("data/08_reporting/*.tmp")
        for f in tempFiles:
            os.remove(f)


class DataCatalogHooks:
    @property
    def _logger(self):
        return logging.getLogger(self.__class__.__name__)

    @hook_impl
    def after_catalog_created(self, catalog: DataCatalog) -> None:
        # ----------------------------------------------------------------
        # This section creates csv datasets based on the partitioned
        # dataset stored in the intermediate directory
        # Also based on those partitions, it creates further csv datasets
        # for the evaluation durations and the evaluation results
        # ----------------------------------------------------------------

        # get all the partitions created by the "pre" pipeline
        partitions = glob.glob("data/02_intermediate/*.csv")

        for partition in partitions:
            # ------------------------------------------------------------------
            # Create csv dataset from partitioned dataset for partitions
            # ------------------------------------------------------------------

            # partition loader
            evaluation_partitions_name = f"evaluation_partition_{Path(partition).stem}"
            input_dataset = CSVDataSet(filepath=partition)
            catalog.add(evaluation_partitions_name, input_dataset)

            # ------------------------------------------------------------------
            # Create txt dataset from partitioned dataset for qasm circuits
            # ------------------------------------------------------------------

            # qasm_circuits loader
            qasm_circuits = partition.replace(
                "02_intermediate", "03_qasm_circuits"
            ).replace(".csv", ".txt")
            qasm_circuits_name = f"qasm_circuit_{Path(qasm_circuits).stem}"
            input_dataset = TextDataSet(filepath=qasm_circuits)
            catalog.add(qasm_circuits_name, input_dataset)

            # ------------------------------------------------------------------
            # Create csv dataset from partitioned dataset for measures
            # ------------------------------------------------------------------

            # evaluation durations
            measures = partition.replace("02_intermediate", "04_measures")
            measures_name = f"measure_{Path(measures).stem}"
            input_dataset = CSVDataSet(filepath=measures)
            catalog.add(measures_name, input_dataset)

            # ------------------------------------------------------------------
            # Create csv dataset from partitioned dataset for evaluation durations
            # ------------------------------------------------------------------

            # evaluation durations
            execution_duration = partition.replace(
                "02_intermediate", "06_execution_durations"
            )
            execution_duration_name = (
                f"execution_duration_{Path(execution_duration).stem}"
            )
            input_dataset = CSVDataSet(filepath=execution_duration)
            catalog.add(execution_duration_name, input_dataset)

            # ------------------------------------------------------------------
            # Create csv dataset from partitioned dataset for evaluation results
            # ------------------------------------------------------------------

            # evaluation results
            execution_result = partition.replace(
                "02_intermediate", "05_execution_results"
            )
            execution_result_name = f"execution_result_{Path(execution_result).stem}"
            input_dataset = CSVDataSet(filepath=execution_result)
            catalog.add(execution_result_name, input_dataset)

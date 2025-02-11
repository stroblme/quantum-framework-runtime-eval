"""
This is a boilerplate pipeline 'data_science'
generated using Kedro 0.18.3
"""

from kedro.pipeline import node, pipeline
from quafel.pipelines.data_generation.nodes import (
    extract_partition_data,
    generate_random_qasm_circuit_from_partition,
    calculate_measures,
    generate_evaluation_matrix,
    generate_evaluation_partitions,
    qasm_circuit_to_qiskit,
)


def create_pipeline(partitions, existing_circuits, existing_measures, **kwargs) -> dict:
    pl_generate_evaluation_partitions = pipeline(
        [
            node(
                func=generate_evaluation_matrix,
                inputs={
                    "min_qubits": "params:data_generation.min_qubits",
                    "max_qubits": "params:data_generation.max_qubits",
                    "qubits_increment": "params:data_generation.qubits_increment",
                    "qubits_type": "params:data_generation.qubits_type",
                    "min_depth": "params:data_generation.min_depth",
                    "max_depth": "params:data_generation.max_depth",
                    "depth_increment": "params:data_generation.depth_increment",
                    "depth_type": "params:data_generation.depth_type",
                    "min_shots": "params:data_generation.min_shots",
                    "max_shots": "params:data_generation.max_shots",
                    "max_shots": "params:data_generation.max_shots",
                    "shots_increment": "params:data_generation.shots_increment",
                    "shots_type": "params:data_generation.shots_type",
                    "frameworks": "params:data_generation.frameworks",
                },
                outputs={
                    "evaluation_matrix": "evaluation_matrix",
                },
            ),
            node(
                func=generate_evaluation_partitions,
                inputs={
                    "evaluation_matrix": "evaluation_matrix",
                    "skip_combinations": "params:data_generation.skip_combinations",
                },
                outputs={
                    "evaluation_partitions": "evaluation_partitions",
                },
            ),
        ],
        outputs={
            "evaluation_partitions": "evaluation_partitions",
            "evaluation_matrix": "evaluation_matrix",
        },
    )

    pl_generate_qasm_circuits = pipeline(
        [
            *[
                node(
                    func=generate_random_qasm_circuit_from_partition,
                    inputs={
                        "partition": f"evaluation_partition_{i}",
                        "seed": "params:data_generation.seed",
                    },
                    outputs={
                        "qasm_circuit": f"qasm_circuit_{i}",
                        "n_shots": f"n_shots_{i}",
                        "framework": f"framework_{i}",
                        "qiskit_circuit": f"qiskit_circuit_{i}",
                    },
                    tags=["dynamic"],
                    name=f"generate_random_qasm_circuit_from_partition_{i}",
                )
                for i in partitions
                if i not in existing_circuits
            ],
            *[
                node(
                    func=extract_partition_data,
                    inputs={
                        "partition": f"evaluation_partition_{i}",
                    },
                    outputs={
                        "qubits": f"qubits_{i}",
                        "depth": f"depth_{i}",
                        "n_shots": f"n_shots_{i}",
                        "framework": f"framework_{i}",
                    },
                    tags=["dynamic"],
                    name=f"extract_partition_data_{i}",
                )
                for i in partitions
                if i in existing_circuits
            ],
            *[
                node(
                    func=qasm_circuit_to_qiskit,
                    inputs={
                        "qasm_circuit": f"qasm_circuit_{i}",
                    },
                    outputs={
                        "qiskit_circuit": f"qiskit_circuit_{i}",
                    },
                    tags=["dynamic"],
                    name=f"extract_circuit_{i}",
                )
                for i in partitions
                if i in existing_circuits
            ],
            *[
                node(
                    func=calculate_measures,
                    inputs={
                        "circuit": f"qiskit_circuit_{i}",
                        "samples_per_parameter": "params:data_generation.samples_per_parameter",  # noqa
                        "haar_samples_per_qubit": "params:data_generation.haar_samples_per_qubit",  # noqa
                        "seed": "params:data_generation.seed",
                    },
                    outputs={
                        "measure": f"measure_{i}",
                    },
                    tags=["dynamic"],
                    name=f"calculate_measures_{i}",
                )
                for i in partitions
                if i not in existing_measures
            ],
        ],
        inputs={
            # note that this dataset is dynamically created in the hooks,
            # so it is not directly available in the catalog
            **{
                f"evaluation_partition_{i}": f"evaluation_partition_{i}"
                for i in partitions
            },
        },
        outputs={
            **{f"n_shots_{i}": f"n_shots_{i}" for i in partitions},
            **{f"framework_{i}": f"framework_{i}" for i in partitions},
            **{
                f"qasm_circuit_{i}": f"qasm_circuit_{i}"
                for i in partitions
                if i not in existing_circuits
            },
            **{
                f"measure_{i}": f"measure_{i}"
                for i in partitions
                if i not in existing_measures
            },
        },
    )

    return {
        "pl_generate_evaluation_partitions": pl_generate_evaluation_partitions,
        "pl_generate_qasm_circuits": pl_generate_qasm_circuits,
    }
